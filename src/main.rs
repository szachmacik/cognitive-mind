//! CognitiveMind WebSocket Hub
//! 
//! Centralny układ nerwowy ekosystemu ofshore.dev.
//! 
//! Architektura:
//!   PostgreSQL (pg_notify 'mind_events')
//!       ↓ LISTEN (tokio-postgres)
//!   Rust Hub (tokio + axum WebSocket)
//!       ↓ broadcast (dashmap channels)  
//!   Wszystkie agenty (brain-router, researchers, CF Workers, n8n)
//!
//! Każdy agent:
//!   1. Łączy się przez WebSocket → rejestruje się (agent_id + typ + subskrypcje)
//!   2. Odbiera eventy pasujące do jego subskrypcji w ~0ms
//!   3. Może wysyłać eventy do wszystkich (publish/subscribe)
//!   4. Knowledge Bus: kb_write/kb_read przez WS message

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};
use uuid::Uuid;
mod upstash;

// ── Config ─────────────────────────────────────────────────────────────────

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

// ── Types ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MindMessage {
    // Agent → Hub
    Join {
        agent_id:     String,
        agent_type:   String,
        capabilities: serde_json::Value,
        subscriptions: Vec<String>,  // "*" for all
    },
    Ping { ts: u64 },
    Publish {
        event_type: String,
        payload:    serde_json::Value,
        priority:   Option<u8>,
    },
    KbWrite {
        key:   String,
        value: serde_json::Value,
        ttl:   Option<u32>,
    },
    KbRead { key: String },

    // Hub → Agent
    Pong { ts: u64, connected_agents: u32 },
    Event {
        event_type: String,
        source:     String,
        payload:    serde_json::Value,
        ts:         u64,
    },
    KbValue {
        key:   String,
        value: Option<serde_json::Value>,
    },
    Welcome {
        agent_id:         String,
        hub_version:      String,
        connected_agents: u32,
        ts:               u64,
    },
    AgentList {
        agents: Vec<AgentInfo>,
    },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub agent_id:    String,
    pub agent_type:  String,
    pub connected_at: u64,
    pub last_ping:   u64,
}

// Broadcast envelope (lightweight clone)
#[derive(Debug, Clone)]
pub struct BroadcastEvent {
    pub event_type: String,
    pub source:     String,
    pub payload:    String,  // JSON string
    pub ts:         u64,
}

// ── Shared State ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct HubState {
    /// Broadcast channel — sends to ALL connected agents
    pub broadcast_tx: broadcast::Sender<BroadcastEvent>,
    /// Connected agents registry: agent_id → AgentInfo
    pub agents: Arc<DashMap<String, AgentInfo>>,
    /// Knowledge bus: key → JSON value
    pub knowledge: Arc<DashMap<String, (serde_json::Value, u64)>>,  // (value, expires_ts)
    /// Total connections ever
    pub total_connections: Arc<AtomicU64>,
}

impl HubState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            broadcast_tx:     tx,
            agents:           Arc::new(DashMap::new()),
            knowledge:        Arc::new(DashMap::new()),
            total_connections: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn connected_count(&self) -> u32 {
        self.agents.len() as u32
    }

    pub fn publish(&self, event: BroadcastEvent) {
        let _ = self.broadcast_tx.send(event);
    }
}

// ── WebSocket Handler ──────────────────────────────────────────────────────

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<HubState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: HubState) {
    let conn_id = Uuid::new_v4().to_string();
    let (mut sink, mut stream) = socket.split();
    let mut broadcast_rx = state.broadcast_tx.subscribe();
    let mut agent_id: Option<String> = None;
    let mut subscriptions: HashSet<String> = ["*".to_string()].into();

    state.total_connections.fetch_add(1, Ordering::Relaxed);
    info!("🔌 New connection: {}", &conn_id[..8]);

    loop {
        tokio::select! {
            // Incoming message from agent
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<MindMessage>(&text) {
                            Ok(mind_msg) => {
                                if let Some(reply) = process_message(
                                    mind_msg, &state, &mut agent_id, &mut subscriptions
                                ).await {
                                    let json = serde_json::to_string(&reply).unwrap_or_default();
                                    let _ = sink.send(Message::Text(json)).await;
                                }
                            }
                            Err(e) => {
                                let err = MindMessage::Error { message: e.to_string() };
                                let _ = sink.send(Message::Text(serde_json::to_string(&err).unwrap())).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = sink.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }

            // Outgoing broadcast to agent
            event = broadcast_rx.recv() => {
                match event {
                    Ok(ev) => {
                        // Filter by subscriptions
                        let should_send = subscriptions.contains("*") || subscriptions.contains(&ev.event_type);
                        if should_send {
                            let payload: serde_json::Value = serde_json::from_str(&ev.payload)
                                .unwrap_or(serde_json::Value::Null);
                            let msg = MindMessage::Event {
                                event_type: ev.event_type,
                                source:     ev.source,
                                payload,
                                ts:         ev.ts,
                            };
                            let json = serde_json::to_string(&msg).unwrap_or_default();
                            if sink.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Agent {} lagged {} events", conn_id, n);
                    }
                    Err(_) => break,
                }
            }
        }
    }

    // Cleanup on disconnect
    if let Some(id) = &agent_id {
        state.agents.remove(id);
        state.publish(BroadcastEvent {
            event_type: "agent_left".to_string(),
            source:     id.clone(),
            payload:    format!(r#"{{"agent_id":"{}"}}"#, id),
            ts:         now_ts(),
        });
        info!("👋 Agent disconnected: {}", id);
    }
}

async fn process_message(
    msg: MindMessage,
    state: &HubState,
    agent_id: &mut Option<String>,
    subscriptions: &mut HashSet<String>,
) -> Option<MindMessage> {
    match msg {
        MindMessage::Join { agent_id: id, agent_type, capabilities: _, subscriptions: subs } => {
            let info = AgentInfo {
                agent_id:    id.clone(),
                agent_type:  agent_type.clone(),
                connected_at: now_ts(),
                last_ping:   now_ts(),
            };
            state.agents.insert(id.clone(), info);
            *agent_id = Some(id.clone());
            *subscriptions = subs.into_iter().collect();
            subscriptions.insert("*".to_string());

            // Broadcast join event
            state.publish(BroadcastEvent {
                event_type: "agent_joined".to_string(),
                source:     id.clone(),
                payload:    format!(r#"{{"agent_id":"{}","type":"{}"}}"#, id, agent_type),
                ts:         now_ts(),
            });

            info!("✅ Agent joined: {} ({})", id, agent_type);

            Some(MindMessage::Welcome {
                agent_id: id,
                hub_version: "1.0.0".to_string(),
                connected_agents: state.connected_count(),
                ts: now_ts(),
            })
        }

        MindMessage::Ping { ts: _ } => {
            if let Some(id) = agent_id {
                if let Some(mut a) = state.agents.get_mut(id) {
                    a.last_ping = now_ts();
                }
            }
            Some(MindMessage::Pong {
                ts: now_ts(),
                connected_agents: state.connected_count(),
            })
        }

        MindMessage::Publish { event_type, payload, priority: _ } => {
            let src = agent_id.clone().unwrap_or("unknown".to_string());
            state.publish(BroadcastEvent {
                event_type,
                source:  src,
                payload: serde_json::to_string(&payload).unwrap_or_default(),
                ts:      now_ts(),
            });
            None  // No reply needed
        }

        MindMessage::KbWrite { key, value, ttl } => {
            let ttl_secs = ttl.unwrap_or(3600) as u64;
            let expires = now_ts() + ttl_secs;
            state.knowledge.insert(key.clone(), (value, expires));
            state.publish(BroadcastEvent {
                event_type: "kb_write".to_string(),
                source:     agent_id.clone().unwrap_or("unknown".to_string()),
                payload:    format!(r#"{{"key":"{}"}}"#, key),
                ts:         now_ts(),
            });
            None
        }

        MindMessage::KbRead { key } => {
            let value = state.knowledge.get(&key).and_then(|e| {
                if e.value().1 > now_ts() { Some(e.value().0.clone()) } else { None }
            });
            Some(MindMessage::KbValue { key, value })
        }

        _ => None,
    }
}

// ── Health + Stats ─────────────────────────────────────────────────────────

async fn health_handler(State(state): State<HubState>) -> axum::Json<serde_json::Value> {
    let agents: Vec<serde_json::Value> = state.agents.iter().map(|a| {
        serde_json::json!({
            "id":   a.key(),
            "type": a.value().agent_type,
            "last_ping": a.value().last_ping,
        })
    }).collect();

    axum::Json(serde_json::json!({
        "status": "ok",
        "service": "cognitive-mind",
        "version": "1.0.0",
        "connected_agents": state.connected_count(),
        "total_connections": state.total_connections.load(Ordering::Relaxed),
        "kb_entries": state.knowledge.len(),
        "agents": agents,
        "ts": now_ts(),
    }))
}

// ── PostgreSQL pg_notify listener ─────────────────────────────────────────

async fn pg_listener(db_url: String, hub: HubState) {
    info!("🐘 Connecting to PostgreSQL pg_notify...");
    loop {
        match tokio_postgres::connect(&db_url, NoTls).await {
            Ok((client, connection)) => {
                // Spawn connection handler
                let hub2 = hub.clone();
                tokio::spawn(async move { connection.await.ok(); });

                // LISTEN on mind_events channel
                if let Err(e) = client.execute("LISTEN mind_events", &[]).await {
                    error!("LISTEN failed: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                info!("👂 Listening on pg_notify 'mind_events'");

                // Also register hub itself as an agent in DB
                let _ = client.execute(
                    "SELECT public.ws_agent_join($1,$2,$3)",
                    &[&"cognitive-mind-hub", &"hub",
                      &serde_json::json!({"rust":true,"version":"1.0.0"}).to_string()]
                ).await;

                // Wait for notifications
                loop {
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        futures::future::poll_fn(|cx| {
                            use std::future::Future;
                            std::pin::Pin::new(&mut {
                                let client: &tokio_postgres::Client = &client;
                                async move { client.query_opt("SELECT pg_sleep(0)", &[]).await }
                            }).poll(cx)
                        })
                    ).await {
                        Ok(_) => {},
                        Err(_) => {
                            // Timeout — check for notifications
                        }
                    }

                    // Process pending notifications
                    // Note: tokio-postgres streams notifications through the connection
                    // We use a workaround: poll with a simple query and check notifications
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Err(e) => {
                error!("DB connect failed: {}. Retrying in 5s...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// Better pg_notify listener using the Notifications stream
async fn pg_listener_v2(db_url: String, hub: HubState) {
    info!("🐘 PG Listener v2 starting...");
    loop {
        match tokio_postgres::connect(&db_url, NoTls).await {
            Ok((mut client, mut connection)) => {
                info!("✅ Connected to PostgreSQL");

                // LISTEN
                client.execute("LISTEN mind_events", &[]).await.ok();

                // Register hub in DB
                client.execute(
                    "SELECT public.ws_agent_join($1, $2, $3::jsonb, ARRAY['*']::TEXT[])",
                    &[&"cognitive-mind-hub", &"hub",
                      &r#"{"rust":true,"version":"1.0.0"}"#]
                ).await.ok();

                info!("👂 LISTEN mind_events active — forwarding to WebSocket");

                // Process notifications from the connection stream
                loop {
                    // Poll connection for notifications
                    let notif = futures::future::poll_fn(|cx| {
                        use tokio_postgres::AsyncMessage;
                        match std::pin::Pin::new(&mut connection).poll_message(cx) {
                            std::task::Poll::Ready(Some(Ok(msg))) => std::task::Poll::Ready(Some(msg)),
                            std::task::Poll::Ready(Some(Err(e))) => {
                                error!("Connection error: {}", e);
                                std::task::Poll::Ready(None)
                            }
                            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                            std::task::Poll::Pending => std::task::Poll::Pending,
                        }
                    });

                    match tokio::time::timeout(Duration::from_secs(60), notif).await {
                        Ok(Some(tokio_postgres::AsyncMessage::Notification(n))) => {
                            let payload = n.payload();
                            info!("📨 pg_notify: {}...", &payload[..payload.len().min(60)]);

                            // Parse and broadcast
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                                let event_type = json.get("e")
                                    .or_else(|| json.get("event"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("event")
                                    .to_string();
                                hub.publish(BroadcastEvent {
                                    event_type,
                                    source: "postgresql".to_string(),
                                    payload: payload.to_string(),
                                    ts: now_ts(),
                                });
                            }
                        }
                        Ok(Some(tokio_postgres::AsyncMessage::Notice(_))) => {}
                        Ok(None) => {
                            warn!("PG connection closed. Reconnecting...");
                            break;
                        }
                        Err(_) => {
                            // Timeout — send keepalive
                            client.execute("SELECT 1", &[]).await.ok();
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("DB connect failed: {}. Retrying...", e);
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

// ── Heartbeat broadcaster ──────────────────────────────────────────────────

async fn heartbeat_task(hub: HubState) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        hub.publish(BroadcastEvent {
            event_type: "heartbeat".to_string(),
            source:     "cognitive-mind-hub".to_string(),
            payload:    format!(r#"{{"connected":{},"ts":{}}}"#, hub.connected_count(), now_ts()),
            ts:         now_ts(),
        });

        // Cleanup expired KB entries
        hub.knowledge.retain(|_, v| v.1 > now_ts());
    }
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("cognitive_mind=info,tower_http=warn")
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let port: u16 = std::env::var("PORT")
        .unwrap_or("8765".to_string())
        .parse()
        .unwrap_or(8765);

    info!("🧠 CognitiveMind Hub starting on :{}", port);

    let state = HubState::new();

    // Start pg_notify listener (separate task)
    let pg_state = state.clone();
    let pg_url = db_url.clone();
    tokio::spawn(async move { pg_listener_v2(pg_url, pg_state).await });

    // Start heartbeat task
    let hb_state = state.clone();
    tokio::spawn(async move { heartbeat_task(hb_state).await });

    // Start Upstash bridge (reads Supabase-published events, re-broadcasts to WS)
    if let (Ok(ups_url), Ok(ups_tok)) = (
        std::env::var("UPSTASH_URL"),
        std::env::var("UPSTASH_TOKEN"),
    ) {
        let ups_state = state.clone();
        tokio::spawn(async move {
            upstash_bridge_task(ups_state, ups_url, ups_tok).await
        });
        info!("🔗 Upstash bridge enabled");
    } else {
        warn!("⚠️ UPSTASH_URL/TOKEN not set — bridge disabled");
    }

    // Build HTTP/WS router
    let app = Router::new()
        .route("/ws",     get(ws_handler))
        .route("/health", get(health_handler))
        .route("/",       get(|| async { "🧠 CognitiveMind Hub v1.0" }))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("🚀 Listening on {} (WS: ws://{}:{}/ws)", addr, "0.0.0.0", port);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
