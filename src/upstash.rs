
// src/upstash.rs — Upstash Redis REST client for Rust Hub
use std::time::Duration;

#[derive(Clone)]
pub struct UpstashClient {
    pub url:   String,
    pub token: String,
    client:    reqwest::Client,
}

impl UpstashClient {
    pub fn new(url: &str, token: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(1500))
            .build()
            .unwrap();
        Self { url: url.to_string(), token: token.to_string(), client }
    }

    pub async fn exec(&self, args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let r = self.client.post(&self.url)
            .bearer_auth(&self.token)
            .json(&args)
            .send()
            .await?;
        let d: serde_json::Value = r.json().await?;
        Ok(d["result"].clone())
    }

    // Publish event to channel (LPUSH + LTRIM)
    pub async fn publish(&self, channel: &str, payload: &serde_json::Value) -> anyhow::Result<()> {
        let msg = {
            let mut m = payload.clone();
            m["_ts"] = serde_json::json!(chrono::Utc::now().timestamp_millis());
            m.to_string()
        };
        self.exec(serde_json::json!(["LPUSH", format!("ch:{}", channel), msg])).await?;
        self.exec(serde_json::json!(["LTRIM", format!("ch:{}", channel), "0", "999"])).await?;
        Ok(())
    }

    // Read recent channel messages since timestamp
    pub async fn pop_messages(&self, channel: &str, since_ms: i64) -> anyhow::Result<Vec<serde_json::Value>> {
        let raw = self.exec(serde_json::json!(["LRANGE", format!("ch:{}", channel), "0", "49"])).await?;
        let msgs = raw.as_array().cloned().unwrap_or_default();
        let result: Vec<serde_json::Value> = msgs.iter()
            .filter_map(|m| {
                let s = m.as_str()?;
                serde_json::from_str(s).ok()
            })
            .filter(|m: &serde_json::Value| {
                m["_ts"].as_i64().unwrap_or(0) > since_ms
            })
            .collect();
        Ok(result)
    }

    // Knowledge Bus operations
    pub async fn kb_write(&self, key: &str, value: &serde_json::Value, ttl_secs: u64) -> anyhow::Result<()> {
        self.exec(serde_json::json!(["SET", format!("kb:{}", key), value.to_string(), "EX", ttl_secs.to_string()])).await?;
        Ok(())
    }

    pub async fn kb_read(&self, key: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let raw = self.exec(serde_json::json!(["GET", format!("kb:{}", key)])).await?;
        if raw.is_null() { return Ok(None); }
        let s = raw.as_str().unwrap_or("");
        Ok(serde_json::from_str(s).ok())
    }

    // Agent registry
    pub async fn register_agent(&self, id: &str, agent_type: &str) -> anyhow::Result<()> {
        let info = serde_json::json!({
            "id": id, "type": agent_type,
            "ts": chrono::Utc::now().timestamp_millis(), "online": true,
        });
        self.exec(serde_json::json!(["SET", format!("agent:{}", id), info.to_string(), "EX", "120"])).await?;
        self.exec(serde_json::json!(["SADD", "agents:online", id])).await?;
        Ok(())
    }

    pub async fn get_online_count(&self) -> anyhow::Result<usize> {
        let r = self.exec(serde_json::json!(["SCARD", "agents:online"])).await?;
        Ok(r.as_u64().unwrap_or(0) as usize)
    }
}
