use serde_json::Value;
use crate::{error::AppResult, redis_client::RedisClient};
use tracing::info;

pub struct ImportProgressManager {
    redis_client: RedisClient,
}

impl ImportProgressManager {
    pub fn new(redis_client: RedisClient) -> Self {
        Self { redis_client }
    }

    // Start import progress tracking - matches Python ImportProgressManager.start_import
    pub async fn start_import(&self, user_id: i32, total_podcasts: i32) -> AppResult<()> {
        let progress_data = serde_json::json!({
            "current": 0,
            "total": total_podcasts,
            "current_podcast": ""
        });
        
        let key = format!("import_progress:{}", user_id);
        self.redis_client.set_ex(&key, &progress_data.to_string(), 3600).await?;
        
        Ok(())
    }

    // Update import progress - matches Python ImportProgressManager.update_progress
    pub async fn update_progress(&self, user_id: i32, current: i32, current_podcast: &str) -> AppResult<()> {
        let key = format!("import_progress:{}", user_id);
        
        // Get current progress
        if let Some(progress_json) = self.redis_client.get::<String>(&key).await? {
            if let Ok(mut progress) = serde_json::from_str::<Value>(&progress_json) {
                progress["current"] = serde_json::Value::Number(serde_json::Number::from(current));
                progress["current_podcast"] = serde_json::Value::String(current_podcast.to_string());
                
                self.redis_client.set_ex(&key, &progress.to_string(), 3600).await?;
            }
        }
        
        Ok(())
    }

    // Get import progress - matches Python ImportProgressManager.get_progress
    pub async fn get_progress(&self, user_id: i32) -> AppResult<(i32, i32, String)> {
        let key = format!("import_progress:{}", user_id);
        
        if let Some(progress_json) = self.redis_client.get::<String>(&key).await? {
            if let Ok(progress) = serde_json::from_str::<Value>(&progress_json) {
                let current = progress.get("current").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let total = progress.get("total").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let current_podcast = progress.get("current_podcast").and_then(|v| v.as_str()).unwrap_or("").to_string();
                
                return Ok((current, total, current_podcast));
            }
        }
        
        Ok((0, 0, "".to_string()))
    }

    // Clear import progress - matches Python ImportProgressManager.clear_progress
    pub async fn clear_progress(&self, user_id: i32) -> AppResult<()> {
        let key = format!("import_progress:{}", user_id);
        self.redis_client.delete(&key).await?;
        Ok(())
    }
}

// Notification manager for sending test notifications. Real-event (new content /
// playback) dispatch lives in `services::notifications::dispatch`; this remains
// as the settings-page "Send Test Notification" path.
pub struct NotificationManager;

impl NotificationManager {
    pub fn new() -> Self {
        Self
    }

    // Send test notification - matches Python notification functionality
    pub async fn send_test_notification(&self, user_id: i32, platform: &str, settings: &serde_json::Value) -> AppResult<bool> {
        info!("Sending test notification for user {} on platform {}", user_id, platform);
        crate::services::notifications::send_test(
            user_id,
            platform,
            settings,
            "Test notification from PinePods",
        )
        .await
    }
}