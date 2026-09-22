//! Unified outbound notification delivery (ntfy / Gotify / generic HTTP).
//!
//! Two callers use this module:
//!   * the test button in settings (`NotificationManager::send_test_notification`),
//!     which targets one explicitly-chosen platform, and
//!   * real events via [`dispatch`], which fans out to every *enabled* platform
//!     after checking the user's category toggles in `UserNotificationPreferences`.
//!
//! Real events are split into two categories: new content (a new episode arrived)
//! and playback (your own episode started / progressed / finished). Everything is
//! best-effort: a failed platform never fails the request that triggered it.

use std::time::Duration;

use serde_json::Value;
use tracing::{info, warn};

use crate::database::{DatabasePool, EpisodeNotificationInfo};
use crate::error::AppResult;

/// Per-platform HTTP timeout. Matches the historical new-episode behavior.
const NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(2);

/// Category of a real (non-test) notification, mapped onto the user's
/// `UserNotificationPreferences` toggles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationCategory {
    /// A new episode arrived for a subscribed podcast.
    NewContent,
    /// The user's own playback: started, 25/50/75% milestones, finished.
    Playback,
}

impl NotificationCategory {
    fn enabled_by(self, notify_new_content: bool, notify_playback: bool) -> bool {
        match self {
            NotificationCategory::NewContent => notify_new_content,
            NotificationCategory::Playback => notify_playback,
        }
    }
}

/// Client used for real notification dispatch (short timeout so a dead
/// notification server never stalls a request or websocket reader).
pub fn default_client() -> AppResult<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(NOTIFICATION_TIMEOUT)
        .build()
        .map_err(crate::error::AppError::Http)
}

/// Load the user's category prefs and send `title`/`message` to every enabled
/// platform. Returns true when at least one platform accepted the notification.
pub async fn dispatch(
    db: &DatabasePool,
    user_id: i32,
    category: NotificationCategory,
    title: &str,
    message: &str,
) -> AppResult<bool> {
    let (notify_new_content, notify_playback) = db.get_notification_preferences(user_id).await?;
    if !category.enabled_by(notify_new_content, notify_playback) {
        return Ok(false);
    }

    let settings = db.get_notification_settings(user_id).await?;
    if settings.is_empty() {
        return Ok(false);
    }

    let client = default_client()?;
    let mut sent_any = false;

    for setting in settings.iter() {
        let platform = setting
            .get("platform")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let enabled = setting
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !enabled {
            continue;
        }

        let result = match platform {
            "ntfy" => send_ntfy(&client, setting, title, message).await,
            "gotify" => send_gotify(&client, setting, title, message).await,
            "http" => send_http(&client, setting, title, message).await,
            _ => {
                warn!("Unknown notification platform: {}", platform);
                continue;
            }
        };

        match result {
            Ok(true) => sent_any = true,
            Ok(false) => {}
            Err(e) => warn!("Notification via {} failed: {}", platform, e),
        }
    }

    Ok(sent_any)
}

/// Send one test notification for an explicitly chosen platform. No category
/// gating: the user pressed the button, so always attempt delivery.
pub async fn send_test(
    platform: &str,
    settings: &Value,
    message: &str,
) -> AppResult<bool> {
    let client = reqwest::Client::new();
    let title = "PinePods Test";

    match platform {
        "ntfy" => send_ntfy(&client, settings, title, message).await,
        "gotify" => send_gotify(&client, settings, title, message).await,
        "http" => send_http(&client, settings, title, message).await,
        _ => {
            info!("Unsupported notification platform: {}", platform);
            Ok(false)
        }
    }
}

// =========================== Message formatting ===========================

/// Human-readable episode label: "Episode Title from Podcast Name".
pub fn episode_label(info: &EpisodeNotificationInfo) -> String {
    if info.show_name.is_empty() {
        info.title.clone()
    } else {
        format!("{} from {}", info.title, info.show_name)
    }
}

/// (title, message) for a playback-start notification.
pub fn playback_start_message(
    info: &EpisodeNotificationInfo,
    device_name: Option<&str>,
) -> (String, String) {
    let device = device_name.unwrap_or_default().trim();
    let message = if device.is_empty() {
        format!("Started playing {}", episode_label(info))
    } else {
        format!("Started playing {} on {}", episode_label(info), device)
    };
    ("Playback Started".to_string(), message)
}

/// (title, message) for a milestone notification.
pub fn playback_progress_message(
    info: &EpisodeNotificationInfo,
    percent: i32,
) -> (String, String) {
    (
        "Playback Progress".to_string(),
        format!("{}% through {}", percent, episode_label(info)),
    )
}

/// (title, message) for a playback-finished notification.
pub fn playback_finish_message(info: &EpisodeNotificationInfo) -> (String, String) {
    (
        "Playback Finished".to_string(),
        format!("Finished {}", episode_label(info)),
    )
}

// ============================== Senders ==============================

/// ntfy: plain-text body, optional `Title` header (skipped when the title has
/// non-ASCII characters, which would be an invalid HTTP header value).
pub async fn send_ntfy(
    client: &reqwest::Client,
    settings: &Value,
    title: &str,
    message: &str,
) -> AppResult<bool> {
    let topic = settings.get("ntfy_topic").and_then(|v| v.as_str()).unwrap_or("");
    let server_url = settings
        .get("ntfy_server_url")
        .and_then(|v| v.as_str())
        .unwrap_or("https://ntfy.sh");
    let username = settings.get("ntfy_username").and_then(|v| v.as_str());
    let password = settings.get("ntfy_password").and_then(|v| v.as_str());
    let access_token = settings.get("ntfy_access_token").and_then(|v| v.as_str());

    if topic.is_empty() {
        return Ok(false);
    }

    let url = format!("{}/{}", server_url.trim_end_matches('/'), topic);
    let mut request = client
        .post(&url)
        .header("Content-Type", "text/plain")
        .body(message.to_string());

    if let Ok(value) = reqwest::header::HeaderValue::from_str(title) {
        request = request.header("Title", value);
    }

    // Add authentication if provided
    if let Some(token) = access_token.filter(|t| !t.is_empty()) {
        // Use access token (preferred method)
        request = request.header("Authorization", format!("Bearer {}", token));
    } else if let (Some(user), Some(pass)) = (
        username.filter(|u| !u.is_empty()),
        password.filter(|p| !p.is_empty()),
    ) {
        // Use username/password basic auth
        request = request.basic_auth(user, Some(pass));
    }

    match request.send().await {
        Ok(response) => {
            if response.status().is_success() {
                info!("Successfully sent NTFY notification to {}", url);
                Ok(true)
            } else {
                warn!("NTFY notification failed with status: {}", response.status());
                Ok(false)
            }
        }
        Err(e) => {
            warn!("Failed to send NTFY notification: {}", e);
            Ok(false)
        }
    }
}

/// Gotify: JSON `{title, message, priority}` to `{url}/message?token=…`.
pub async fn send_gotify(
    client: &reqwest::Client,
    settings: &Value,
    title: &str,
    message: &str,
) -> AppResult<bool> {
    let server_url = settings.get("gotify_url").and_then(|v| v.as_str()).unwrap_or("");
    let token = settings.get("gotify_token").and_then(|v| v.as_str()).unwrap_or("");

    if server_url.is_empty() || token.is_empty() {
        return Ok(false);
    }

    let url = format!("{}/message?token={}", server_url.trim_end_matches('/'), token);

    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "message": message,
            "title": title,
            "priority": 5
        }))
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                info!("Successfully sent Gotify notification to {}", url);
                Ok(true)
            } else {
                warn!("Gotify notification failed with status: {}", response.status());
                Ok(false)
            }
        }
        Err(e) => {
            warn!("Failed to send Gotify notification: {}", e);
            Ok(false)
        }
    }
}

/// Generic HTTP webhook: GET with a `message` query param, or POST with a JSON
/// body. `api.telegram.org` gets its bot-API payload shape. An optional token is
/// sent as a Bearer header (or embedded in the URL for Telegram).
pub async fn send_http(
    client: &reqwest::Client,
    settings: &Value,
    title: &str,
    message: &str,
) -> AppResult<bool> {
    let http_url = settings.get("http_url").and_then(|v| v.as_str()).unwrap_or("");
    let http_token = settings.get("http_token").and_then(|v| v.as_str()).unwrap_or("");
    let http_method = settings
        .get("http_method")
        .and_then(|v| v.as_str())
        .unwrap_or("POST");

    if http_url.is_empty() {
        info!("HTTP URL is empty, cannot send notification");
        return Ok(false);
    }

    // Build the request based on method
    let request_builder = match http_method.to_uppercase().as_str() {
        "GET" => {
            // For GET requests, add message as query parameter
            let separator = if http_url.contains('?') { '&' } else { '?' };
            let url_with_params = format!(
                "{}{}message={}",
                http_url,
                separator,
                urlencoding::encode(message)
            );
            client.get(&url_with_params)
        }
        "POST" | _ => {
            // For POST requests, send JSON payload
            let payload = if http_url.contains("api.telegram.org") {
                // Special handling for Telegram Bot API
                let chat_id = if let Some(chat_id_str) = http_token.split(':').nth(1) {
                    // Extract chat_id from token if it contains chat_id (format: bot_token:chat_id)
                    chat_id_str
                } else {
                    // Default chat_id - user needs to configure this properly
                    "YOUR_CHAT_ID"
                };

                serde_json::json!({
                    "chat_id": chat_id,
                    "text": message
                })
            } else {
                // Generic JSON payload
                serde_json::json!({
                    "title": title,
                    "message": message,
                    "text": message
                })
            };

            client
                .post(http_url)
                .header("Content-Type", "application/json")
                .json(&payload)
        }
    };

    // Add authorization header if a token is provided (Telegram embeds it in the URL)
    let request_builder = if !http_token.is_empty() && !http_url.contains("api.telegram.org") {
        request_builder.header("Authorization", format!("Bearer {}", http_token))
    } else {
        request_builder
    };

    match request_builder.send().await {
        Ok(response) => {
            let status = response.status();
            let is_success = status.is_success();

            if !is_success {
                let response_text = response.text().await.unwrap_or_default();
                warn!(
                    "HTTP notification failed with status: {} - Response: {}",
                    status, response_text
                );
            }

            Ok(is_success)
        }
        Err(e) => {
            warn!("HTTP notification request failed: {}", e);
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> EpisodeNotificationInfo {
        EpisodeNotificationInfo {
            title: "Ep 1".to_string(),
            show_name: "Show".to_string(),
            duration: 3600,
        }
    }

    #[test]
    fn category_gating_matches_prefs() {
        assert!(NotificationCategory::NewContent.enabled_by(true, false));
        assert!(!NotificationCategory::NewContent.enabled_by(false, true));
        assert!(NotificationCategory::Playback.enabled_by(false, true));
        assert!(!NotificationCategory::Playback.enabled_by(true, false));
    }

    #[test]
    fn start_message_includes_device_when_present() {
        let (_, message) = playback_start_message(&info(), Some("Pixel"));
        assert!(message.contains("Ep 1 from Show"));
        assert!(message.contains("Pixel"));

        let (_, message) = playback_start_message(&info(), None);
        assert!(!message.contains(" on "));
    }

    #[test]
    fn progress_and_finish_messages() {
        let (_, message) = playback_progress_message(&info(), 50);
        assert!(message.contains("50%"));
        assert!(message.contains("Ep 1 from Show"));

        let (_, message) = playback_finish_message(&info());
        assert_eq!(message, "Finished Ep 1 from Show");
    }
}
