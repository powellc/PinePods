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

/// Machine-readable event kinds carried in the HTTP payload's `event` field.
pub const EVENT_NEW_CONTENT: &str = "new_content";
pub const EVENT_PLAYBACK_STARTED: &str = "playback_started";
pub const EVENT_PLAYBACK_PROGRESS: &str = "playback_progress";
pub const EVENT_PLAYBACK_FINISHED: &str = "playback_finished";

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

/// Extra context attached to playback events for the rich HTTP payload.
#[derive(Debug, Clone, Default)]
pub struct PlaybackContext {
    pub position_sec: Option<f64>,
    pub percent: Option<i32>,
    pub device_name: Option<String>,
}

/// A real notification event. `episode` carries the podcast/episode metadata
/// used to build the rich HTTP payload; it is `None` for events without an
/// episode (e.g. the settings-page test notification).
pub struct NotificationEvent<'a> {
    pub category: NotificationCategory,
    pub kind: &'a str,
    pub title: &'a str,
    pub message: &'a str,
    pub user_id: i32,
    pub episode: Option<&'a EpisodeNotificationInfo>,
    pub playback: Option<PlaybackContext>,
}

/// Client used for real notification dispatch (short timeout so a dead
/// notification server never stalls a request or websocket reader).
pub fn default_client() -> AppResult<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(NOTIFICATION_TIMEOUT)
        .build()
        .map_err(crate::error::AppError::Http)
}

/// Load the user's category prefs and send the event to every enabled platform.
/// Returns true when at least one platform accepted the notification.
pub async fn dispatch(db: &DatabasePool, event: &NotificationEvent<'_>) -> AppResult<bool> {
    let user_id = event.user_id;
    let (notify_new_content, notify_playback) = db.get_notification_preferences(user_id).await?;
    if !event.category.enabled_by(notify_new_content, notify_playback) {
        return Ok(false);
    }

    let settings = db.get_notification_settings(user_id).await?;
    if settings.is_empty() {
        tracing::debug!("No notification platforms configured for user {user_id}");
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
            "ntfy" => send_ntfy(&client, setting, event).await,
            "gotify" => send_gotify(&client, setting, event).await,
            "http" => send_http(&client, setting, event).await,
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

    if !sent_any {
        tracing::debug!("No enabled notification platform for user {user_id}");
    }

    Ok(sent_any)
}

/// Send one test notification for an explicitly chosen platform. No category
/// gating: the user pressed the button, so always attempt delivery.
pub async fn send_test(
    user_id: i32,
    platform: &str,
    settings: &Value,
    message: &str,
) -> AppResult<bool> {
    let client = reqwest::Client::new();
    let event = NotificationEvent {
        category: NotificationCategory::Playback, // unused for the test path
        kind: "test",
        title: "PinePods Test",
        message,
        user_id,
        episode: None,
        playback: None,
    };

    match platform {
        "ntfy" => send_ntfy(&client, settings, &event).await,
        "gotify" => send_gotify(&client, settings, &event).await,
        "http" => send_http(&client, settings, &event).await,
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
    event: &NotificationEvent<'_>,
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
        .body(event.message.to_string());

    if let Ok(value) = reqwest::header::HeaderValue::from_str(event.title) {
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
    event: &NotificationEvent<'_>,
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
            "message": event.message,
            "title": event.title,
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

/// Build the POST body for a generic HTTP webhook. Telegram gets its bot-API
/// shape; everything else gets a stable JSON envelope with the full podcast /
/// episode metadata (see the module docs for the exact shape).
pub fn build_http_payload(
    event: &NotificationEvent<'_>,
    http_url: &str,
    http_token: &str,
) -> serde_json::Value {
    if http_url.contains("api.telegram.org") {
        let chat_id = http_token.split(':').nth(1).unwrap_or("YOUR_CHAT_ID");
        return serde_json::json!({ "chat_id": chat_id, "text": event.message });
    }

    let podcast = event.episode.map(|info| {
        serde_json::json!({
            "id": info.podcast_id,
            "name": info.show_name,
            "author": info.podcast_author,
            "artwork_url": info.podcast_artwork,
            "feed_url": info.podcast_feed_url,
            "website_url": info.podcast_website_url,
            "categories": info.podcast_categories,
            "explicit": info.podcast_explicit,
            "is_youtube": info.is_youtube,
        })
    });
    let episode = event.episode.map(|info| {
        serde_json::json!({
            "id": info.episode_id,
            "title": info.title,
            "description": info.episode_description,
            "url": info.episode_url,
            "artwork_url": info.episode_artwork,
            "duration_sec": info.duration,
            "published_at": info.episode_pub_date,
            "guid": info.episode_guid,
            "is_video": info.is_video,
            "is_youtube": info.is_youtube,
        })
    });
    let playback = event.playback.as_ref().map(|p| {
        serde_json::json!({
            "position_sec": p.position_sec,
            "percent": p.percent,
            "device_name": p.device_name,
        })
    });

    serde_json::json!({
        "event": event.kind,
        "title": event.title,
        "message": event.message,
        "text": event.message,
        "user_id": event.user_id,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "podcast": podcast,
        "episode": episode,
        "playback": playback,
    })
}

/// Generic HTTP webhook: GET with `message`/`event` query params, or POST with
/// the rich JSON body from [`build_http_payload`]. `api.telegram.org` gets its
/// bot-API payload shape. An optional token is sent as a Bearer header (or
/// embedded in the URL for Telegram).
pub async fn send_http(
    client: &reqwest::Client,
    settings: &Value,
    event: &NotificationEvent<'_>,
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
            // GET can't carry a body; send the essentials as query parameters.
            let separator = if http_url.contains('?') { '&' } else { '?' };
            let mut params = format!(
                "message={}&event={}",
                urlencoding::encode(event.message),
                urlencoding::encode(event.kind)
            );
            if let Some(info) = event.episode {
                params.push_str(&format!(
                    "&episode_id={}&podcast_id={}",
                    info.episode_id, info.podcast_id
                ));
            }
            client.get(format!("{}{}{}", http_url, separator, params))
        }
        "POST" | _ => {
            let payload = build_http_payload(event, http_url, http_token);
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
            episode_id: 7,
            episode_description: Some("An episode".to_string()),
            episode_url: Some("https://example.com/1.mp3".to_string()),
            episode_artwork: Some("https://example.com/1.jpg".to_string()),
            episode_pub_date: Some("2026-01-02T03:04:05".to_string()),
            episode_guid: Some("guid-1".to_string()),
            is_video: false,
            is_youtube: false,
            podcast_id: 3,
            podcast_author: Some("Author".to_string()),
            podcast_artwork: Some("https://example.com/show.jpg".to_string()),
            podcast_feed_url: Some("https://example.com/feed.xml".to_string()),
            podcast_website_url: Some("https://example.com".to_string()),
            podcast_categories: Some("Tech".to_string()),
            podcast_explicit: Some(false),
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

    #[test]
    fn http_payload_carries_full_metadata() {
        let info = info();
        let event = NotificationEvent {
            category: NotificationCategory::Playback,
            kind: EVENT_PLAYBACK_STARTED,
            title: "Playback Started",
            message: "Started playing Ep 1 from Show",
            user_id: 2,
            episode: Some(&info),
            playback: Some(PlaybackContext {
                position_sec: Some(30.0),
                percent: Some(0),
                device_name: Some("Pixel".to_string()),
            }),
        };

        let payload = build_http_payload(&event, "https://example.com/hook", "");
        assert_eq!(payload["event"], EVENT_PLAYBACK_STARTED);
        assert_eq!(payload["user_id"], 2);
        assert_eq!(payload["podcast"]["id"], 3);
        assert_eq!(payload["podcast"]["name"], "Show");
        assert_eq!(payload["podcast"]["author"], "Author");
        assert_eq!(payload["episode"]["id"], 7);
        assert_eq!(payload["episode"]["title"], "Ep 1");
        assert_eq!(payload["episode"]["duration_sec"], 3600);
        assert_eq!(payload["episode"]["guid"], "guid-1");
        assert_eq!(payload["playback"]["position_sec"], 30.0);
        assert_eq!(payload["playback"]["device_name"], "Pixel");
        assert!(payload["timestamp"].is_string());

        // Events without an episode still produce the stable envelope.
        let bare = NotificationEvent {
            category: NotificationCategory::Playback,
            kind: "test",
            title: "PinePods Test",
            message: "Test notification from PinePods",
            user_id: 2,
            episode: None,
            playback: None,
        };
        let payload = build_http_payload(&bare, "https://example.com/hook", "");
        assert!(payload["podcast"].is_null());
        assert!(payload["episode"].is_null());
        assert!(payload["playback"].is_null());

        // Telegram keeps its bot-API shape.
        let payload = build_http_payload(
            &bare,
            "https://api.telegram.org/bot123/sendMessage",
            "bot123:chat456",
        );
        assert_eq!(payload["chat_id"], "chat456");
        assert_eq!(payload["text"], "Test notification from PinePods");
    }
}
