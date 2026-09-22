//! Server-side playback event notifications: started / milestone / finished.
//!
//! Detection lives on the server so every client (web, mobile, third-party) gets
//! playback pushes without client changes:
//!   * started  - the now-playing websocket `Report` fires immediately (and has
//!                the device name); the first `record_listen_duration` report is
//!                the fallback for clients that don't use that socket
//!   * progress - 25 / 50 / 75% milestones observed while duration ticks arrive
//!   * finished - auto-complete during reporting, plus a recent-near-end
//!                heuristic on `mark_episode_completed` (natural playback only;
//!                bulk/sync paths never reach it)
//!
//! Redis `SET NX` keys deduplicate across ticks and multiple devices. If Redis
//! is unavailable the event is skipped rather than re-sent, so notification
//! storms are impossible. Every helper is best-effort and safe to `tokio::spawn`
//! from a request/websocket handler.

use crate::database::EpisodeNotificationInfo;
use crate::error::AppResult;
use crate::services::notifications::{
    dispatch, playback_finish_message, playback_progress_message, playback_start_message,
    NotificationCategory, NotificationEvent, PlaybackContext, EVENT_PLAYBACK_FINISHED,
    EVENT_PLAYBACK_PROGRESS, EVENT_PLAYBACK_STARTED,
};
use crate::AppState;

const START_TTL_SECS: u64 = 24 * 60 * 60;
const MILESTONE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const FINISH_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const MILESTONES: [i32; 3] = [25, 50, 75];

fn media_key_part(is_youtube: bool) -> &'static str {
    if is_youtube { "yt" } else { "ep" }
}

fn start_key(user_id: i32, episode_id: i32, is_youtube: bool) -> String {
    format!(
        "notif:playback:start:{}:{}:{}",
        user_id,
        media_key_part(is_youtube),
        episode_id
    )
}

fn milestone_key(user_id: i32, episode_id: i32, is_youtube: bool, milestone: i32) -> String {
    format!(
        "notif:playback:milestone:{}:{}:{}:{}",
        user_id,
        media_key_part(is_youtube),
        episode_id,
        milestone
    )
}

fn finish_key(user_id: i32, episode_id: i32, is_youtube: bool) -> String {
    format!(
        "notif:playback:finish:{}:{}:{}",
        user_id,
        media_key_part(is_youtube),
        episode_id
    )
}

/// Cheap gate so hot paths can skip metadata fetches for opted-out users.
/// A missing preferences row means "enabled" (same default as dispatch).
async fn playback_enabled(state: &AppState, user_id: i32) -> bool {
    match state.db_pool.get_notification_preferences(user_id).await {
        Ok((_, true)) => true,
        Ok((_, false)) => {
            tracing::debug!("Playback notifications disabled for user {user_id}");
            false
        }
        Err(e) => {
            // Usually a missing UserNotificationPreferences table (migration 060
            // not applied). Surface it so silent no-ops are diagnosable.
            tracing::warn!(
                "Playback notification preference check failed for user {user_id}: {e}"
            );
            false
        }
    }
}

async fn episode_info(
    state: &AppState,
    episode_id: i32,
    is_youtube: bool,
) -> Option<EpisodeNotificationInfo> {
    match state
        .db_pool
        .get_episode_notification_info(episode_id, is_youtube)
        .await
    {
        Ok(Some(info)) => Some(info),
        Ok(None) => {
            tracing::debug!(
                "No episode metadata for notification (episode {episode_id}, youtube={is_youtube})"
            );
            None
        }
        Err(e) => {
            tracing::warn!("Failed to load episode metadata for notification: {e}");
            None
        }
    }
}

/// Build a playback event around the fetched episode metadata.
fn playback_event<'a>(
    user_id: i32,
    kind: &'a str,
    title: &'a str,
    message: &'a str,
    info: &'a EpisodeNotificationInfo,
    playback: Option<PlaybackContext>,
) -> NotificationEvent<'a> {
    NotificationEvent {
        category: NotificationCategory::Playback,
        kind,
        title,
        message,
        user_id,
        episode: Some(info),
        playback,
    }
}

/// Playback started. `device_name`/`position_sec` are set when the event came
/// from the now-playing websocket, which produces the timeliest signal.
pub async fn notify_start(
    state: &AppState,
    user_id: i32,
    episode_id: i32,
    is_youtube: bool,
    device_name: Option<&str>,
    position_sec: Option<f64>,
) -> AppResult<()> {
    if !playback_enabled(state, user_id).await {
        return Ok(());
    }

    let key = start_key(user_id, episode_id, is_youtube);
    match state.redis_client.set_nx(&key, 1, START_TTL_SECS).await {
        Ok(true) => {}
        Ok(false) => return Ok(()),
        Err(e) => {
            tracing::debug!("Playback start notification skipped (redis): {}", e);
            return Ok(());
        }
    }

    let Some(info) = episode_info(state, episode_id, is_youtube).await else {
        return Ok(());
    };
    let (title, message) = playback_start_message(&info, device_name);
    let event = playback_event(
        user_id,
        EVENT_PLAYBACK_STARTED,
        &title,
        &message,
        &info,
        Some(PlaybackContext {
            position_sec,
            percent: percent_of(position_sec, info.duration),
            device_name: device_name.map(str::to_string),
        }),
    );
    let _ = dispatch(&state.db_pool, &event).await;
    Ok(())
}

/// Whole-number playback percentage, when the duration is known.
fn percent_of(position_sec: Option<f64>, duration: i32) -> Option<i32> {
    if duration <= 0 {
        return None;
    }
    position_sec.map(|p| ((p / duration as f64) * 100.0).floor() as i32)
}

/// A `record_listen_duration` report. Handles the start fallback (for clients
/// not on the websocket) and milestone progress in a single metadata fetch.
pub async fn notify_report(
    state: &AppState,
    user_id: i32,
    episode_id: i32,
    is_youtube: bool,
    position_sec: f64,
    previous_position: i32,
) -> AppResult<()> {
    if !playback_enabled(state, user_id).await {
        return Ok(());
    }

    let Some(info) = episode_info(state, episode_id, is_youtube).await else {
        return Ok(());
    };

    // Start fallback: no stored position before this report.
    if previous_position <= 0 {
        let key = start_key(user_id, episode_id, is_youtube);
        if matches!(
            state.redis_client.set_nx(&key, 1, START_TTL_SECS).await,
            Ok(true)
        ) {
            let (title, message) = playback_start_message(&info, None);
            let event = playback_event(
                user_id,
                EVENT_PLAYBACK_STARTED,
                &title,
                &message,
                &info,
                Some(PlaybackContext {
                    position_sec: Some(position_sec),
                    percent: percent_of(Some(position_sec), info.duration),
                    device_name: None,
                }),
            );
            let _ = dispatch(&state.db_pool, &event).await;
        }
    }

    if info.duration <= 0 {
        return Ok(());
    }

    let percent = ((position_sec / info.duration as f64) * 100.0).floor() as i32;
    let mut highest_new: Option<i32> = None;
    for milestone in MILESTONES {
        if percent < milestone {
            continue;
        }
        let key = milestone_key(user_id, episode_id, is_youtube, milestone);
        if matches!(
            state
                .redis_client
                .set_nx(&key, 1, MILESTONE_TTL_SECS)
                .await,
            Ok(true)
        ) {
            highest_new = Some(milestone);
        }
    }

    if let Some(milestone) = highest_new {
        let (title, message) = playback_progress_message(&info, milestone);
        let event = playback_event(
            user_id,
            EVENT_PLAYBACK_PROGRESS,
            &title,
            &message,
            &info,
            Some(PlaybackContext {
                position_sec: Some(position_sec),
                percent: Some(milestone),
                device_name: None,
            }),
        );
        let _ = dispatch(&state.db_pool, &event).await;
    }

    Ok(())
}

/// Playback finished. Called from the auto-complete path and from the
/// natural-completion heuristic; the dedup key makes double calls harmless.
pub async fn notify_finish(
    state: &AppState,
    user_id: i32,
    episode_id: i32,
    is_youtube: bool,
) -> AppResult<()> {
    if !playback_enabled(state, user_id).await {
        return Ok(());
    }

    let key = finish_key(user_id, episode_id, is_youtube);
    match state.redis_client.set_nx(&key, 1, FINISH_TTL_SECS).await {
        Ok(true) => {}
        Ok(false) => return Ok(()),
        Err(e) => {
            tracing::debug!("Playback finish notification skipped (redis): {}", e);
            return Ok(());
        }
    }

    let Some(info) = episode_info(state, episode_id, is_youtube).await else {
        return Ok(());
    };
    let (title, message) = playback_finish_message(&info);
    let event = playback_event(
        user_id,
        EVENT_PLAYBACK_FINISHED,
        &title,
        &message,
        &info,
        Some(PlaybackContext {
            position_sec: None,
            percent: Some(100),
            device_name: None,
        }),
    );
    let _ = dispatch(&state.db_pool, &event).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_keys_are_media_scoped() {
        assert_eq!(start_key(1, 2, false), "notif:playback:start:1:ep:2");
        assert_eq!(start_key(1, 2, true), "notif:playback:start:1:yt:2");
        assert_eq!(
            milestone_key(1, 2, false, 50),
            "notif:playback:milestone:1:ep:2:50"
        );
        assert_eq!(finish_key(9, 8, true), "notif:playback:finish:9:yt:8");
    }
}
