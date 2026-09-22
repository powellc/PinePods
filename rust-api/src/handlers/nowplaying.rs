use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::HeaderMap,
    response::{Json, Response},
};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};

use crate::{
    error::AppError,
    handlers::{check_user_access, extract_api_key},
    models::NowPlayingSnapshot,
    AppState,
};

// ============================ HTTP: list devices ============================

#[derive(Deserialize, utoipa::IntoParams)]
pub struct NowPlayingDevicesQuery {
    pub user_id: i32,
}

/// List the user's active devices and what each is currently playing. Backed by
/// ephemeral Valkey snapshots with a TTL, so a device that stops reporting drops
/// off automatically. Powers the "see / control my other devices" UI for clients
/// that aren't holding a live socket.
#[utoipa::path(
    get,
    path = "/now_playing_devices",
    tag = "nowplaying",
    params(NowPlayingDevicesQuery),
    security(("api_key" = [])),
    responses(
        (status = 200, description = "Active devices and their now-playing state", body = crate::models::NowPlayingDevicesResponse),
        (status = 401, description = "Invalid or missing API key"),
        (status = 403, description = "Not authorized for this user"),
    ),
)]
pub async fn now_playing_devices(
    Query(query): Query<NowPlayingDevicesQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<crate::models::NowPlayingDevicesResponse>, AppError> {
    let api_key = extract_api_key(&headers)?;

    if !state.db_pool.verify_api_key(&api_key).await? {
        return Err(AppError::unauthorized("Invalid API key"));
    }
    if !check_user_access(&state, &api_key, query.user_id).await? {
        return Err(AppError::forbidden("You can only view your own devices!"));
    }

    let devices = state
        .redis_client
        .get_now_playing_devices(query.user_id)
        .await?;

    Ok(Json(crate::models::NowPlayingDevicesResponse { devices }))
}

// ============================ Connection registry ============================

type DeviceSenders = HashMap<String, mpsc::UnboundedSender<Message>>;

/// In-process registry of live now-playing sockets, keyed by `(user_id, device_id)`
/// so the server can address or exclude an individual device (unlike the task/refresh
/// sockets, which are keyed by user only). Cross-instance fan-out is added later via
/// Valkey pub/sub; within one instance this is the source of routing.
#[derive(Default)]
pub struct NowPlayingManager {
    connections: RwLock<HashMap<i32, DeviceSenders>>,
}

impl NowPlayingManager {
    pub fn new() -> Self {
        Self::default()
    }

    async fn add(&self, user_id: i32, device_id: String, tx: mpsc::UnboundedSender<Message>) {
        self.connections
            .write()
            .await
            .entry(user_id)
            .or_default()
            .insert(device_id, tx);
    }

    async fn remove(&self, user_id: i32, device_id: &str) {
        let mut conns = self.connections.write().await;
        if let Some(devs) = conns.get_mut(&user_id) {
            devs.remove(device_id);
            if devs.is_empty() {
                conns.remove(&user_id);
            }
        }
    }

    /// Relay a message to one specific device. Returns true if a live connection
    /// accepted it (the receiver still applies it locally — delivery is not
    /// execution). Non-blocking: an unbounded send never awaits.
    async fn send_to_device(&self, user_id: i32, device_id: &str, msg: Message) -> bool {
        let conns = self.connections.read().await;
        conns
            .get(&user_id)
            .and_then(|devs| devs.get(device_id))
            .map(|tx| tx.send(msg).is_ok())
            .unwrap_or(false)
    }

    /// Send a message to all of a user's connected devices except the origin.
    async fn broadcast_user_except(&self, user_id: i32, except_device: &str, msg: Message) {
        let conns = self.connections.read().await;
        if let Some(devs) = conns.get(&user_id) {
            for (device_id, tx) in devs.iter() {
                if device_id != except_device {
                    let _ = tx.send(msg.clone());
                }
            }
        }
    }
}

// ============================ Wire protocol ============================

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    /// This device's current playback state (sent on the client's playback tick).
    Report {
        episode_id: i32,
        #[serde(default)]
        is_youtube: bool,
        #[serde(default)]
        title: String,
        #[serde(default)]
        artwork_url: String,
        #[serde(default)]
        position_sec: f64,
        #[serde(default)]
        duration_sec: f64,
        #[serde(default)]
        playing: bool,
        #[serde(default)]
        speed: f64,
    },
    /// Keep the snapshot TTL alive without a state change (e.g. while paused).
    Heartbeat,
    /// Advisory remote-control command aimed at another of the user's devices.
    Command {
        target_device_id: String,
        action: String,
        #[serde(default)]
        args: Value,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMsg {
    /// The user's current set of active devices and their now-playing state.
    Devices { devices: Vec<NowPlayingSnapshot> },
    /// A command relayed from another device, to be applied on the local player.
    Command {
        from_device_id: String,
        action: String,
        args: Value,
    },
    /// Result of a command the client sent (relayed vs. target offline).
    Ack { ok: bool, detail: String },
}

fn to_ws(msg: &ServerMsg) -> Message {
    Message::Text(
        serde_json::to_string(msg)
            .unwrap_or_else(|_| "{}".to_string())
            .into(),
    )
}

fn forbidden(detail: &'static str) -> Response {
    axum::response::Response::builder()
        .status(403)
        .body(detail.into())
        .unwrap()
}

// ============================ WebSocket: now playing ============================

#[derive(Deserialize)]
pub struct NowPlayingWsQuery {
    api_key: String,
    device_id: String,
    #[serde(default)]
    device_name: String,
    #[serde(default)]
    device_type: String,
}

/// Full-duplex now-playing socket: `/ws/api/nowplaying/{user_id}`. Carries device
/// reports (client→server) and device-list updates + relayed commands (server→client).
/// Authenticated by `?api_key=` (browsers can't set WS headers), owner-or-system-user.
pub async fn now_playing_websocket(
    ws: WebSocketUpgrade,
    Path(user_id): Path<i32>,
    Query(query): Query<NowPlayingWsQuery>,
    State(state): State<AppState>,
) -> Response {
    match state.db_pool.verify_api_key(&query.api_key).await {
        Ok(true) => match state.db_pool.get_user_id_from_api_key(&query.api_key).await {
            Ok(key_user_id) if key_user_id == user_id || key_user_id == 1 => {
                ws.on_upgrade(move |socket| handle_now_playing_socket(socket, user_id, query, state))
            }
            Ok(_) => forbidden("API key does not belong to requested user"),
            Err(_) => forbidden("Invalid API key"),
        },
        Ok(false) | Err(_) => forbidden("Invalid API key"),
    }
}

async fn handle_now_playing_socket(
    socket: WebSocket,
    user_id: i32,
    query: NowPlayingWsQuery,
    state: AppState,
) {
    let device_id = query.device_id.clone();
    let device_name = query.device_name.clone();
    let device_type = query.device_type.clone();

    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    state
        .now_playing_manager
        .add(user_id, device_id.clone(), tx.clone())
        .await;

    // Send the current device list immediately on connect.
    let devices = state
        .redis_client
        .get_now_playing_devices(user_id)
        .await
        .unwrap_or_default();
    let _ = tx.send(to_ws(&ServerMsg::Devices { devices }));

    // Writer: drain queued messages to the socket.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Reader: handle inbound client messages.
    let reader_state = state.clone();
    let reader_device = device_id.clone();
    let reader_tx = tx.clone();
    let reader = tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            match msg {
                Message::Text(text) => {
                    let parsed: Result<ClientMsg, _> = serde_json::from_str(text.as_str());
                    match parsed {
                        Ok(ClientMsg::Report {
                            episode_id,
                            is_youtube,
                            title,
                            artwork_url,
                            position_sec,
                            duration_sec,
                            playing,
                            speed,
                        }) => {
                            let snapshot = NowPlayingSnapshot {
                                device_id: reader_device.clone(),
                                device_name: device_name.clone(),
                                device_type: device_type.clone(),
                                episode_id,
                                is_youtube,
                                title,
                                artwork_url,
                                position_sec,
                                duration_sec,
                                playing,
                                speed,
                                updated_at: 0, // stamped by upsert
                            };
                            let _ = reader_state
                                .redis_client
                                .upsert_now_playing(user_id, snapshot)
                                .await;
                            notify_devices_changed(&reader_state, user_id, &reader_device).await;

                            // Timely playback-start push. Deduped in Redis, so the
                            // 15s report tick can't re-fire it; spawned so a slow
                            // notification server never blocks the socket reader.
                            if playing {
                                let notif_state = reader_state.clone();
                                let device = device_name.clone();
                                tokio::spawn(async move {
                                    if let Err(e) =
                                        crate::services::playback_notifications::notify_start(
                                            &notif_state,
                                            user_id,
                                            episode_id,
                                            is_youtube,
                                            Some(&device),
                                            Some(position_sec),
                                        )
                                        .await
                                    {
                                        tracing::warn!(
                                            "Playback start notification failed: {e}"
                                        );
                                    }
                                });
                            }
                        }
                        Ok(ClientMsg::Heartbeat) => {
                            let _ = reader_state
                                .redis_client
                                .touch_now_playing(user_id, &reader_device)
                                .await;
                        }
                        Ok(ClientMsg::Command {
                            target_device_id,
                            action,
                            args,
                        }) => {
                            let relay = to_ws(&ServerMsg::Command {
                                from_device_id: reader_device.clone(),
                                action: action.clone(),
                                args: args.clone(),
                            });
                            let delivered = reader_state
                                .now_playing_manager
                                .send_to_device(user_id, &target_device_id, relay)
                                .await;
                            let ack = if delivered {
                                ServerMsg::Ack {
                                    ok: true,
                                    detail: "relayed".to_string(),
                                }
                            } else {
                                // Target isn't on this instance — fan the command out
                                // over pub/sub so whichever replica holds it delivers.
                                let other_instance = publish_command(
                                    &reader_state,
                                    user_id,
                                    &target_device_id,
                                    &reader_device,
                                    &action,
                                    &args,
                                )
                                .await;
                                ServerMsg::Ack {
                                    ok: other_instance,
                                    detail: if other_instance {
                                        "relayed to another instance".to_string()
                                    } else {
                                        "device offline".to_string()
                                    },
                                }
                            };
                            let _ = reader_tx.send(to_ws(&ack));
                        }
                        Err(_) => { /* ignore malformed frames */ }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    // When either half ends, tear down and let the other devices know.
    tokio::select! {
        _ = writer => {},
        _ = reader => {},
    }

    state.now_playing_manager.remove(user_id, &device_id).await;
    let _ = state
        .redis_client
        .remove_now_playing(user_id, &device_id)
        .await;
    notify_devices_changed(&state, user_id, &device_id).await;
}

/// Push the freshly-computed device list to all of the user's *other* connected
/// devices on THIS instance.
async fn broadcast_devices(state: &AppState, user_id: i32, origin_device: &str) {
    let devices = state
        .redis_client
        .get_now_playing_devices(user_id)
        .await
        .unwrap_or_default();
    state
        .now_playing_manager
        .broadcast_user_except(user_id, origin_device, to_ws(&ServerMsg::Devices { devices }))
        .await;
}

// ======================= Multi-replica pub/sub bridge =======================
//
// The connection registry is in-process, so with more than one API replica a
// report/command handled on replica A must reach the sockets on replica B. We
// bridge those two events over a single Valkey channel; snapshots already live in
// shared Valkey, so device lists are correct cross-instance without any sync.
// Everything degrades cleanly to in-process fan-out when pub/sub is unavailable
// (the default single-container deploy never needs the bridge).

const NOW_PLAYING_CHANNEL: &str = "nowplaying:events";

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NpBridgeMsg {
    /// A user's device list changed (report/disconnect) — other replicas should
    /// re-broadcast the fresh list to their local sockets for that user.
    DevicesChanged {
        user_id: i32,
        origin_device: String,
        instance_id: String,
    },
    /// A command whose target device wasn't connected to the origin instance —
    /// whichever replica holds the target delivers it.
    Command {
        user_id: i32,
        target_device_id: String,
        from_device_id: String,
        action: String,
        args: Value,
        instance_id: String,
    },
}

impl NpBridgeMsg {
    fn instance_id(&self) -> &str {
        match self {
            NpBridgeMsg::DevicesChanged { instance_id, .. } => instance_id,
            NpBridgeMsg::Command { instance_id, .. } => instance_id,
        }
    }
}

/// Publish a bridge message; returns the number of subscribers that received it
/// (0 on serialize/publish failure). Our own bridge subscription counts as one.
async fn publish_bridge(state: &AppState, msg: &NpBridgeMsg) -> i64 {
    match serde_json::to_string(msg) {
        Ok(payload) => state
            .redis_client
            .publish(NOW_PLAYING_CHANNEL, &payload)
            .await
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Broadcast the fresh device list to this instance's other sockets AND fan the
/// change out to the other replicas so their sockets update too.
async fn notify_devices_changed(state: &AppState, user_id: i32, origin_device: &str) {
    broadcast_devices(state, user_id, origin_device).await;
    let msg = NpBridgeMsg::DevicesChanged {
        user_id,
        origin_device: origin_device.to_string(),
        instance_id: state.instance_id.to_string(),
    };
    let _ = publish_bridge(state, &msg).await;
}

/// Fan a command out to the other replicas (used when the target device isn't on
/// this instance). Returns true if at least one *other* instance is subscribed
/// (so the caller can ack "relayed" rather than "device offline").
async fn publish_command(
    state: &AppState,
    user_id: i32,
    target_device_id: &str,
    from_device_id: &str,
    action: &str,
    args: &Value,
) -> bool {
    let msg = NpBridgeMsg::Command {
        user_id,
        target_device_id: target_device_id.to_string(),
        from_device_id: from_device_id.to_string(),
        action: action.to_string(),
        args: args.clone(),
        instance_id: state.instance_id.to_string(),
    };
    // Subscriber count includes our own bridge subscription, so >1 means another
    // replica is listening and could hold the target.
    publish_bridge(state, &msg).await > 1
}

/// Start the Valkey pub/sub subscriber that relays now-playing events from other
/// API replicas onto this instance's local sockets. Best-effort: on any failure
/// it logs and retries; the in-process fan-out keeps working meanwhile.
pub fn spawn_nowplaying_bridge(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = run_nowplaying_bridge(&state).await {
                tracing::warn!("now-playing pub/sub bridge stopped ({e}); retrying in 5s");
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

async fn run_nowplaying_bridge(state: &AppState) -> crate::error::AppResult<()> {
    let mut pubsub = state.redis_client.get_pubsub().await?;
    pubsub.subscribe(NOW_PLAYING_CHANNEL).await?;
    tracing::info!("now-playing pub/sub bridge subscribed to {NOW_PLAYING_CHANNEL}");

    let mut messages = pubsub.on_message();
    while let Some(msg) = messages.next().await {
        let payload: String = match msg.get_payload() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let bridge: NpBridgeMsg = match serde_json::from_str(&payload) {
            Ok(b) => b,
            Err(_) => continue,
        };
        // Ignore what we published ourselves — already handled locally.
        if bridge.instance_id() == state.instance_id.as_ref() {
            continue;
        }
        match bridge {
            NpBridgeMsg::DevicesChanged {
                user_id,
                origin_device,
                ..
            } => {
                broadcast_devices(state, user_id, &origin_device).await;
            }
            NpBridgeMsg::Command {
                user_id,
                target_device_id,
                from_device_id,
                action,
                args,
                ..
            } => {
                let relay = to_ws(&ServerMsg::Command {
                    from_device_id,
                    action,
                    args,
                });
                let _ = state
                    .now_playing_manager
                    .send_to_device(user_id, &target_device_id, relay)
                    .await;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txt(s: &str) -> Message {
        Message::Text(s.to_string().into())
    }

    #[tokio::test]
    async fn registry_targets_and_excludes_devices() {
        let mgr = NowPlayingManager::new();
        let (tx_a, mut rx_a) = mpsc::unbounded_channel::<Message>();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel::<Message>();
        mgr.add(1, "A".to_string(), tx_a).await;
        mgr.add(1, "B".to_string(), tx_b).await;

        // Command aimed at B reaches only B.
        assert!(mgr.send_to_device(1, "B", txt("cmd")).await);
        assert!(rx_b.try_recv().is_ok());
        assert!(rx_a.try_recv().is_err());

        // Unknown target is reported as undelivered.
        assert!(!mgr.send_to_device(1, "ghost", txt("cmd")).await);

        // Broadcast excludes the origin device.
        mgr.broadcast_user_except(1, "A", txt("devices")).await;
        assert!(rx_b.try_recv().is_ok());
        assert!(rx_a.try_recv().is_err());

        // A different user's device never receives another user's traffic.
        let (tx_c, mut rx_c) = mpsc::unbounded_channel::<Message>();
        mgr.add(2, "C".to_string(), tx_c).await;
        mgr.broadcast_user_except(1, "A", txt("devices")).await;
        assert!(rx_c.try_recv().is_err());

        // After removal the device is no longer addressable.
        mgr.remove(1, "B").await;
        assert!(!mgr.send_to_device(1, "B", txt("cmd")).await);
    }
}
