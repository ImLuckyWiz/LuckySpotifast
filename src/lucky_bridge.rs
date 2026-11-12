//! lucky_bridge.rs - Servidor HTTP REST y WebSocket integrado para control remoto y streaming.
//!
//! 1. Servidor de control LAN (por defecto en `0.0.0.0:8766` o `SPOTIFAST_LISTEN_ADDR` / `SPOTIFAST_PORT`):
//!    Permite a agentes y servicios en la red local (ej. Hermes) consultar estado, letras,
//!    cola, buscar y reproducir música, saltar pistas, ajustar volumen, etc. tanto por HTTP
//!    REST como por WebSocket bidireccional con eventos en tiempo real.
//!
//! 2. Cliente WebSocket saliente:
//!    Se conecta automáticamente en segundo plano a `ws://127.0.0.1:8765/spotifast`
//!    (o `SPOTIFAST_WS_URL` / `SPOTIFAST_HUB_URL`) para sincronizar con herramientas de streaming.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio_tungstenite::tungstenite::Message;

/// Representación simplificada de una canción para el bridge y la API REST/WS.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SimplifiedTrack {
    pub uri: String,
    pub name: String,
    pub artists: String,
    pub cover: String,
    pub duration_ms: u32,
    pub explicit: bool,
}

impl From<&crate::api::models::Track> for SimplifiedTrack {
    fn from(t: &crate::api::models::Track) -> Self {
        let cover = t
            .album
            .as_ref()
            .and_then(|a| a.images.get(1).or_else(|| a.images.first()))
            .map(|img| img.url.clone())
            .unwrap_or_default();
        Self {
            uri: t.uri.clone(),
            name: t.name.clone(),
            artists: t.artist_names(),
            cover,
            duration_ms: t.duration_ms,
            explicit: t.explicit,
        }
    }
}

/// Contenido individual de una línea de letra formateada para el hub.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct HubLyricsContent {
    pub text: String,
    pub start_time: f64,
    pub end_time: f64,
}

/// Payload completo de letras que espera el bridge WebSocket / overlay de letras.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct HubLyricsPayload {
    pub r#type: String,
    pub content: Vec<HubLyricsContent>,
}

/// Convierte la estructura de letras de Spotifast al formato Line del bridge WebSocket.
pub fn format_lyrics_for_hub(
    lyrics: &crate::lyrics::Lyrics,
    duration_ms: u32,
) -> Option<HubLyricsPayload> {
    if !lyrics.synced || lyrics.lines.is_empty() {
        return None;
    }
    let mut content = Vec::new();
    for (i, line) in lyrics.lines.iter().enumerate() {
        let Some(at_ms) = line.at_ms else { continue };
        let start_time = at_ms as f64 / 1000.0;
        let end_time = lyrics
            .lines
            .get(i + 1)
            .and_then(|next| next.at_ms)
            .map(|next_ms| next_ms as f64 / 1000.0)
            .unwrap_or_else(|| (duration_ms as f64 / 1000.0).max(start_time + 5.0));

        content.push(HubLyricsContent {
            text: line.text.clone(),
            start_time,
            end_time,
        });
    }
    if content.is_empty() {
        return None;
    }
    Some(HubLyricsPayload {
        r#type: "Line".to_string(),
        content,
    })
}

/// Petición de catálogo resuelta por el Worker del backend de Spotifast.
pub enum CatalogRequest {
    TrackByUri {
        id: String,
        reply: oneshot::Sender<Result<SimplifiedTrack, String>>,
    },
    SearchTracks {
        query: String,
        limit: Option<usize>,
        reply: oneshot::Sender<Result<Vec<SimplifiedTrack>, String>>,
    },
    ArtistById {
        id: String,
        reply: oneshot::Sender<Result<(String, String), String>>,
    },
}

/// Comando de UI/reproductor procesado en el frame principal de Spotifast.
pub enum UiCommand {
    Enqueue {
        uri: String,
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Dequeue {
        uri: String,
        reply: oneshot::Sender<Result<Value, String>>,
    },
    QueueUris {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Skip {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Previous {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Play {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    PlayUri {
        uri: String,
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Pause {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    TogglePlay {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Seek {
        position_ms: u32,
        reply: oneshot::Sender<Result<Value, String>>,
    },
    GetVolume {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    SetVolume {
        volume: f64,
        reply: oneshot::Sender<Result<Value, String>>,
    },
    ClearQueue {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    GetNowPlaying {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    GetQueue {
        reply: oneshot::Sender<Result<Value, String>>,
    },
}

/// Eventos salientes desde Spotifast hacia los clientes WebSocket.
#[derive(Clone, Debug)]
pub enum BridgeEvent {
    Track(Value),
    Progress(Value),
    Lyrics(Value),
    Log(String),
}

/// Estado en memoria compartido para respuestas rápidas de la API.
#[derive(Default, Clone, Debug)]
pub struct BridgeState {
    pub last_track: Option<Value>,
    pub last_progress: Option<Value>,
    pub last_lyrics: Option<Value>,
}

/// Handle mantenido por App para interactuar con la tarea de WebSocket en segundo plano.
pub struct AppBridgeHandle {
    ui_cmd_rx: std_mpsc::Receiver<UiCommand>,
    event_tx: mpsc::UnboundedSender<BridgeEvent>,
    connect_generation: Arc<AtomicU64>,
}

impl AppBridgeHandle {
    /// Drena el siguiente comando de UI pendiente (no bloqueante).
    pub fn try_recv_ui_cmd(&self) -> Option<UiCommand> {
        self.ui_cmd_rx.try_recv().ok()
    }

    /// Drena todos los comandos de UI pendientes.
    pub fn drain_ui_commands(&self) -> Vec<UiCommand> {
        let mut cmds = Vec::new();
        while let Ok(cmd) = self.ui_cmd_rx.try_recv() {
            cmds.push(cmd);
        }
        cmds
    }

    /// Envía un evento hacia el hub y los clientes conectados.
    pub fn send_event(&self, event: BridgeEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Retorna un clon del sender de eventos para enviar eventos sin retener un préstamo de AppBridgeHandle.
    pub fn event_sender(&self) -> mpsc::UnboundedSender<BridgeEvent> {
        self.event_tx.clone()
    }

    /// Retorna el contador de conexiones exitosas para detectar reconexiones.
    pub fn connect_generation(&self) -> u64 {
        self.connect_generation.load(Ordering::Relaxed)
    }
}

// ---------- Algoritmo de normalización y ranking de canciones ----------

/// Detecta links y URIs de Spotify en el texto.
pub fn extract_uri(text: &str) -> Option<String> {
    if let Some(pos) = text.find("open.spotify.com/") {
        let rest = &text[pos..];
        if let Some(track_pos) = rest.find("/track/") {
            let after_track = &rest[track_pos + 7..];
            let id: String = after_track
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();
            if !id.is_empty() {
                return Some(format!("spotify:track:{id}"));
            }
        }
    }
    if let Some(pos) = text.find("spotify:track:") {
        let after_prefix = &text[pos + 14..];
        let id: String = after_prefix
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        if !id.is_empty() {
            return Some(format!("spotify:track:{id}"));
        }
    }
    None
}

/// Normaliza texto a minúsculas, elimina diacríticos comunes, apóstrofes y signos.
pub fn norm(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c.to_ascii_lowercase() {
            'á' | 'à' | 'ä' | 'â' | 'ã' | 'å' => out.push('a'),
            'é' | 'è' | 'ë' | 'ê' => out.push('e'),
            'í' | 'ì' | 'ï' | 'î' => out.push('i'),
            'ó' | 'ò' | 'ö' | 'ô' | 'õ' => out.push('o'),
            'ú' | 'ù' | 'ü' | 'û' => out.push('u'),
            'ñ' => out.push('n'),
            'ç' => out.push('c'),
            '\'' | '’' | '`' => {}
            c if c.is_ascii_alphanumeric() => out.push(c),
            _ => out.push(' '),
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn words_of(q: &str) -> Vec<String> {
    norm(q)
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

pub fn matched(t: &SimplifiedTrack, words: &[String]) -> usize {
    let combined = format!("{} {}", t.name, t.artists);
    let have: HashSet<String> = words_of(&combined).into_iter().collect();
    words.iter().filter(|w| have.contains(*w)).count()
}

pub fn best_match(list: &[SimplifiedTrack], query: &str) -> Option<SimplifiedTrack> {
    if list.is_empty() {
        return None;
    }
    let words = words_of(query);
    let mut best_idx = 0;
    let mut best_score = f64::NEG_INFINITY;
    for (i, t) in list.iter().enumerate() {
        let score = (matched(t, &words) as f64) - (i as f64 * 0.01);
        if score > best_score {
            best_score = score;
            best_idx = i;
        }
    }
    Some(list[best_idx].clone())
}

pub fn rank_list(pool: Vec<SimplifiedTrack>, query: &str, limit: usize) -> Vec<SimplifiedTrack> {
    let words = words_of(query);
    let mut scored: Vec<(SimplifiedTrack, f64)> = pool
        .into_iter()
        .enumerate()
        .map(|(i, t)| {
            let score = (matched(&t, &words) as f64) - (i as f64 * 0.01);
            (t, score)
        })
        .collect();

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (t, _) in scored {
        let k = format!("{}|{}", norm(&t.name), norm(&t.artists));
        if seen.contains(&k) {
            continue;
        }
        seen.insert(k);
        out.push(t);
        if out.len() >= limit {
            break;
        }
    }
    out
}

fn dedupe_into(pool: &mut Vec<SimplifiedTrack>, extra: Vec<SimplifiedTrack>) {
    let mut seen: HashSet<String> = pool.iter().map(|t| t.uri.clone()).collect();
    for item in extra {
        if !seen.contains(&item.uri) {
            seen.insert(item.uri.clone());
            pool.push(item);
        }
    }
}

/// Ejecuta la búsqueda de catálogo con pool extendido si la cobertura es baja.
pub async fn execute_catalog_search(
    api: Arc<crate::api::gateway::ApiGateway>,
    query: String,
    limit: Option<usize>,
) -> Result<Vec<SimplifiedTrack>, String> {
    let client = api
        .client_for(crate::api::gateway::Operation::CatalogSearch)
        .await
        .map_err(|e| format!("Error de cliente de API: {e}"))?;

    let search_res = client
        .search(&query, &["track"])
        .await
        .map_err(|e| format!("Fallo en búsqueda: {e}"))?;

    let items = search_res
        .tracks
        .map(|page| page.items)
        .unwrap_or_default();

    let mut pool: Vec<SimplifiedTrack> = items.iter().map(SimplifiedTrack::from).collect();

    let words = words_of(&query);
    let cover = pool.iter().map(|t| matched(t, &words)).max().unwrap_or(0);

    if !pool.is_empty() && words.len() >= 3 && (cover as f64) < (words.len() as f64 * 0.75) {
        let mut artist_words = HashSet::new();
        for t in pool.iter().take(5) {
            for w in words_of(&t.artists) {
                artist_words.insert(w);
            }
        }
        let reduced: Vec<String> = words
            .iter()
            .filter(|w| !artist_words.contains(*w))
            .cloned()
            .collect();

        if reduced.len() >= 2 && reduced.len() < words.len() {
            let q2 = reduced.join(" ");
            log::info!("[lucky_bridge] Búsqueda reducida alternativa: \"{q2}\"");
            if let Ok(search2) = client.search(&q2, &["track"]).await {
                let items2 = search2.tracks.map(|p| p.items).unwrap_or_default();
                let extra: Vec<SimplifiedTrack> = items2.iter().map(SimplifiedTrack::from).collect();
                dedupe_into(&mut pool, extra);
            }
        }
    }

    match limit {
        Some(lim) => Ok(rank_list(pool, &query, lim)),
        None => {
            if let Some(best) = best_match(&pool, &query) {
                Ok(vec![best])
            } else {
                Ok(Vec::new())
            }
        }
    }
}

// ---------- Inicialización del Bridge ----------

/// Inicia las tareas del bridge dentro del runtime Tokio dedicado.
pub fn start(
    waker: fastframe_shell::Waker,
    backend_sender: mpsc::UnboundedSender<crate::backend::Command>,
) -> AppBridgeHandle {
    let (ui_cmd_tx, ui_cmd_rx) = std_mpsc::channel::<UiCommand>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<BridgeEvent>();
    let connect_generation = Arc::new(AtomicU64::new(0));

    let gen_clone = Arc::clone(&connect_generation);
    let state = Arc::new(RwLock::new(BridgeState::default()));
    let (server_broadcast_tx, _) = broadcast::channel::<String>(128);
    let (hub_event_tx, hub_event_rx) = mpsc::unbounded_channel::<BridgeEvent>();

    let waker_clone = waker.clone();
    let backend_clone = backend_sender.clone();
    let ui_cmd_clone = ui_cmd_tx.clone();
    let state_clone = Arc::clone(&state);
    let bcast_clone = server_broadcast_tx.clone();

    if let Err(err) = std::thread::Builder::new()
        .name("spotifast-bridge".to_string())
        .spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => {
                    // Tarea 1: Enrutador de eventos y caché en memoria
                    let state_disp = Arc::clone(&state_clone);
                    let bcast_disp = bcast_clone.clone();
                    rt.spawn(async move {
                        while let Some(event) = event_rx.recv().await {
                            let (json_str, is_track, is_prog, is_lyr) = match &event {
                                BridgeEvent::Track(data) => (
                                    serde_json::json!({ "event": "track", "data": data }).to_string(),
                                    Some(data.clone()),
                                    None,
                                    None,
                                ),
                                BridgeEvent::Progress(data) => (
                                    serde_json::json!({ "event": "progress", "data": data }).to_string(),
                                    None,
                                    Some(data.clone()),
                                    None,
                                ),
                                BridgeEvent::Lyrics(data) => (
                                    serde_json::json!({ "event": "lyrics", "data": data }).to_string(),
                                    None,
                                    None,
                                    Some(data.clone()),
                                ),
                                BridgeEvent::Log(data) => (
                                    serde_json::json!({ "event": "log", "data": data }).to_string(),
                                    None,
                                    None,
                                    None,
                                ),
                            };

                            // Actualiza estado en memoria
                            if is_track.is_some() || is_prog.is_some() || is_lyr.is_some() {
                                let mut s = state_disp.write().await;
                                if let Some(t) = is_track {
                                    s.last_track = Some(t);
                                }
                                if let Some(p) = is_prog {
                                    s.last_progress = Some(p);
                                }
                                if let Some(l) = is_lyr {
                                    s.last_lyrics = Some(l);
                                }
                            }

                            // Transmite a clientes conectados al servidor LAN
                            let _ = bcast_disp.send(json_str);

                            // Reenvía al cliente saliente de streaming
                            let _ = hub_event_tx.send(event);
                        }
                    });

                    // Tarea 2: Servidor LAN HTTP REST + WebSocket
                    let srv_backend = backend_clone.clone();
                    let srv_uicmd = ui_cmd_clone.clone();
                    let srv_waker = waker_clone.clone();
                    let srv_state = Arc::clone(&state_clone);
                    let srv_bcast = bcast_clone.clone();
                    rt.spawn(async move {
                        run_server(srv_state, srv_bcast, srv_backend, srv_uicmd, srv_waker).await;
                    });

                    // Tarea 3: Cliente saliente de streaming
                    rt.block_on(run_client(
                        waker_clone,
                        backend_clone,
                        ui_cmd_clone,
                        hub_event_rx,
                        gen_clone,
                    ));
                }
                Err(err) => {
                    log::error!("Failed to create Tokio runtime for bridge: {err}");
                }
            }
        })
    {
        log::error!("Failed to spawn bridge thread: {err}");
    }

    AppBridgeHandle {
        ui_cmd_rx,
        event_tx,
        connect_generation,
    }
}

// ---------- Servidor LAN HTTP REST + WebSocket ----------

async fn run_server(
    state: Arc<RwLock<BridgeState>>,
    broadcast_tx: broadcast::Sender<String>,
    backend_sender: mpsc::UnboundedSender<crate::backend::Command>,
    ui_cmd_tx: std_mpsc::Sender<UiCommand>,
    waker: fastframe_shell::Waker,
) {
    let listen_addr = std::env::var("SPOTIFAST_LISTEN_ADDR").unwrap_or_else(|_| {
        let port = std::env::var("SPOTIFAST_PORT").unwrap_or_else(|_| "8766".to_string());
        format!("0.0.0.0:{port}")
    });

    let listener = match tokio::net::TcpListener::bind(&listen_addr).await {
        Ok(l) => {
            log::info!("[lucky_bridge] Servidor HTTP/WS iniciado en http://{listen_addr}");
            l
        }
        Err(e) => {
            log::warn!("[lucky_bridge] No se pudo iniciar el servidor en {listen_addr}: {e}");
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let s_state = Arc::clone(&state);
                let s_bcast = broadcast_tx.clone();
                let s_backend = backend_sender.clone();
                let s_uicmd = ui_cmd_tx.clone();
                let s_waker = waker.clone();
                tokio::spawn(async move {
                    handle_incoming_connection(stream, s_bcast, s_state, s_backend, s_uicmd, s_waker).await;
                });
            }
            Err(e) => {
                log::debug!("[lucky_bridge] Error aceptando conexion: {e}");
            }
        }
    }
}

async fn handle_incoming_connection(
    stream: tokio::net::TcpStream,
    broadcast_tx: broadcast::Sender<String>,
    state: Arc<RwLock<BridgeState>>,
    backend_sender: mpsc::UnboundedSender<crate::backend::Command>,
    ui_cmd_tx: std_mpsc::Sender<UiCommand>,
    waker: fastframe_shell::Waker,
) {
    let mut buf = [0u8; 1024];
    let n = match stream.peek(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let peeked = String::from_utf8_lossy(&buf[..n]);

    if peeked.to_ascii_lowercase().contains("upgrade: websocket") {
        match tokio_tungstenite::accept_async(stream).await {
            Ok(ws_stream) => {
                handle_server_websocket(ws_stream, broadcast_tx, backend_sender, ui_cmd_tx, waker).await;
            }
            Err(e) => {
                log::debug!("[lucky_bridge] Error en handshake WebSocket: {e}");
            }
        }
    } else {
        handle_http_connection(stream, state, backend_sender, ui_cmd_tx, waker).await;
    }
}

async fn handle_server_websocket(
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    broadcast_tx: broadcast::Sender<String>,
    backend_sender: mpsc::UnboundedSender<crate::backend::Command>,
    ui_cmd_tx: std_mpsc::Sender<UiCommand>,
    waker: fastframe_shell::Waker,
) {
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let mut bcast_rx = broadcast_tx.subscribe();

    loop {
        tokio::select! {
            res = bcast_rx.recv() => {
                match res {
                    Ok(msg) => {
                        if ws_write.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
            msg_opt = ws_read.next() => {
                let Some(msg_res) = msg_opt else { break };
                match msg_res {
                    Ok(Message::Text(text)) => {
                        handle_rpc_message(&text, &mut ws_write, &backend_sender, &ui_cmd_tx, &waker).await;
                    }
                    Ok(Message::Ping(_)) => {
                        let _ = ws_write.flush().await;
                    }
                    Ok(Message::Close(_)) => break,
                    Err(_) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn handle_http_connection(
    mut stream: tokio::net::TcpStream,
    state: Arc<RwLock<BridgeState>>,
    backend_sender: mpsc::UnboundedSender<crate::backend::Command>,
    ui_cmd_tx: std_mpsc::Sender<UiCommand>,
    waker: fastframe_shell::Waker,
) {
    let Some((method, full_path, _headers, body_json)) = read_http_request(&mut stream).await else {
        return;
    };

    if method == "OPTIONS" {
        send_http_cors_preflight(&mut stream).await;
        return;
    }

    let (path, query_str) = split_path_query(&full_path);

    match (method.as_str(), path) {
        ("GET", "/") | ("GET", "/api") | ("GET", "/api/help") | ("GET", "/api/status") => {
            let info = serde_json::json!({
                "app": "Spotifast",
                "status": "running",
                "endpoints": [
                    "GET    /api/now",
                    "GET    /api/lyrics",
                    "GET    /api/queue",
                    "GET    /api/queue/uris",
                    "GET    /api/search?q=...&limit=...",
                    "POST   /api/play (json: { uri?: string, query?: string })",
                    "POST   /api/pause",
                    "POST   /api/toggle",
                    "POST   /api/next",
                    "POST   /api/previous",
                    "POST   /api/seek (json: { positionMs: number })",
                    "GET    /api/volume",
                    "POST   /api/volume (json: { volume: 0.0-1.0 })",
                    "POST   /api/queue (json: { uri?: string, query?: string })",
                    "POST   /api/dequeue (json: { uri: string })",
                    "POST   /api/clear"
                ]
            });
            send_http_json(&mut stream, 200, "OK", &info).await;
        }

        ("GET", "/api/now") | ("GET", "/api/current") => {
            let live = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::GetNowPlaying { reply }).await;
            match live {
                Ok(val) if !val.is_null() => {
                    send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "track": val })).await;
                }
                _ => {
                    let cached = state.read().await.last_track.clone();
                    send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "track": cached })).await;
                }
            }
        }

        ("GET", "/api/lyrics") => {
            let lyrics = state.read().await.last_lyrics.clone();
            send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "lyrics": lyrics })).await;
        }

        ("GET", "/api/queue") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::GetQueue { reply }).await;
            match res {
                Ok(q) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "queue": q })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("GET", "/api/queue/uris") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::QueueUris { reply }).await;
            match res {
                Ok(uris) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "uris": uris })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("GET", "/api/search") | ("POST", "/api/search") => {
            let q_param = parse_query_param(query_str, "q")
                .or_else(|| parse_query_param(query_str, "query"));
            let q_body = body_json.as_ref().and_then(|b| b.get("query").or_else(|| b.get("q"))).and_then(|v| v.as_str());
            let query = q_param.as_deref().or(q_body).unwrap_or_default().trim().to_string();

            if query.is_empty() {
                send_http_json(&mut stream, 400, "Bad Request", &serde_json::json!({ "ok": false, "error": "Parámetro 'q' o 'query' requerido" })).await;
                return;
            }

            let limit = parse_query_param(query_str, "limit")
                .and_then(|s| s.parse::<usize>().ok())
                .or_else(|| body_json.as_ref().and_then(|b| b.get("limit")).and_then(|v| v.as_u64()).map(|n| n as usize))
                .unwrap_or(5)
                .clamp(1, 20);

            match resolve_search(&backend_sender, query, Some(limit)).await {
                Ok(tracks) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "tracks": tracks })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/play") | ("GET", "/api/play") => {
            let uri_param = parse_query_param(query_str, "uri");
            let uri_body = body_json.as_ref().and_then(|b| b.get("uri")).and_then(|v| v.as_str());
            let uri = uri_param.as_deref().or(uri_body).map(|s| s.to_string());

            let q_param = parse_query_param(query_str, "q").or_else(|| parse_query_param(query_str, "query"));
            let q_body = body_json.as_ref().and_then(|b| b.get("query").or_else(|| b.get("q"))).and_then(|v| v.as_str());
            let query = q_param.as_deref().or(q_body).map(|s| s.to_string());

            if let Some(track_uri) = uri {
                let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::PlayUri { uri: track_uri.clone(), reply }).await;
                match res {
                    Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "played": track_uri })).await,
                    Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
                }
            } else if let Some(q) = query {
                match resolve_search(&backend_sender, q, None).await {
                    Ok(tracks) if !tracks.is_empty() => {
                        let top = &tracks[0];
                        let _ = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::PlayUri { uri: top.uri.clone(), reply }).await;
                        send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "played": top })).await;
                    }
                    Ok(_) => {
                        send_http_json(&mut stream, 404, "Not Found", &serde_json::json!({ "ok": false, "error": "No se encontraron resultados" })).await;
                    }
                    Err(e) => {
                        send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await;
                    }
                }
            } else {
                let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Play { reply }).await;
                match res {
                    Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "resumed": true })).await,
                    Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
                }
            }
        }

        ("POST", "/api/pause") | ("GET", "/api/pause") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Pause { reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "paused": true })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/toggle") | ("POST", "/api/playpause") | ("GET", "/api/toggle") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::TogglePlay { reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "toggled": true })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/next") | ("POST", "/api/skip") | ("GET", "/api/next") | ("GET", "/api/skip") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Skip { reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "skipped": true })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/previous") | ("POST", "/api/prev") | ("GET", "/api/previous") | ("GET", "/api/prev") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Previous { reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "previous": true })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/seek") | ("GET", "/api/seek") => {
            let pos = parse_query_param(query_str, "ms")
                .or_else(|| parse_query_param(query_str, "positionMs"))
                .or_else(|| parse_query_param(query_str, "pos"))
                .and_then(|s| s.parse::<u32>().ok())
                .or_else(|| {
                    body_json
                        .as_ref()
                        .and_then(|b| b.get("positionMs").or_else(|| b.get("position_ms")).or_else(|| b.get("ms")))
                        .and_then(|v| v.as_u64())
                        .map(|n| n as u32)
                })
                .unwrap_or(0);

            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Seek { position_ms: pos, reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "positionMs": pos })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("GET", "/api/volume") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::GetVolume { reply }).await;
            match res {
                Ok(v) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "volume": v })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/volume") => {
            let vol = parse_query_param(query_str, "volume")
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| parse_query_param(query_str, "percent").and_then(|s| s.parse::<f64>().ok()).map(|p| p / 100.0))
                .or_else(|| {
                    body_json.as_ref().and_then(|b| {
                        b.get("volume")
                            .and_then(|v| v.as_f64())
                            .or_else(|| b.get("percent").and_then(|v| v.as_f64()).map(|p| p / 100.0))
                    })
                })
                .unwrap_or(1.0)
                .clamp(0.0, 1.0);

            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::SetVolume { volume: vol, reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "volume": vol })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/queue") => {
            let uri_param = parse_query_param(query_str, "uri");
            let uri_body = body_json.as_ref().and_then(|b| b.get("uri")).and_then(|v| v.as_str());
            let uri = uri_param.as_deref().or(uri_body).map(|s| s.to_string());

            let q_param = parse_query_param(query_str, "q").or_else(|| parse_query_param(query_str, "query"));
            let q_body = body_json.as_ref().and_then(|b| b.get("query").or_else(|| b.get("q"))).and_then(|v| v.as_str());
            let query = q_param.as_deref().or(q_body).map(|s| s.to_string());

            if let Some(track_uri) = uri {
                let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Enqueue { uri: track_uri.clone(), reply }).await;
                match res {
                    Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "queued": track_uri })).await,
                    Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
                }
            } else if let Some(q) = query {
                match resolve_search(&backend_sender, q, None).await {
                    Ok(tracks) if !tracks.is_empty() => {
                        let top = &tracks[0];
                        let _ = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Enqueue { uri: top.uri.clone(), reply }).await;
                        send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "queued": top })).await;
                    }
                    Ok(_) => {
                        send_http_json(&mut stream, 404, "Not Found", &serde_json::json!({ "ok": false, "error": "No se encontraron resultados" })).await;
                    }
                    Err(e) => {
                        send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
                    }
                }
            } else {
                send_http_json(&mut stream, 400, "Bad Request", &serde_json::json!({ "ok": false, "error": "Parámetro 'uri' o 'query' requerido" })).await;
            }
        }

        ("POST", "/api/dequeue") | ("DELETE", "/api/queue") => {
            let uri_param = parse_query_param(query_str, "uri");
            let uri_body = body_json.as_ref().and_then(|b| b.get("uri")).and_then(|v| v.as_str());
            let uri = uri_param.as_deref().or(uri_body).unwrap_or_default().to_string();

            if uri.is_empty() {
                send_http_json(&mut stream, 400, "Bad Request", &serde_json::json!({ "ok": false, "error": "Parámetro 'uri' requerido" })).await;
                return;
            }

            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::Dequeue { uri: uri.clone(), reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "dequeued": uri })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        ("POST", "/api/clear") | ("DELETE", "/api/queue/all") => {
            let res = send_ui_cmd(&ui_cmd_tx, &waker, |reply| UiCommand::ClearQueue { reply }).await;
            match res {
                Ok(_) => send_http_json(&mut stream, 200, "OK", &serde_json::json!({ "ok": true, "cleared": true })).await,
                Err(e) => send_http_json(&mut stream, 500, "Internal Server Error", &serde_json::json!({ "ok": false, "error": e })).await,
            }
        }

        _ => {
            send_http_json(
                &mut stream,
                404,
                "Not Found",
                &serde_json::json!({ "ok": false, "error": format!("Ruta desconocida: {method} {path}") }),
            )
            .await;
        }
    }
}

// ---------- Helpers HTTP ----------

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> Option<(String, String, String, Option<Value>)> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::with_capacity(4096);
    let mut temp = [0u8; 2048];
    let mut header_end = None;
    let mut content_length = 0usize;

    loop {
        let n = stream.read(&mut temp).await.ok()?;
        if n == 0 {
            if buf.is_empty() {
                return None;
            }
            break;
        }
        buf.extend_from_slice(&temp[..n]);

        if header_end.is_none() {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                header_end = Some(pos + 4);
            } else if let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
                header_end = Some(pos + 2);
            }
            if let Some(pos) = header_end {
                let head_str = String::from_utf8_lossy(&buf[..pos]);
                for line in head_str.lines() {
                    if let Some(val) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = val.trim().parse::<usize>().unwrap_or(0);
                    }
                }
            }
        }

        if let Some(pos) = header_end {
            if buf.len() >= pos + content_length {
                break;
            }
        }
        if buf.len() > 65536 {
            break;
        }
    }

    let end_headers = header_end.unwrap_or(buf.len());
    let head_str = String::from_utf8_lossy(&buf[..end_headers]).to_string();
    let mut lines = head_str.lines();
    let req_line = lines.next()?;
    let mut parts = req_line.split_whitespace();
    let method = parts.next()?.to_ascii_uppercase();
    let full_path = parts.next()?.to_string();

    let body_bytes = if end_headers < buf.len() {
        &buf[end_headers..]
    } else {
        &[]
    };
    let body_json = if !body_bytes.is_empty() {
        serde_json::from_slice::<Value>(body_bytes).ok()
    } else {
        None
    };

    Some((method, full_path, head_str, body_json))
}

fn split_path_query(full: &str) -> (&str, &str) {
    if let Some(idx) = full.find('?') {
        (&full[..idx], &full[idx + 1..])
    } else {
        (full, "")
    }
}

fn parse_query_param(query_str: &str, key: &str) -> Option<String> {
    for pair in query_str.split('&') {
        let mut kv = pair.splitn(2, '=');
        let k = kv.next()?;
        let v = kv.next().unwrap_or("");
        if k == key {
            return Some(url_decode(v));
        }
    }
    None
}

fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(val) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(val as char);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            out.push(' ');
            i += 1;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

async fn send_http_json(stream: &mut tokio::net::TcpStream, status: u16, reason: &str, body: &Value) {
    use tokio::io::AsyncWriteExt;
    let json_bytes = serde_json::to_vec(body).unwrap_or_default();
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, OPTIONS, DELETE\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         Connection: close\r\n\r\n",
        json_bytes.len()
    );
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.write_all(&json_bytes).await;
    let _ = stream.flush().await;
}

async fn send_http_cors_preflight(stream: &mut tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;
    let resp = "HTTP/1.1 204 No Content\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, OPTIONS, DELETE\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         Connection: close\r\n\r\n";
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn send_ui_cmd(
    ui_cmd_tx: &std_mpsc::Sender<UiCommand>,
    waker: &fastframe_shell::Waker,
    cmd_ctor: impl FnOnce(oneshot::Sender<Result<Value, String>>) -> UiCommand,
) -> Result<Value, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = cmd_ctor(reply_tx);
    if ui_cmd_tx.send(cmd).is_err() {
        return Err("Canal de UI cerrado".into());
    }
    waker.wake();
    match reply_rx.await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(err)) => Err(err),
        Err(_) => Err("UI no respondió".into()),
    }
}

async fn resolve_search(
    backend_sender: &mpsc::UnboundedSender<crate::backend::Command>,
    query: String,
    limit: Option<usize>,
) -> Result<Vec<SimplifiedTrack>, String> {
    if let Some(uri) = extract_uri(&query) {
        let id = uri.split(':').last().unwrap_or(&uri).to_string();
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = backend_sender.send(crate::backend::Command::LuckyBridge(
            CatalogRequest::TrackByUri { id, reply: reply_tx },
        ));
        match reply_rx.await {
            Ok(Ok(track)) => Ok(vec![track]),
            Ok(Err(err)) => Err(err),
            Err(_) => Err("Backend desconectado".into()),
        }
    } else {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = backend_sender.send(crate::backend::Command::LuckyBridge(
            CatalogRequest::SearchTracks {
                query,
                limit,
                reply: reply_tx,
            },
        ));
        match reply_rx.await {
            Ok(Ok(tracks)) => Ok(tracks),
            Ok(Err(err)) => Err(err),
            Err(_) => Err("Backend desconectado".into()),
        }
    }
}

// ---------- Loop de cliente WebSocket saliente (Streaming) ----------

async fn run_client(
    waker: fastframe_shell::Waker,
    backend_sender: mpsc::UnboundedSender<crate::backend::Command>,
    ui_cmd_tx: std_mpsc::Sender<UiCommand>,
    mut event_rx: mpsc::UnboundedReceiver<BridgeEvent>,
    connect_generation: Arc<AtomicU64>,
) {
    let hub_url = std::env::var("SPOTIFAST_WS_URL")
        .or_else(|_| std::env::var("SPOTIFAST_HUB_URL"))
        .unwrap_or_else(|_| "ws://127.0.0.1:8765/spotifast".to_string());
    let mut retry_ms = 3000u64;

    loop {
        log::debug!("[lucky_bridge] Conectando a {hub_url}...");
        match tokio_tungstenite::connect_async(&hub_url).await {
            Ok((ws_stream, _)) => {
                log::info!("[lucky_bridge] Conectado exitosamente al hub de streaming");
                retry_ms = 3000;
                connect_generation.fetch_add(1, Ordering::SeqCst);
                waker.wake();

                let (mut ws_write, mut ws_read) = ws_stream.split();

                loop {
                    tokio::select! {
                        Some(event) = event_rx.recv() => {
                            let msg_str = match event {
                                BridgeEvent::Track(data) => serde_json::json!({ "event": "track", "data": data }).to_string(),
                                BridgeEvent::Progress(data) => serde_json::json!({ "event": "progress", "data": data }).to_string(),
                                BridgeEvent::Lyrics(data) => serde_json::json!({ "event": "lyrics", "data": data }).to_string(),
                                BridgeEvent::Log(data) => serde_json::json!({ "event": "log", "data": data }).to_string(),
                            };
                            if ws_write.send(Message::Text(msg_str.into())).await.is_err() {
                                log::warn!("[lucky_bridge] Error enviando evento; reconectando...");
                                break;
                            }
                        }
                        msg_opt = ws_read.next() => {
                            let Some(msg_res) = msg_opt else {
                                log::warn!("[lucky_bridge] Socket cerrado por el hub; reconectando...");
                                break;
                            };
                            match msg_res {
                                Ok(Message::Text(text)) => {
                                    handle_rpc_message(
                                        &text,
                                        &mut ws_write,
                                        &backend_sender,
                                        &ui_cmd_tx,
                                        &waker,
                                    ).await;
                                }
                                Ok(Message::Close(_)) => {
                                    log::info!("[lucky_bridge] Recibido cierre de WebSocket");
                                    break;
                                }
                                Ok(Message::Ping(_)) => {
                                    let _ = ws_write.flush().await;
                                }
                                Err(e) => {
                                    log::warn!("[lucky_bridge] Error de lectura WS: {e}; reconectando...");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::debug!("[lucky_bridge] No se pudo conectar a {hub_url}: {e}");
            }
        }

        tokio::time::sleep(Duration::from_millis(retry_ms)).await;
        retry_ms = ((retry_ms as f64 * 1.5) as u64).min(15000);
    }
}

async fn send_rpc_response<S>(
    write: &mut S,
    id: Value,
    ok: bool,
    data: Option<Value>,
    error: Option<String>,
) where
    S: futures_util::Sink<Message> + Unpin,
{
    let resp = if ok {
        serde_json::json!({
            "id": id,
            "ok": true,
            "data": data.unwrap_or(Value::Null)
        })
    } else {
        serde_json::json!({
            "id": id,
            "ok": false,
            "error": error.unwrap_or_else(|| "Error desconocido".into())
        })
    };
    let _ = write.send(Message::Text(resp.to_string().into())).await;
}

async fn handle_rpc_message<S>(
    text: &str,
    write: &mut S,
    backend_sender: &mpsc::UnboundedSender<crate::backend::Command>,
    ui_cmd_tx: &std_mpsc::Sender<UiCommand>,
    waker: &fastframe_shell::Waker,
) where
    S: futures_util::Sink<Message> + Unpin,
{
    let Ok(req): Result<Value, _> = serde_json::from_str(text) else {
        return;
    };
    let id_val = req.get("id").cloned().unwrap_or(Value::Null);
    let Some(action) = req.get("action").and_then(|v| v.as_str()) else {
        send_rpc_response(
            write,
            id_val,
            false,
            None,
            Some("Mensaje sin acción".into()),
        )
        .await;
        return;
    };

    match action {
        "search" => {
            let query = req
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            match resolve_search(backend_sender, query, None).await {
                Ok(mut tracks) => {
                    let data = if tracks.is_empty() {
                        Value::Null
                    } else {
                        serde_json::to_value(tracks.remove(0)).unwrap_or(Value::Null)
                    };
                    send_rpc_response(write, id_val, true, Some(data), None).await;
                }
                Err(err) => {
                    send_rpc_response(write, id_val, false, None, Some(err)).await;
                }
            }
        }
        "searchMany" => {
            let query = req
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let limit = req
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize)
                .unwrap_or(5)
                .clamp(1, 20);
            match resolve_search(backend_sender, query, Some(limit)).await {
                Ok(tracks) => {
                    send_rpc_response(
                        write,
                        id_val,
                        true,
                        Some(serde_json::to_value(tracks).unwrap_or(Value::Null)),
                        None,
                    )
                    .await;
                }
                Err(err) => {
                    send_rpc_response(write, id_val, false, None, Some(err)).await;
                }
            }
        }
        "artistInfo" => {
            let uri = req
                .get("uri")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let id = uri.split(':').last().unwrap_or(&uri).to_string();
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = backend_sender.send(crate::backend::Command::LuckyBridge(
                CatalogRequest::ArtistById { id, reply: reply_tx },
            ));
            match reply_rx.await {
                Ok(Ok((artist_uri, artist_name))) => {
                    let data = serde_json::json!({ "uri": artist_uri, "name": artist_name });
                    send_rpc_response(write, id_val, true, Some(data), None).await;
                }
                Ok(Err(err)) => {
                    send_rpc_response(write, id_val, false, None, Some(err)).await;
                }
                Err(_) => {
                    send_rpc_response(
                        write,
                        id_val,
                        false,
                        None,
                        Some("Backend desconectado".into()),
                    )
                    .await;
                }
            }
        }
        "enqueue" => {
            let uri = req
                .get("uri")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::Enqueue { uri, reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "dequeue" => {
            let uri = req
                .get("uri")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::Dequeue { uri, reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "queueUris" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::QueueUris { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "skip" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::Skip { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "previous" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::Previous { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "play" => {
            let uri_opt = req.get("uri").and_then(|v| v.as_str()).map(|s| s.to_string());
            let res = if let Some(uri) = uri_opt {
                send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::PlayUri { uri, reply }).await
            } else {
                send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::Play { reply }).await
            };
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "pause" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::Pause { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "togglePlay" | "playPause" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::TogglePlay { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "seek" => {
            let pos = req
                .get("positionMs")
                .or_else(|| req.get("position_ms"))
                .or_else(|| req.get("ms"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::Seek { position_ms: pos, reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "getVolume" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::GetVolume { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "setVolume" => {
            let vol = req.get("volume").and_then(|v| v.as_f64()).unwrap_or(1.0);
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::SetVolume { volume: vol, reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "clearQueue" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::ClearQueue { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "nowPlaying" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::GetNowPlaying { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        "getQueue" => {
            let res = send_ui_cmd(ui_cmd_tx, waker, |reply| UiCommand::GetQueue { reply }).await;
            match res {
                Ok(val) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Err(err) => send_rpc_response(write, id_val, false, None, Some(err)).await,
            }
        }
        unknown => {
            send_rpc_response(
                write,
                id_val,
                false,
                None,
                Some(format!("Acción desconocida: {unknown}")),
            )
            .await;
        }
    }
}
