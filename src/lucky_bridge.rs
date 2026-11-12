//! lucky_bridge.rs - Cliente WebSocket nativo para integración con herramientas y overlays externos.
//!
//! Se conecta a `ws://127.0.0.1:8765/spotifast` (o el valor de la variable de
//! entorno `SPOTIFAST_HUB_URL`) y expone un protocolo JSON-RPC y eventos
//! para control de reproducción, sincronización de cola y letras.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

/// Representación simplificada de una canción para el bridge WebSocket.
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
    Play {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Pause {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    GetVolume {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    SetVolume {
        volume: f64,
        reply: oneshot::Sender<Result<Value, String>>,
    },
}

/// Eventos salientes desde Spotifast hacia el hub WebSocket.
pub enum BridgeEvent {
    Track(Value),
    Progress(Value),
    Lyrics(Value),
    Log(String),
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

    /// Envía un evento hacia el hub WebSocket.
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

// ---------- Loop de conexión WebSocket ----------

/// Inicia el cliente WebSocket en segundo plano dentro del runtime Tokio.
pub fn start(
    waker: fastframe_shell::Waker,
    backend_sender: mpsc::UnboundedSender<crate::backend::Command>,
) -> AppBridgeHandle {
    let (ui_cmd_tx, ui_cmd_rx) = std_mpsc::channel::<UiCommand>();
    let (event_tx, event_rx) = mpsc::unbounded_channel::<BridgeEvent>();
    let connect_generation = Arc::new(AtomicU64::new(0));

    let gen_clone = Arc::clone(&connect_generation);
    tokio::spawn(async move {
        run_client(waker, backend_sender, ui_cmd_tx, event_rx, gen_clone).await;
    });

    AppBridgeHandle {
        ui_cmd_rx,
        event_tx,
        connect_generation,
    }
}

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
                log::info!("[lucky_bridge] Conectado exitosamente al hub WebSocket");
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
            if let Some(uri) = extract_uri(&query) {
                let id = uri.split(':').last().unwrap_or(&uri).to_string();
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = backend_sender.send(crate::backend::Command::LuckyBridge(
                    CatalogRequest::TrackByUri { id, reply: reply_tx },
                ));
                match reply_rx.await {
                    Ok(Ok(track)) => {
                        send_rpc_response(
                            write,
                            id_val,
                            true,
                            Some(serde_json::to_value(track).unwrap()),
                            None,
                        )
                        .await;
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
            } else {
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = backend_sender.send(crate::backend::Command::LuckyBridge(
                    CatalogRequest::SearchTracks {
                        query,
                        limit: None,
                        reply: reply_tx,
                    },
                ));
                match reply_rx.await {
                    Ok(Ok(mut tracks)) => {
                        let data = if tracks.is_empty() {
                            Value::Null
                        } else {
                            serde_json::to_value(tracks.remove(0)).unwrap()
                        };
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
                .clamp(1, 8);
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = backend_sender.send(crate::backend::Command::LuckyBridge(
                CatalogRequest::SearchTracks {
                    query,
                    limit: Some(limit),
                    reply: reply_tx,
                },
            ));
            match reply_rx.await {
                Ok(Ok(tracks)) => {
                    send_rpc_response(
                        write,
                        id_val,
                        true,
                        Some(serde_json::to_value(tracks).unwrap()),
                        None,
                    )
                    .await;
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
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::Enqueue {
                uri,
                reply: reply_tx,
            });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
            }
        }
        "dequeue" => {
            let uri = req
                .get("uri")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::Dequeue {
                uri,
                reply: reply_tx,
            });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
            }
        }
        "queueUris" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::QueueUris { reply: reply_tx });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
            }
        }
        "skip" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::Skip { reply: reply_tx });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
            }
        }
        "play" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::Play { reply: reply_tx });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
            }
        }
        "pause" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::Pause { reply: reply_tx });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
            }
        }
        "getVolume" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::GetVolume { reply: reply_tx });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
            }
        }
        "setVolume" => {
            let vol = req.get("volume").and_then(|v| v.as_f64()).unwrap_or(1.0);
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ui_cmd_tx.send(UiCommand::SetVolume {
                volume: vol,
                reply: reply_tx,
            });
            waker.wake();
            match reply_rx.await {
                Ok(Ok(val)) => send_rpc_response(write, id_val, true, Some(val), None).await,
                Ok(Err(err)) => send_rpc_response(write, id_val, false, None, Some(err)).await,
                Err(_) => {
                    send_rpc_response(write, id_val, false, None, Some("UI no respondió".into()))
                        .await
                }
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
