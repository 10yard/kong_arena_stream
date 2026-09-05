from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
import json
import time
import asyncio
import io
import os
import secrets
import hmac
import hashlib
import base64
import urllib.error
import urllib.parse
import urllib.request
import httpx
import discord

from PIL import Image

STALE_STREAM_TIMEOUT = 30
PROGRESS_STALE_STREAM_TIMEOUT = 180

PROGRESS_OVERLAY = Image.open("progress.png").convert("RGBA")

app = FastAPI()

# Discord set up
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI = os.getenv(
    "DISCORD_REDIRECT_URI",
    "https://live.kongarena.com/auth/discord/callback",
)
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID", "1509176714792800406")
AUTH_COOKIE_NAME = "kong_discord_session"
AUTH_STATE_COOKIE_NAME = "kong_discord_oauth_state"
AUTH_COOKIE_SECRET = os.getenv(
    "AUTH_COOKIE_SECRET",
    DISCORD_CLIENT_SECRET or "change-this-secret",
)

def _encode_session(user):
    payload = base64.urlsafe_b64encode(
        json.dumps(user, separators=(",", ":")).encode()
    ).decode().rstrip("=")
    signature = hmac.new(
        AUTH_COOKIE_SECRET.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    return f"{payload}.{signature}"

def _decode_session(value):
    if not value or "." not in value:
        return None
    payload, signature = value.rsplit(".", 1)
    expected = hmac.new(
        AUTH_COOKIE_SECRET.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        padded = payload + "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(padded).decode())
    except Exception:
        return None

def _discord_request(url, data=None, headers=None):
    request_headers = {
        "User-Agent": "KongArena/1.0",
        "Accept": "application/json",
    }

    if headers:
        request_headers.update(headers)

    try:
        if data is not None:
            response = httpx.post(
                url,
                data=data,
                headers=request_headers,
                timeout=15.0,
                follow_redirects=True,
            )
        else:
            response = httpx.get(
                url,
                headers=request_headers,
                timeout=15.0,
                follow_redirects=True,
            )

        if response.status_code >= 400:
            print("[Auth] Discord request failed", flush=True)
            print(f"[Auth] URL: {url}", flush=True)
            print(f"[Auth] HTTP status: {response.status_code}", flush=True)
            print(
                f"[Auth] Response headers: {dict(response.headers)}",
                flush=True,
            )
            print(
                f"[Auth] Response body: {response.text}",
                flush=True,
            )
            response.raise_for_status()

        return response.json()

    except httpx.HTTPStatusError:
        raise
    except httpx.RequestError as exc:
        print(
            f"[Auth] Discord network request failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        raise


async def exchange_discord_code(code):
    return await asyncio.to_thread(
        _discord_request,
        "https://discord.com/api/oauth2/token",
        {
            "client_id": DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": DISCORD_REDIRECT_URI,
        },
        {"Content-Type": "application/x-www-form-urlencoded"},
    )

async def fetch_discord_user(access_token):
    return await asyncio.to_thread(
        _discord_request,
        "https://discord.com/api/users/@me",
        headers={"Authorization": f"Bearer {access_token}"},
    )

async def fetch_discord_guilds(access_token):
    return await asyncio.to_thread(
        _discord_request,
        "https://discord.com/api/users/@me/guilds",
        headers={"Authorization": f"Bearer {access_token}"},
    )

def get_session_from_request(request):
    return _decode_session(request.cookies.get(AUTH_COOKIE_NAME))

discord_intents = discord.Intents.default()
discord_intents.message_content = True
discord_client = discord.Client(intents=discord_intents)
discord_task = None
chat_viewers = set()


# stream_id -> stream information and latest frame
streams = {}

# WebSocket -> set of subscribed stream IDs
viewers = {}

@app.get("/waiting.png")
def waiting_image():
    return FileResponse("waiting.png", media_type="image/png")

@app.get("/auth/discord/login")
async def discord_login():
    if not DISCORD_CLIENT_ID or not DISCORD_CLIENT_SECRET:
        return JSONResponse(
            {"error": "Discord OAuth is not configured"},
            status_code=503,
        )
    state = secrets.token_urlsafe(32)
    params = urllib.parse.urlencode({
        "client_id": DISCORD_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": DISCORD_REDIRECT_URI,
        "scope": "identify guilds",
        "state": state,
        "prompt": "consent",
    })
    response = RedirectResponse(
        f"https://discord.com/oauth2/authorize?{params}"
    )
    response.set_cookie(
        AUTH_STATE_COOKIE_NAME, state, httponly=True,
        secure=True, samesite="lax", max_age=600
    )
    return response

@app.get("/auth/discord/callback")
async def discord_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    saved_state = request.cookies.get(AUTH_STATE_COOKIE_NAME)
    if not code or not state or not saved_state or not hmac.compare_digest(
        state, saved_state
    ):
        return JSONResponse({"error": "Invalid Discord login state"}, status_code=400)
    try:
        token = await exchange_discord_code(code)
        user = await fetch_discord_user(token["access_token"])
        guilds = await fetch_discord_guilds(token["access_token"])
    except httpx.HTTPStatusError as exc:
        print(
            f"[Auth] Discord OAuth failed: HTTP "
            f"{exc.response.status_code}: {exc.response.text}",
            flush=True,
        )
        return JSONResponse({"error": "Discord login failed"}, status_code=502)
    except Exception as exc:
        print(
            f"[Auth] Discord OAuth failed: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return JSONResponse({"error": "Discord login failed"}, status_code=502)

    if not any(str(guild.get("id")) == str(DISCORD_GUILD_ID) for guild in guilds):
        return HTMLResponse(
            "<h1>Discord server membership required</h1>"
            "<p>You must be a member of the Kong Arena Discord server to comment.</p>",
            status_code=403,
        )

    session_user = {
        "id": str(user["id"]),
        "username": user.get("global_name") or user.get("username") or "Discord user",
        "avatar": user.get("avatar"),
    }
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        AUTH_COOKIE_NAME, _encode_session(session_user),
        httponly=True, secure=True, samesite="lax", max_age=604800
    )
    response.delete_cookie(AUTH_STATE_COOKIE_NAME)
    return response

@app.get("/auth/me")
async def auth_me(request: Request):
    user = get_session_from_request(request)
    return {"authenticated": bool(user), "user": user}

@app.get("/auth/logout")
async def auth_logout():
    response = RedirectResponse("/", status_code=303)
    response.delete_cookie(AUTH_COOKIE_NAME)
    return response

@app.websocket("/ws/client")
async def client_stream(websocket: WebSocket):
    await websocket.accept()
    stream_id = None

    try:
        metadata = json.loads(await websocket.receive_text())

        if metadata.get("type") != "start":
            await websocket.close()
            return

        stream_id = metadata["stream_id"]

        streams[stream_id] = {
            "username": metadata["username"],
            "game": metadata["game"],
            "streaming": metadata.get("streaming", "full"),
            "frame": None,
            "last_frame": time.time(),
        }

        print(
            f"Stream started: "
            f"{metadata['username']} - {metadata['game']}"
        )

        # Only announce the stream to viewers once it has received
        # its first frame and is genuinely ready to display.
        stream_announced = False


        while True:
            frame = await websocket.receive_bytes()

            if stream_id in streams:

                if streams[stream_id]["streaming"] == "progress":
                    image = Image.open(
                        io.BytesIO(frame)
                    ).convert("RGBA")

                    if PROGRESS_OVERLAY.size == image.size:
                        overlay = PROGRESS_OVERLAY
                    else:
                        overlay = PROGRESS_OVERLAY.resize(
                            image.size,
                            Image.Resampling.LANCZOS,
                        )

                    image = Image.alpha_composite(
                        image,
                        overlay,
                    )

                    output = io.BytesIO()
                    image.save(
                        output,
                        format="PNG",
                    )

                    frame = output.getvalue()


                streams[stream_id]["frame"] = frame
                streams[stream_id]["last_frame"] = time.time()


            if not stream_announced:
                stream_announced = True
                await broadcast_stream_list()

            await broadcast_frame(stream_id, frame)

    except WebSocketDisconnect:
        pass

    except Exception as e:
        print(f"Client stream error: {e}")

    finally:
        if stream_id and stream_id in streams:
            print(f"Stream ended: {stream_id}")

            # Tell viewers explicitly that this stream has ended before
            # removing it from the active stream list.
            await broadcast_stream_end(stream_id)

            streams.pop(stream_id, None)
            await broadcast_stream_list()


@app.websocket("/ws/viewer")
async def viewer_stream(websocket: WebSocket):
    await websocket.accept()
    viewers[websocket] = set()
    chat_viewers.add(websocket)
    session_user = _decode_session(
        websocket.cookies.get(AUTH_COOKIE_NAME)
    )

    try:
        await send_stream_list(websocket)
        await send_chat_history(websocket)

        while True:
            data = json.loads(await websocket.receive_text())

            if data.get("type") == "subscribe":
                subscriptions = set(data.get("streams", []))
                viewers[websocket] = subscriptions

            elif data.get("type") == "chat_send":
                if not session_user:
                    await websocket.send_text(json.dumps({
                        "type": "chat_error",
                        "message": "Please sign in with Discord to comment.",
                    }))
                    continue

                content = str(data.get("content", "")).strip()

                if content:
                    if len(content) > 2000:
                        content = content[:2000]

                    sent_message = await send_chat_message_to_discord(
                        content,
                        session_user.get("username", "Discord user"),
                    )

                    if sent_message is not None:
                        try:
                            await refresh_chat_history()
                        except Exception as exc:
                            print(
                                f"[Discord] Sent-message history refresh failed: "
                                f"{exc}",
                                flush=True,
                            )

    except WebSocketDisconnect:
        pass

    except Exception as e:
        print(f"Viewer error: {e}")

    finally:
        viewers.pop(websocket, None)
        chat_viewers.discard(websocket)


async def send_stream_list(websocket):
    await websocket.send_text(json.dumps({
        "type": "streams",
        "streams": [
            {
                "stream_id": stream_id,
                "username": stream["username"],
                "game": stream["game"],
            }
            for stream_id, stream in streams.items()
        ],
    }))


async def broadcast_stream_list():
    dead = []

    for viewer in list(viewers):
        try:
            await send_stream_list(viewer)
        except Exception:
            dead.append(viewer)

    for viewer in dead:
        viewers.pop(viewer, None)


async def broadcast_stream_end(stream_id):
    dead = []

    for viewer in list(viewers):
        try:
            await viewer.send_text(json.dumps({
                "type": "stream_end",
                "stream_id": stream_id,
            }))
        except Exception:
            dead.append(viewer)

    for viewer in dead:
        viewers.pop(viewer, None)


async def broadcast_frame(stream_id, frame):
    # Take a snapshot of the viewers first. This prevents a slow viewer
    # from holding up delivery to every other viewer.
    targets = [
        viewer
        for viewer, subscriptions in list(viewers.items())
        if stream_id in subscriptions
    ]

    async def send_frame(viewer):
        # Keep the control message and its frame together for this viewer.
        await viewer.send_text(json.dumps({
            "type": "frame",
            "stream_id": stream_id,
        }))
        await viewer.send_bytes(frame)

    results = await asyncio.gather(
        *(
            send_frame(viewer)
            for viewer in targets
        ),
        return_exceptions=True,
    )

    # Remove only viewers whose send failed.
    for viewer, result in zip(targets, results):
        if isinstance(result, Exception):
            viewers.pop(viewer, None)


async def stale_stream_cleanup_worker():
    while True:
        await asyncio.sleep(5)

        now = time.time()

        stale = [
            stream_id
            for stream_id, stream in list(streams.items())
            if now - stream.get("last_frame", now) > (
                PROGRESS_STALE_STREAM_TIMEOUT
                if stream.get("streaming") == "progress"
                else STALE_STREAM_TIMEOUT
            )
        ]

        for stream_id in stale:
            print(
                f"[Stream] Removing stale stream: {stream_id}",
                flush=True,
            )
            streams.pop(stream_id, None)

        if stale:
            await broadcast_stream_list()


async def cleanup_stale_streams():
    print(
        "[Stream] Stale-stream cleanup supervisor running",
        flush=True,
    )

    while True:
        try:
            await stale_stream_cleanup_worker()

        except asyncio.CancelledError:
            print(
                "[Stream] Stale-stream cleanup supervisor cancelled",
                flush=True,
            )
            raise

        except Exception as exc:
            print(
                f"[Stream] Cleanup worker stopped: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            print(
                "[Stream] Restarting cleanup worker in 1 second",
                flush=True,
            )

            await asyncio.sleep(1)


async def start_discord_bot():
    if not DISCORD_BOT_TOKEN:
        print("[Discord] DISCORD_BOT_TOKEN is not configured", flush=True)
        return

    if not DISCORD_CHANNEL_ID:
        print("[Discord] DISCORD_CHANNEL_ID is not configured", flush=True)
        return

    try:
        print("[Discord] Starting bot connection", flush=True)
        await discord_client.start(DISCORD_BOT_TOKEN)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"[Discord] Connection failed: {exc}", flush=True)


chat_history_cache = []
chat_history_last_refresh = 0.0
chat_history_refresh_lock = asyncio.Lock()

async def refresh_chat_history():
    global chat_history_cache, chat_history_last_refresh
    if not DISCORD_CHANNEL_ID or not discord_client.is_ready():
        return
    async with chat_history_refresh_lock:
        channel = discord_client.get_channel(DISCORD_CHANNEL_ID)
        if channel is None:
            channel = await discord_client.fetch_channel(DISCORD_CHANNEL_ID)
        messages = []
        async for message in channel.history(limit=50, oldest_first=False):
            author = str(message.author.display_name)
            content = message.content
            if message.author == discord_client.user and content.startswith('**') and ':** ' in content:
                name, content = content[2:].split(':** ', 1)
                author = name
            messages.append({
                "id": str(message.id),
                "author": author,
                "content": content,
                "timestamp": message.created_at.isoformat(),
            })

        # Discord returns newest-first; reverse the collected list for display.
        messages.reverse()
        chat_history_cache = messages
        chat_history_last_refresh = time.monotonic()
        print(f"[Discord] History refreshed: {len(messages)} messages; latest={messages[-1]['id'] if messages else 'none'}", flush=True)
        await broadcast_chat_history()

@discord_client.event
async def on_ready():
    print(f"[Discord] Connected as {discord_client.user}; watching channel {DISCORD_CHANNEL_ID}", flush=True)
    try:
        await refresh_chat_history()
    except Exception as exc:
        print(f"[Discord] Initial history refresh failed: {exc}", flush=True)

@discord_client.event
async def on_message(message):
    if message.channel.id != DISCORD_CHANNEL_ID:
        return

    try:
        await refresh_chat_history()
    except Exception as exc:
        print(
            f"[Discord] Message-triggered history refresh failed: {exc}",
            flush=True,
        )


async def broadcast_chat_history():
    payload = json.dumps({"type": "chat_history", "messages": chat_history_cache})
    for viewer in list(chat_viewers):
        try:
            await viewer.send_text(payload)
        except Exception:
            chat_viewers.discard(viewer)

async def chat_history_refresh_loop():
    while True:
        await asyncio.sleep(10)
        try:
            await refresh_chat_history()
        except Exception as exc:
            print(f"[Discord] Periodic history refresh failed: {exc}", flush=True)

async def send_chat_history(websocket):
    await websocket.send_text(json.dumps({
        "type": "chat_history",
        "messages": list(chat_history_cache),
    }))


async def send_chat_message_to_discord(content, author_name):
    if not DISCORD_CHANNEL_ID:
        return None

    channel = discord_client.get_channel(DISCORD_CHANNEL_ID)

    if channel is None:
        try:
            channel = await discord_client.fetch_channel(
                DISCORD_CHANNEL_ID
            )
        except Exception as exc:
            print(
                f"[Discord] Could not fetch chat channel: {exc}",
                flush=True
            )
            return None

    try:
        return await channel.send(f"**{author_name}:** {content}")

    except Exception as exc:
        print(
            f"[Discord] Could not send chat message: {exc}",
            flush=True
        )
        return None

@app.get("/")
async def home():
    return HTMLResponse("""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Kong Arena Live</title>

<style>
* {
    box-sizing: border-box;
}

html,
body {
    width: 100%;
    height: 100%;
    margin: 0;
}

body {
    display: flex;
    flex-direction: column;
    background: #222;
    color: white;
    font-family: Arial, sans-serif;
    overflow: hidden;
}

#auth-bar {
    flex: 0 0 auto;
}

#page {
    display: flex;
    flex: 1 1 auto;
    flex-direction: column;
    width: 100%;
    min-height: 0;
}

#toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 10px 16px;
    background: #111;
    border-bottom: 1px solid #555;
    flex: 0 0 auto;
}

#toolbar h1 {
    margin: 0;
    font-size: 22px;
    white-space: nowrap;
}

#controls {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 12px;
    flex-wrap: wrap;
}

.control-group {
    display: flex;
    align-items: center;
    gap: 6px;
    white-space: nowrap;
}

select,
#back-button {
    font-size: 15px;
    padding: 5px 8px;
    background: #222;
    color: white;
    border: 1px solid #666;
    border-radius: 5px;
}

#back-button {
    cursor: pointer;
}

#back-button:hover {
    background: #3a3a3a;
    border-color: #aaa;
}

#back-button:active {
    background: #333;
}

#streams {
    flex: 1 1 auto;
    min-height: 0;
    display: grid;
    gap: 10px;
    padding: 10px;
    align-content: center;
    justify-content: center;
    overflow: auto;
}

.stream-tile {
    position: relative;
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    background: #111;
    border: 2px solid #666;
    border-radius: 10px;
    padding: 5px;
    overflow: hidden;
    cursor: pointer;
    transition: transform 0.12s ease, border-color 0.12s ease;
    box-shadow:
        0 0 0 1px #111,
        0 4px 14px rgba(0, 0, 0, 0.65);
}

.stream-tile:hover {
    background: #3a3a3a;
    border-color: #aaa;
    transform: scale(1.01);
}

.stream-tile:active {
    background: #466b46;
    border-color: #9acb9a;
    transform: scale(0.99);
}

.stream-tile .stream-name {
    background: #111;
}



.stream-name {
    flex: 0 0 auto;
    min-height: 30px;
    padding: 6px 10px;
    font-weight: bold;
    text-align: center;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.stream-user {
    font-size: 1.2em;
}

.stream-game {
    font-size: 0.9em;
    opacity: 0.85;
}


.stream-image-wrap {
    flex: 1 1 auto;
    min-width: 0;
    min-height: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
}

.stream-image {
    display: block;
    width: 100%;
    height: 100%;
    object-fit: contain;
    image-rendering: pixelated;
    image-rendering: crisp-edges;
}

#empty-message {
    grid-column: 1 / -1;
    align-self: center;
    justify-self: center;
    color: #aaa;
    font-size: 20px;
}

#content {
    display: flex;
    flex: 1 1 auto;
    min-height: 0;
    min-width: 0;
}

#streams {
    flex: 1 1 auto;
    min-width: 0;
}

#tournament-filter {
    width: 150px;
    max-width: 150px;
    overflow: hidden;
    text-overflow: ellipsis;
}

#player-filter {
    width: 150px;
    max-width: 150px;
    overflow: hidden;
    text-overflow: ellipsis;
}

#chat-panel {
    display: flex;
    flex: 0 0 320px;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    background: #171717;
    border-left: 1px solid #555;
}

#chat-panel[hidden] {
    display: none;
}

#chat-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 10px;
    border-bottom: 1px solid #555;
}

#chat-header h2 {
    margin: 0;
    font-size: 18px;
}

#chat-close,
#chat-open,
#chat-send {
    cursor: pointer;
    font-size: 14px;
    padding: 5px 9px;
    background: #222;
    color: white;
    border: 1px solid #666;
    border-radius: 5px;
}

#chat-open {
    display: inline-block;
}

#chat-messages {
    flex: 1 1 auto;
    min-height: 0;
    overflow-y: auto;
    padding: 10px;
}

.chat-message {
    margin-bottom: 9px;
    overflow-wrap: anywhere;
}

.chat-author {
    font-weight: bold;
}

.chat-time {
    color: #888;
    font-size: 11px;
    margin-left: 5px;
}

.chat-content {
    margin-top: 2px;
    white-space: pre-wrap;
}

#chat-form {
    display: flex;
    gap: 6px;
    padding: 10px;
    border-top: 1px solid #555;
}

#chat-input {
    flex: 1 1 auto;
    min-width: 0;
    padding: 7px;
    color: white;
    background: #222;
    border: 1px solid #666;
    border-radius: 5px;
}

@media (max-width: 850px) {
    #content {
        flex-direction: column;
    }

    #chat-panel {
        flex: 0 0 260px;
        border-left: 0;
        border-top: 1px solid #555;
    }
}

@media (max-width: 850px) {
    #toolbar {
        align-items: flex-start;
        flex-direction: column;
    }

    #controls {
        justify-content: flex-start;
    }
}

@media (max-width: 600px) {
    #toolbar h1 {
        font-size: 17px;
    }

    #toolbar {
        padding: 8px 10px;
    }

    #controls {
        gap: 8px;
    }

    .control-group {
        gap: 4px;
    }
}
</style>
</head>

<body>
<div id="auth-bar" style="display:flex;justify-content:flex-end;gap:10px;align-items:center;padding:8px 12px;">
    <span id="auth-status">Checking Discord login...</span>
    <a id="auth-login" href="/auth/discord/login" style="display:none;">Sign in with Discord</a>
    <a id="auth-logout" href="/auth/logout" style="display:none;">Sign out</a>
</div>

<div id="page">

    <div id="toolbar">
        <h1>Kong Arena Live</h1>

        <div id="controls">

            <button
                id="back-button"
                type="button"
                hidden
            >
                ← Back to multi-view
            </button>

            <div class="control-group">
                <label for="tournament-filter">
                    Filter:
                </label>

                <select id="tournament-filter">
                    <option value="">All games in play</option>
                </select>
            </div>

            <div class="control-group">
                <label for="player-filter">
                    Player:
                </label>

                <select id="player-filter">
                    <option value="">All players</option>
                </select>
            </div>

            <div class="control-group">
                <label for="max-streams">
                    Limit:
                </label>

                <select id="max-streams">
                    <option value="1">1</option>
                    <option value="2">2</option>
                    <option value="3">3</option>
                    <option value="4" selected>4</option>
                    <option value="6">6</option>
                    <option value="8">8</option>
                </select>
            </div>

            <button id="chat-open" type="button">Hide Chat</button>

        </div>
    </div>

    <div id="content">
        <div id="streams">
            <div id="empty-message">
                Waiting for streams...
            </div>
        </div>

        <aside id="chat-panel">
            <div id="chat-header">
                <h2>Live chat</h2>
                            </div>
            <div id="chat-messages" aria-live="polite"></div>
            <form id="chat-form">
                <input id="chat-input" maxlength="2000" autocomplete="off" placeholder="Write a message...">
                <button id="chat-send" type="submit">Send</button>
            </form>
        </aside>
    </div>

</div>

<script>
const protocol =
    location.protocol === "https:" ? "wss" : "ws";

const ws = new WebSocket(
    `${protocol}://${location.host}/ws/viewer`
);

ws.binaryType = "blob";

const streamsElement =
    document.getElementById("streams");

const tournamentFilterElement =
    document.getElementById("tournament-filter");

const playerFilterElement =
    document.getElementById("player-filter");

const maxStreamsElement =
    document.getElementById("max-streams");

const backButton =
    document.getElementById("back-button");

const chatOpenButton =
    document.getElementById("chat-open");
const chatPanel =
    document.getElementById("chat-panel");
const chatMessagesElement =
    document.getElementById("chat-messages");
const chatForm =
    document.getElementById("chat-form");
const chatInput =
    document.getElementById("chat-input");

const authStatusElement =
    document.getElementById("auth-status");
const authLoginElement =
    document.getElementById("auth-login");
const authLogoutElement =
    document.getElementById("auth-logout");

let authenticatedUser = null;

function updateAuthenticationUI(user) {
    authenticatedUser = user || null;
    const authenticated = Boolean(authenticatedUser);

    authStatusElement.textContent = authenticated
        ? `Signed in as ${authenticatedUser.username}`
        : "You are not signed in";
    authLoginElement.style.display = authenticated ? "none" : "inline-block";
    authLogoutElement.style.display = authenticated ? "inline-block" : "none";
    chatInput.disabled = !authenticated;
    chatInput.placeholder = authenticated
        ? "Write a message..."
        : "Sign in with Discord to comment";
    chatForm.querySelector("button[type=submit]").disabled = !authenticated;
}

async function checkAuthentication() {
    try {
        const response = await fetch("/auth/me", {
            credentials: "same-origin",
            cache: "no-store",
        });

        if (!response.ok) {
            throw new Error(`Authentication status ${response.status}`);
        }

        const data = await response.json();
        updateAuthenticationUI(data.authenticated ? data.user : null);
    } catch (error) {
        console.error("Could not check Discord login:", error);
        updateAuthenticationUI(null);
        authStatusElement.textContent = "Unable to check Discord login";
    }
}

checkAuthentication();

const MAX_OPTIONS = {
    1:  [1, 1],
    2:  [2, 1],
    3:  [3, 1],
    4:  [2, 2],
    6:  [3, 2],
    8:  [4, 2],
    10: [5, 2],
};

let allStreams = [];
let selectedStreamIds = [];
let streamTiles = new Map();
let expectedFrameStreamId = null;


function getMaxStreams() {
    return Number(maxStreamsElement.value);
}


function getFilteredStreams() {
    const tournament =
        tournamentFilterElement.value;

    const player =
        playerFilterElement.value;

    return allStreams.filter(stream => {
        if (
            tournament &&
            stream.game !== tournament
        ) {
            return false;
        }

        if (
            player &&
            stream.stream_id !== player
        ) {
            return false;
        }

        return true;
    });
}


function updateTournamentOptions() {
    const previousValue =
        tournamentFilterElement.value;

    const tournaments = [
        ...new Set(
            allStreams
                .map(stream => stream.game)
                .filter(Boolean)
        )
    ];

    tournamentFilterElement.replaceChildren();

    const allOption =
        document.createElement("option");

    allOption.value = "";
    allOption.textContent =
        "All games in play";

    tournamentFilterElement.appendChild(allOption);

    for (const tournament of tournaments) {
        const option =
            document.createElement("option");

        option.value = tournament;
        option.textContent = tournament;

        tournamentFilterElement.appendChild(option);
    }

    if (tournaments.includes(previousValue)) {
        tournamentFilterElement.value =
            previousValue;
    }
}


function updatePlayerOptions() {
    const previousValue =
        playerFilterElement.value;

    const tournament =
        tournamentFilterElement.value;

    const availableStreams =
        allStreams.filter(stream => {
            return !tournament ||
                stream.game === tournament;
        });

    playerFilterElement.replaceChildren();

    const allOption =
        document.createElement("option");

    allOption.value = "";
    allOption.textContent = "All players";

    playerFilterElement.appendChild(allOption);

    for (const stream of availableStreams) {
        const option =
            document.createElement("option");

        option.value = stream.stream_id;
        option.textContent = stream.username;

        playerFilterElement.appendChild(option);
    }

    const playerStillExists =
        availableStreams.some(
            stream =>
                stream.stream_id === previousValue
        );

    if (playerStillExists) {
        playerFilterElement.value =
            previousValue;
    } else {
        playerFilterElement.value = "";
    }
}


function clearImage(tile) {
    const oldUrl = tile.image.dataset.url;

    if (oldUrl) {
        URL.revokeObjectURL(oldUrl);
        delete tile.image.dataset.url;
    }

    tile.image.src = "/waiting.png";
}


function removeTile(streamId) {
    const tile = streamTiles.get(streamId);

    if (!tile) {
        return;
    }

    clearImage(tile);
    tile.element.remove();
    streamTiles.delete(streamId);
}


function createTile(stream) {
    const element = document.createElement("div");
    element.className = "stream-tile";
    element.dataset.streamId = stream.stream_id;

    element.addEventListener("click", () => {
        playerFilterElement.value =
            stream.stream_id;

        updateStreams();
    });

    const name = document.createElement("div");
    name.className = "stream-name";
    name.textContent = stream.username;

    const imageWrap = document.createElement("div");
    imageWrap.className = "stream-image-wrap";

    const image = document.createElement("img");
    image.className = "stream-image";
    image.alt = stream.username;
    image.src = "/waiting.png";

    imageWrap.appendChild(image);
    element.appendChild(name);
    element.appendChild(imageWrap);

    streamsElement.appendChild(element);

    const tile = {
        element: element,
        image: image,
        name: name,
    };

    streamTiles.set(stream.stream_id, tile);

    return tile;
}


function setGridLayout(count) {
    if (count === 0) {
        streamsElement.style.gridTemplateColumns = "1fr";
        streamsElement.style.gridTemplateRows = "1fr";
        return;
    }

    // A single player always uses the full available viewer area.
    if (count === 1) {
        streamsElement.style.gridTemplateColumns = "1fr";
        streamsElement.style.gridTemplateRows = "1fr";
        return;
    }

    const maxStreams = getMaxStreams();
    const layout = MAX_OPTIONS[maxStreams];
    const maxColumns = layout[0];
    const maxRows = layout[1];

    let columns = Math.min(
        maxColumns,
        Math.ceil(Math.sqrt(count))
    );

    if (maxRows === 1) {
        columns = Math.min(maxColumns, count);
    }

    columns = Math.max(1, columns);

    let rows = Math.ceil(count / columns);

    while (
        rows > maxRows &&
        columns < maxColumns
    ) {
        columns += 1;
        rows = Math.ceil(count / columns);
    }

    streamsElement.style.gridTemplateColumns =
        `repeat(${columns}, minmax(0, 1fr))`;

    streamsElement.style.gridTemplateRows =
        `repeat(${rows}, minmax(0, 1fr))`;
}


function updateBackButton() {
    backButton.hidden =
        !playerFilterElement.value;
}


function updateStreams() {
    updateBackButton();

    const filteredStreams =
        getFilteredStreams();

    const player =
        playerFilterElement.value;

    let displayStreams;

    // Player selection is a focused single-player view.
    if (player) {
        displayStreams =
            filteredStreams.filter(
                stream =>
                    stream.stream_id === player
            );
    } else {
        displayStreams =
            filteredStreams.slice(
                0,
                getMaxStreams()
            );
    }

    selectedStreamIds =
        displayStreams.map(
            stream => stream.stream_id
        );

    const selectedSet =
        new Set(selectedStreamIds);

    for (
        const streamId
        of Array.from(streamTiles.keys())
    ) {
        if (!selectedSet.has(streamId)) {
            removeTile(streamId);
        }
    }

    const emptyMessage =
        document.getElementById("empty-message");

    if (selectedStreamIds.length === 0) {
        if (!emptyMessage) {
            const message =
                document.createElement("div");

            message.id = "empty-message";

            message.textContent =
                allStreams.length === 0
                    ? "Waiting for streams..."
                    : "No matching streams online.";

            streamsElement.appendChild(message);
        } else {
            emptyMessage.textContent =
                allStreams.length === 0
                    ? "Waiting for streams..."
                    : "No matching streams online.";
        }

        setGridLayout(0);
    } else {
        if (emptyMessage) {
            emptyMessage.remove();
        }

        for (const stream of displayStreams) {
            if (
                !streamTiles.has(
                    stream.stream_id
                )
            ) {
                createTile(stream);
            }

            const tile =
                streamTiles.get(
                    stream.stream_id
                );



			tile.name.replaceChildren();

			const user = document.createElement("strong");
			user.className = "stream-user";
			user.textContent = stream.username;

			const separator = document.createTextNode("\u00A0\u00A0•\u00A0\u00A0");

			const game = document.createElement("span");
			game.className = "stream-game";
			game.textContent = stream.game;

			tile.name.append(
				user,
				separator,
				game
			);


            tile.image.alt =
                stream.username;
        }

        // Reorder tiles to match the current display order.
        for (const stream of displayStreams) {
            const tile =
                streamTiles.get(
                    stream.stream_id
                );

            if (tile) {
                streamsElement.appendChild(
                    tile.element
                );
            }
        }

        setGridLayout(
            selectedStreamIds.length
        );

    }

    if (
        ws.readyState ===
        WebSocket.OPEN
    ) {
        ws.send(JSON.stringify({
            type: "subscribe",
            streams: selectedStreamIds,
        }));
    }
}


function setChatVisible(visible) {
    chatPanel.hidden = !visible;
    chatOpenButton.hidden = false;
    chatOpenButton.textContent = visible
        ? "Hide Chat"
        : "Show Chat";

    if (visible) {
        chatMessagesElement.scrollTop =
            chatMessagesElement.scrollHeight;
    }
}

function colourForUsername(username) {
    let hash = 0;

    for (let i = 0; i < username.length; i++) {
        hash = ((hash << 5) - hash) + username.charCodeAt(i);
        hash |= 0;
    }

    // Use a bright, reasonably saturated colour.
    // The hue is repeatable for the same username.
    const hue = Math.abs(hash) % 360;

    return `hsl(${hue}, 80%, 75%)`;
}

function addChatMessage(message) {
    const row = document.createElement("div");
    row.className = "chat-message";

    const author = document.createElement("span");
    author.className = "chat-author";
    
    const username = message.author || "Unknown";
    author.textContent = username;
    author.style.color = colourForUsername(username);

    const time = document.createElement("span");
    time.className = "chat-time";
    if (message.timestamp) {
        const date = new Date(message.timestamp);
        time.textContent = date.toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
        });
    }

    const content = document.createElement("div");
    content.className = "chat-content";
    content.textContent = message.content || "";

    row.append(author, time, content);
    chatMessagesElement.appendChild(row);
    chatMessagesElement.scrollTop = chatMessagesElement.scrollHeight;
}

chatOpenButton.addEventListener("click", () => {
    setChatVisible(chatPanel.hidden);
});

chatForm.addEventListener("submit", event => {
    event.preventDefault();
    const content = chatInput.value.trim();
    if (!content || ws.readyState !== WebSocket.OPEN) {
        return;
    }

    ws.send(JSON.stringify({
        type: "chat_send",
        content,
    }));
    chatInput.value = "";
});

// Keep only the newest frame for each stream. If decoding/rendering
// briefly falls behind, older frames are discarded rather than queued.
const pendingFrames = new Map();
let renderScheduled = false;

function updateFrame(streamId, blob) {
    pendingFrames.set(streamId, blob);

    if (!renderScheduled) {
        renderScheduled = true;
        requestAnimationFrame(renderPendingFrames);
    }
}

function renderPendingFrames() {
    renderScheduled = false;

    // Take a snapshot so frames that arrive during this render cycle
    // are left pending for the next animation frame.
    const framesToRender =
        new Map(pendingFrames);

    pendingFrames.clear();

    for (
        const [streamId, blob]
        of framesToRender
    ) {
        const tile =
            streamTiles.get(streamId);

        if (!tile) {
            continue;
        }

        const url =
            URL.createObjectURL(blob);

        const oldUrl =
            tile.image.dataset.url;

        tile.image.src = url;
        tile.image.dataset.url = url;

        if (oldUrl) {
            URL.revokeObjectURL(oldUrl);
        }
    }

    // If newer frames arrived while rendering, schedule one more
    // browser paint cycle and again use only the latest frame.
    if (pendingFrames.size > 0) {
        renderScheduled = true;
        requestAnimationFrame(
            renderPendingFrames
        );
    }
}


backButton.addEventListener(
    "click",
    () => {
        // Return to the current tournament's multi-player view.
        playerFilterElement.value = "";
        updateStreams();
    }
);


tournamentFilterElement.addEventListener(
    "change",
    () => {
        // Changing tournament can make the selected
        // player unavailable, so rebuild the player list.
        updatePlayerOptions();
        updateStreams();
    }
);


playerFilterElement.addEventListener(
    "change",
    updateStreams
);


maxStreamsElement.addEventListener(
    "change",
    updateStreams
);


ws.onmessage = function(event) {
    if (typeof event.data === "string") {
        const data =
            JSON.parse(event.data);

        if (data.type === "chat_history") {
            chatMessagesElement.replaceChildren();
            for (const message of data.messages || []) {
                addChatMessage(message);
            }
        }
        else if (data.type === "chat_error") {
        alert(data.message);
    }

    if (data.type === "chat_message") {
            addChatMessage(data.message);
        }
        else if (data.type === "streams") {
            allStreams = data.streams;

            // The stream list is the authoritative state. Remove any
            // screen immediately if its stream is no longer active.
            const activeStreamIds = new Set(
                allStreams.map(
                    stream => stream.stream_id
                )
            );

            for (
                const streamId
                of Array.from(
                    streamTiles.keys()
                )
            ) {
                if (
                    !activeStreamIds.has(
                        streamId
                    )
                ) {
                    removeTile(streamId);
                }
            }

            selectedStreamIds =
                selectedStreamIds.filter(
                    streamId =>
                        activeStreamIds.has(
                            streamId
                        )
                );

            updateTournamentOptions();
            updatePlayerOptions();
            updateStreams();
        }

        else if (data.type === "stream_end") {
            const streamId = data.stream_id;

            // Immediately remove the ended stream as an additional
            // fast cleanup. The following streams message is still
            // the authoritative active-stream state.
            removeTile(streamId);

            selectedStreamIds =
                selectedStreamIds.filter(
                    id => id !== streamId
                );

            allStreams =
                allStreams.filter(
                    stream =>
                        stream.stream_id !== streamId
                );

            updateTournamentOptions();
            updatePlayerOptions();
            updateStreams();
        }

        else if (
            data.type === "frame" &&
            selectedStreamIds.includes(
                data.stream_id
            )
        ) {
            expectedFrameStreamId =
                data.stream_id;
        }
    }

    else if (expectedFrameStreamId) {
        updateFrame(
            expectedFrameStreamId,
            event.data
        );

        expectedFrameStreamId = null;
    }
};


ws.onopen = function() {
    console.log("Viewer connected");
};


ws.onclose = function() {
    console.log("Viewer disconnected");

    for (
        const streamId
        of Array.from(
            streamTiles.keys()
        )
    ) {
        removeTile(streamId);
    }

    selectedStreamIds = [];
    allStreams = [];

    updateTournamentOptions();
    updatePlayerOptions();
    updateStreams();
};


ws.onerror = function(error) {
    console.error(
        "WebSocket error:",
        error
    );
};
</script>

</body>
</html>
""")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "active_streams": len(streams),
        "viewers": len(viewers),
    }

@app.on_event("startup")
async def start_cleanup():
    global discord_task
    print("[Stream] Starting stale-stream cleanup task", flush=True)
    asyncio.create_task(cleanup_stale_streams())
    discord_task = asyncio.create_task(start_discord_bot())
    asyncio.create_task(chat_history_refresh_loop())

@app.on_event("shutdown")
async def stop_discord():
    global discord_task
    if discord_task:
        discord_task.cancel()
    if not discord_client.is_closed():
        await discord_client.close()