import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import json, time

app = FastAPI()
streams = {}
viewers = {}

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
        streams[stream_id] = {"username": metadata["username"], "game": metadata["game"], "frame": None, "last_frame": time.time()}
        print(f"Stream started: {metadata['username']} - {metadata['game']}")
        await broadcast_stream_list()
        while True:
            frame = await websocket.receive_bytes()
            if stream_id in streams:
                streams[stream_id]["frame"] = frame
                streams[stream_id]["last_frame"] = time.time()
            await broadcast_frame(stream_id, frame)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"Client stream error: {e}")
    finally:
        if stream_id and stream_id in streams:
            print(f"Stream ended: {stream_id}")
            streams.pop(stream_id, None)
            await broadcast_stream_list()

@app.websocket("/ws/viewer")
async def viewer_stream(websocket: WebSocket):
    await websocket.accept()
    viewers[websocket] = set()
    try:
        await send_stream_list(websocket)
        while True:
            data = json.loads(await websocket.receive_text())
            if data.get("type") == "subscribe":
                viewers[websocket] = set(data.get("streams", []))
                for stream_id in viewers[websocket]:
                    stream = streams.get(stream_id)
                    if stream and stream["frame"]:
                        await websocket.send_text(json.dumps({"type": "frame", "stream_id": stream_id}))
                        await websocket.send_bytes(stream["frame"])
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"Viewer error: {e}")
    finally:
        viewers.pop(websocket, None)

async def send_stream_list(websocket):
    await websocket.send_text(json.dumps({
        "type": "streams",
        "streams": [{"stream_id": sid, "username": s["username"], "game": s["game"]} for sid, s in streams.items()]
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

async def broadcast_frame(stream_id, frame):
    dead = []
    for viewer, subscriptions in list(viewers.items()):
        try:
            if stream_id not in subscriptions:
                continue
            await viewer.send_text(json.dumps({"type": "frame", "stream_id": stream_id}))
            await viewer.send_bytes(frame)
        except Exception:
            dead.append(viewer)
    for viewer in dead:
        viewers.pop(viewer, None)

@app.get("/")
async def home():
    return HTMLResponse("""
<!DOCTYPE html>
<html>
<head>
<title>Kong Arena Live Stream</title>
<style>
body { background:#222; color:white; font-family:Arial,sans-serif; text-align:center; }
#game { margin:20px auto; padding:15px; background:#333; width:fit-content; }
#frame { display:block; width:448px; height:512px; image-rendering:pixelated; image-rendering:crisp-edges; }
#info { padding:10px; }
</style>
</head>
<body>
<h1>Kong Arena Live Stream</h1>
<div id="game"><div id="info">Waiting for stream...</div><img id="frame"></div>
<script>
const protocol = location.protocol === "https:" ? "wss" : "ws";
const ws = new WebSocket(`${protocol}://${location.host}/ws/viewer`);
ws.binaryType = "blob";
const img = document.getElementById("frame");
const info = document.getElementById("info");
let currentStream = null;
let expectingFrame = false;

ws.onmessage = function(event) {
    if (typeof event.data === "string") {
        const data = JSON.parse(event.data);
        if (data.type === "streams") {
            console.log("Streams:", data.streams);
            if (data.streams.length > 0 && currentStream === null) {
                const stream = data.streams[0];
                currentStream = stream.stream_id;
                info.textContent = stream.username + " - " + stream.game;
                ws.send(JSON.stringify({type:"subscribe", streams:[currentStream]}));
                console.log("Subscribed:", currentStream);
            }
        } else if (data.type === "frame" && data.stream_id === currentStream) {
            expectingFrame = true;
        }
    } else if (expectingFrame) {
        const url = URL.createObjectURL(event.data);
        const oldUrl = img.dataset.url;
        img.src = url;
        img.dataset.url = url;
        if (oldUrl) URL.revokeObjectURL(oldUrl);
        expectingFrame = false;
    }
};
ws.onopen = () => console.log("Viewer connected");
ws.onclose = () => { console.log("Viewer disconnected"); info.textContent = "Viewer disconnected"; };
ws.onerror = error => console.error("WebSocket error:", error);
</script>
</body>
</html>
""")

@app.get("/health")
async def health():
    return {"status": "ok", "active_streams": len(streams), "viewers": len(viewers)}
