from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import json
import time
import asyncio
import io

from PIL import Image

STALE_STREAM_TIMEOUT = 30

PROGRESS_OVERLAY = Image.open("progress.png").convert("RGBA")

app = FastAPI()

# stream_id -> stream information and latest frame
streams = {}

# WebSocket -> set of subscribed stream IDs
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

                    image = Image.alpha_composite(
                        image,
                        PROGRESS_OVERLAY,
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

    try:
        await send_stream_list(websocket)

        while True:
            data = json.loads(await websocket.receive_text())

            if data.get("type") == "subscribe":
                subscriptions = set(data.get("streams", []))
                viewers[websocket] = subscriptions

                # Send the latest frame immediately after subscribing.
                for stream_id in subscriptions:
                    stream = streams.get(stream_id)

                    if stream and stream["frame"]:
                        await websocket.send_text(json.dumps({
                            "type": "frame",
                            "stream_id": stream_id,
                        }))
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


async def cleanup_stale_streams():
    while True:
        await asyncio.sleep(5)

        now = time.time()

        stale = [
            stream_id
            for stream_id, stream in streams.items()
            if now - stream["last_frame"]
            > STALE_STREAM_TIMEOUT
        ]

        for stream_id in stale:
            print(
                f"[Stream] Removing stale stream: "
                f"{stream_id}"
            )

            streams.pop(
                stream_id,
                None
            )

        if stale:
            await broadcast_stream_list()

@app.get("/")
async def home():
    return HTMLResponse("""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Kong Arena Live View (Experimental)</title>

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
    background: #222;
    color: white;
    font-family: Arial, sans-serif;
    overflow: hidden;
}

#page {
    display: flex;
    flex-direction: column;
    width: 100%;
    height: 100%;
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

<div id="page">

    <div id="toolbar">
        <h1>Kong Arena Live View (Experimental)</h1>

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
                    Game Filter:
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
                    Max streams:
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

        </div>
    </div>

    <div id="streams">
        <div id="empty-message">
            Waiting for streams...
        </div>
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

    tile.image.removeAttribute("src");
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

        if (data.type === "streams") {
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
    asyncio.create_task(
        cleanup_stale_streams()
    )
