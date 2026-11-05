const WS_URL = "ws://localhost:3223";

let socket = null;
let reconnectAttempts = 0;
const maxReconnectDelay = 5000;

// DOM Elements
const statusEl = document.getElementById("connection-status");
const logContainer = document.getElementById("log-container");
const roomIdInput = document.getElementById("room-id-input");
const messageInput = document.getElementById("message-input");

// Buttons
const btnJoin = document.getElementById("btn-join");
const btnLeave = document.getElementById("btn-leave");
const btnBroadcast = document.getElementById("btn-broadcast");
const btnRoomMsg = document.getElementById("btn-room-msg");
const btnClear = document.getElementById("btn-clear");

function logMessage(text, type = "normal") {
    const div = document.createElement("div");
    div.className = `log-item ${type}`;
    const timestamp = new Date().toLocaleTimeString();
    div.textContent = `[${timestamp}] ${text}`;
    logContainer.appendChild(div);
    logContainer.scrollTop = logContainer.scrollHeight;
}

function connectWebSocket() {
    logMessage(`Connecting to server at ${WS_URL}...`, "system");
    socket = new WebSocket(WS_URL);

    socket.onopen = () => {
        logMessage("Connected to WebSocket server successfully!", "system");
        statusEl.textContent = "Connected";
        statusEl.className = "status connected";
        reconnectAttempts = 0;
    };

    socket.onmessage = (event) => {
        try {
            const resp = JSON.parse(event.data);
            logMessage(`[INCOMING] Type: ${resp.type} | Sender: ${resp.senderID || 'Server'} | Room: ${resp.roomID || 'N/A'} | Data: ${JSON.stringify(resp.data)}`);
        } catch (e) {
            logMessage(`[INCOMING RAW] ${event.data}`);
        }
    };

    socket.onclose = (event) => {
        logMessage(`Connection closed (Code: ${event.code}). Reconnecting...`, "error");
        statusEl.textContent = "Disconnected";
        statusEl.className = "status disconnected";

        // Exponential backoff reconnect
        reconnectAttempts++;
        const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts), maxReconnectDelay);
        setTimeout(connectWebSocket, delay);
    };

    socket.onerror = (error) => {
        logMessage(`WebSocket Error encountered`, "error");
    };
}

function sendPayload(type, data, roomID = "") {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
        alert("WebSocket is not connected!");
        return;
    }

    const req = {
        type: type,
        data: data,
        roomID: roomID
    };

    socket.send(JSON.stringify(req));
    logMessage(`[SENT] Type: ${type} | Room: ${roomID || 'N/A'} | Data: ${JSON.stringify(data)}`, "sent");
}

// Event Bindings
btnJoin.addEventListener("click", () => {
    const roomID = roomIdInput.value.trim();
    if (!roomID) return alert("Please specify a Room ID");
    sendPayload("join-room", "Joining room request", roomID);
});

btnLeave.addEventListener("click", () => {
    const roomID = roomIdInput.value.trim();
    if (!roomID) return alert("Please specify a Room ID");
    sendPayload("leave-room", "Leaving room request", roomID);
});

btnBroadcast.addEventListener("click", () => {
    const text = messageInput.value.trim();
    if (!text) return alert("Please type a broadcast message");
    sendPayload("broadcast", text);
});

btnRoomMsg.addEventListener("click", () => {
    const roomID = roomIdInput.value.trim();
    const text = messageInput.value.trim();
    if (!roomID || !text) return alert("Please provide both Room ID and Message content");
    sendPayload("room-message", text, roomID);
});

btnClear.addEventListener("click", () => {
    logContainer.innerHTML = "";
});

// Kick off initial connection
connectWebSocket();
