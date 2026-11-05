# Go WebSocket Server

A small WebSocket server in Go with server-wide broadcasts, named rooms, and a browser-based test client. The project is intentionally compact: its main learning surface is how connections are upgraded, how messages move between goroutines, and how the server routes messages to clients.

## Features

- WebSocket connections using [Gorilla WebSocket](https://github.com/gorilla/websocket).
- Broadcast messages delivered to every connected client except the sender.
- Room join/leave operations and room-only messages, also excluding the sender.
- A browser client for manually exercising the protocol.
- Integration tests for broadcast delivery and room membership/message routing.

## Requirements

- Go `1.27.3` or newer, as declared by [`ws/go.mod`](ws/go.mod).
- GNU Make, if using the convenience targets.
- A browser for the manual client.

The Go module is in `ws/`, not the repository root. Run Go and Make commands from that directory (or use `make -C ws ...` from the root).

## Quick Start

Start the server from the repository root:

```sh
make -C ws run
```

Or run it from the module directory:

```sh
cd ws
make run
```

The server listens on port `3223` and upgrades requests at `/`. Open [`client/index.html`](client/index.html) in a browser. The client connects to `ws://localhost:3223`; there is no HTTP server for the static client in this project. For a second participant, open the client in another tab or browser window.

Stop the server with `Ctrl+C`.

## Repository Layout

```text
.
├── README.md
├── client/                   # Static browser test client
│   ├── index.html
│   ├── script.js
│   └── style.css
└── ws/                        # Go module: github.com/everestp/ws
		├── cmd/server/main.go     # Executable entry point
		├── internal/
		│   ├── client/client.go   # Protocol types and per-connection loops
		│   ├── room/room.go       # Room membership and client count
		│   └── server/            # Routing, lifecycle, and integration tests
		├── Makefile
		├── go.mod
		└── go.sum
```

The `internal` packages are implementation details of this module; Go prevents other modules from importing them.

## WebSocket Protocol

Clients send one JSON object per WebSocket text message. `type` selects the operation, `data` is an application-defined JSON value, and `roomID` identifies the room for room operations.

| Request type   | Meaning                                             | Required room ID |
| -------------- | --------------------------------------------------- | ---------------- |
| `broadcast`    | Send to all connected clients other than the sender | No               |
| `join-room`    | Add the sender to a room, creating it if needed     | Yes              |
| `leave-room`   | Remove the sender from a room                       | Yes              |
| `room-message` | Send to the other members of a room                 | Yes              |

Example broadcast:

```json
{
  "type": "broadcast",
  "data": "Hello, everyone",
  "roomID": ""
}
```

Example room message:

```json
{
  "type": "room-message",
  "data": { "text": "Hello, room" },
  "roomID": "engineering"
}
```

The server attaches the connection's client identity after decoding the request; clients cannot choose the sender identity. Recipients receive:

```json
{
  "type": "room-message",
  "data": { "text": "Hello, room" },
  "senderID": "server-assigned-id",
  "roomID": "engineering"
}
```

The response `type` is the same operation type as the request. The sender does not receive its own broadcast or room message. Join and leave operations do not currently produce an acknowledgment or error response. Unknown types and malformed JSON are logged and ignored.

## How It Works

### Connection lifecycle

1. `cmd/server/main.go` creates a `server.Server` and starts its HTTP listener.
2. `HandleWS` upgrades an HTTP request to WebSocket, creates a client, and starts that client's read and write loops.
3. The read loop decodes requests, injects the server-owned client reference, and forwards each operation to a server channel.
4. When a connection closes, the read loop reports the departure so the server removes the client from its rooms.

### Routing and concurrency

`Server.AcceptLoop` is the central event loop. It handles joins, leaves, broadcasts, and room messages in one `select`, so those operations serialize mutations of the server's client and room maps. Each client has a buffered outgoing-message channel and one writer goroutine; keeping WebSocket writes in that loop avoids concurrent writes to the same connection.

Broadcast and room routing select recipients before starting message delivery. Room membership is checked before a room message is routed. The room's count uses `atomic.Int64`, while membership itself is managed by the server event loop.

This is an actor-like design, not a complete production concurrency or lifecycle framework. The server has no graceful-shutdown API, the `Server.Mu` field is currently unused, and the test-result helpers are not general-purpose production APIs.

## Run and Test

From the repository root:

```sh
make -C ws run
make -C ws test
```

Or, from `ws/`:

```sh
make run
make test
```

The test target runs `go test -v ./internal/server/...`. These integration tests start a real listener on port `3223`, so stop the development server before testing and do not run multiple copies of the suite concurrently. A port conflict fails the test before its assertions run.

To run all Go packages directly:

```sh
cd ws
go test ./...
```

## Manual Browser Checks

With the server running, open the browser client in two tabs:

1. Send a broadcast from one tab. The other tab should receive it; the sending tab only logs its outgoing action.
2. Set both tabs to the same room ID and join from each.
3. Send a room message from one tab. The other room member should receive it.
4. Leave the room from one tab and send another room message. The former member should no longer receive it.

Room membership is connection-scoped and in-memory. Disconnecting removes that client's membership; restarting the process clears all clients and rooms.

## Current Limitations

This repository is a learning and local-testing project, not a hardened public service:

- The WebSocket upgrader accepts every origin (`CheckOrigin` always returns `true`). Restrict origins before exposing the server beyond trusted local use.
- There is no authentication, authorization, TLS termination, input-size policy, rate limiting, or application-level validation of `data` and `roomID`.
- There are no acknowledgments, delivery guarantees, persistence, room cleanup, or graceful server shutdown.
- Slow recipients can block message delivery when their buffered outgoing channel fills; the buffer is not a substitute for an explicit backpressure policy.
- The browser client hardcodes `ws://localhost:3223`; it is intended for local development and is not configured for secure remote hosting.

## Useful Extension Points

When taking this beyond a local learning project, consider these changes in roughly this order:

1. Configure the listener address and allowed origins rather than hardcoding permissive behavior.
2. Define request validation and return structured errors/operation acknowledgments.
3. Decide how slow clients are handled (disconnect, bounded queue with drop policy, or another explicit policy).
4. Add graceful shutdown and ensure all connection goroutines and sockets are closed.
5. Add authentication and room-level authorization if rooms are not public.
6. Add tests for malformed requests, duplicate joins/leaves, disconnect cleanup, slow consumers, and shutdown.

## Dependencies

The server's direct WebSocket dependency is `github.com/gorilla/websocket`. Dependency versions are recorded in [`ws/go.mod`](ws/go.mod) and [`ws/go.sum`](ws/go.sum).
