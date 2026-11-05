package client

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

type MsgType string

const (
	MsgType_Broadcast MsgType = "broadcast"
	MsgType_JoinRoom  MsgType = "join-room"
	MsgType_LeaveRoom MsgType = "leave-room"
	MsgType_RoomMsg   MsgType = "room-message"
)

type ReqMsg struct {
	MsgType MsgType     `json:"type"`
	Client  *Client     // Injected by server after unmarshaling
	Data    interface{} `json:"data"`
	RoomID  string      `json:"roomID"`
}

type RespMsg struct {
	MsgType  MsgType     `json:"type"`
	Data     interface{} `json:"data"`
	SenderID string      `json:"senderID"`
	RoomID   string      `json:"roomID"`
}

func NewRespMsg(msg *ReqMsg) *RespMsg {
	return &RespMsg{
		MsgType:  msg.MsgType,
		Data:     msg.Data,
		SenderID: msg.Client.ID,
		RoomID:   msg.RoomID,
	}
}

type Client struct {
	ID    string
	mu    *sync.RWMutex
	Conn  *websocket.Conn
	MsgCH chan *RespMsg
	Done  chan struct{}
}

func NewClient(conn *websocket.Conn) *Client {
	ID := rand.Text()[:9]
	return &Client{
		ID:    ID,
		mu:    new(sync.RWMutex),
		Conn:  conn,
		MsgCH: make(chan *RespMsg, 64), // Buffered channel to prevent slow reader blocking
		Done:  make(chan struct{}),
	}
}

// WriteMsgLoop flushes outgoing messages from the buffered channel to the WebSocket connection.
func (c *Client) WriteMsgLoop() {
	defer c.Conn.Close()
	for {
		select {
		case <-c.Done:
			return
		case msg := <-c.MsgCH:
			err := c.Conn.WriteJSON(msg)
			if err != nil {
				fmt.Printf("error sending msg to clientID = %s: %v\n", c.ID, err)
				return
			}
		}
	}
}

// ReadMsgLoop continuously reads frames from the WebSocket connection and forwards them to server channels.
func (c *Client) ReadMsgLoop(
	onJoinServer chan<- *Client,
	onLeaveServer chan<- *Client,
	broadcastCH chan<- *ReqMsg,
	joinRoomCH chan<- *ReqMsg,
	leaveRoomCH chan<- *ReqMsg,
	roomMsgCH chan<- *ReqMsg,
) {
	defer func() {
		close(c.Done)
		onLeaveServer <- c
	}()

	for {
		_, b, err := c.Conn.ReadMessage()
		if err != nil {
			return
		}

		msg := new(ReqMsg)
		err = json.Unmarshal(b, msg)
		if err != nil {
			fmt.Printf("unable to unmarshal the msg %v\n", err)
			continue
		}
		msg.Client = c

		switch msg.MsgType {
		case MsgType_Broadcast:
			broadcastCH <- msg
		case MsgType_JoinRoom:
			joinRoomCH <- msg
		case MsgType_LeaveRoom:
			leaveRoomCH <- msg
		case MsgType_RoomMsg:
			roomMsgCH <- msg
		default:
			fmt.Println("unknown msg type -> ignoring it!")
		}
	}
}
