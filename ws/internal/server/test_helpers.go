package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/everestp/ws/internal/client"
	"github.com/gorilla/websocket"
)

var (
	TestHost  = "ws://localhost"
	RoomOneID = "ROOM_ONE"
	RoomTwoID = "ROOM_TWO"
)

type TestConfig struct {
	ClientCount    int
	Wg             *sync.WaitGroup
	MsgCount       *atomic.Int64
	MsgCountROne   *atomic.Int64
	MsgCountRTwo   *atomic.Int64
	TargetMsgCount int
}

type TestClient struct {
	Conn   *websocket.Conn
	MsgCH  chan *client.ReqMsg
	Ctx    context.Context
	RoomID string
}

func NewTestClient(conn *websocket.Conn, ctx context.Context) *TestClient {
	return &TestClient{
		Conn:  conn,
		MsgCH: make(chan *client.ReqMsg, 64),
		Ctx:   ctx,
	}
}

func (c *TestClient) WriteLoop() {
	for {
		select {
		case <-c.Ctx.Done():
			return
		case msg := <-c.MsgCH:
			err := c.Conn.WriteJSON(&msg)
			if err != nil {
				fmt.Printf("error sending test msg: %v\n", err)
				return
			}
		}
	}
}

func (c *TestClient) RoomMsgLoop(counter1 *atomic.Int64, counter2 *atomic.Int64) {
	defer c.Conn.Close()
	exit := make(chan struct{})
	go func() {
		defer close(exit)
		for {
			_, b, err := c.Conn.ReadMessage()
			if err != nil {
				return
			}
			resp := new(client.RespMsg)
			if err := json.Unmarshal(b, resp); err != nil {
				log.Fatal(err)
			}

			if resp.RoomID == RoomOneID {
				counter1.Add(1)
			} else {
				counter2.Add(1)
			}
		}
	}()

	select {
	case <-exit:
		return
	case <-c.Ctx.Done():
		return
	}
}

func (c *TestClient) JoinRoom(targetRoomID string) {
	msg := &client.ReqMsg{
		MsgType: client.MsgType_JoinRoom,
		Data:    "wanna join room",
		RoomID:  targetRoomID,
	}
	c.MsgCH <- msg
}

func (c *TestClient) LeaveRoom(targetRoomID string) {
	msg := &client.ReqMsg{
		MsgType: client.MsgType_LeaveRoom,
		Data:    "wanna leave room",
		RoomID:  targetRoomID,
	}
	c.MsgCH <- msg
}

func (c *TestClient) SendRoomMsg(targetRoomID string) {
	msg := &client.ReqMsg{
		MsgType: client.MsgType_RoomMsg,
		Data:    "wanna send room message",
		RoomID:  targetRoomID,
	}
	c.MsgCH <- msg
}

func JoinServer() *websocket.Conn {
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", TestHost, WSPort), nil)
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func DialServer(tc *TestConfig) *websocket.Conn {
	exit := make(chan struct{})
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", TestHost, WSPort), nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			time.Sleep(50 * time.Millisecond)
			if tc.TargetMsgCount == int(tc.MsgCount.Load()) {
				close(exit)
				return
			}
		}
	}()

	go func() {
		<-exit
		conn.Close()
		tc.Wg.Done()
	}()

	go func() {
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if len(b) > 0 {
				tc.MsgCount.Add(1)
			}
		}
	}()

	return conn
}
