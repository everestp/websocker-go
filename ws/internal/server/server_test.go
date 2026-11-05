package server

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/everestp/ws/internal/client"
)

func TestConnection(t *testing.T) {
	s := NewServer()
	go s.CreateWSServer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	time.Sleep(500 * time.Millisecond)

	clientCount := 200
	brCount := 50

	tc := TestConfig{
		ClientCount:    clientCount,
		Wg:             new(sync.WaitGroup),
		MsgCount:       new(atomic.Int64),
		TargetMsgCount: clientCount * brCount,
	}
	tc.Wg.Add(tc.ClientCount + 1)

	brConn := DialServer(&tc)
	brClient := NewTestClient(brConn, ctx)
	go brClient.WriteLoop()

	for range tc.ClientCount {
		go DialServer(&tc)
	}
	time.Sleep(200 * time.Millisecond)

	for range brCount {
		msg := client.ReqMsg{
			MsgType: client.MsgType_Broadcast,
			Data:    "hello from tests",
		}
		brClient.MsgCH <- &msg
	}

	tc.Wg.Wait()
	fmt.Println("TestConnection completed successfully")
}

func TestRooms(t *testing.T) {
	s := NewServer()
	go s.CreateWSServer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	time.Sleep(500 * time.Millisecond)

	clientCount := 10
	msgCount := 5

	tc := TestConfig{
		ClientCount:    clientCount,
		Wg:             new(sync.WaitGroup),
		MsgCountROne:   new(atomic.Int64),
		MsgCountRTwo:   new(atomic.Int64),
		TargetMsgCount: (clientCount - 1) * msgCount,
	}

	clientRTwoCount := 0
	clients := []*TestClient{}

	for idx := range tc.ClientCount {
		conn := JoinServer()
		c := NewTestClient(conn, ctx)
		go c.RoomMsgLoop(tc.MsgCountROne, tc.MsgCountRTwo)
		go c.WriteLoop()

		clients = append(clients, c)
		rID := RoomOneID
		if rand.IntN(10) < 5 || idx == 0 {
			rID = RoomTwoID
			clientRTwoCount++
		}
		c.RoomID = rID
		c.JoinRoom(rID)
	}

	clientROneCount := tc.ClientCount - clientRTwoCount
	expectedMsgCount1 := 0
	expectedMsgCount2 := 0

	for idx := range msgCount {
		if clients[idx].RoomID == RoomOneID {
			expectedMsgCount1 += clientROneCount - 1
		} else {
			expectedMsgCount2 += clientRTwoCount - 1
		}
	}

	for {
		time.Sleep(100 * time.Millisecond)
		tr1 := s.GetRoomTestResults(RoomOneID)
		tr2 := s.GetRoomTestResults(RoomTwoID)

		if tr1.ClientsCount == clientROneCount && tr2.ClientsCount == clientRTwoCount {
			break
		}
	}

	for idx := range msgCount {
		sender := clients[idx]
		sender.SendRoomMsg(sender.RoomID)
	}

	for {
		time.Sleep(100 * time.Millisecond)
		currCount1 := tc.MsgCountROne.Load()
		currCount2 := tc.MsgCountRTwo.Load()

		if currCount1 == int64(expectedMsgCount1) && currCount2 == int64(expectedMsgCount2) {
			break
		}
	}

	for _, c := range clients {
		go c.LeaveRoom(c.RoomID)
	}

	for {
		time.Sleep(100 * time.Millisecond)
		tr := s.GetRoomTestResults(RoomOneID)
		if tr.ClientsCount == 0 {
			break
		}
	}

	for {
		time.Sleep(100 * time.Millisecond)
		activeClients := s.GetServerTestResults()
		if activeClients == 0 {
			break
		}
	}

	fmt.Println("TestRooms completed successfully")
}
