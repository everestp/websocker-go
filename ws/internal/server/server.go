package server

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/everestp/ws/internal/client"
	"github.com/everestp/ws/internal/room"

	"github.com/gorilla/websocket"
)

var WSPort = ":3223"

type Server struct {
	Clients       map[string]*client.Client
	Rooms         map[string]*room.Room
	Mu            *sync.RWMutex
	JoinServerCH  chan *client.Client
	LeaveServerCH chan *client.Client
	BroadcastCH   chan *client.ReqMsg
	JoinRoomCH    chan *client.ReqMsg
	LeaveRoomCH   chan *client.ReqMsg
	RoomMsgCH     chan *client.ReqMsg

	ReqCH         chan struct{}
	ClientCountCH chan int
	listener      net.Listener // Tracks active listener for clean shutdown/testing
}

func NewServer() *Server {
	return &Server{
		Clients:       map[string]*client.Client{},
		Rooms:         map[string]*room.Room{},
		Mu:            new(sync.RWMutex),
		JoinServerCH:  make(chan *client.Client, 64),
		LeaveServerCH: make(chan *client.Client, 64),
		BroadcastCH:   make(chan *client.ReqMsg, 64),
		JoinRoomCH:    make(chan *client.ReqMsg, 64),
		LeaveRoomCH:   make(chan *client.ReqMsg, 64),
		RoomMsgCH:     make(chan *client.ReqMsg, 64),

		ReqCH:         make(chan struct{}),
		ClientCountCH: make(chan int),
	}
}

func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  512,
		WriteBufferSize: 512,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Error on HTTP conn upgrade %v\n", err)
		return
	}

	c := client.NewClient(conn)
	s.JoinServerCH <- c

	go c.WriteMsgLoop()
	go c.ReadMsgLoop(
		s.JoinServerCH,
		s.LeaveServerCH,
		s.BroadcastCH,
		s.JoinRoomCH,
		s.LeaveRoomCH,
		s.RoomMsgCH,
	)
}

// AcceptLoop implements the Actor model pattern, serializing all state mutations over channels to avoid race conditions.
func (s *Server) AcceptLoop() {
	for {
		select {
		case c := <-s.JoinServerCH:
			s.joinServer(c)
		case c := <-s.LeaveServerCH:
			s.leaveServer(c)
		case msg := <-s.JoinRoomCH:
			s.joinRoom(msg)
		case msg := <-s.LeaveRoomCH:
			s.leaveRoom(msg)
		case msg := <-s.RoomMsgCH:
			s.roomMsg(msg)
		case msg := <-s.BroadcastCH:
			s.broadcast(msg)
		case <-s.ReqCH:
			s.ClientCountCH <- len(s.Clients)
		}
	}
}

func (s *Server) joinServer(c *client.Client) {
	s.Clients[c.ID] = c
	fmt.Printf("client joined the server, cID = %s\n", c.ID)
}

func (s *Server) leaveServer(c *client.Client) {
	delete(s.Clients, c.ID)

	for _, r := range s.Rooms {
		if _, ok := r.Clients[c.ID]; ok {
			delete(r.Clients, c.ID)
			r.ClientsCount.Add(-1)
		}
	}

	fmt.Printf("client left the server, cID = %s\n", c.ID)
}

func (s *Server) broadcast(msg *client.ReqMsg) {
	cls := map[string]*client.Client{}
	for id, c := range s.Clients {
		if id != msg.Client.ID {
			cls[id] = c
		}
	}

	go s.sendMsg(msg, cls)
}

func (s *Server) roomMsg(msg *client.ReqMsg) {
	rID := msg.RoomID
	roomObj, ok := s.Rooms[rID]
	if !ok {
		fmt.Printf("the room does not exist -> cannot send msg\n")
		return
	}

	if _, ok := roomObj.Clients[msg.Client.ID]; !ok {
		fmt.Printf("client = %s does not belong to room %s\n", msg.Client.ID, rID)
		return
	}

	cls := map[string]*client.Client{}
	for id, c := range roomObj.Clients {
		if id != msg.Client.ID {
			cls[id] = c
		}
	}

	go s.sendMsg(msg, cls)
}

func (s *Server) sendMsg(msg *client.ReqMsg, cls map[string]*client.Client) {
	resp := client.NewRespMsg(msg)
	for _, c := range cls {
		c.MsgCH <- resp
	}
}

func (s *Server) joinRoom(msg *client.ReqMsg) {
	rID := msg.RoomID
	roomObj, ok := s.Rooms[rID]
	if !ok {
		roomObj = room.NewRoom(rID)
		s.Rooms[rID] = roomObj
	}

	roomObj.Clients[msg.Client.ID] = msg.Client
	roomObj.ClientsCount.Add(1)
	fmt.Printf("client joined Room %s, cID = %s\n", rID, msg.Client.ID)
}

func (s *Server) leaveRoom(msg *client.ReqMsg) {
	rID := msg.RoomID
	roomObj, ok := s.Rooms[rID]
	if !ok {
		fmt.Printf("cannot leave room that does not exist rID = %s\n", rID)
		return
	}
	delete(roomObj.Clients, msg.Client.ID)
	roomObj.ClientsCount.Add(-1)
	fmt.Printf("client left room rID = %s, cID = %s\n", rID, msg.Client.ID)
}

type TestRoomResults struct {
	RoomID       string
	ClientsCount int
}

func (s *Server) GetRoomTestResults(roomID string) *TestRoomResults {
	roomObj := s.Rooms[roomID]
	if roomObj == nil {
		return &TestRoomResults{RoomID: roomID, ClientsCount: 0}
	}
	return &TestRoomResults{
		RoomID:       roomID,
		ClientsCount: int(roomObj.ClientsCount.Load()),
	}
}

func (s *Server) GetServerTestResults() int {
	s.ReqCH <- struct{}{}
	return <-s.ClientCountCH
}

// CreateWSServer initializes an isolated ServeMux to prevent global route conflicts during testing.
func (s *Server) CreateWSServer() {
	s.Start(WSPort)
}

// Start spins up the HTTP and WebSocket server using an isolated router mux.
func (s *Server) Start(port string) {
	go s.AcceptLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.HandleWS)

	fmt.Printf("starting server on port: %s\n", port)
	log.Fatal(http.ListenAndServe(port, mux))
}
