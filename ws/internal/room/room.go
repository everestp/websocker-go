package room

import (
	"sync/atomic"

	"github.com/everestp/ws/internal/client"
)

// Room groups multiple connected clients together for localized broadcasting.
type Room struct {
	Clients      map[string]*client.Client
	ID           string
	ClientsCount *atomic.Int64
}

// NewRoom initializes an empty room with thread-safe counters.
func NewRoom(id string) *Room {
	return &Room{
		ID:           id,
		Clients:      map[string]*client.Client{},
		ClientsCount: new(atomic.Int64),
	}
}
