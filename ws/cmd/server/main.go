package main

import (
	"github.com/everestp/ws/internal/server"
)

// Learning Resource Notes:
// - Concurrency Model: Uses Go channels as an Actor model queue to isolate state mutations.
// - Avoids Race Conditions: Maps (clients & rooms) are only mutated inside AcceptLoop().
// - Memory Management: Buffered channels prevent slow consumer deadlocks; cleanup defers prevent leaks.
func main() {
	s := server.NewServer()
	s.CreateWSServer()
}
