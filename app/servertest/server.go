package servertest

import (
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/Admiral-Piett/goaws/app/models"

	"github.com/Admiral-Piett/goaws/app/router"
	log "github.com/sirupsen/logrus"

	"strings"
)

// Server is a fake SQS / SNS server for testing purposes.
type Server struct {
	closed   bool
	handler  http.Handler
	listener net.Listener
	mu       sync.Mutex
}

// Quit closes down the server.
func (srv *Server) Quit() error {
	srv.mu.Lock()
	srv.closed = true
	srv.mu.Unlock()

	return srv.listener.Close()
}

// URL returns a URL for the server.
func (srv *Server) URL() string {
	return "http://" + srv.listener.Addr().String()
}

// New starts a new server and returns it.
func New(addr string) (*Server, error) {
	if addr == "" {
		addr = "localhost:0"
	}
	localURL := strings.Split(addr, ":")
	models.CurrentEnvironment.Host = localURL[0]
	models.CurrentEnvironment.Port = localURL[1]
	log.WithFields(log.Fields{
		"host": models.CurrentEnvironment.Host,
		"port": models.CurrentEnvironment.Port,
	}).Info("URL Sarting to listen")

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cannot listen on localhost: %v", err)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot listen on localhost: %v", err)
	}

	srv := Server{listener: l, handler: router.New()}

	go http.Serve(l, &srv)

	return &srv, nil
}

func (srv *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	srv.mu.Lock()
	closed := srv.closed
	srv.mu.Unlock()

	if closed {
		hj := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close()
		return
	}

	srv.handler.ServeHTTP(w, req)
}
