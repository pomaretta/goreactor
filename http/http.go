package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gabrielperezs/goreactor/reactor"
	"github.com/julienschmidt/httprouter"
)

var (
	defaultHttpPort       = 1080
	defaultHealthySeconds = 86400
)

func ok(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write(json.RawMessage(`{"message": "ok"}`))
}

func ko(w http.ResponseWriter) {
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Header().Set("Content-Type", "application/json")
	w.Write(json.RawMessage(`{"error": "reactor not healthy"}`))
}

type HTTPServer struct {
	handler  *httprouter.Router
	reactors []*reactor.Reactor
	mu       sync.Mutex
	Port     int
}

func New(icfg interface{}) (*HTTPServer, error) {

	s := &HTTPServer{
		handler: httprouter.New(),
		Port:    defaultHttpPort,
	}

	cfg, ok := icfg.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Can't read the configuration (hint: HTTP Server)")
	}
	for k, v := range cfg {
		switch strings.ToLower(k) {
		case "port":
			s.Port = int(v.(int64))
		}
	}

	return s, nil
}

func (s *HTTPServer) SetReactors(reactors []*reactor.Reactor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reactors = reactors
}

func (s *HTTPServer) Run() error {

	s.handler.GET("/ping", s.health)
	s.handler.GET("/ping/:seconds", s.health)

	return http.ListenAndServe(fmt.Sprintf(":%d", s.Port), s.handler)
}

func (s *HTTPServer) health(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

	seconds := params.ByName("seconds")
	if seconds == "" {
		seconds = fmt.Sprintf("%d", defaultHealthySeconds)
	}
	parsedSeconds, err := strconv.Atoi(seconds)
	if err != nil && seconds != "" {
		parsedSeconds = defaultHealthySeconds
	}

	// NOTE: Check if there are any reactors running
	if len(s.reactors) == 0 {
		ko(w)
		return
	}

	// NOTE: Check if all reactors are healthy
	delayed := false
	for _, r := range s.reactors {
		last := r.GetLastSuccessfullExecution()
		errt := r.GetLastErrorExecution()
		if last.IsZero() && errt.IsZero() {
			continue
		}
		if time.Since(last) < time.Duration(parsedSeconds)*time.Second {
			ok(w)
			return
		}
		delayed = true
	}
	if delayed {
		ko(w)
		return
	}

	ok(w)
}
