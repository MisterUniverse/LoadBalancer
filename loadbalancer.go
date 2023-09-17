package main

import (
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type LoadBalancer struct {
	mu                   sync.RWMutex
	healthyBackends      []string
	currentBackendIndex  int
	healthCheckChannel   chan string
	connQueue            chan net.Conn
	backendSelectChannel chan string
	backendSelectTrigger chan bool
	connectionPools      map[string]chan net.Conn
}

func NewLoadBalancer(backends []string) *LoadBalancer {
	connectionPools := make(map[string]chan net.Conn)
	for _, backend := range backends {
		connectionPools[backend] = make(chan net.Conn, 5)
	}
	return &LoadBalancer{
		healthyBackends:      backends,
		healthCheckChannel:   make(chan string, 10),
		connQueue:            make(chan net.Conn, 100),
		backendSelectChannel: make(chan string),
		backendSelectTrigger: make(chan bool),
		connectionPools:      connectionPools,
	}
}

func (lb *LoadBalancer) backendSelector() {
	for range lb.backendSelectTrigger {
		lb.mu.Lock()
		if len(lb.healthyBackends) == 0 {
			lb.mu.Unlock()
			continue
		}
		backendAddr := lb.healthyBackends[lb.currentBackendIndex]
		lb.currentBackendIndex = (lb.currentBackendIndex + 1) % len(lb.healthyBackends)
		lb.mu.Unlock()
		lb.backendSelectChannel <- backendAddr
	}
}

func (lb *LoadBalancer) HandleConnection(clientConn net.Conn) {
	lb.backendSelectTrigger <- true
	backendAddr := <-lb.backendSelectChannel

	var backendConn net.Conn
	var err error

	select {
	case backendConn = <-lb.connectionPools[backendAddr]:
	default:
		backendConn, err = net.Dial("tcp", backendAddr)
		if err != nil {
			log.Printf("Could not connect to backend %v: %v", backendAddr, err)
			clientConn.Close()
			return
		}
	}

	go func() {
		io.Copy(backendConn, clientConn)
		lb.connectionPools[backendAddr] <- backendConn
	}()
	io.Copy(clientConn, backendConn)
	clientConn.Close()
}

func (lb *LoadBalancer) HealthCheck(backend string) {
	for {
		time.Sleep(10 * time.Second)
		conn, err := net.Dial("tcp", backend)
		if err == nil {
			lb.healthCheckChannel <- backend
			conn.Close()
		}
	}
}

func (lb *LoadBalancer) UpdateHealthyBackends() {
	for backend := range lb.healthCheckChannel {
		lb.mu.Lock()
		lb.healthyBackends = append(lb.healthyBackends, backend)
		lb.mu.Unlock()
	}
}

func main() {
	initialBackends := []string{"localhost:9001", "localhost:9002"}
	lb := NewLoadBalancer(initialBackends)

	for _, backend := range initialBackends {
		go lb.HealthCheck(backend)
	}

	go lb.UpdateHealthyBackends()

	go lb.backendSelector()

	for i := 0; i < 5; i++ {
		go func() {
			for conn := range lb.connQueue {
				lb.HandleConnection(conn)
			}
		}()
	}

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting TCP listener: %v", err)
	}
	defer listener.Close()

	log.Println("Load balancer started at :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		lb.connQueue <- conn
	}
}
