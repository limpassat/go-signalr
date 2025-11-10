package main

import (
	"log"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
)

type Client struct {
	conn      *websocket.Conn
	send      chan []byte
	lastPong  time.Time
	closeOnce sync.Once
}

type Hub struct {
	clients map[*Client]bool
	lock    sync.Mutex
}

func newHub() *Hub {
	return &Hub{
		clients: make(map[*Client]bool),
	}
}

func (h *Hub) addClient(c *Client) {
	h.lock.Lock()
	h.clients[c] = true
	h.lock.Unlock()
}

func (h *Hub) removeClient(c *Client) {
	h.lock.Lock()
	delete(h.clients, c)
	h.lock.Unlock()
}

func (h *Hub) broadcast(sender *Client, data []byte) {
	h.lock.Lock()
	defer h.lock.Unlock()

	for client := range h.clients {
		if client == sender {
			continue
		}
		select {
		case client.send <- data:
		default:
			client.closeOnce.Do(func() {
				close(client.send)
				client.conn.Close()
			})
			delete(h.clients, client)
		}
	}
}
func main() {
	app := fiber.New()
	hub := newHub()

	app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get("/ws", websocket.New(func(conn *websocket.Conn) {
		client := &Client{
			conn:     conn,
			send:     make(chan []byte, 8),
			lastPong: time.Now(),
		}

		hub.addClient(client)
		defer func() {
			hub.removeClient(client)
			client.closeOnce.Do(func() {
				close(client.send)
				conn.Close()
			})
			log.Printf("Client disconnected: %v", conn.RemoteAddr())
		}()

		log.Printf("Client connected: %v", conn.RemoteAddr())

		// Обработка Pong — обновляем таймер
		conn.SetPongHandler(func(appData string) error {
			client.lastPong = time.Now()
			return nil
		})

		// Writer goroutine
		go func(c *Client) {
			pingTicker := time.NewTicker(30 * time.Second)
			defer pingTicker.Stop()

			for {
				select {
				case msg, ok := <-c.send:
					if !ok {
						return
					}
					c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
					if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
						log.Printf("write error: %v", err)
						return
					}

				case <-pingTicker.C:
					// Проверяем, не протух ли клиент
					if time.Since(c.lastPong) > 40*time.Second {
						log.Printf("client %v timed out", c.conn.RemoteAddr())
						return
					}
					c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
					if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
						log.Printf("ping error: %v", err)
						return
					}
				}
			}
		}(client)

		// Reader loop
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			log.Printf("Received: %s", msg)
			hub.broadcast(client, msg)
		}
	}))

	ticker := time.NewTicker(500 * time.Millisecond)

	go (func() {
		for tm := range ticker.C {
			msg := tm.String()
			for c, _ := range hub.clients {
				c.send <- []byte(msg)
				c.send <- []byte(msg)
				c.send <- []byte(msg)
				c.send <- []byte(msg)
				c.send <- []byte(msg)
			}
		}
	})()

	log.Fatal(app.Listen(":8080"))
}
