package singnalr

import (
	"context"
	"sync"
)

type InvocationHandler = func(msg InvocationMessage, ctx context.Context) ([]byte, error)

type StreamInvocationHandler = func(msg StreamInvocationMessage, ctx context.Context) (chan []byte, error)

type IHandlersRegistry interface {
	RegisterInvocationHandler(
		name string,
		handler InvocationHandler,
	)

	RegisterStreamInvocationHandler(
		name string,
		handler StreamInvocationHandler,
	)

	GetInvocationHandler(name string) (InvocationHandler, bool)

	GetStreamInvocationHandler(name string) (StreamInvocationHandler, bool)
}

func NewHandlersRegistry() IHandlersRegistry {
	return &HandlersRegistry{
		invocationHandlersMap:       map[string]InvocationHandler{},
		streamInvocationHandlersMap: map[string]StreamInvocationHandler{},
		mu:                          &sync.RWMutex{},
	}
}

type HandlersRegistry struct {
	invocationHandlersMap       map[string]InvocationHandler
	streamInvocationHandlersMap map[string]StreamInvocationHandler

	mu *sync.RWMutex
}

var _ IHandlersRegistry = &HandlersRegistry{}

func (r *HandlersRegistry) RegisterInvocationHandler(
	name string,
	handler InvocationHandler,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invocationHandlersMap[name] = handler
}

func (r *HandlersRegistry) RegisterStreamInvocationHandler(
	name string,
	handler StreamInvocationHandler,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.streamInvocationHandlersMap[name] = handler
}

func (r *HandlersRegistry) GetInvocationHandler(name string) (InvocationHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.invocationHandlersMap[name]
	return h, ok
}

func (r *HandlersRegistry) GetStreamInvocationHandler(name string) (StreamInvocationHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.streamInvocationHandlersMap[name]
	return h, ok
}
