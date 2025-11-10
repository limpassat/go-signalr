package singnalr

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type HubConnection[Meta any] struct {
	Info Meta

	invocationChan        chan InvocationMessage
	streamItemChan        chan StreamItemMessage
	completionChan        chan CompletionMessage
	streamChan            chan StreamInvocationMessage
	cancelChan            chan CancelInvocationMessage
	pingChan              chan PingMessage
	closeChan             chan CloseMessage
	ackChan               chan AckMessage
	sequenceChan          chan SequenceMessage
	handshakeRequestChan  chan HandshakeRequest
	handshakeResponseChan chan HandshakeResponse

	customHandleInvocation        func(msg InvocationMessage)
	customHandleStreamItem        func(msg StreamItemMessage)
	customHandleCompletion        func(msg CompletionMessage)
	customHandleStreamInvocation  func(msg StreamInvocationMessage)
	customHandleCancelInvocation  func(msg CancelInvocationMessage)
	customHandlePing              func(msg PingMessage)
	customHandleClose             func(msg CloseMessage)
	customHandleAck               func(msg AckMessage)
	customHandleSequence          func(msg SequenceMessage)
	customHandleHandshakeRequest  func(msg HandshakeRequest)
	customHandleHandshakeResponse func(msg HandshakeResponse)

	incoming  chan []byte
	outcoming chan []byte

	ctx context.Context

	handlersRegistry IHandlersRegistry

	invocationCounter       *int32
	invocationDescMap       *sync.Map
	streamInvocationDescMap *sync.Map
}

type invocationDesc struct {
	invocationId string
	cancel       context.CancelFunc
	ctx          context.Context
}

type streamInvocationDesc struct {
	invocationId string
	cancel       context.CancelFunc
	ctx          context.Context
}

func NewHubConnection[Meta any](
	info Meta,
	handlersRegistry IHandlersRegistry,
	incoming chan []byte,
	outcoming chan []byte,
	ctx context.Context,
	keepAlive time.Duration,

	customHandleInvocation func(msg InvocationMessage),
	customHandleStreamItem func(msg StreamItemMessage),
	customHandleCompletion func(msg CompletionMessage),
	customHandleStreamInvocation func(msg StreamInvocationMessage),
	customHandleCancelInvocation func(msg CancelInvocationMessage),
	customHandlePing func(msg PingMessage),
	customHandleClose func(msg CloseMessage),
	customHandleAck func(msg AckMessage),
	customHandleSequence func(msg SequenceMessage),
	customHandleHandshakeRequest func(msg HandshakeRequest),
	customHandleHandshakeResponse func(msg HandshakeResponse),
) *HubConnection[Meta] {

	counter := int32(0)

	c := &HubConnection[Meta]{
		ctx:                     ctx,
		Info:                    info,
		incoming:                incoming,
		outcoming:               outcoming,
		invocationCounter:       &counter,
		handlersRegistry:        handlersRegistry,
		invocationDescMap:       &sync.Map{},
		streamInvocationDescMap: &sync.Map{},

		invocationChan:        make(chan InvocationMessage, 1),
		streamItemChan:        make(chan StreamItemMessage, 1),
		completionChan:        make(chan CompletionMessage, 1),
		streamChan:            make(chan StreamInvocationMessage, 1),
		cancelChan:            make(chan CancelInvocationMessage, 1),
		pingChan:              make(chan PingMessage, 1),
		closeChan:             make(chan CloseMessage, 1),
		ackChan:               make(chan AckMessage, 1),
		sequenceChan:          make(chan SequenceMessage, 1),
		handshakeRequestChan:  make(chan HandshakeRequest, 1),
		handshakeResponseChan: make(chan HandshakeResponse, 1),

		customHandleInvocation:        customHandleInvocation,
		customHandleStreamItem:        customHandleStreamItem,
		customHandleCompletion:        customHandleCompletion,
		customHandleStreamInvocation:  customHandleStreamInvocation,
		customHandleCancelInvocation:  customHandleCancelInvocation,
		customHandlePing:              customHandlePing,
		customHandleClose:             customHandleClose,
		customHandleAck:               customHandleAck,
		customHandleSequence:          customHandleSequence,
		customHandleHandshakeRequest:  customHandleHandshakeRequest,
		customHandleHandshakeResponse: customHandleHandshakeResponse,
	}

	go (func() {
		defer (func() {
			close(c.invocationChan)
			close(c.streamItemChan)
			close(c.completionChan)
			close(c.streamChan)
			close(c.cancelChan)
			close(c.pingChan)
			close(c.closeChan)
			close(c.ackChan)
			close(c.sequenceChan)
			close(c.handshakeRequestChan)
			close(c.handshakeResponseChan)

			c.invocationDescMap.Clear()
			c.streamInvocationDescMap.Clear()
		})()
		for {
			select {
			case <-ctx.Done():
				{
					return
				}
			case msg := <-c.incoming:
				{
					c.handleIncoming(msg)
				}
			}
		}
	})()

	go (func() {
		ticker := time.NewTicker(keepAlive)
		for {
			select {
			case <-ctx.Done():
				{
					ticker.Stop()
					return
				}
			case <-ticker.C:
				{
					c.SendPing(NewPingMessage())
				}
			}
		}
	})()

	go c.listenInvocations()
	go c.listenStreamInvocations()

	go (func() {
		for {
			select {
			case <-ctx.Done():
				{
					return
				}
			case msg := <-c.pingChan:
				{
					c.customHandlePing(msg)
				}
			case msg := <-c.ackChan:
				{
					c.customHandleAck(msg)
				}
			case msg := <-c.sequenceChan:
				{
					c.customHandleSequence(msg)
				}
			case msg := <-c.handshakeRequestChan:
				{
					c.customHandleHandshakeRequest(msg)
				}
			case msg := <-c.handshakeResponseChan:
				{
					c.customHandleHandshakeResponse(msg)
				}
			}
		}
	})()

	return c
}

func (c *HubConnection[T]) listenInvocations() {
	for {
		select {
		case msg := <-c.closeChan:
			{
				c.customHandleClose(msg)
				return
			}
		case <-c.ctx.Done():
			{
				return
			}
		case msg := <-c.cancelChan:
			{
				c.customHandleCancelInvocation(msg)
				desc, ok := c.invocationDescMap.Load(msg.InvocationId)
				if !ok {
					continue
				}
				d := desc.(*invocationDesc)
				if d == nil {
					continue
				}
				d.cancel()
			}
		case msg := <-c.invocationChan:
			{
				c.customHandleInvocation(msg)
				handler, ok := c.handlersRegistry.GetInvocationHandler(msg.Target)
				if !ok {
					err := fmt.Sprintf("Invocation handler for target %s not found", msg.Target)
					c.SendCompletion(NewCompletionMessage(msg.InvocationId, []byte{}, &err))
					continue
				}
				ctx, cancel := context.WithCancel(context.Background())
				c.invocationDescMap.Store(msg.InvocationId, &invocationDesc{
					invocationId: msg.InvocationId,
					cancel:       cancel,
					ctx:          ctx,
				})

				go (func() {

					dataChan := make(chan []byte, 1)

					errChan := make(chan error, 1)

					go (func() {
						data, err := handler(msg, ctx)
						if err != nil {
							errChan <- err
						} else {
							dataChan <- data
						}
					})()

					for {
						select {
						case err := <-errChan:
							{
								errStr := err.Error()
								c.SendCompletion(NewCompletionMessage(msg.InvocationId, []byte{}, &errStr))
								c.invocationDescMap.Delete(msg.InvocationId)
								return
							}
						case data := <-dataChan:
							{
								c.SendCompletion(NewCompletionMessage(msg.InvocationId, data, nil))
								c.invocationDescMap.Delete(msg.InvocationId)
								return
							}
						case cls := <-c.closeChan:
							{
								c.customHandleClose(cls)
								c.invocationDescMap.Delete(msg.InvocationId)
								return
							}
						case <-c.ctx.Done():
							{
								c.invocationDescMap.Delete(msg.InvocationId)
								return
							}
						case <-ctx.Done():
							{
								c.invocationDescMap.Delete(msg.InvocationId)
								return
							}
						}
					}
				})()

				continue
			}
		}
	}
}

func (c *HubConnection[T]) listenStreamInvocations() {
	for {
		select {
		case cls := <-c.closeChan:
			{
				c.customHandleClose(cls)
				return
			}
		case <-c.ctx.Done():
			{
				return
			}
		case msg := <-c.cancelChan:
			{
				c.customHandleCancelInvocation(msg)
				desc, ok := c.streamInvocationDescMap.Load(msg.InvocationId)
				if !ok {
					continue
				}
				d := desc.(*streamInvocationDesc)
				if d == nil {
					continue
				}
				d.cancel()
			}
		case msg := <-c.streamChan:
			{
				c.customHandleStreamInvocation(msg)
				handler, ok := c.handlersRegistry.GetStreamInvocationHandler(msg.Target)
				if !ok {
					err := fmt.Sprintf("Stream handler for target %s not found", msg.Target)
					c.SendCompletion(NewCompletionMessage(msg.InvocationId, []byte{}, &err))
					c.streamInvocationDescMap.Delete(msg.InvocationId)
					continue
				}
				ctx, cancel := context.WithCancel(context.Background())
				c.streamInvocationDescMap.Store(msg.InvocationId, &streamInvocationDesc{
					invocationId: msg.InvocationId,
					cancel:       cancel,
					ctx:          ctx,
				})

				streamChan, err := handler(msg, ctx)

				if err != nil {
					errStr := err.Error()
					c.SendCompletion(NewCompletionMessage(msg.InvocationId, []byte{}, &errStr))
					c.streamInvocationDescMap.Delete(msg.InvocationId)
					continue
				}

				go (func() {
					for {
						select {
						case data := <-streamChan:
							{
								c.SendStreamItem(NewStreamItemMessage(msg.InvocationId, data))
							}
						case cls := <-c.closeChan:
							{
								c.customHandleClose(cls)
								c.streamInvocationDescMap.Delete(msg.InvocationId)
								return
							}
						case <-c.ctx.Done():
							{
								c.streamInvocationDescMap.Delete(msg.InvocationId)
								return
							}
						case <-ctx.Done():
							{
								c.streamInvocationDescMap.Delete(msg.InvocationId)
								return
							}
						}
					}
				})()

				continue
			}
		}
	}
}

func (c *HubConnection[T]) getInvocationId() string {
	counter := *c.invocationCounter
	atomic.AddInt32(c.invocationCounter, 1)
	return fmt.Sprintf("%d", counter)
}

func (c *HubConnection[T]) handleIncoming(b []byte) {
	splatted := unmarshal(b)
	for _, msg := range splatted {
		if len(msg) == 0 {
			continue
		}

		typ, val := parseMessage(msg)

		switch typ {
		case InvocationMessageType:
			{
				c.invocationChan <- val.(InvocationMessage)
			}
		case StreamItemMessageType:
			{
				c.streamItemChan <- val.(StreamItemMessage)
			}
		case CompletionMessageType:
			{
				c.completionChan <- val.(CompletionMessage)
			}
		case StreamInvocationMessageType:
			{
				c.streamChan <- val.(StreamInvocationMessage)
			}
		case CancelInvocationMessageType:
			{
				c.cancelChan <- val.(CancelInvocationMessage)
			}
		case PingMessageType:
			{
				c.pingChan <- val.(PingMessage)
			}
		case CloseMessageType:
			{
				c.closeChan <- val.(CloseMessage)
			}
		case AckMessageType:
			{
				c.ackChan <- val.(AckMessage)
			}
		case SequenceMessageType:
			{
				c.sequenceChan <- val.(SequenceMessage)
			}
		case HandshakeRequestType:
			{
				c.handshakeRequestChan <- val.(HandshakeRequest)
			}
		case HandshakeResponseType:
			{
				c.handshakeResponseChan <- val.(HandshakeResponse)
			}
		}
	}
}

func (c *HubConnection[T]) SendBytes(b []byte) {
	if c.ctx.Err() != nil {
		return
	}
	// always add separator to end (microsoft wankers, can't do anything about it)
	b = marshal(b)
	c.outcoming <- b
}

func (c *HubConnection[T]) SendMessage(msg any) {
	b, err := json.Marshal(msg)
	if err != nil {
		return
	}
	c.SendBytes(b)
}

func (c *HubConnection[T]) SendInvocation(msg *InvocationMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendStreamInvocation(msg *StreamInvocationMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendStreamItem(msg *StreamItemMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendCompletion(msg *CompletionMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendCancelInvocation(msg *CancelInvocationMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendPing(msg *PingMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendClose(msg *CloseMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendAck(msg *AckMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendSequence(msg *AckMessage) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendHandshakeRequest(msg HandshakeRequest) {
	c.SendMessage(msg)
}

func (c *HubConnection[T]) SendHandshakeResponse(msg HandshakeResponse) {
	c.SendMessage(msg)
}
