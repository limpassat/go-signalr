package singnalr

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type testConnMeta struct {
	Id string
}

func Test_CallerInvoke(t *testing.T) {

	meta := testConnMeta{
		Id: "1",
	}

	registry := NewHandlersRegistry()

	incoming := make(chan []byte, 1)

	outcoming := make(chan []byte, 1)

	ctx, _ := context.WithCancel(t.Context())

	conn := NewHubConnection[testConnMeta](
		meta,
		registry,
		incoming,
		outcoming,
		ctx,
		5*time.Second,

		func(msg InvocationMessage) {},
		func(msg StreamItemMessage) {},
		func(msg CompletionMessage) {},
		func(msg StreamInvocationMessage) {},
		func(msg CancelInvocationMessage) {},
		func(msg PingMessage) {},
		func(msg CloseMessage) {},
		func(msg AckMessage) {},
		func(msg SequenceMessage) {},
		func(msg HandshakeRequest) {},
		func(msg HandshakeResponse) {},
	)

	go (func() {
		for data := range outcoming {
			msgs := unmarshal(data)
			for _, msg := range msgs {
				typ, val := parseMessage(msg)
				switch typ {
				case InvocationMessageType:
					{

						inv := val.(InvocationMessage)

						switch inv.Target {
						case "Split":
							{
								var arg1 = inv.Arguments[0].(float64)
								var arg2 = inv.Arguments[1].(string)

								splatted := fmt.Sprintf("%d %s", int(arg1), arg2)
								jsonBytes, _ := json.Marshal(splatted)
								completion := NewCompletionMessage(inv.InvocationId, jsonBytes, nil)
								completionBytes, _ := json.Marshal(completion)
								incoming <- marshal(completionBytes)
							}
						}
					}
				}
			}
		}
	})()

	invokeCtx, _ := context.WithCancel(t.Context())

	resp := conn.Invoke(invokeCtx, "Split", []any{1, "str"})

	if resp.Status != InvocationStatusSuccess {
		t.Fatalf("Incorrent invoke status got %s; wait %s", resp.Status, InvocationStatusSuccess)
	}

	var respData string
	json.Unmarshal(resp.Data, &respData)

	if string(respData) != "1 str" {
		t.Fatalf("Incorrent invoke response got %s; wait %s", string(respData), "1 str")
	}

}

func Test_CallerStream(t *testing.T) {

	meta := testConnMeta{
		Id: "1",
	}

	registry := NewHandlersRegistry()

	incoming := make(chan []byte, 1)

	outcoming := make(chan []byte, 1)

	ctx, _ := context.WithCancel(t.Context())

	conn := NewHubConnection[testConnMeta](
		meta,
		registry,
		incoming,
		outcoming,
		ctx,
		5*time.Second,

		func(msg InvocationMessage) {},
		func(msg StreamItemMessage) {},
		func(msg CompletionMessage) {},
		func(msg StreamInvocationMessage) {},
		func(msg CancelInvocationMessage) {},
		func(msg PingMessage) {},
		func(msg CloseMessage) {},
		func(msg AckMessage) {},
		func(msg SequenceMessage) {},
		func(msg HandshakeRequest) {},
		func(msg HandshakeResponse) {},
	)

	streamHandlerChan := make(chan string, 1)

	go (func() {

		calleeStreamCtx, calleeStreamCancel := context.WithCancel(t.Context())

		for data := range outcoming {
			msgs := unmarshal(data)
			for _, msg := range msgs {
				typ, val := parseMessage(msg)
				switch typ {
				case StreamInvocationMessageType:
					{

						inv := val.(StreamInvocationMessage)

						switch inv.Target {
						case "TestStream":
							{

								go (func() {
									for {
										select {
										case <-calleeStreamCtx.Done():
											{
												return
											}
										case s := <-streamHandlerChan:
											{
												jsonBytes, _ := json.Marshal(s)
												item := NewStreamItemMessage(inv.InvocationId, jsonBytes)
												itemBytes, _ := json.Marshal(item)
												incoming <- marshal(itemBytes)
											}
										}
									}
								})()

							}
						}
					}
				case CancelInvocationMessageType:
					{
						calleeStreamCancel()
					}
				}
			}
		}
	})()

	streamCtx, streamCancel := context.WithCancel(t.Context())

	dataChan, statusChan, status := conn.SubscribeStream(streamCtx, "TestStream", []any{"message"})

	if status != StreamInvocationStatusWaitMessages {
		t.Fatalf("Incorrent stream status got %s; wait %s", status, StreamInvocationStatusWaitMessages)
	}

	wg := &sync.WaitGroup{}

	wg.Add(2)

	received := []string{}

	go (func() {
		defer wg.Done()
		for data := range dataChan {
			var respData string
			json.Unmarshal(data, &respData)
			received = append(received, respData)
		}
	})()

	go (func() {
		defer wg.Done()
		for s := range statusChan {
			status = s
		}
	})()

	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"
	streamHandlerChan <- "message"

	time.Sleep(1 * time.Second)

	streamCancel()

	wg.Wait()

	expected := []string{
		"message",
		"message",
		"message",
		"message",
		"message",
		"message",
		"message",
		"message",
		"message",
		"message",
	}

	if !reflect.DeepEqual(received, expected) {
		t.Fatalf("Incorrent received got %v; wait %v", received, expected)
	}

	if status != StreamInvocationStatusCancelled {
		t.Fatalf("Incorrent stream status got %s; wait %s", status, StreamInvocationStatusCancelled)
	}

}

func Test_CalleeInvoke(t *testing.T) {

	meta := testConnMeta{
		Id: "1",
	}

	registry := NewHandlersRegistry()

	type joinResult struct {
		Result string `json:"result"`
	}

	joinedBytes, _ := json.Marshal(joinResult{Result: "1 str"})

	registry.RegisterInvocationHandler("Join", func(msg InvocationMessage, ctx context.Context) ([]byte, error) {
		var arg1 = msg.Arguments[0].(float64)
		var arg2 = msg.Arguments[1].(string)

		joined := fmt.Sprintf("%d %s", int(arg1), arg2)
		jsonBytes, _ := json.Marshal(joinResult{Result: joined})
		return jsonBytes, nil
	})

	incoming := make(chan []byte, 1)

	outcoming := make(chan []byte, 1)

	ctx, _ := context.WithCancel(t.Context())

	NewHubConnection[testConnMeta](
		meta,
		registry,
		incoming,
		outcoming,
		ctx,
		5*time.Second,

		func(msg InvocationMessage) {},
		func(msg StreamItemMessage) {},
		func(msg CompletionMessage) {},
		func(msg StreamInvocationMessage) {},
		func(msg CancelInvocationMessage) {},
		func(msg PingMessage) {},
		func(msg CloseMessage) {},
		func(msg AckMessage) {},
		func(msg SequenceMessage) {},
		func(msg HandshakeRequest) {},
		func(msg HandshakeResponse) {},
	)

	invocation := NewInvocationMessage("0", "Join", []any{1, "str"}, nil)
	invocationBytes, _ := json.Marshal(invocation)
	incoming <- marshal(invocationBytes)

	received := []any{}

	go (func() {
		for data := range outcoming {
			msgs := unmarshal(data)
			for _, msg := range msgs {
				_, anyVal := parseMessage(msg)
				received = append(received, anyVal)
			}
		}
	})()

	time.Sleep(1 * time.Second)

	answerCompletion := *NewCompletionMessage("0", joinedBytes, nil)

	expected := []any{
		answerCompletion,
	}

	if !reflect.DeepEqual(received, expected) {
		t.Fatalf("Incorrent received got %v; wait %v", received, expected)
	}

}
