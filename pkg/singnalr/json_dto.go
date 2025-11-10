package singnalr

import "encoding/json"

type MessageType = int

const (
	InvocationMessageType       MessageType = 1
	StreamItemMessageType       MessageType = 2
	CompletionMessageType       MessageType = 3
	StreamInvocationMessageType MessageType = 4
	CancelInvocationMessageType MessageType = 5
	PingMessageType             MessageType = 6
	CloseMessageType            MessageType = 7
	AckMessageType              MessageType = 8
	SequenceMessageType         MessageType = 9

	// HACK for unify parsing
	HandshakeRequestType  MessageType = 1000
	HandshakeResponseType MessageType = 2000
)

type InvocationMessage struct {
	Type         MessageType `json:"type"`
	InvocationId string      `json:"invocationId,omitempty"`
	Target       string      `json:"target"`
	Arguments    []any       `json:"arguments"`
	StreamIds    *[]string   `json:"streamIds,omitempty"`
}

func NewInvocationMessage(
	invId string,
	target string,
	args []any,
	streamIds *[]string,
) *InvocationMessage {
	return &InvocationMessage{
		Type:         InvocationMessageType,
		InvocationId: invId,
		Target:       target,
		Arguments:    args,
		StreamIds:    streamIds,
	}
}

type StreamInvocationMessage struct {
	Type         MessageType `json:"type"`
	InvocationId string      `json:"invocationId"`
	Target       string      `json:"target"`
	Arguments    []any       `json:"arguments"`
}

func NewStreamInvocationMessage(
	invId string,
	target string,
	args []any,
) *StreamInvocationMessage {
	return &StreamInvocationMessage{
		Type:         StreamInvocationMessageType,
		InvocationId: invId,
		Target:       target,
		Arguments:    args,
	}
}

type StreamItemMessage struct {
	Type         MessageType     `json:"type"`
	InvocationId string          `json:"invocationId"`
	Item         json.RawMessage `json:"item"`
}

func NewStreamItemMessage(
	invId string,
	item []byte,
) *StreamItemMessage {
	return &StreamItemMessage{
		Type:         StreamItemMessageType,
		InvocationId: invId,
		Item:         item,
	}
}

type CompletionMessage struct {
	Type         MessageType      `json:"type"`
	InvocationId string           `json:"invocationId"`
	Result       *json.RawMessage `json:"result,omitempty"`
	Error        *string          `json:"error,omitempty"`
}

func NewCompletionMessage(
	invId string,
	result []byte,
	err *string,
) *CompletionMessage {
	r := json.RawMessage(result)
	return &CompletionMessage{
		Type:         CompletionMessageType,
		InvocationId: invId,
		Result:       &r,
		Error:        err,
	}
}

type CancelInvocationMessage struct {
	Type         MessageType `json:"type"`
	InvocationId string      `json:"invocationId"`
}

func NewCancelInvocationMessage(invId string) *CancelInvocationMessage {
	return &CancelInvocationMessage{
		Type:         CancelInvocationMessageType,
		InvocationId: invId,
	}
}

type PingMessage struct {
	Type MessageType `json:"type"`
}

func NewPingMessage() *PingMessage {
	return &PingMessage{
		Type: PingMessageType,
	}
}

type CloseMessage struct {
	Type           MessageType `json:"type"`
	Error          string      `json:"error"`
	AllowReconnect bool        `json:"allowReconnect"`
}

func NewCloseMessage() *CloseMessage {
	return &CloseMessage{
		Type: CloseMessageType,
	}
}

type AckMessage struct {
	Type MessageType `json:"type"`
}

func NewAckMessage() *AckMessage {
	return &AckMessage{
		Type: AckMessageType,
	}
}

type SequenceMessage struct {
	Type       MessageType `json:"type"`
	SequenceId int         `json:"sequenceId"`
}

func NewSequenceMessage(seqId int) *SequenceMessage {
	return &SequenceMessage{
		Type:       SequenceMessageType,
		SequenceId: seqId,
	}
}

type HandshakeRequest struct {
	Protocol string `json:"protocol"`
	Version  int    `json:"version"`
}

type HandshakeResponse struct {
	Error string `json:"error,omitempty"`
}
