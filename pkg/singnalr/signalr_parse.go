package singnalr

import "encoding/json"

type awesomeMsg struct {
	Type int `json:"type"`
}

func parseMessage(msg []byte) (MessageType, any) {
	awesome := awesomeMsg{}
	err := json.Unmarshal(msg, &awesome)
	if err != nil || awesome.Type < 1 {
		handRequest := HandshakeRequest{}
		err = json.Unmarshal(msg, &handRequest)
		if err != nil || handRequest.Protocol == "" {
			handResponse := HandshakeResponse{}
			err = json.Unmarshal(msg, &handResponse)
			if err == nil {
				return HandshakeResponseType, handResponse
			}
		} else {
			return HandshakeRequestType, handRequest
		}
	} else {
		switch awesome.Type {
		case InvocationMessageType:
			{
				m := InvocationMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return InvocationMessageType, m
				}
			}
		case StreamItemMessageType:
			{
				m := StreamItemMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return StreamItemMessageType, m
				}
			}
		case CompletionMessageType:
			{
				m := CompletionMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return CompletionMessageType, m
				}
			}
		case StreamInvocationMessageType:
			{
				m := StreamInvocationMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return StreamInvocationMessageType, m
				}
			}
		case CancelInvocationMessageType:
			{
				m := CancelInvocationMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return CancelInvocationMessageType, m
				}
			}
		case PingMessageType:
			{
				m := PingMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return PingMessageType, m
				}
			}
		case CloseMessageType:
			{
				m := CloseMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return CloseMessageType, m
				}
			}
		case AckMessageType:
			{
				m := AckMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return AckMessageType, m
				}
			}
		case SequenceMessageType:
			{
				m := SequenceMessage{}
				jsonErr := json.Unmarshal(msg, &m)
				if jsonErr == nil {
					return SequenceMessageType, m
				}
			}
		}
	}

	return 0, struct{}{}
}
