package singnalr

type StreamInvocationStatus = string

const (
	StreamInvocationStatusCancelled        InvocationResponseStatus = "cancelled"
	StreamInvocationStatusConnectionClosed InvocationResponseStatus = "connection_closed"
	StreamInvocationStatusError            InvocationResponseStatus = "error"
	StreamInvocationStatusCompleted        InvocationResponseStatus = "completed"
	StreamInvocationStatusWaitMessages     InvocationResponseStatus = "wait_messages"
)
