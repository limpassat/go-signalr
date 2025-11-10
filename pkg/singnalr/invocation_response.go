package singnalr

type InvocationResponseStatus = string

const (
	InvocationStatusSuccess          InvocationResponseStatus = "success"
	InvocationStatusCancelled        InvocationResponseStatus = "cancelled"
	InvocationStatusConnectionClosed InvocationResponseStatus = "connection_closed"
	InvocationStatusError            InvocationResponseStatus = "error"
)

type InvocationResponse struct {
	Status InvocationResponseStatus
	Data   []byte
	Error  *string
}
