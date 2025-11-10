package singnalr

import "context"

func (c *HubConnection[T]) SubscribeStream(ctx context.Context, name string, args []any) (chan []byte, chan StreamInvocationStatus, StreamInvocationStatus) {

	dataChan := make(chan []byte, 1)
	statusChan := make(chan StreamInvocationStatus, 1)

	if c.ctx.Err() != nil {
		close(dataChan)
		close(statusChan)
		return dataChan, statusChan, StreamInvocationStatusConnectionClosed
	}

	id := c.getInvocationId()
	c.SendStreamInvocation(NewStreamInvocationMessage(id, name, args))

	go (func() {
		for {
			select {
			case <-ctx.Done():
				{
					c.SendCancelInvocation(NewCancelInvocationMessage(id))
					statusChan <- StreamInvocationStatusCancelled
					close(dataChan)
					close(statusChan)
					return
				}
			case <-c.ctx.Done():
				{
					statusChan <- StreamInvocationStatusConnectionClosed
					close(dataChan)
					close(statusChan)
					return
				}
			case <-c.closeChan:
				{
					statusChan <- StreamInvocationStatusConnectionClosed
					close(dataChan)
					close(statusChan)
					return
				}
			case msg := <-c.streamItemChan:
				{
					c.customHandleStreamItem(msg)
					if msg.InvocationId != id {
						continue
					}
					dataChan <- msg.Item
				}
			case msg := <-c.completionChan:
				{
					c.customHandleCompletion(msg)
					if msg.InvocationId != id {
						continue
					}
					if msg.Error != nil {
						statusChan <- StreamInvocationStatusError
					} else {
						statusChan <- StreamInvocationStatusCompleted
					}
					close(dataChan)
					close(statusChan)
					return
				}
			}
		}
	})()

	return dataChan, statusChan, StreamInvocationStatusWaitMessages
}

func (c *HubConnection[T]) Invoke(ctx context.Context, name string, args []any) InvocationResponse {

	if c.ctx.Err() != nil {
		return InvocationResponse{
			Status: InvocationStatusConnectionClosed,
		}
	}

	id := c.getInvocationId()
	c.SendInvocation(NewInvocationMessage(id, name, args, nil))
	resp := InvocationResponse{}

	for {
		select {
		case <-c.closeChan:
			{
				resp.Status = InvocationStatusConnectionClosed
				return resp
			}
		case <-c.ctx.Done():
			{
				resp.Status = InvocationStatusConnectionClosed
				return resp
			}
		case <-ctx.Done():
			{
				c.SendCancelInvocation(NewCancelInvocationMessage(id))
				resp.Status = InvocationStatusCancelled
				return resp
			}
		case comp := <-c.completionChan:
			{
				c.customHandleCompletion(comp)
				if comp.InvocationId != id {
					continue
				}
				if comp.Error != nil {
					resp.Status = InvocationStatusError
					resp.Error = comp.Error
					return resp
				}
				resp.Status = InvocationStatusSuccess
				if comp.Result != nil {
					b := *comp.Result
					resp.Data = b
				}
				return resp
			}
		}
	}
	return resp
}
