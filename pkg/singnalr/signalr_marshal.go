package singnalr

import "bytes"

const signalRSeparator = byte(0x1e)

func marshal(msg []byte) []byte {
	return append(msg, signalRSeparator)
}

func unmarshal(msg []byte) [][]byte {
	splatted := bytes.Split(msg, []byte{signalRSeparator})
	if len(splatted) == 0 {
		return [][]byte{}
	}
	return splatted[:len(splatted)-1]
}
