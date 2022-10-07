package message

import (
	"encoding/binary"
	"google.golang.org/protobuf/proto"
	"log"
	"net"
)

type MessageHandler struct {
	conn     net.Conn
	hostPort string
}

func NewMessageHandler(conn net.Conn, hostPort string) *MessageHandler {
	m := &MessageHandler{
		conn:     conn,
		hostPort: hostPort,
	}

	return m
}

func (m *MessageHandler) readN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) {
		n, err := m.conn.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (m *MessageHandler) writeN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) {
		n, err := m.conn.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

func (m *MessageHandler) Send(wrapper *Wrapper) error {
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.writeN(prefix)
	m.writeN(serialized)

	return nil
}

func (m *MessageHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	m.readN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.readN(payload)

	wrapper := &Wrapper{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

func (m *MessageHandler) Retry() (*MessageHandler, error) {
	if m.conn != nil {
		m.conn.Close()
	}

	var err error
	m.conn, err = net.Dial("tcp", m.hostPort)

	if err != nil {
		log.Println(err)
		return m, err
	}

	return m, nil
}

func (m *MessageHandler) Close() {
	m.conn.Close()
}
