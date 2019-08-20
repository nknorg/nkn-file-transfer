package main

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
)

type ctrlMsg struct {
	msgType    MessageType
	msgBody    interface{}
	src        string
	receivedBy int
}

func ParseMessage(buf []byte) (interface{}, MessageType, error) {
	msg := &Message{}
	err := proto.Unmarshal(buf, msg)
	if err != nil {
		return nil, 0, err
	}

	switch msg.Type {
	case MSG_REQUEST_SEND_FILE:
		msgBody := &RequestSendFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_ACCEPT_SEND_FILE:
		msgBody := &AcceptSendFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_REJECT_SEND_FILE:
		msgBody := &RejectSendFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_FILE_CHUNK:
		msgBody := &FileChunk{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_FILE_CHUNK_ACK:
		msgBody := &FileChunkAck{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_REQUEST_GET_FILE:
		msgBody := &RequestGetFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_ACCEPT_GET_FILE:
		msgBody := &AcceptGetFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_REJECT_GET_FILE:
		msgBody := &RejectGetFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	}

	return nil, msg.Type, fmt.Errorf("unknown message type %v", msg.Type)
}

func NewRequestSendFileMessage(requestID uint32, fileName string, fileSize int64) ([]byte, error) {
	msgBody := &RequestSendFile{
		RequestId: requestID,
		FileName:  fileName,
		FileSize:  fileSize,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_REQUEST_SEND_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewAcceptSendFileMessage(requestID, fileID, chunkSize, chunksBufSize uint32, clients []uint32) ([]byte, error) {
	msgBody := &AcceptSendFile{
		RequestId:     requestID,
		FileId:        fileID,
		ChunkSize:     chunkSize,
		ChunksBufSize: chunksBufSize,
		Clients:       clients,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_ACCEPT_SEND_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewRejectSendFileMessage(requestID uint32) ([]byte, error) {
	msgBody := &RejectSendFile{
		RequestId: requestID,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_REJECT_SEND_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewFileChunkMessage(fileID, chunkID uint32, data []byte) ([]byte, error) {
	msgBody := &FileChunk{
		FileId:  fileID,
		ChunkId: chunkID,
		Data:    data,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_FILE_CHUNK,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewFileChunkAckMessage(fileID, chunkID uint32) ([]byte, error) {
	msgBody := &FileChunkAck{
		FileId:  fileID,
		ChunkId: chunkID,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_FILE_CHUNK_ACK,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewRequestGetFileMessage(fileName string, fileID, chunkSize, chunksBufSize uint32, clients []uint32) ([]byte, error) {
	msgBody := &RequestGetFile{
		FileName:      fileName,
		FileId:        fileID,
		ChunkSize:     chunkSize,
		ChunksBufSize: chunksBufSize,
		Clients:       clients,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_REQUEST_GET_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewAcceptGetFileMessage(fileID uint32, fileSize int64) ([]byte, error) {
	msgBody := &AcceptGetFile{
		FileId:   fileID,
		FileSize: fileSize,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_ACCEPT_GET_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewRejectGetFileMessage(fileID uint32) ([]byte, error) {
	msgBody := &RejectGetFile{
		FileId: fileID,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_REJECT_GET_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}
