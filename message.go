package main

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
)

func ParseMessage(buf []byte) (interface{}, MessageType, error) {
	msg := &Message{}
	err := proto.Unmarshal(buf, msg)
	if err != nil {
		return nil, 0, err
	}

	switch msg.Type {
	case MSG_REQUEST_TO_SEND_FILE:
		msgBody := &RequestToSendFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_ACCEPT_FILE:
		msgBody := &AcceptFile{}
		err = proto.Unmarshal(msg.Body, msgBody)
		if err != nil {
			return nil, msg.Type, err
		}
		return msgBody, msg.Type, nil
	case MSG_REJECT_FILE:
		msgBody := &RejectFile{}
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
	}

	return nil, msg.Type, fmt.Errorf("Unknown message type %v", msg.Type)
}

func NewRequestToSendFileMessage(requestID uint32, fileName string, fileSize int64) ([]byte, error) {
	msgBody := &RequestToSendFile{
		RequestId: requestID,
		FileName:  fileName,
		FileSize:  fileSize,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_REQUEST_TO_SEND_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewAcceptFileMessage(requestID, fileID, chunkSize, chunksBufSize uint32, clients []uint32) ([]byte, error) {
	msgBody := &AcceptFile{
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
		Type: MSG_ACCEPT_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}

func NewRejectFileMessage(requestID uint32) ([]byte, error) {
	msgBody := &RejectFile{
		RequestId: requestID,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_REJECT_FILE,
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
