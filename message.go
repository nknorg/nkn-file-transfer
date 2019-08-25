package main

import (
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/nknorg/nkn/crypto/ed25519"
	"github.com/nknorg/nkn/pb"
	"github.com/nknorg/nkn/util/address"
	"golang.org/x/crypto/nacl/box"
)

type receivedMsg struct {
	msgType    MessageType
	msgBody    interface{}
	src        string
	receivedBy uint32
}

func encryptMessage(message []byte, sharedKey *[sharedKeySize]byte) ([]byte, error) {
	var nonce [nonceSize]byte
	_, err := rand.Read(nonce[:])
	if err != nil {
		return nil, err
	}

	encrypted := make([]byte, len(message)+box.Overhead+nonceSize)
	copy(encrypted[:nonceSize], nonce[:])
	box.SealAfterPrecomputation(encrypted[nonceSize:nonceSize], message, &nonce, sharedKey)

	return encrypted, nil
}

func decryptMessage(message []byte, sharedKey *[sharedKeySize]byte) ([]byte, error) {
	if len(message) < nonceSize+box.Overhead {
		return nil, fmt.Errorf("encrypted message should have at least %d bytes", nonceSize+box.Overhead)
	}

	var nonce [nonceSize]byte
	copy(nonce[:], message[:nonceSize])
	decrypted := make([]byte, len(message)-nonceSize-box.Overhead)
	_, ok := box.OpenAfterPrecomputation(decrypted[:0], message[nonceSize:], &nonce, sharedKey)
	if !ok {
		return nil, errors.New("decrypt message failed")
	}

	return decrypted, nil
}

func (t *transmitter) getOrComputeSharedKey(remotePublicKey []byte) (*[sharedKeySize]byte, error) {
	if v, ok := t.sharedKeyCache.Get(remotePublicKey); ok {
		if sharedKey, ok := v.(*[sharedKeySize]byte); ok {
			return sharedKey, nil
		}
	}

	if len(remotePublicKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key length is %d, expecting %d", len(remotePublicKey), ed25519.PublicKeySize)
	}

	var pk [ed25519.PublicKeySize]byte
	copy(pk[:], remotePublicKey)
	curve25519PublicKey, ok := ed25519.PublicKeyToCurve25519PublicKey(&pk)
	if !ok {
		return nil, fmt.Errorf("converting public key %x to curve25519 public key failed", remotePublicKey)
	}

	var sk [ed25519.PrivateKeySize]byte
	copy(sk[:], t.account.PrivateKey)
	curve25519PrivateKey := ed25519.PrivateKeyToCurve25519PrivateKey(&sk)

	var sharedKey [sharedKeySize]byte
	box.Precompute(&sharedKey, curve25519PublicKey, curve25519PrivateKey)

	t.sharedKeyCache.Set(remotePublicKey, &sharedKey)

	return &sharedKey, nil
}

func (t *transmitter) send(clientID uint32, dest string, msg []byte) error {
	_, destPublicKey, _, err := address.ParseClientAddress(dest)
	if err != nil {
		return err
	}

	sharedKey, err := t.getOrComputeSharedKey(destPublicKey)
	if err != nil {
		return err
	}

	encrypted, err := encryptMessage(msg, sharedKey)
	if err != nil {
		return err
	}

	return t.clients[clientID].Send([]string{dest}, encrypted, 0)
}

func (t *transmitter) sendAll(remoteAddr string, msg []byte, remoteClients []uint32) bool {
	clientIDs := t.getAvailableClientIDs()
	success := false
	if len(remoteClients) == 0 {
		for i := 0; i < len(t.clients); i++ {
			dest := addIdentifier(remoteAddr, i, t.getRemoteMode())
			if err := t.send(clientIDs[i%len(clientIDs)], dest, msg); err == nil {
				success = true
			}
		}
	} else {
		for i, remoteClientID := range remoteClients {
			dest := addIdentifier(remoteAddr, int(remoteClientID), t.getRemoteMode())
			if err := t.send(clientIDs[i%len(clientIDs)], dest, msg); err == nil {
				success = true
			}
		}
	}
	return success
}

func (t *transmitter) parseMessage(pbmsg *pb.InboundMessage) (interface{}, MessageType, error) {
	_, srcPublicKey, _, err := address.ParseClientAddress(pbmsg.Src)
	if err != nil {
		return nil, 0, err
	}

	sharedKey, err := t.getOrComputeSharedKey(srcPublicKey)
	if err != nil {
		return nil, 0, err
	}

	decrypted, err := decryptMessage(pbmsg.Payload, sharedKey)
	if err != nil {
		return nil, 0, err
	}

	msg := &Message{}
	err = proto.Unmarshal(decrypted, msg)
	if err != nil {
		return nil, 0, err
	}

	var msgBody proto.Message
	switch msg.Type {
	case MSG_REQUEST_SEND_FILE:
		msgBody = &RequestSendFile{}
	case MSG_ACCEPT_SEND_FILE:
		msgBody = &AcceptSendFile{}
	case MSG_REJECT_SEND_FILE:
		msgBody = &RejectSendFile{}
	case MSG_GET_FILE_CHUNK:
		msgBody = &GetFileChunk{}
	case MSG_FILE_CHUNK:
		msgBody = &FileChunk{}
	case MSG_FILE_CHUNK_ACK:
		msgBody = &FileChunkAck{}
	case MSG_REQUEST_GET_FILE:
		msgBody = &RequestGetFile{}
	case MSG_ACCEPT_GET_FILE:
		msgBody = &AcceptGetFile{}
	case MSG_REJECT_GET_FILE:
		msgBody = &RejectGetFile{}
	case MSG_CANCEL_FILE:
		msgBody = &CancelFile{}
	default:
		return nil, msg.Type, fmt.Errorf("unknown message type %v", msg.Type)
	}

	err = proto.Unmarshal(msg.Body, msgBody)
	if err != nil {
		return nil, msg.Type, err
	}
	return msgBody, msg.Type, nil
}

func NewRequestSendFileMessage(requestID uint32, fileName string, fileSize int64, mode TransmitMode, clients []uint32) ([]byte, error) {
	msgBody := &RequestSendFile{
		RequestId: requestID,
		FileName:  fileName,
		FileSize:  fileSize,
		Mode:      mode,
		Clients:   clients,
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

func NewGetFileChunkMessage(fileID, chunkID uint32) ([]byte, error) {
	msgBody := &GetFileChunk{
		FileId:  fileID,
		ChunkId: chunkID,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_GET_FILE_CHUNK,
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

func NewRequestGetFileMessage(fileName string, fileID, chunkSize, chunksBufSize uint32, clients []uint32, ranges []int64, mode TransmitMode) ([]byte, error) {
	msgBody := &RequestGetFile{
		FileName:      fileName,
		FileId:        fileID,
		ChunkSize:     chunkSize,
		ChunksBufSize: chunksBufSize,
		Clients:       clients,
		Ranges:        ranges,
		Mode:          mode,
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

func NewAcceptGetFileMessage(fileID uint32, fileSize int64, clients []uint32) ([]byte, error) {
	msgBody := &AcceptGetFile{
		FileId:   fileID,
		FileSize: fileSize,
		Clients:  clients,
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

func NewCancelFileMessage(fileID uint32) ([]byte, error) {
	msgBody := &CancelFile{
		FileId: fileID,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &Message{
		Type: MSG_CANCEL_FILE,
		Body: buf,
	}

	return proto.Marshal(msg)
}
