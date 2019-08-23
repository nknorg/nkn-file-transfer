package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nknorg/consequential"
)

type Receiver struct {
	*transmitter
	dataChan      sync.Map
	pendingFiles  sync.Map
	chunkSize     uint32
	chunksBufSize uint32
}

func NewReceiver(config *Config) (*Receiver, error) {
	switch config.Mode {
	case MODE_RECEIVE:
	case MODE_GET:
	default:
		return nil, fmt.Errorf("unknown receiver mode: %v", config.Mode)
	}

	t, err := newTransmitter(config.Mode, config.Seed, config.Identifier, int(config.NumClients))
	if err != nil {
		return nil, err
	}

	receiver := &Receiver{
		transmitter:   t,
		chunkSize:     config.ChunkSize,
		chunksBufSize: config.ChunksBufSize,
	}

	return receiver, nil
}

func (receiver *Receiver) RequestToGetFile(ctx context.Context, senderAddr, fileName string, ranges []int64) (uint32, int64, error) {
	fileID := rand.Uint32()
	msg, err := NewRequestGetFileMessage(fileName, fileID, receiver.chunkSize, receiver.chunksBufSize, receiver.getAvailableClientIDs(), ranges)
	if err != nil {
		return 0, 0, err
	}

	receiver.pendingFiles.Store(fileID, nil)

	for i := 0; i < len(receiver.clients); i++ {
		if receiver.clients[i] == nil {
			continue
		}
		fmt.Printf("Request to get file %v from %s using client %d\n", fileName, senderAddr, i)
		addr := addIdentifier(senderAddr, i, receiver.getRemoteMode())
		err = receiver.send(uint32(i), addr, msg)
		if err != nil {
			fmt.Printf("Send RequestGetFile msg using client %d error: %v\n", i, err)
			continue
		}
		timeout := time.After(5 * time.Second)
		for {
			select {
			case reply := <-receiver.ctrlMsgChan:
				switch reply.msgType {
				case MSG_ACCEPT_GET_FILE:
					acceptGetFile := reply.msgBody.(*AcceptGetFile)
					if acceptGetFile.FileId != fileID {
						fmt.Printf("Ignore AcceptGetFile message from %v with incorrect file id %d\n", reply.src, acceptGetFile.FileId)
						continue
					}
					fmt.Printf("Sender %s accepted to send file %v (%d bytes) with ID %d\n", senderAddr, fileName, acceptGetFile.FileSize, fileID)
					return acceptGetFile.FileId, acceptGetFile.FileSize, nil
				case MSG_REJECT_GET_FILE:
					rejectGetFile := reply.msgBody.(*RejectGetFile)
					if rejectGetFile.FileId != fileID {
						fmt.Printf("Ignore RejectGetFile message from %v with incorrect file id %d\n", reply.src, rejectGetFile.FileId)
						continue
					}
					return 0, 0, fmt.Errorf("sender %s rejected to send file %s", senderAddr, fileName)
				default:
					fmt.Printf("Ignore message type %v\n", reply.msgType)
					continue
				}
			case <-timeout:
				fmt.Println("Wait for accept get file msg timeout")
				break
			case <-ctx.Done():
				return 0, 0, ctx.Err()
			}
			break
		}
	}

	receiver.pendingFiles.Delete(fileID)

	return 0, 0, fmt.Errorf("all clients failed")
}

func (receiver *Receiver) ReceiveFile(ctx context.Context, w io.Writer, fileID uint32, totalSize int64, chunkSize, chunksBufSize uint32) error {
	numChunks := uint32((totalSize-1)/int64(chunkSize)) + 1
	var bytesReceived int64

	receiveChunk := func(ctx context.Context, workerID, chunkID uint32) (interface{}, bool) {
		c, _ := receiver.dataChan.LoadOrStore(chanKey(fileID, chunkID), make(chan []byte, 1))
		var data []byte
		select {
		case data = <-c.(chan []byte):
		case <-ctx.Done():
			return 0, false
		}
		select {
		case c.(chan []byte) <- data:
			atomic.AddInt64(&bytesReceived, int64(len(data)))
		default:
		}
		return len(data), true
	}

	saveChunk := func(ctx context.Context, chunkID uint32, result interface{}) bool {
		key := chanKey(fileID, chunkID)
		c, ok := receiver.dataChan.Load(key)
		if !ok {
			fmt.Printf("Data chan for fileID %d chunkID %d does not exist", fileID, chunkID)
			return false
		}
		var data []byte
		select {
		case data = <-c.(chan []byte):
		case <-ctx.Done():
			return false
		}
		_, err := w.Write(data)
		if err != nil {
			fmt.Printf("Write to file error: %v", err)
			return false
		}
		receiver.dataChan.Delete(key)
		return true
	}

	cs, err := consequential.NewConSequential(&consequential.Config{
		StartJobID:          0,
		EndJobID:            numChunks - 1,
		JobBufSize:          chunksBufSize,
		WorkerPoolSize:      chunksBufSize,
		MaxWorkerFails:      0,
		WorkerStartInterval: 0,
		RunJob:              receiveChunk,
		FinishJob:           saveChunk,
	})
	if err != nil {
		return err
	}

	finished := false
	timeStart := time.Now()
	fmt.Printf("Start receiving file %d (%d bytes)\n", fileID, totalSize)

	go func() {
		time.Sleep(100 * time.Millisecond)
		receiver.pendingFiles.Delete(fileID)
	}()

	go func() {
		for {
			time.Sleep(time.Second)
			if finished {
				break
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
			received := atomic.LoadInt64(&bytesReceived)
			sec := float64(time.Since(timeStart)) / float64(time.Second)
			fmt.Printf("Time elapsed %3.0f s\t", sec)
			fmt.Printf("Received %10d bytes (%5.1f%%)\t", received, float64(received)/float64(totalSize)*100)
			fmt.Printf("%10.1f bytes/s\n", float64(received)/sec)
		}
	}()

	err = cs.Start(ctx)
	if err != nil {
		return err
	}

	finished = true
	duration := float64(time.Since(timeStart)) / float64(time.Second)
	fmt.Printf("Finish receiving file %d (%d bytes)\n", fileID, totalSize)
	fmt.Printf("Time used: %.1f s, %.0f bytes/s\n\n", duration, float64(totalSize)/duration)

	return nil
}

func (receiver *Receiver) CancelFile(senderAddr string, fileID uint32) {
	msg, err := NewCancelFileMessage(fileID)
	if err != nil {
		fmt.Printf("Create CancelFile message error: %v\n", err)
		return
	}
	for i := 0; i < len(receiver.clients); i++ {
		if receiver.clients[i] == nil {
			continue
		}
		addr := addIdentifier(senderAddr, i, receiver.getRemoteMode())
		receiver.send(uint32(i), addr, msg)
	}
}

func (receiver *Receiver) RequestAndGetFile(ctx context.Context, senderAddr, filePath, fileName string) error {
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		return fmt.Errorf("file %s exists", filePath)
	}

	fileID, fileSize, err := receiver.RequestToGetFile(ctx, senderAddr, fileName, nil)

	defer func() {
		select {
		case <-ctx.Done():
			receiver.CancelFile(senderAddr, fileID)
		default:
		}
	}()

	if err != nil {
		return fmt.Errorf("request to get file error: %v", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create file error: %v", err)
	}

	defer file.Close()

	err = receiver.ReceiveFile(ctx, file, fileID, fileSize, receiver.chunkSize, receiver.chunksBufSize)
	if err != nil {
		return fmt.Errorf("receive file error: %v", err)
	}

	return nil
}

func (receiver *Receiver) startHandleMsg() {
	for i := 0; i < len(receiver.clients); i++ {
		if receiver.clients[i] == nil {
			continue
		}
		go func(i int) {
			for {
				msg := <-receiver.clients[i].OnMessage
				if msg == nil {
					continue
				}
				msgBody, msgType, err := receiver.parseMessage(msg)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					continue
				}
				switch msgType {
				case MSG_REQUEST_SEND_FILE, MSG_ACCEPT_GET_FILE, MSG_REJECT_GET_FILE, MSG_CANCEL_FILE:
					cm := &ctrlMsg{
						msgType:    msgType,
						msgBody:    msgBody,
						src:        msg.Src,
						receivedBy: uint32(i),
					}
					select {
					case receiver.ctrlMsgChan <- cm:
					default:
						fmt.Printf("Control msg chan full, disgarding msg\n")
					}
				case MSG_FILE_CHUNK:
					fileChunk := msgBody.(*FileChunk)
					key := chanKey(fileChunk.FileId, fileChunk.ChunkId)
					var v interface{}
					var ok bool
					if _, ok = receiver.pendingFiles.Load(fileChunk.FileId); ok {
						v, _ = receiver.dataChan.LoadOrStore(key, make(chan []byte, 1))
					} else {
						v, ok = receiver.dataChan.Load(key)
					}
					if ok {
						select {
						case v.(chan []byte) <- fileChunk.Data:
						default:
						}
					}
					reply, err := NewFileChunkAckMessage(fileChunk.FileId, fileChunk.ChunkId)
					if err != nil {
						fmt.Printf("Create FileChunkAck message error: %v\n", err)
						continue
					}
					err = receiver.send(uint32(i), msg.Src, reply)
					if err != nil {
						fmt.Printf("Client %d send message error: %v\n", i, err)
						continue
					}
				default:
					fmt.Printf("Ignore message type %v\n", msgType)
				}
			}
		}(i)
	}
}

func (receiver *Receiver) shouldAcceptSendRequest(filepath string) error {
	ok, err := allowPath(filepath)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("path %s is not allowd", filepath)
	}

	_, err = os.Stat(filepath)

	if !os.IsNotExist(err) {
		return fmt.Errorf("file %s exists", filepath)
	}

	return nil
}

func (receiver *Receiver) startReceiveMode() {
	fmt.Printf("Start receiver in receive mode at %s\n", receiver.addr)
	for {
		msg := <-receiver.ctrlMsgChan
		switch msg.msgType {
		case MSG_REQUEST_SEND_FILE:
			requestSendFile := msg.msgBody.(*RequestSendFile)
			file, err := func() (*os.File, error) {
				if err := receiver.shouldAcceptSendRequest(requestSendFile.FileName); err != nil {
					return nil, err
				}
				return os.Create(requestSendFile.FileName)
			}()
			if err == nil {
				fileID := rand.Uint32()
				fmt.Printf("Accepting file %s (%d bytes) with ID %d\n", requestSendFile.FileName, requestSendFile.FileSize, fileID)
				err = func() error {
					reply, err := NewAcceptSendFileMessage(requestSendFile.RequestId, fileID, receiver.chunkSize, receiver.chunksBufSize, receiver.getAvailableClientIDs())
					if err != nil {
						return fmt.Errorf("create AcceptSendFile message error: %v", err)
					}
					err = receiver.send(msg.receivedBy, msg.src, reply)
					if err != nil {
						return fmt.Errorf("client %d send message error: %v", msg.receivedBy, err)
					}
					return nil
				}()
				if err != nil {
					fmt.Println(err)
					file.Close()
					os.Remove(requestSendFile.FileName)
					continue
				}
				go func() {
					defer file.Close()
					ctx, cancel := context.WithCancel(context.Background())
					receiver.cancelFunc.Store(fileID, cancel)
					err := receiver.ReceiveFile(ctx, file, fileID, requestSendFile.FileSize, receiver.chunkSize, receiver.chunksBufSize)
					if err != nil {
						fmt.Printf("Receive file error: %v\n", err)
					}
					receiver.cancelFunc.Delete(fileID)
				}()
			} else {
				fmt.Printf("Reject to send file %s: %v\n", requestSendFile.FileName, err)
				reply, err := NewRejectSendFileMessage(requestSendFile.RequestId)
				if err != nil {
					fmt.Printf("Create RejectSendFile message error: %v\n", err)
					continue
				}
				err = receiver.send(msg.receivedBy, msg.src, reply)
				if err != nil {
					fmt.Printf("Client %d send message error: %v\n", msg.receivedBy, err)
					continue
				}
				continue
			}
		case MSG_CANCEL_FILE:
			cancelFile := msg.msgBody.(*CancelFile)
			if v, ok := receiver.cancelFunc.Load(cancelFile.FileId); ok {
				if cf, ok := v.(context.CancelFunc); ok {
					cf()
				}
			}
		default:
			fmt.Printf("Ignore message type %v\n", msg.msgType)
			continue
		}
	}
}

func (receiver *Receiver) startGetMode() {
	fmt.Printf("Start receiver in get mode at %s\n", receiver.addr)
}

func (receiver *Receiver) Start(mode Mode) error {
	receiver.startHandleMsg()
	switch mode {
	case MODE_RECEIVE:
		go receiver.startReceiveMode()
	case MODE_GET:
		go receiver.startGetMode()
	default:
		return fmt.Errorf("unknown receiver mode %v", mode)
	}
	return nil
}
