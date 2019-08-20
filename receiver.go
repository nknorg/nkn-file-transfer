package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nknorg/nkn/node/consequential"
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

func (receiver *Receiver) RequestToGetFile(senderAddr, fileName string) (uint32, int64, error) {
	fileID := rand.Uint32()
	msg, err := NewRequestGetFileMessage(fileName, fileID, receiver.chunkSize, receiver.chunksBufSize, receiver.getAvailableClientIDs())
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
		err = receiver.clients[i].Send([]string{addr}, msg, 0)
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
			}
			break
		}
	}

	receiver.pendingFiles.Delete(fileID)

	return 0, 0, fmt.Errorf("all clients failed")
}

func (receiver *Receiver) ReceiveFile(fileID uint32, filePath string, fileSize int64, chunkSize, chunksBufSize uint32) error {
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		return fmt.Errorf("file %s exists", filePath)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	numChunks := uint32((fileSize-1)/int64(chunkSize)) + 1
	var bytesReceived int64

	receiveChunk := func(workerID, chunkID uint32) (interface{}, bool) {
		c, _ := receiver.dataChan.LoadOrStore(chanKey(fileID, chunkID), make(chan []byte, 1))
		data := <-c.(chan []byte)
		select {
		case c.(chan []byte) <- data:
			atomic.AddInt64(&bytesReceived, int64(len(data)))
		default:
		}
		return len(data), true
	}

	saveChunk := func(chunkID uint32, result interface{}) bool {
		key := chanKey(fileID, chunkID)
		c, ok := receiver.dataChan.Load(key)
		if !ok {
			fmt.Printf("Data chan for fileID %d chunkID %d does not exist", fileID, chunkID)
			return false
		}
		data := <-c.(chan []byte)
		_, err := file.Write(data)
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
	fmt.Printf("Start receiving file %v (%d bytes)\n", filePath, fileSize)

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
			received := atomic.LoadInt64(&bytesReceived)
			sec := float64(time.Since(timeStart)) / float64(time.Second)
			fmt.Printf("Time elapsed %3.0f s\t", sec)
			fmt.Printf("Received %10d bytes (%5.1f%%)\t", received, float64(received)/float64(fileSize)*100)
			fmt.Printf("%10.1f bytes/s\n", float64(received)/sec)
		}
	}()

	err = cs.Start()
	if err != nil {
		return err
	}

	finished = true
	duration := float64(time.Since(timeStart)) / float64(time.Second)
	fmt.Printf("Finish receiving file %v (%d bytes)\n", filePath, fileSize)
	fmt.Printf("Time used: %.1f s, %.0f bytes/s\n\n", duration, float64(fileSize)/duration)

	return nil
}

func (receiver *Receiver) RequestAndGetFile(senderAddr, filePath, fileName string) error {
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		return fmt.Errorf("file %s exists", filePath)
	}

	fileID, fileSize, err := receiver.RequestToGetFile(senderAddr, fileName)
	if err != nil {
		return fmt.Errorf("request to get file error: %v", err)
	}

	err = receiver.ReceiveFile(fileID, filePath, fileSize, receiver.chunkSize, receiver.chunksBufSize)
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
				msgBody, msgType, err := ParseMessage(msg.Payload)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					continue
				}
				switch msgType {
				case MSG_REQUEST_SEND_FILE, MSG_ACCEPT_GET_FILE, MSG_REJECT_GET_FILE:
					cm := &ctrlMsg{
						msgType:    msgType,
						msgBody:    msgBody,
						src:        msg.Src,
						receivedBy: i,
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
					err = receiver.clients[i].Send([]string{msg.Src}, reply, 0)
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
			if err := receiver.shouldAcceptSendRequest(requestSendFile.FileName); err == nil {
				fileID := rand.Uint32()
				fmt.Printf("Accepting file %s (%d bytes) with ID %d\n", requestSendFile.FileName, requestSendFile.FileSize, fileID)
				reply, err := NewAcceptSendFileMessage(requestSendFile.RequestId, fileID, receiver.chunkSize, receiver.chunksBufSize, receiver.getAvailableClientIDs())
				if err != nil {
					fmt.Printf("Create AcceptSendFile message error: %v\n", err)
					continue
				}
				err = receiver.clients[msg.receivedBy].Send([]string{msg.src}, reply, 0)
				if err != nil {
					fmt.Printf("Client %d send message error: %v\n", msg.receivedBy, err)
					continue
				}
				go func() {
					err := receiver.ReceiveFile(fileID, requestSendFile.FileName, requestSendFile.FileSize, receiver.chunkSize, receiver.chunksBufSize)
					if err != nil {
						fmt.Printf("Receive file error: %v\n", err)
					}
				}()
			} else {
				fmt.Printf("Reject to send file %s: %v\n", requestSendFile.FileName, err)
				reply, err := NewRejectSendFileMessage(requestSendFile.RequestId)
				if err != nil {
					fmt.Printf("Create RejectSendFile message error: %v\n", err)
					continue
				}
				err = receiver.clients[msg.receivedBy].Send([]string{msg.src}, reply, 0)
				if err != nil {
					fmt.Printf("Client %d send message error: %v\n", msg.receivedBy, err)
					continue
				}
				continue
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
