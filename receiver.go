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
	dataChan         sync.Map
	minChunkID       sync.Map
	fileTransmitMode sync.Map
	chunkSize        uint32
	chunksBufSize    uint32
}

func NewReceiver(config *Config) (*Receiver, error) {
	switch config.Mode {
	case MODE_RECEIVE:
	case MODE_GET:
	default:
		return nil, fmt.Errorf("unknown receiver mode: %v", config.Mode)
	}

	t, err := newTransmitter(config.Mode, config.Seed, config.Identifier, config.NumClients, config.NumWorkers)
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

func (receiver *Receiver) RequestToGetFile(ctx context.Context, senderAddr, fileName string, ranges []int64, mode TransmitMode) (uint32, int64, []uint32, error) {
	fileID := rand.Uint32()
	msg, err := NewRequestGetFileMessage(fileName, fileID, receiver.chunkSize, receiver.chunksBufSize, receiver.getAvailableClientIDs(), ranges, mode)
	if err != nil {
		return 0, 0, nil, err
	}

	receiver.minChunkID.LoadOrStore(fileID, uint32(0))
	receiver.fileTransmitMode.Store(fileID, mode)

	if ok := receiver.sendAll(senderAddr, msg, nil); !ok {
		return 0, 0, nil, fmt.Errorf("send RequestGetFile msg using all clients failed")
	}

	c, _ := receiver.replyChan.LoadOrStore(fileID, make(chan *receivedMsg, 0))
	defer receiver.replyChan.Delete(fileID)

	select {
	case reply := <-c.(chan *receivedMsg):
		switch reply.msgType {
		case MSG_ACCEPT_GET_FILE:
			acceptGetFile := reply.msgBody.(*AcceptGetFile)
			fmt.Printf("Sender %s accepted to send file %v (%d bytes) with ID %d\n", senderAddr, fileName, acceptGetFile.FileSize, fileID)
			return acceptGetFile.FileId, acceptGetFile.FileSize, acceptGetFile.Clients, nil
		case MSG_REJECT_GET_FILE:
			return 0, 0, nil, fmt.Errorf("sender %s rejected to send file %s", senderAddr, fileName)
		default:
			return 0, 0, nil, fmt.Errorf("invalid reply type %v", reply.msgType)
		}
	case <-time.After(replyTimeout):
		return 0, 0, nil, fmt.Errorf("wait for accept get file msg timeout")
	case <-ctx.Done():
		return 0, 0, nil, ctx.Err()
	}
}

func (receiver *Receiver) ReceiveFile(ctx context.Context, w io.Writer, senderAddr string, senderClients []uint32, fileID uint32, totalSize int64, mode TransmitMode) error {
	defer time.AfterFunc(5*time.Second, func() {
		receiver.CancelFile(senderAddr, fileID, senderClients)
	})

	defer time.AfterFunc(time.Minute, func() {
		receiver.minChunkID.Delete(fileID)
		receiver.fileTransmitMode.Delete(fileID)
	})

	numChunks := uint32((totalSize-1)/int64(receiver.chunkSize)) + 1
	var bytesReceived int64
	receiverClients := receiver.getAvailableClientIDs()

	receiveChunk := func(ctx context.Context, workerID, chunkID uint32) (interface{}, bool) {
		var timeout <-chan time.Time
		if mode == TRANSMIT_MODE_PULL {
			msg, err := NewGetFileChunkMessage(fileID, chunkID)
			if err != nil {
				fmt.Printf("Create GetFileChunk message error: %v\n", err)
				return 0, false
			}

			senderIdx, receiverIdx := dispatchWorker(int(workerID), len(senderClients), len(receiverClients))
			senderClientID := senderClients[senderIdx]
			receiverClientID := receiverClients[receiverIdx]
			addr := addIdentifier(senderAddr, int(senderClientID), receiver.getRemoteMode())

			err = receiver.send(receiverClientID, addr, msg)
			if err != nil {
				fmt.Printf("Send message from client %d to receiver client %d error: %v\n", senderClientID, receiverClientID, err)
				return 0, false
			}

			timeout = time.After(replyTimeout)
		}

		c, _ := receiver.dataChan.LoadOrStore(chanKey(fileID, chunkID), make(chan []byte, 1))
		var data []byte
		select {
		case data = <-c.(chan []byte):
		case <-timeout:
			return 0, false
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
			fmt.Printf("Data chan for fileID %d chunkID %d does not exist\n", fileID, chunkID)
			return false
		}
		var data []byte
		select {
		case data = <-c.(chan []byte):
		case <-ctx.Done():
			return false
		}
		receiver.minChunkID.Store(fileID, chunkID+1)
		_, err := w.Write(data)
		if err != nil {
			fmt.Printf("Write to file error: %v\n", err)
			return false
		}
		receiver.dataChan.Delete(key)
		return true
	}

	cs, err := consequential.NewConSequential(&consequential.Config{
		StartJobID:          0,
		EndJobID:            numChunks - 1,
		JobBufSize:          receiver.chunksBufSize,
		WorkerPoolSize:      receiver.numWorkers,
		MaxWorkerFails:      maxClientFails,
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

	defer func() {
		finished = true
		duration := float64(time.Since(timeStart)) / float64(time.Second)
		fmt.Printf("Finish receiving file %d (%d bytes)\n", fileID, totalSize)
		fmt.Printf("Time used: %.1f s, %.0f bytes/s\n\n", duration, float64(totalSize)/duration)
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

	return nil
}

func (receiver *Receiver) CancelFile(senderAddr string, fileID uint32, senderClients []uint32) {
	msg, err := NewCancelFileMessage(fileID)
	if err != nil {
		fmt.Printf("Create CancelFile message error: %v\n", err)
		return
	}
	receiver.sendAll(senderAddr, msg, senderClients)
}

func (receiver *Receiver) RequestAndGetFile(ctx context.Context, senderAddr, filePath, fileName string, mode TransmitMode) error {
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		return fmt.Errorf("file %s exists", filePath)
	}

	fileID, fileSize, senderClients, err := receiver.RequestToGetFile(ctx, senderAddr, fileName, nil, mode)

	defer func() {
		select {
		case <-ctx.Done():
			receiver.CancelFile(senderAddr, fileID, senderClients)
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

	err = receiver.ReceiveFile(ctx, file, senderAddr, senderClients, fileID, fileSize, mode)
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
			for msg := range receiver.clients[i].OnMessage {
				msgBody, msgType, err := receiver.parseMessage(msg)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					continue
				}
				switch msgType {
				case MSG_REQUEST_SEND_FILE, MSG_CANCEL_FILE:
					m := &receivedMsg{
						msgType:    msgType,
						msgBody:    msgBody,
						src:        msg.Src,
						receivedBy: uint32(i),
					}
					select {
					case receiver.ctrlMsgChan <- m:
					default:
						fmt.Printf("Control msg chan full, disgarding msg\n")
					}
				case MSG_ACCEPT_GET_FILE:
					acceptGetFile := msgBody.(*AcceptGetFile)
					if c, ok := receiver.replyChan.Load(acceptGetFile.FileId); ok {
						m := &receivedMsg{
							msgType:    msgType,
							msgBody:    msgBody,
							src:        msg.Src,
							receivedBy: uint32(i),
						}
						select {
						case c.(chan *receivedMsg) <- m:
						default:
						}
					}
				case MSG_REJECT_GET_FILE:
					rejectGetFile := msgBody.(*RejectGetFile)
					if c, ok := receiver.replyChan.Load(rejectGetFile.FileId); ok {
						m := &receivedMsg{
							msgType:    msgType,
							msgBody:    msgBody,
							src:        msg.Src,
							receivedBy: uint32(i),
						}
						select {
						case c.(chan *receivedMsg) <- m:
						default:
						}
					}
				case MSG_FILE_CHUNK:
					fileChunk := msgBody.(*FileChunk)
					key := chanKey(fileChunk.FileId, fileChunk.ChunkId)
					if v, ok := receiver.minChunkID.Load(fileChunk.FileId); ok {
						if fileChunk.ChunkId >= v.(uint32) {
							v, _ = receiver.dataChan.LoadOrStore(key, make(chan []byte, 1))
							select {
							case v.(chan []byte) <- fileChunk.Data:
							default:
							}
						}
					}
					if v, ok := receiver.fileTransmitMode.Load(fileChunk.FileId); ok {
						if v.(TransmitMode) == TRANSMIT_MODE_PULL {
							continue
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
			senderAddr, err := removeIdentifier(msg.src)
			if err != nil {
				fmt.Printf("Remove identifier error: %v\n", err)
				continue
			}
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
					if ok := receiver.sendAll(senderAddr, reply, requestSendFile.Clients); !ok {
						return fmt.Errorf("send AcceptSendFile message failed")
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
					receiver.minChunkID.LoadOrStore(fileID, uint32(0))
					receiver.fileTransmitMode.Store(fileID, requestSendFile.Mode)
					ctx, cancel := context.WithCancel(context.Background())
					receiver.cancelFunc.Store(fileID, cancel)
					err := receiver.ReceiveFile(ctx, file, senderAddr, requestSendFile.Clients, fileID, requestSendFile.FileSize, requestSendFile.Mode)
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
				if ok := receiver.sendAll(senderAddr, reply, requestSendFile.Clients); !ok {
					fmt.Printf("Send RejectSendFile message failed\n")
					continue
				}
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
