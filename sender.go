package main

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nknorg/nkn/node/consequential"
)

const (
	maxClientFails   = 3
	sendChunkTimeout = 3 * time.Second
)

type Sender struct {
	*transmitter
	ackChan    sync.Map
	numWorkers uint32
}

func NewSender(config *Config) (*Sender, error) {
	switch config.Mode {
	case MODE_SEND:
	case MODE_HOST:
	default:
		return nil, fmt.Errorf("unknown sender mode: %v", config.Mode)
	}

	t, err := newTransmitter(config.Mode, config.Seed, config.Identifier, int(config.NumClients))
	if err != nil {
		return nil, err
	}

	sender := &Sender{
		transmitter: t,
		numWorkers:  config.NumWorkers,
	}

	return sender, nil
}

func (sender *Sender) RequestToSendFile(receiverAddr, fileName string, fileSize int64) (uint32, uint32, uint32, []uint32, error) {
	requestID := rand.Uint32()
	msg, err := NewRequestSendFileMessage(requestID, fileName, fileSize)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	for i := 0; i < len(sender.clients); i++ {
		if sender.clients[i] == nil {
			continue
		}
		fmt.Printf("Request to send file %v (%d bytes) to %s using client %d\n", fileName, fileSize, receiverAddr, i)
		addr := addIdentifier(receiverAddr, i, sender.getRemoteMode())
		err = sender.clients[i].Send([]string{addr}, msg, 0)
		if err != nil {
			fmt.Printf("Send RequestSendFile msg using client %d error: %v\n", i, err)
			continue
		}
		timeout := time.After(5 * time.Second)
		for {
			select {
			case reply := <-sender.ctrlMsgChan:
				switch reply.msgType {
				case MSG_ACCEPT_SEND_FILE:
					acceptSendFile := reply.msgBody.(*AcceptSendFile)
					if acceptSendFile.RequestId != requestID {
						fmt.Printf("Ignore AcceptSendFile message from %v with incorrect request id %d\n", reply.src, acceptSendFile.RequestId)
						continue
					}
					fmt.Printf("Receiver %s accepted file %v (%d bytes)\n", receiverAddr, fileName, fileSize)
					fmt.Printf(
						"File ID: %d\nChunkSize: %d\nChunksBufSize: %d\nReceiverClients: %d\n",
						acceptSendFile.FileId, acceptSendFile.ChunkSize, acceptSendFile.ChunksBufSize, len(acceptSendFile.Clients),
					)
					return acceptSendFile.FileId, acceptSendFile.ChunkSize, acceptSendFile.ChunksBufSize, acceptSendFile.Clients, nil
				case MSG_REJECT_SEND_FILE:
					rejectSendFile := reply.msgBody.(*RejectSendFile)
					if rejectSendFile.RequestId != requestID {
						fmt.Printf("Ignore RejectSendFile message from %v with incorrect request id %d\n", reply.src, rejectSendFile.RequestId)
						continue
					}
					return 0, 0, 0, nil, fmt.Errorf("receiver %s rejected file %s", receiverAddr, fileName)
				default:
					fmt.Printf("Ignore message type %v\n", reply.msgType)
					continue
				}
			case <-timeout:
				fmt.Println("Wait for accept send file msg timeout")
				break
			}
			break
		}
	}

	return 0, 0, 0, nil, fmt.Errorf("all clients failed")
}

func (sender *Sender) SendFile(receiverAddr, filePath string, fileID, chunkSize, chunksBufSize uint32, receiverClients []uint32) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	fileName := fileInfo.Name()
	fileSize := fileInfo.Size()
	numChunks := uint32((fileSize-1)/int64(chunkSize)) + 1
	senderClients := sender.getAvailableClientIDs()

	var bytesSent, bytesConfirmed int64

	sendChunk := func(workerID, chunkID uint32) (interface{}, bool) {
		b := make([]byte, chunkSize)
		n, err := file.ReadAt(b, int64(chunkID)*int64(chunkSize))
		if err != nil && err != io.EOF {
			fmt.Printf("Read file error: %v\n", err)
			return nil, false
		}

		msg, err := NewFileChunkMessage(fileID, chunkID, b[:n])
		if err != nil {
			fmt.Printf("Create FileChunk message error: %v\n", err)
			return nil, false
		}

		idx := int(workerID) % (len(senderClients) * len(receiverClients))
		senderClientID := senderClients[idx%len(senderClients)]
		receiverClientID := receiverClients[idx/len(senderClients)]
		addr := addIdentifier(receiverAddr, int(receiverClientID), sender.getRemoteMode())
		err = sender.clients[senderClientID].Send([]string{addr}, msg, 0)
		if err != nil {
			fmt.Printf("Send message from client %d to receiver client %d error: %v\n", senderClientID, receiverClientID, err)
			return nil, false
		}

		key := chanKey(fileID, chunkID)
		c, loaded := sender.ackChan.LoadOrStore(key, make(chan struct{}, 1))
		if !loaded {
			atomic.AddInt64(&bytesSent, int64(n))
		}

		select {
		case <-c.(chan struct{}):
			atomic.AddInt64(&bytesConfirmed, int64(n))
			sender.ackChan.Delete(key)
			return n, true
		case <-time.After(sendChunkTimeout):
			return nil, false
		}
	}

	finishJob := func(chunkID uint32, result interface{}) bool {
		return true
	}

	cs, err := consequential.NewConSequential(&consequential.Config{
		StartJobID:          0,
		EndJobID:            numChunks - 1,
		JobBufSize:          chunksBufSize,
		WorkerPoolSize:      sender.numWorkers,
		MaxWorkerFails:      maxClientFails,
		WorkerStartInterval: 0,
		RunJob:              sendChunk,
		FinishJob:           finishJob,
	})
	if err != nil {
		return err
	}

	finished := false
	timeStart := time.Now()
	fmt.Printf("Start sending file %v (%d bytes) to %s\n", fileName, fileSize, receiverAddr)

	go func() {
		for {
			time.Sleep(time.Second)
			if finished {
				break
			}
			sent := atomic.LoadInt64(&bytesSent)
			confirmed := atomic.LoadInt64(&bytesConfirmed)
			sec := float64(time.Since(timeStart)) / float64(time.Second)
			fmt.Printf("Time elapsed %3.0f s\t", sec)
			fmt.Printf("Sent %10d bytes (%5.1f%%)\t\t", sent, float64(sent)/float64(fileSize)*100)
			fmt.Printf("Confirmed %10d bytes (%5.1f%%)\t", confirmed, float64(confirmed)/float64(fileSize)*100)
			fmt.Printf("%10.1f bytes/s\n", float64(confirmed)/sec)
		}
	}()

	err = cs.Start()
	if err != nil {
		return err
	}

	finished = true
	duration := float64(time.Since(timeStart)) / float64(time.Second)
	fmt.Printf("Finish sending file %v (%d bytes) to %s\n", fileName, fileSize, receiverAddr)
	fmt.Printf("Time used: %.1f s, %.0f bytes/s\n\n", duration, float64(fileSize)/duration)

	return nil
}

func (sender *Sender) RequestAndSendFile(receiverAddr, filePath, fileName string, fileSize int64) error {
	fileID, chunkSize, chunksBufSize, availableClients, err := sender.RequestToSendFile(receiverAddr, fileName, fileSize)
	if err != nil {
		return fmt.Errorf("request to send file error: %v", err)
	}

	err = sender.SendFile(receiverAddr, filePath, fileID, chunkSize, chunksBufSize, availableClients)
	if err != nil {
		return fmt.Errorf("send file error: %v", err)
	}

	return nil
}

func (sender *Sender) startHandleMsg() {
	for i := 0; i < len(sender.clients); i++ {
		if sender.clients[i] == nil {
			continue
		}
		go func(i int) {
			for {
				msg := <-sender.clients[i].OnMessage
				if msg == nil {
					continue
				}
				msgBody, msgType, err := ParseMessage(msg.Payload)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					continue
				}
				switch msgType {
				case MSG_ACCEPT_SEND_FILE, MSG_REJECT_SEND_FILE, MSG_REQUEST_GET_FILE:
					cm := &ctrlMsg{
						msgType:    msgType,
						msgBody:    msgBody,
						src:        msg.Src,
						receivedBy: i,
					}
					select {
					case sender.ctrlMsgChan <- cm:
					default:
						fmt.Printf("Control msg chan full, disgarding msg\n")
					}
				case MSG_FILE_CHUNK_ACK:
					fileChunkAck := msgBody.(*FileChunkAck)
					if v, ok := sender.ackChan.Load(chanKey(fileChunkAck.FileId, fileChunkAck.ChunkId)); ok {
						select {
						case v.(chan struct{}) <- struct{}{}:
						default:
						}
					}
				default:
					fmt.Printf("Ignore message type %v\n", msgType)
				}
			}
		}(i)
	}
}

func (sender *Sender) startSendMode() {
	fmt.Printf("Start sender in send mode at %s\n", sender.addr)
}

func (sender *Sender) shouldAcceptGetRequest(filepath string) error {
	ok, err := allowPath(filepath)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("path %s is not allowd", filepath)
	}

	_, err = os.Stat(filepath)

	if os.IsNotExist(err) {
		return fmt.Errorf("file %s does not exist", filepath)
	}

	return nil
}

func (sender *Sender) startHostMode() {
	fmt.Printf("Start sender in host mode at %s\n", sender.addr)
	for {
		msg := <-sender.ctrlMsgChan
		switch msg.msgType {
		case MSG_REQUEST_GET_FILE:
			requestGetFile := msg.msgBody.(*RequestGetFile)
			if err := sender.shouldAcceptGetRequest(requestGetFile.FileName); err == nil {
				fileInfo, err := os.Stat(requestGetFile.FileName)
				if err != nil {
					fmt.Printf("Get file info of %s error: %v\n", requestGetFile.FileName, err)
					continue
				}
				fileSize := fileInfo.Size()
				fmt.Printf("Sending file %s (%d bytes) with ID %d\n", requestGetFile.FileName, fileSize, requestGetFile.FileId)
				reply, err := NewAcceptGetFileMessage(requestGetFile.FileId, fileSize)
				if err != nil {
					fmt.Printf("Create AcceptGetFile message error: %v\n", err)
					continue
				}
				err = sender.clients[msg.receivedBy].Send([]string{msg.src}, reply, 0)
				if err != nil {
					fmt.Printf("Client %d send message error: %v\n", msg.receivedBy, err)
					continue
				}
				baseAddr, err := removeIdentifier(msg.src)
				if err != nil {
					fmt.Printf("Remove identifier error: %v\n", err)
					continue
				}
				go func() {
					err := sender.SendFile(baseAddr, requestGetFile.FileName, requestGetFile.FileId, requestGetFile.ChunkSize, requestGetFile.ChunksBufSize, requestGetFile.Clients)
					if err != nil {
						fmt.Printf("Send file error: %v\n", err)
					}
				}()
			} else {
				fmt.Printf("Reject to get file %s: %v\n", requestGetFile.FileName, err)
				reply, err := NewRejectGetFileMessage(requestGetFile.FileId)
				if err != nil {
					fmt.Printf("Create RejectGetFile message error: %v\n", err)
					continue
				}
				err = sender.clients[msg.receivedBy].Send([]string{msg.src}, reply, 0)
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

func (sender *Sender) Start(mode Mode) error {
	sender.startHandleMsg()
	switch mode {
	case MODE_SEND:
		go sender.startSendMode()
	case MODE_HOST:
		go sender.startHostMode()
	default:
		return fmt.Errorf("unknown sender mode %v", mode)
	}
	return nil
}
