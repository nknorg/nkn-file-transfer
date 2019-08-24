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

const (
	sendChunkTimeout       = 5 * time.Second
	maxGetFileChunkChanLen = 4096
)

type Sender struct {
	*transmitter
	getFileChunkChan sync.Map
	ackChan          sync.Map
	sentFiles        sync.Map
}

type getFileChunkMsg struct {
	chunkID    uint32
	src        string
	receivedBy uint32
}

func getFileChunkChanLen(chunksBufSize uint32) int {
	if chunksBufSize > maxGetFileChunkChanLen {
		return maxGetFileChunkChanLen
	}
	return int(chunksBufSize)
}

func NewSender(config *Config) (*Sender, error) {
	switch config.Mode {
	case MODE_SEND:
	case MODE_HOST:
	default:
		return nil, fmt.Errorf("unknown sender mode: %v", config.Mode)
	}

	t, err := newTransmitter(config.Mode, config.Seed, config.Identifier, config.NumClients, config.NumWorkers)
	if err != nil {
		return nil, err
	}

	sender := &Sender{
		transmitter: t,
	}

	return sender, nil
}

func (sender *Sender) RequestToSendFile(ctx context.Context, receiverAddr, fileName string, fileSize int64, mode TransmitMode) (uint32, uint32, uint32, []uint32, error) {
	requestID := rand.Uint32()
	msg, err := NewRequestSendFileMessage(requestID, fileName, fileSize, mode, sender.getAvailableClientIDs())
	if err != nil {
		return 0, 0, 0, nil, err
	}

	for i := 0; i < len(sender.clients); i++ {
		if sender.clients[i] == nil {
			continue
		}
		fmt.Printf("Request to send file %v (%d bytes) to %s using client %d\n", fileName, fileSize, receiverAddr, i)
		addr := addIdentifier(receiverAddr, i, sender.getRemoteMode())
		err = sender.send(uint32(i), addr, msg)
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
					if mode == TRANSMIT_MODE_PULL {
						sender.getFileChunkChan.LoadOrStore(acceptSendFile.FileId, make(chan *getFileChunkMsg, getFileChunkChanLen(acceptSendFile.ChunksBufSize)))
					}
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
			case <-ctx.Done():
				return 0, 0, 0, nil, ctx.Err()
			}
			break
		}
	}

	return 0, 0, 0, nil, fmt.Errorf("all clients failed")
}

func (sender *Sender) SendFile(ctx context.Context, rs io.ReadSeeker, receiverAddr string, fileID, chunkSize, chunksBufSize uint32, receiverClients []uint32, ranges []int64, mode TransmitMode) error {
	_, loaded := sender.sentFiles.LoadOrStore(fileID, struct{}{})
	if loaded {
		return fmt.Errorf("fileID %d has been sent", fileID)
	}

	defer time.AfterFunc(5*time.Second, func() {
		defer sender.CancelFile(receiverAddr, fileID)
	})

	defer sender.getFileChunkChan.Delete(fileID)

	fileSize, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek error: %v", err)
	}

	startPos := int64(0)
	endPos := fileSize - 1
	if len(ranges) > 0 {
		if ranges[0] >= fileSize || ranges[0] < -fileSize {
			return fmt.Errorf("startPos %d out of range", ranges[0])
		}
		startPos = ranges[0] % fileSize
	}
	if len(ranges) > 1 {
		if ranges[1] >= fileSize || ranges[1] < -fileSize {
			return fmt.Errorf("endPos %d out of range", ranges[1])
		}
		endPos = ranges[1] % fileSize
	}
	if startPos > endPos {
		return fmt.Errorf("startPos %d is greater than endPos %d", startPos, endPos)
	}
	totalSize := endPos - startPos + 1
	numChunks := uint32((totalSize-1)/int64(chunkSize)) + 1
	senderClients := sender.getAvailableClientIDs()

	var bytesSent, bytesConfirmed int64
	var readerLock sync.Mutex

	readChunk := func(chunkID uint32) ([]byte, error) {
		chunkStart := startPos + int64(chunkID)*int64(chunkSize)
		chunkEnd := chunkStart + int64(chunkSize) - 1
		if chunkEnd > endPos {
			chunkEnd = endPos
		}

		b := make([]byte, chunkEnd-chunkStart+1)

		readerLock.Lock()
		defer readerLock.Unlock()

		_, err = rs.Seek(chunkStart, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seeker error: %v", err)
		}

		n, err := rs.Read(b)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read file error: %v", err)
		}

		return b[:n], nil
	}

	finished := false
	timeStart := time.Now()
	fmt.Printf("Start sending file %d (%d bytes) to %s\n", fileID, totalSize, receiverAddr)

	defer func() {
		finished = true
		duration := float64(time.Since(timeStart)) / float64(time.Second)
		fmt.Printf("Finish sending file %d (%d bytes) to %s\n", fileID, totalSize, receiverAddr)
		fmt.Printf("Time used: %.1f s, %.0f bytes/s\n\n", duration, float64(totalSize)/duration)
	}()

	switch mode {
	case TRANSMIT_MODE_PUSH:
		sendChunk := func(ctx context.Context, workerID, chunkID uint32) (interface{}, bool) {
			data, err := readChunk(chunkID)
			if err != nil {
				fmt.Printf("Read chunk error: %v\n", err)
				return 0, false
			}

			msg, err := NewFileChunkMessage(fileID, chunkID, data)
			if err != nil {
				fmt.Printf("Create FileChunk message error: %v\n", err)
				return 0, false
			}

			idx := int(workerID) % (len(senderClients) * len(receiverClients))
			senderClientID := senderClients[idx%len(senderClients)]
			receiverClientID := receiverClients[idx/len(senderClients)]
			addr := addIdentifier(receiverAddr, int(receiverClientID), sender.getRemoteMode())
			err = sender.send(senderClientID, addr, msg)
			if err != nil {
				fmt.Printf("Send message from client %d to receiver client %d error: %v\n", senderClientID, receiverClientID, err)
				return 0, false
			}

			key := chanKey(fileID, chunkID)
			c, loaded := sender.ackChan.LoadOrStore(key, make(chan struct{}, 1))
			if !loaded {
				atomic.AddInt64(&bytesSent, int64(len(data)))
			}

			select {
			case <-c.(chan struct{}):
				atomic.AddInt64(&bytesConfirmed, int64(len(data)))
				sender.ackChan.Delete(key)
				return len(data), true
			case <-time.After(sendChunkTimeout):
				return 0, false
			case <-ctx.Done():
				return 0, false
			}
		}

		finishJob := func(ctx context.Context, chunkID uint32, result interface{}) bool {
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
				sent := atomic.LoadInt64(&bytesSent)
				confirmed := atomic.LoadInt64(&bytesConfirmed)
				sec := float64(time.Since(timeStart)) / float64(time.Second)
				fmt.Printf("Time elapsed %3.0f s\t", sec)
				fmt.Printf("Sent %10d bytes (%5.1f%%)\t\t", sent, float64(sent)/float64(totalSize)*100)
				fmt.Printf("Confirmed %10d bytes (%5.1f%%)\t", confirmed, float64(confirmed)/float64(totalSize)*100)
				fmt.Printf("%10.1f bytes/s\n", float64(confirmed)/sec)
			}
		}()

		err = cs.Start(ctx)
		if err != nil {
			return err
		}
	case TRANSMIT_MODE_PULL:
		v, _ := sender.getFileChunkChan.LoadOrStore(fileID, make(chan *getFileChunkMsg, getFileChunkChanLen(chunksBufSize)))
	out:
		for {
			select {
			case m, ok := <-v.(chan *getFileChunkMsg):
				if !ok {
					break out
				}

				data, err := readChunk(m.chunkID)
				if err != nil {
					fmt.Printf("Read chunk error: %v\n", err)
					continue
				}

				msg, err := NewFileChunkMessage(fileID, m.chunkID, data)
				if err != nil {
					return fmt.Errorf("create FileChunk message error: %v", err)
				}

				err = sender.send(m.receivedBy, m.src, msg)
				if err != nil {
					fmt.Printf("Send message from client %d to %s error: %v\n", m.receivedBy, m.src, err)
					continue
				}
			case <-ctx.Done():
				if mode == TRANSMIT_MODE_PULL {
					fmt.Println(ctx.Err())
					return nil
				}
				return ctx.Err()
			}
		}
	default:
		return fmt.Errorf("unknown mode: %v", mode)
	}

	return nil
}

func (sender *Sender) CancelFile(receiverAddr string, fileID uint32) {
	msg, err := NewCancelFileMessage(fileID)
	if err != nil {
		fmt.Printf("Create CancelFile message error: %v\n", err)
		return
	}
	for i := 0; i < len(sender.clients); i++ {
		if sender.clients[i] == nil {
			continue
		}
		addr := addIdentifier(receiverAddr, i, sender.getRemoteMode())
		sender.send(uint32(i), addr, msg)
	}
}

func (sender *Sender) RequestAndSendFile(ctx context.Context, receiverAddr, filePath, fileName string, fileSize int64, mode TransmitMode) error {
	fileID, chunkSize, chunksBufSize, availableClients, err := sender.RequestToSendFile(ctx, receiverAddr, fileName, fileSize, mode)

	defer func() {
		select {
		case <-ctx.Done():
			sender.CancelFile(receiverAddr, fileID)
		default:
		}
	}()

	if err != nil {
		return fmt.Errorf("request to send file error: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file error: %v", err)
	}

	defer file.Close()

	err = sender.SendFile(ctx, file, receiverAddr, fileID, chunkSize, chunksBufSize, availableClients, nil, mode)
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
			for msg := range sender.clients[i].OnMessage {
				msgBody, msgType, err := sender.parseMessage(msg)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					continue
				}
				switch msgType {
				case MSG_ACCEPT_SEND_FILE, MSG_REJECT_SEND_FILE, MSG_REQUEST_GET_FILE, MSG_CANCEL_FILE:
					cm := &ctrlMsg{
						msgType:    msgType,
						msgBody:    msgBody,
						src:        msg.Src,
						receivedBy: uint32(i),
					}
					select {
					case sender.ctrlMsgChan <- cm:
					default:
						fmt.Printf("Control msg chan full, disgarding msg\n")
					}
				case MSG_GET_FILE_CHUNK:
					getFileChunk := msgBody.(*GetFileChunk)
					v, ok := sender.getFileChunkChan.Load(getFileChunk.FileId)
					if !ok {
						continue
					}
					m := &getFileChunkMsg{
						chunkID:    getFileChunk.ChunkId,
						src:        msg.Src,
						receivedBy: uint32(i),
					}
					select {
					case v.(chan *getFileChunkMsg) <- m:
					default:
						fmt.Printf("getFileChunkChan of fileID %d is full, discarding msg...\n", getFileChunk.FileId)
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
			baseAddr, err := removeIdentifier(msg.src)
			if err != nil {
				fmt.Printf("Remove identifier error: %v\n", err)
				continue
			}
			file, err := func() (*os.File, error) {
				if err := sender.shouldAcceptGetRequest(requestGetFile.FileName); err != nil {
					return nil, err
				}
				return os.Open(requestGetFile.FileName)
			}()
			if err == nil {
				err = func() error {
					fileInfo, err := file.Stat()
					if err != nil {
						return fmt.Errorf("get file info of %s error: %v", requestGetFile.FileName, err)
					}
					fileSize := fileInfo.Size()
					fmt.Printf("Sending file %s (%d bytes) with ID %d\n", requestGetFile.FileName, fileSize, requestGetFile.FileId)
					reply, err := NewAcceptGetFileMessage(requestGetFile.FileId, fileSize, sender.getAvailableClientIDs())
					if err != nil {
						return fmt.Errorf("create AcceptGetFile message error: %v", err)
					}
					err = sender.send(msg.receivedBy, msg.src, reply)
					if err != nil {
						return fmt.Errorf("client %d send message error: %v", msg.receivedBy, err)
					}
					return nil
				}()
				if err != nil {
					fmt.Println(err)
					file.Close()
					continue
				}
				go func() {
					defer file.Close()
					if requestGetFile.Mode == TRANSMIT_MODE_PULL {
						sender.getFileChunkChan.LoadOrStore(requestGetFile.FileId, make(chan *getFileChunkMsg, getFileChunkChanLen(requestGetFile.ChunksBufSize)))
					}
					ctx, cancel := context.WithCancel(context.Background())
					sender.cancelFunc.Store(requestGetFile.FileId, cancel)
					err := sender.SendFile(ctx, file, baseAddr, requestGetFile.FileId, requestGetFile.ChunkSize, requestGetFile.ChunksBufSize, requestGetFile.Clients, requestGetFile.Ranges, requestGetFile.Mode)
					if err != nil {
						fmt.Printf("Send file error: %v\n", err)
					}
					sender.cancelFunc.Delete(requestGetFile.FileId)
				}()
			} else {
				fmt.Printf("Reject to get file %s: %v\n", requestGetFile.FileName, err)
				reply, err := NewRejectGetFileMessage(requestGetFile.FileId)
				if err != nil {
					fmt.Printf("Create RejectGetFile message error: %v\n", err)
					continue
				}
				err = sender.send(msg.receivedBy, msg.src, reply)
				if err != nil {
					fmt.Printf("Client %d send message error: %v\n", msg.receivedBy, err)
					continue
				}
				continue
			}
		case MSG_CANCEL_FILE:
			cancelFile := msg.msgBody.(*CancelFile)
			if v, ok := sender.cancelFunc.Load(cancelFile.FileId); ok {
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
