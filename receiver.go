package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	nknsdk "github.com/nknorg/nkn-sdk-go"
	"github.com/nknorg/nkn/node/consequential"
)

const (
	dataChanLen = 1024
)

type Receiver struct {
	addr          string
	clients       []*nknsdk.Client
	dataChan      sync.Map
	chunkSize     uint32
	chunksBufSize uint32
}

func NewReceiver(addr string, clients []*nknsdk.Client, chunkSize, chunksBufSize uint32) *Receiver {
	return &Receiver{
		addr:          addr,
		clients:       clients,
		chunkSize:     chunkSize,
		chunksBufSize: chunksBufSize,
	}
}

func (receiver *Receiver) receiveFile(fileID uint32, fileName string, fileSize int64, chunkSize, chunksBufSize uint32) error {
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		return fmt.Errorf("File %s exists", fileName)
	}

	file, err := os.Create(fileName)
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
	fmt.Printf("Start receiving file %v (%d bytes)\n", fileName, fileSize)

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
	fmt.Printf("Finish receiving file %v (%d bytes)\n", fileName, fileSize)
	fmt.Printf("Time used: %.1f s, %.0f bytes/s\n", duration, float64(fileSize)/duration)

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
				msgBody, msgType, err := ParseMessage(msg.Payload)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					continue
				}
				switch msgType {
				case MSG_REQUEST_TO_SEND_FILE:
					requestToSendFile := msgBody.(*RequestToSendFile)
					if _, err := os.Stat(requestToSendFile.FileName); !os.IsNotExist(err) {
						fmt.Printf("File %s exists, reject request\n", requestToSendFile.FileName)
						reply, err := NewRejectFileMessage(requestToSendFile.RequestId)
						if err != nil {
							fmt.Printf("Create RejectFile message error: %v\n", err)
							continue
						}
						err = receiver.clients[i].Send([]string{msg.Src}, reply, 0)
						if err != nil {
							fmt.Printf("Client %d send message error: %v\n", i, err)
							continue
						}
						continue
					}
					fileID := rand.Uint32()
					fmt.Printf("Accepting file %s (%d bytes) with ID %d\n", requestToSendFile.FileName, requestToSendFile.FileSize, fileID)
					availableClients := make([]uint32, 0)
					for i := 0; i < len(receiver.clients); i++ {
						if receiver.clients[i] != nil {
							availableClients = append(availableClients, uint32(i))
						}
					}
					reply, err := NewAcceptFileMessage(requestToSendFile.RequestId, fileID, receiver.chunkSize, receiver.chunksBufSize, availableClients)
					if err != nil {
						fmt.Printf("Create AcceptFile message error: %v\n", err)
						continue
					}
					err = receiver.clients[i].Send([]string{msg.Src}, reply, 0)
					if err != nil {
						fmt.Printf("Client %d send message error: %v\n", i, err)
						continue
					}
					go func() {
						err := receiver.receiveFile(fileID, requestToSendFile.FileName, requestToSendFile.FileSize, receiver.chunkSize, receiver.chunksBufSize)
						if err != nil {
							fmt.Printf("Receive file error: %v\n", err)
						}
					}()
				case MSG_FILE_CHUNK:
					fileChunk := msgBody.(*FileChunk)
					if v, ok := receiver.dataChan.Load(chanKey(fileChunk.FileId, fileChunk.ChunkId)); ok {
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

func (receiver *Receiver) Start() {
	fmt.Printf("Start receiver at %s\n", receiver.addr)
	receiver.startHandleMsg()
	select {}
}
