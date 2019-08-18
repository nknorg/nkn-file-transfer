package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nknsdk "github.com/nknorg/nkn-sdk-go"
	"github.com/nknorg/nkn/node/consequential"
	"github.com/nknorg/nkn/pb"
)

const (
	maxClientFails   = 3
	sendChunkTimeout = 3 * time.Second
)

type Sender struct {
	addr        string
	clients     []*nknsdk.Client
	ctrlMsgChan chan *pb.InboundMessage
	ackChan     sync.Map
	numWorkers  uint32
}

func NewSender(addr string, clients []*nknsdk.Client, numWorkers uint32) *Sender {
	return &Sender{
		addr:        addr,
		clients:     clients,
		ctrlMsgChan: make(chan *pb.InboundMessage, 100),
		numWorkers:  numWorkers,
	}
}

func getFilePath(scanner *bufio.Scanner) (string, os.FileInfo, error) {
	fmt.Print("\n\nEnter file path to send: ")
	scanner.Scan()
	filePath := scanner.Text()
	filePath = strings.Trim(filePath, " ")
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return "", nil, err
	}
	if fileInfo.IsDir() {
		return "", nil, fmt.Errorf("%s is a directory", filePath)
	}
	fmt.Printf("Name: %s\nSize: %d bytes\n", fileInfo.Name(), fileInfo.Size())
	return filePath, fileInfo, nil
}

func getReceiverAddr(scanner *bufio.Scanner) (string, error) {
	fmt.Print("Enter receiver address: ")
	scanner.Scan()
	receiverAddr := scanner.Text()
	receiverAddr = strings.Trim(receiverAddr, " ")
	if len(receiverAddr) == 0 {
		return "", fmt.Errorf("empty receiver address")
	}
	return receiverAddr, nil
}

func (sender *Sender) RequestToSendFile(receiverAddr, fileName string, fileSize int64) (uint32, uint32, uint32, []uint32, error) {
	requestID := rand.Uint32()
	msg, err := NewRequestToSendFileMessage(requestID, fileName, fileSize)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	for i := 0; i < len(sender.clients); i++ {
		if sender.clients[i] == nil {
			continue
		}
		fmt.Printf("Request to send file %v (%d bytes) to %s using client %d\n", fileName, fileSize, receiverAddr, i)
		addr := addIdentifier(receiverAddr, i, true)
		err = sender.clients[i].Send([]string{addr}, msg, 0)
		if err != nil {
			return 0, 0, 0, nil, err
		}
		timeout := time.After(5 * time.Second)
		for {
			select {
			case reply := <-sender.ctrlMsgChan:
				msgBody, msgType, err := ParseMessage(reply.Payload)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					break
				}
				switch msgType {
				case MSG_ACCEPT_FILE:
					acceptFile := msgBody.(*AcceptFile)
					if acceptFile.RequestId != requestID {
						fmt.Printf("Ignore AcceptFile message from %v with incorrect request id %d\n", reply.Src, acceptFile.RequestId)
						continue
					}
					fmt.Printf("Receiver %s accepted file %v (%d bytes)\n", receiverAddr, fileName, fileSize)
					fmt.Printf(
						"File ID: %d\nChunkSize: %d\nChunksBufSize: %d\nReceiverClients: %d\n",
						acceptFile.FileId, acceptFile.ChunkSize, acceptFile.ChunksBufSize, len(acceptFile.Clients),
					)
					return acceptFile.FileId, acceptFile.ChunkSize, acceptFile.ChunksBufSize, acceptFile.Clients, nil
				case MSG_REJECT_FILE:
					rejectFile := msgBody.(*RejectFile)
					if rejectFile.RequestId != requestID {
						fmt.Printf("Ignore RejectFile message from %v with incorrect request id %d\n", reply.Src, rejectFile.RequestId)
						continue
					}
					return 0, 0, 0, nil, fmt.Errorf("Receiver %s rejected file %s", receiverAddr, fileName)
				default:
					fmt.Printf("Ignore message type %v\n", msgType)
					continue
				}
			case <-timeout:
				fmt.Println("Wait for accept file msg timeout")
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
	senderClients := make([]uint32, 0)
	for i := 0; i < len(sender.clients); i++ {
		if sender.clients[i] != nil {
			senderClients = append(senderClients, uint32(i))
		}
	}
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
		addr := addIdentifier(receiverAddr, int(receiverClientID), true)
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
	fmt.Printf("Time used: %.1f s, %.0f bytes/s\n", duration, float64(fileSize)/duration)

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
				msgBody, msgType, err := ParseMessage(msg.Payload)
				if err != nil {
					fmt.Printf("Parse message error: %v\n", err)
					continue
				}
				switch msgType {
				case MSG_ACCEPT_FILE:
					fallthrough
				case MSG_REJECT_FILE:
					sender.ctrlMsgChan <- msg
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

func (sender *Sender) Start() {
	fmt.Printf("Start sender at %s\n", sender.addr)
	sender.startHandleMsg()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		filePath, fileInfo, err := getFilePath(scanner)
		if err != nil {
			fmt.Printf("Get file path error: %v\n", err)
			continue
		}

		receiverAddr, err := getReceiverAddr(scanner)
		if err != nil {
			fmt.Printf("Get receiver address error: %v\n", err)
			continue
		}

		fileID, chunkSize, chunksBufSize, availableClients, err := sender.RequestToSendFile(receiverAddr, fileInfo.Name(), fileInfo.Size())
		if err != nil {
			fmt.Printf("Request to send file error: %v\n", err)
			continue
		}

		err = sender.SendFile(receiverAddr, filePath, fileID, chunkSize, chunksBufSize, availableClients)
		if err != nil {
			fmt.Printf("Send file error: %v\n", err)
			continue
		}
	}
}
