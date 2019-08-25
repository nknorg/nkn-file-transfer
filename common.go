package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	nknsdk "github.com/nknorg/nkn-sdk-go"
	"github.com/nknorg/nkn/common"
	"github.com/nknorg/nkn/crypto"
	"github.com/nknorg/nkn/util/address"
	"github.com/nknorg/nkn/vault"
)

var seedNodeList = []string{
	"http://mainnet-seed-0001.nkn.org:30003",
	"http://mainnet-seed-0002.nkn.org:30003",
	"http://mainnet-seed-0003.nkn.org:30003",
	"http://mainnet-seed-0004.nkn.org:30003",
	"http://mainnet-seed-0005.nkn.org:30003",
	"http://mainnet-seed-0006.nkn.org:30003",
	"http://mainnet-seed-0007.nkn.org:30003",
	"http://mainnet-seed-0008.nkn.org:30003",
	"http://mainnet-seed-0009.nkn.org:30003",
	"http://mainnet-seed-0010.nkn.org:30003",
	"http://mainnet-seed-0011.nkn.org:30003",
	"http://mainnet-seed-0012.nkn.org:30003",
	"http://mainnet-seed-0013.nkn.org:30003",
	"http://mainnet-seed-0014.nkn.org:30003",
	"http://mainnet-seed-0015.nkn.org:30003",
	"http://mainnet-seed-0016.nkn.org:30003",
	"http://mainnet-seed-0017.nkn.org:30003",
	"http://mainnet-seed-0018.nkn.org:30003",
	"http://mainnet-seed-0019.nkn.org:30003",
	"http://mainnet-seed-0020.nkn.org:30003",
	"http://mainnet-seed-0021.nkn.org:30003",
	"http://mainnet-seed-0022.nkn.org:30003",
	"http://mainnet-seed-0023.nkn.org:30003",
	"http://mainnet-seed-0024.nkn.org:30003",
	"http://mainnet-seed-0025.nkn.org:30003",
	"http://mainnet-seed-0026.nkn.org:30003",
	"http://mainnet-seed-0027.nkn.org:30003",
	"http://mainnet-seed-0028.nkn.org:30003",
	"http://mainnet-seed-0029.nkn.org:30003",
	"http://mainnet-seed-0030.nkn.org:30003",
	"http://mainnet-seed-0031.nkn.org:30003",
	"http://mainnet-seed-0032.nkn.org:30003",
	"http://mainnet-seed-0033.nkn.org:30003",
	"http://mainnet-seed-0034.nkn.org:30003",
	"http://mainnet-seed-0035.nkn.org:30003",
	"http://mainnet-seed-0036.nkn.org:30003",
	"http://mainnet-seed-0037.nkn.org:30003",
	"http://mainnet-seed-0038.nkn.org:30003",
	"http://mainnet-seed-0039.nkn.org:30003",
	"http://mainnet-seed-0040.nkn.org:30003",
	"http://mainnet-seed-0041.nkn.org:30003",
	"http://mainnet-seed-0042.nkn.org:30003",
	"http://mainnet-seed-0043.nkn.org:30003",
	"http://mainnet-seed-0044.nkn.org:30003",
}

const (
	maxRetries                    = 3
	ctrlMsgChanLen                = 128
	nonceSize                     = 24
	sharedKeySize                 = 32
	sharedKeyCacheExpiration      = time.Hour
	sharedKeyCacheCleanupInterval = 10 * time.Minute
	maxClientFails                = 3
	replyTimeout                  = 5 * time.Second
)

type Config struct {
	Mode          Mode
	Seed          []byte
	Identifier    string
	NumClients    uint32
	NumWorkers    uint32
	ChunkSize     uint32
	ChunksBufSize uint32
}

type transmitter struct {
	mode           Mode
	addr           string
	account        *vault.Account
	clients        []*nknsdk.Client
	ctrlMsgChan    chan *receivedMsg
	replyChan      sync.Map
	sharedKeyCache common.Cache
	cancelFunc     sync.Map
	numWorkers     uint32
}

func newTransmitter(mode Mode, seed []byte, identifier string, numClients, numWorkers uint32) (*transmitter, error) {
	var account *vault.Account
	var err error
	if len(seed) > 0 {
		privateKey := crypto.GetPrivateKeyFromSeed(seed)
		if err = crypto.CheckPrivateKey(privateKey); err != nil {
			return nil, fmt.Errorf("invalid secret seed: %v", err)
		}

		account, err = vault.NewAccountWithPrivatekey(privateKey)
		if err != nil {
			return nil, fmt.Errorf("open account from secret seed error: %v", err)
		}
	} else {
		account, err = vault.NewAccount()
		if err != nil {
			return nil, fmt.Errorf("new account error: %v", err)
		}
	}

	addr := address.MakeAddressString(account.PubKey().EncodePoint(), identifier)

	clients, err := CreateClients(account, identifier, int(numClients), mode)
	if err != nil {
		return nil, fmt.Errorf("create clients error: %v", err)
	}

	t := &transmitter{
		mode:           mode,
		addr:           addr,
		account:        account,
		clients:        clients,
		numWorkers:     numWorkers,
		ctrlMsgChan:    make(chan *receivedMsg, ctrlMsgChanLen),
		sharedKeyCache: common.NewGoCache(sharedKeyCacheExpiration, sharedKeyCacheCleanupInterval),
	}

	return t, nil
}

func (t *transmitter) getAvailableClientIDs() []uint32 {
	availableClients := make([]uint32, 0)
	for i := 0; i < len(t.clients); i++ {
		if t.clients[i] != nil {
			availableClients = append(availableClients, uint32(i))
		}
	}
	return availableClients
}

func (t *transmitter) getRemoteMode() Mode {
	switch t.mode {
	case MODE_SEND:
		return MODE_RECEIVE
	case MODE_RECEIVE:
		return MODE_SEND
	case MODE_GET:
		return MODE_HOST
	case MODE_HOST:
		return MODE_GET
	}
	panic(fmt.Errorf("Unknown mode %v", t.mode))
}

func chanKey(fileID, chunkID uint32) string {
	return fmt.Sprintf("%d-%d", fileID, chunkID)
}

func getIdentifier(clientID int, mode Mode) string {
	return strconv.Itoa(clientID) + "." + strconv.Itoa(int(mode))
}

func addIdentifier(baseAddr string, clientID int, mode Mode) string {
	return getIdentifier(clientID, mode) + "." + baseAddr
}

func removeIdentifier(addr string) (string, error) {
	subs := strings.SplitN(addr, ".", 3)
	if len(subs) != 3 {
		return "", fmt.Errorf("invalid address %s", addr)
	}
	return subs[2], nil
}

func CreateClients(account *vault.Account, baseIdentifier string, numClients int, mode Mode) ([]*nknsdk.Client, error) {
	var wg sync.WaitGroup
	clients := make([]*nknsdk.Client, numClients)

	fmt.Printf("Creating %d clients...\n", numClients)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			identifier := getIdentifier(i, mode)
			if len(baseIdentifier) > 0 {
				identifier += "." + baseIdentifier
			}

			for retry := 0; retry < maxRetries; retry++ {
				client, err := nknsdk.NewClient(account, identifier, nknsdk.ClientConfig{
					SeedRPCServerAddr: seedNodeList[rand.Intn(len(seedNodeList))],
					ConnectRetries:    0,
				})
				if err != nil {
					fmt.Printf("Create sender %d error: %v, retry now...\n", i, err)
					continue
				}

				go func() {
					for _ = range client.OnBlock {
					}
				}()

				select {
				case <-client.OnConnect:
					clients[i] = client
					fmt.Printf("Client %s connect to node %s\n", client.Address, client.GetNodeInfo().Address)
					go func(client *nknsdk.Client) {
						for _ = range client.OnConnect {
							fmt.Printf("Client %s connect to node %s\n", client.Address, client.GetNodeInfo().Address)
						}
					}(client)
					return
				case <-time.After(3 * time.Second):
					fmt.Printf("Sender %d connect timeout, retry now...\n", i)
					continue
				}
			}
		}(i)
	}
	wg.Wait()

	created := 0
	for i := 0; i < numClients; i++ {
		if clients[i] != nil {
			created++
		}
	}

	fmt.Printf("Created %d clients\n", created)

	return clients, nil
}

func allowPath(path string) (bool, error) {
	if strings.Contains(path, "..") {
		return false, nil
	}

	wd, err := os.Getwd()
	if err != nil {
		return false, err
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}

	relPath, err := filepath.Rel(wd, absPath)
	if err != nil {
		return false, err
	}

	return !strings.HasPrefix(relPath, ".."), nil
}

func dispatchWorker(workerID, numSenders, numRececivers int) (int, int) {
	senderIdx := workerID % numSenders
	receiverIdx := (senderIdx + workerID/numSenders) % numRececivers
	return senderIdx, receiverIdx
}
