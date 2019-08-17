package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	nknsdk "github.com/nknorg/nkn-sdk-go"
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

const maxRetries = 3

func chanKey(fileID, chunkID uint32) string {
	return fmt.Sprintf("%d-%d", fileID, chunkID)
}

func addIdentifier(baseAddr string, i int) string {
	return strconv.Itoa(i) + "." + baseAddr
}

func CreateClients(account *vault.Account, baseIdentifier string, numClients int) ([]*nknsdk.Client, error) {
	var wg sync.WaitGroup
	clients := make([]*nknsdk.Client, numClients)
	rand.Seed(time.Now().Unix())

	fmt.Printf("Creating %d clients...\n", numClients)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			identifier := strconv.Itoa(i)
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
