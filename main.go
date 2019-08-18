package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	nknsdk "github.com/nknorg/nkn-sdk-go"
	"github.com/nknorg/nkn/common"
	"github.com/nknorg/nkn/crypto"
	"github.com/nknorg/nkn/util/address"
	"github.com/nknorg/nkn/vault"
)

func main() {
	receiveModePtr := flag.Bool("receive", false, "Receive mode")
	numClientsPtr := flag.Int("clients", 8, "Number of clients")
	seedPtr := flag.String("seed", "", "Secret seed")
	identifierPtr := flag.String("identifier", "", "NKN address identifier")
	numWorkersPtr := flag.Int("workers", 1024, "Number of workers (sender mode only)")
	chunkSizePtr := flag.Int("chunksize", 1024, "File chunk size in bytes (receive mode only)")
	bufSizePtr := flag.Int("bufsize", 32, "File buffer size in megabytes (receive mode only)")

	flag.Parse()

	if *numClientsPtr <= 0 {
		fmt.Printf("Number of clients needs to be greater than 0")
	}
	if *numWorkersPtr <= 0 {
		fmt.Printf("Number of workers per client needs to be greater than 0")
	}
	if *chunkSizePtr <= 0 {
		fmt.Printf("File chunk size needs to be greater than 0")
	}
	if *bufSizePtr <= 0 {
		fmt.Printf("File buffer size needs to be greater than 0")
	}

	rand.Seed(time.Now().Unix())

	nknsdk.Init()

	var account *vault.Account
	var err error

	if len(*seedPtr) > 0 {
		seed, err := common.HexStringToBytes(*seedPtr)
		if err != nil {
			fmt.Printf("Invalid secret seed: %v\n", err)
			os.Exit(1)
		}

		privateKey := crypto.GetPrivateKeyFromSeed(seed)
		if err = crypto.CheckPrivateKey(privateKey); err != nil {
			fmt.Printf("Invalid secret seed: %v\n", err)
			os.Exit(1)
		}

		account, err = vault.NewAccountWithPrivatekey(privateKey)
		if err != nil {
			fmt.Printf("Open account from secret seed error: %v\n", err)
			os.Exit(1)
		}
	} else {
		account, err = vault.NewAccount()
		if err != nil {
			fmt.Printf("New account error: %v\n", err)
			os.Exit(1)
		}
	}

	addr := address.MakeAddressString(account.PubKey().EncodePoint(), *identifierPtr)

	clients, err := CreateClients(account, *identifierPtr, *numClientsPtr, *receiveModePtr)
	if err != nil {
		fmt.Printf("Create clients error: %v\n", err)
		os.Exit(1)
	}

	if *receiveModePtr {
		receiver := NewReceiver(addr, clients, uint32(*chunkSizePtr), uint32((*bufSizePtr)*1024*1024/(*chunkSizePtr)))
		receiver.Start()
	} else {
		sender := NewSender(addr, clients, uint32(*numWorkersPtr))
		sender.Start()
	}
}
