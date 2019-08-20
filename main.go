package main

import (
	"bufio"
	cryptorand "crypto/rand"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	nknsdk "github.com/nknorg/nkn-sdk-go"
	"github.com/nknorg/nkn/common"
	"golang.org/x/crypto/ed25519"
)

func getStringFromScanner(scanner *bufio.Scanner, allowEmpty bool) (string, error) {
	scanner.Scan()
	s := scanner.Text()
	s = strings.Trim(s, " ")
	if !allowEmpty && len(s) == 0 {
		return "", fmt.Errorf("empty string")
	}
	return s, nil
}

func getFileInfo(filePath string) (os.FileInfo, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	if fileInfo.IsDir() {
		return nil, fmt.Errorf("%s is a directory", filePath)
	}
	fmt.Printf("Name: %s\nSize: %d bytes\n", fileInfo.Name(), fileInfo.Size())
	return fileInfo, nil
}

func main() {
	sendModePtr := flag.Bool("send", false, "Enable send mode")
	receiveModePtr := flag.Bool("receive", false, "Enable receive mode")
	getModePtr := flag.Bool("get", false, "Enable get mode")
	hostModePtr := flag.Bool("host", false, "Enable host mode")
	seedPtr := flag.String("seed", "", "Secret seed")
	identifierPtr := flag.String("identifier", "", "NKN address identifier")
	numClientsPtr := flag.Int("clients", 8, "Number of clients")
	numWorkersPtr := flag.Int("workers", 1024, "Number of workers (sender mode only)")
	chunkSizePtr := flag.Int("chunksize", 1024, "File chunk size in bytes (receive mode only)")
	bufSizePtr := flag.Int("bufsize", 32, "File buffer size in megabytes (receive mode only)")

	flag.Parse()

	if !*sendModePtr && !*receiveModePtr && !*getModePtr && !*hostModePtr {
		fmt.Printf("At least one mode should be enabled\n")
		os.Exit(1)
	}

	if *numClientsPtr <= 0 {
		fmt.Println("Number of clients needs to be greater than 0")
		os.Exit(1)
	}
	if *numWorkersPtr <= 0 {
		fmt.Println("Number of workers per client needs to be greater than 0")
		os.Exit(1)
	}
	if *chunkSizePtr <= 0 {
		fmt.Println("File chunk size needs to be greater than 0")
		os.Exit(1)
	}
	if *bufSizePtr <= 0 {
		fmt.Println("File buffer size needs to be greater than 0")
		os.Exit(1)
	}

	rand.Seed(time.Now().Unix())

	nknsdk.Init()

	var seed []byte
	var err error
	if len(*seedPtr) > 0 {
		seed, err = common.HexStringToBytes(*seedPtr)
		if err != nil {
			fmt.Printf("Invalid secret seed: %v\n", err)
			os.Exit(1)
		}
		if len(seed) != ed25519.SeedSize {
			fmt.Printf("Invalid seed length %d, should be %d\n", len(seed), ed25519.SeedSize)
			os.Exit(1)
		}
	} else {
		seed = make([]byte, ed25519.SeedSize)
		if _, err = io.ReadFull(cryptorand.Reader, seed); err != nil {
			fmt.Printf("Generate secret seed error: %v\n", err)
			os.Exit(1)
		}
	}

	baseConfig := Config{
		Seed:          seed,
		Identifier:    *identifierPtr,
		NumClients:    uint32(*numClientsPtr),
		NumWorkers:    uint32(*numWorkersPtr),
		ChunkSize:     uint32(*chunkSizePtr),
		ChunksBufSize: uint32((*bufSizePtr) * 1024 * 1024 / (*chunkSizePtr)),
	}

	var sender, hoster *Sender
	var receiver, getter *Receiver
	var wg sync.WaitGroup

	if *sendModePtr {
		wg.Add(1)
		go func() {
			defer wg.Done()
			config := baseConfig
			config.Mode = MODE_SEND
			sender, err = NewSender(&config)
			if err != nil {
				fmt.Printf("Create sender error: %v\n", err)
				os.Exit(1)
			}
			err = sender.Start(MODE_SEND)
			if err != nil {
				fmt.Printf("Start sender error: %v\n", err)
				os.Exit(1)
			}
		}()
	}

	if *receiveModePtr {
		wg.Add(1)
		go func() {
			defer wg.Done()
			config := baseConfig
			config.Mode = MODE_RECEIVE
			receiver, err = NewReceiver(&config)
			if err != nil {
				fmt.Printf("Create receiver error: %v\n", err)
				os.Exit(1)
			}
			err = receiver.Start(MODE_RECEIVE)
			if err != nil {
				fmt.Printf("Start receiver error: %v\n", err)
				os.Exit(1)
			}
		}()
	}

	if *getModePtr {
		wg.Add(1)
		go func() {
			defer wg.Done()
			config := baseConfig
			config.Mode = MODE_GET
			getter, err = NewReceiver(&config)
			if err != nil {
				fmt.Printf("Create receiver error: %v\n", err)
				os.Exit(1)
			}
			err = getter.Start(MODE_GET)
			if err != nil {
				fmt.Printf("Start receiver error: %v\n", err)
				os.Exit(1)
			}
		}()
	}

	if *hostModePtr {
		wg.Add(1)
		go func() {
			defer wg.Done()
			config := baseConfig
			config.Mode = MODE_HOST
			hoster, err = NewSender(&config)
			if err != nil {
				fmt.Printf("Create sender error: %v\n", err)
				os.Exit(1)
			}
			err = hoster.Start(MODE_HOST)
			if err != nil {
				fmt.Printf("Start sender error: %v\n", err)
				os.Exit(1)
			}
		}()
	}

	wg.Wait()

	time.Sleep(30 * time.Millisecond)

	scanner := bufio.NewScanner(os.Stdin)

	if *sendModePtr || *getModePtr {
		for {
			var mode Mode
			var remoteAddr, filePath string

			if *sendModePtr && *getModePtr {
				fmt.Print("\nEnter command (format: GET remoteFileAddr [localFilePath] or PUT remoteAddr localFilePath): ")
				cmdStr, err := getStringFromScanner(scanner, false)
				if err != nil {
					fmt.Printf("Get string error: %v\n", err)
					continue
				}

				cmds := strings.SplitN(cmdStr, " ", 3)

				switch strings.ToLower(cmds[0]) {
				case "get":
					if len(cmds) < 2 {
						fmt.Printf("Not enough arguments. Get format should be: GET remoteFileAddr\n")
						continue
					}
					remoteAddr = cmds[1]
					if len(cmds) >= 3 {
						filePath = cmds[2]
					} else {
						filePath = ""
					}
					mode = MODE_GET
				case "put", "send":
					if len(cmds) < 3 {
						fmt.Printf("Not enough arguments. Put format should be: PUT remoteAddr filePath\n")
						continue
					}
					remoteAddr = cmds[1]
					filePath = cmds[2]
					mode = MODE_SEND
				default:
					fmt.Printf("Unknown action: %v\n", cmds[0])
					continue
				}
			} else if *sendModePtr {
				fmt.Print("\nEnter receive address (format: receiverAddress or receiverAddress/fileName): ")
				remoteAddr, err = getStringFromScanner(scanner, false)
				if err != nil {
					fmt.Printf("Get string error: %v\n", err)
					continue
				}

				fmt.Print("Enter file path to send: ")
				filePath, err = getStringFromScanner(scanner, false)
				if err != nil {
					fmt.Printf("Get receiver address error: %v\n", err)
					continue
				}
				mode = MODE_SEND
			} else if *getModePtr {
				fmt.Print("\nEnter file address (format: hostAddress/fileName): ")
				remoteAddr, err = getStringFromScanner(scanner, false)
				if err != nil {
					fmt.Printf("Get string error: %v\n", err)
					continue
				}

				fmt.Print("Enter file name to save, use blank for default name: ")
				filePath, err = getStringFromScanner(scanner, true)
				if err != nil {
					fmt.Printf("Get receiver address error: %v\n", err)
					continue
				}
				mode = MODE_GET
			}

			switch mode {
			case MODE_SEND:
				fileInfo, err := getFileInfo(filePath)
				if err != nil {
					fmt.Printf("Get file info error: %v\n", err)
					continue
				}

				sub := strings.SplitN(remoteAddr, "/", 2)
				receiverAddr := sub[0]
				fileName := fileInfo.Name()
				if len(sub) == 2 {
					fileName = sub[1]
				}

				err = sender.RequestAndSendFile(receiverAddr, filePath, fileName, fileInfo.Size())
				if err != nil {
					fmt.Printf("RequestAndSendFile error: %v\n", err)
					continue
				}
			case MODE_GET:
				sub := strings.SplitN(remoteAddr, "/", 2)
				if len(sub) != 2 {
					fmt.Printf("Invalid file address. Format should be: serverAddress/fileName\n")
					continue
				}

				senderAddr := sub[0]
				fileName := sub[1]

				if len(filePath) == 0 {
					filePath = fileName
				}

				err = getter.RequestAndGetFile(senderAddr, filePath, fileName)
				if err != nil {
					fmt.Printf("RequestAndGetFile error: %v\n", err)
					continue
				}
			default:
				fmt.Printf("Unknown mode: %v\n", mode)
				continue
			}
		}
	} else {
		select {}
	}
}
