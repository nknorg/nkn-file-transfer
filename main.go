package main

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
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
	httpModePtr := flag.Bool("http", false, "Enable HTTP mode (for get/send mode)")
	httpListenAddrPtr := flag.String("http-listen-addr", "127.0.0.1:8080", "HTTP listen address")
	transmitModePtr := flag.String("transmit-mode", "push", "Transmit mode (push or pull)")
	seedPtr := flag.String("seed", "", "Secret seed")
	identifierPtr := flag.String("identifier", "", "NKN address identifier")
	numClientsPtr := flag.Int("clients", 8, "Number of clients")
	numWorkersPtr := flag.Int("workers", 1024, "Number of workers")
	chunkSizePtr := flag.Int("chunksize", 1024, "File chunk size in bytes (for receive/get mode)")
	bufSizePtr := flag.Int("bufsize", 32, "File buffer size in megabytes (for receive/get mode)")

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

	var transmitMode TransmitMode
	switch *transmitModePtr {
	case "push":
		transmitMode = TRANSMIT_MODE_PUSH
	case "pull":
		transmitMode = TRANSMIT_MODE_PULL
	default:
		fmt.Printf("Unrecognized transmit mode: %v\n", *transmitModePtr)
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

	if *httpModePtr {
		gin.SetMode(gin.ReleaseMode)
		router := gin.Default()

		if *getModePtr {
			router.GET("/:remoteAddr/*fileName", func(c *gin.Context) {
				reqRanges := func() []int64 {
					ranges := []int64{}
					rangeHeader := strings.Split(c.GetHeader("Range"), "=")
					if strings.ToLower(strings.Trim(rangeHeader[0], " ")) == "bytes" && len(rangeHeader) > 1 {
						r := strings.Split(strings.Split(rangeHeader[1], ",")[0], "-")
						if len(r) == 2 {
							if len(r[0]) > 0 {
								start, err := strconv.ParseInt(r[0], 10, 64)
								if err != nil {
									return ranges
								}
								ranges = append(ranges, start)

								if len(r[1]) > 0 {
									end, err := strconv.ParseInt(r[1], 10, 64)
									if err != nil {
										return ranges
									}
									ranges = append(ranges, end)
								}
							} else if len(r[1]) > 0 {
								start, err := strconv.ParseInt(r[1], 10, 64)
								if err != nil {
									return ranges
								}
								ranges = append(ranges, -start)
							}
						}
					}
					return ranges
				}()

				fileName := c.Param("fileName")
				if len(fileName) > 0 && fileName[0] == '/' {
					fileName = fileName[1:]
				}

				fileID, fileSize, hostClients, err := getter.RequestToGetFile(c.Request.Context(), c.Param("remoteAddr"), fileName, reqRanges, TRANSMIT_MODE_PULL)
				if err != nil {
					fmt.Printf("Request to get file error: %v\n", err)
					c.Status(http.StatusNotFound)
					return
				}

				startPos := int64(0)
				endPos := fileSize - 1
				if len(reqRanges) > 1 && reqRanges[1] < endPos {
					endPos = reqRanges[1]
				}
				if len(reqRanges) > 0 && reqRanges[0] <= endPos {
					startPos = reqRanges[0]
				}
				totalSize := endPos - startPos + 1

				c.Header("Content-Length", strconv.FormatInt(totalSize, 10))
				c.Header("Content-Type", mime.TypeByExtension(filepath.Ext(fileName)))
				c.Header("Accept-Ranges", "bytes")

				if len(reqRanges) > 0 {
					c.Status(http.StatusPartialContent)
					c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startPos, endPos, fileSize))
				} else {
					c.Status(http.StatusOK)
				}

				ctx, cancel := context.WithCancel(c.Request.Context())

				reader, writer := io.Pipe()
				defer writer.Close()

				go func() {
					b := make([]byte, 1024)
					for {
						n, err := reader.Read(b)
						if err != nil {
							return
						}
						_, err = c.Writer.Write(b[:n])
						if err != nil {
							cancel()
							return
						}
						c.Writer.Flush()
					}
				}()

				err = getter.ReceiveFile(ctx, writer, c.Param("remoteAddr"), hostClients, fileID, totalSize, TRANSMIT_MODE_PULL)
				if err != nil {
					select {
					case <-ctx.Done():
						getter.CancelFile(c.Param("remoteAddr"), fileID, hostClients)
					default:
					}
					fmt.Printf("Receive file error: %v\n", err)
				}
			})

			router.HEAD("/:remoteAddr/*fileName", func(c *gin.Context) {
				fileName := c.Param("fileName")
				if len(fileName) > 0 && fileName[0] == '/' {
					fileName = fileName[1:]
				}

				reqRanges := []int64{0, 0}

				_, fileSize, _, err := getter.RequestToGetFile(c.Request.Context(), c.Param("remoteAddr"), fileName, reqRanges, TRANSMIT_MODE_PULL)
				if err != nil {
					fmt.Printf("Request to get file error: %v\n", err)
					c.Status(http.StatusNotFound)
					return
				}

				c.Header("Content-Length", strconv.FormatInt(fileSize, 10))
				c.Header("Content-Type", mime.TypeByExtension(filepath.Ext(fileName)))
				c.Header("Accept-Ranges", "bytes")
				c.Status(http.StatusOK)
			})
		}

		if *sendModePtr {
			router.PUT("/:remoteAddr/*fileName", func(c *gin.Context) {
				fileName := c.Param("fileName")
				if len(fileName) > 0 && fileName[0] == '/' {
					fileName = fileName[1:]
				}

				data, err := c.GetRawData()
				if err != nil {
					fmt.Printf("Get request raw data error: %v\n", err)
					c.Status(http.StatusBadRequest)
					return
				}

				ctx := context.Background()
				fileID, chunkSize, chunksBufSize, receiverClients, err := sender.RequestToSendFile(ctx, c.Param("remoteAddr"), fileName, int64(len(data)), TRANSMIT_MODE_PUSH)
				if err != nil {
					fmt.Printf("Request to send file error: %v\n", err)
					c.Status(http.StatusForbidden)
					return
				}

				err = sender.SendFile(ctx, bytes.NewReader(data), c.Param("remoteAddr"), receiverClients, fileID, chunkSize, chunksBufSize, nil, TRANSMIT_MODE_PUSH)
				if err != nil {
					fmt.Printf("Send file error: %v\n", err)
					c.Status(http.StatusForbidden)
					return
				}

				c.Status(http.StatusOK)
			})
		}

		fmt.Printf("Starting HTTP server on %s\n", *httpListenAddrPtr)
		router.Run(*httpListenAddrPtr)
	}

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

				err = sender.RequestAndSendFile(context.Background(), receiverAddr, filePath, fileName, fileInfo.Size(), transmitMode)
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

				err = getter.RequestAndGetFile(context.Background(), senderAddr, filePath, fileName, transmitMode)
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
