This library is deprecated as it's intended to be a PoC. Please use the new
session mode in NKN SDK multiclient (e.g.
https://github.com/nknorg/nkn-sdk-go and
https://github.com/nknorg/nkn-sdk-js) for library, or https://nftp.nkn.org
for ready to use application.

# NKN File Transfer

A decentralized file transfer application using NKN client.

This application uses simple control mechanism similar to TCP. If you want to
use it in production, it's recommended to implement more advanced mechanism
(such as erasure coding) to achieve higher performance and reliability.

## Get Started

You need Go 1.12+ to build from source:

```shell
go get -u github.com/nknorg/nkn-file-transfer
```

Then you can use

```shell
go run github.com/nknorg/nkn-file-transfer -$MODE
```

to run it, where `$MODE` can be one of `send`, `receive`, `get`, `host`. `send`
mode should be used with `receive` mode to send file to peer, while `get` mode
should be used with `host` mode to fetch file to peer.

When starting as `receive` mode or `host`, you will see something like `Start
receiver in receive mode at xxx` or `Start sender in host mode at xxx`, where
`xxx` will be your NKN address, and you will need that address when sending or
getting files from other peers.

You can enable multiple modes together. For example, you can enable `receive`
mode together with `host` mode to create a file storage server that can accept
both send request (equivalent to HTTP PUT) and get request (equivalent to HTTP
GET), and you can use `send` mode with `get` mode to create a client that can
upload to (`send`) and download from (`get`) the aforementioned server. Actually
if you enable both `send` mode and `get` mode together, you can use
```
GET hostAddress/fileName
```
to download file, and
```
PUT receiveAddress/fileName localFilePath
```
to upload file.

There is also a HTTP mode that can be enabled by `-http` together with `host`
and `receive` mode. By default it will start a HTTP server at
`http://127.0.0.1:8080` and can accept HTTP GET/PUT/HEAD request with route
`remoteAddr/fileName`. It accepts HTTP GET request with Range header so browser
can stream video while downloading.

By default a random NKN address (key pair) will be generated each time. You can
use a specific address by passing `-seed` and `-identifier` argument, same as a
NKN client. Use `-help` argument to see all supported arguments.

## Contributing

**Can I submit a bug, suggestion or feature request?**

Yes. Please [open an issue](https://github.com/nknorg/nkn/issues/new) for that.

**Can I contribute patches to NKN project?**

Yes, we appreciate your help! To make contributions, please fork the repo, push
your changes to the forked repo with signed-off commits, and open a pull request
here.

Please follow our [Golang Style Guide](https://github.com/nknorg/nkn/wiki/NKN-Golang-Style-Guide)
for coding style.

Please sign off your commit. This means adding a line "Signed-off-by: Name
<email>" at the end of each commit, indicating that you wrote the code and have
the right to pass it on as an open source patch. This can be done automatically
by adding -s when committing:

```shell
git commit -s
```

## Community

* [Discord](https://discord.gg/c7mTynX)
* [Telegram](https://t.me/nknorg)
* [Reddit](https://www.reddit.com/r/nknblockchain/)
* [Twitter](https://twitter.com/NKN_ORG)
