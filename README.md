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
go run github.com/nknorg/nkn-file-transfer
```

to run it as sender, or use

```shell
go run github.com/nknorg/nkn-file-transfer -receive
```

to run it as receiver. When starting as receiver mode, you will see something
like `Start receiver at xxx`, where `xxx` will be your receiver address (NKN
address), and you need that address when sending a file.

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
