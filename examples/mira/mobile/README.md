# Mobile facade example

`mobile/` is a Go facade intended for gomobile binding. It supports certificate
based TLS bootstrap, channel send/receive, transport receipts, and explicit
path rebinding. Platform background execution, OS notifications, push delivery,
and reconnect policy remain application responsibilities.

Build prerequisites:

- Android: Go, `gomobile`, Android SDK/NDK, and a Java toolchain.
- iOS: macOS, Xcode command line tools, Go, and `gomobile`.

```sh
make mira-mobile-android
make mira-mobile-ios
```

The output is written to the ignored `build/mobile/` directory. The build
scripts do not validate physical-device behavior or app-store packaging. Call
`Client.RebindAndWait` after the OS reports a network change while the process
and session are still alive; after process death, reconnect and resume durable
application events from a persisted cursor.
