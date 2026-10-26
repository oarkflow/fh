# Bounded download client

Start the [streaming upload server](../streaming-upload/README.md), then run
this example from the repository root:

```sh
go run ./examples/bounded-download -out /tmp/fh-sample.bin
```

The default `atomic` mode uses `Response.SaveAtomic`. If a response exceeds
`ResponseBodyLimit`, it returns `ErrClientBodyTooLarge` and leaves any existing
destination file untouched. Try a lower limit to see the error:

```sh
go run ./examples/bounded-download -out /tmp/fh-sample.bin -limit 1024
```

The other modes show `Response.DownloadTo` and `Response.Reader`:

```sh
go run ./examples/bounded-download -mode writer > /tmp/fh-writer.bin
go run ./examples/bounded-download -mode reader > /tmp/fh-reader.bin
```

Both streaming modes expose at most the configured number of bytes before
returning `ErrClientBodyTooLarge`. Their output may already contain those
bytes when the error occurs; use `atomic` when the destination must remain
unchanged on failure.
