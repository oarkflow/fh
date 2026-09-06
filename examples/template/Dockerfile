FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/{{APP_NAME}} ./cmd/api

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/{{APP_NAME}} /{{APP_NAME}}
USER nonroot:nonroot
ENTRYPOINT ["/{{APP_NAME}}"]
