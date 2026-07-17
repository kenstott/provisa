FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o /out/airport_shim .

FROM alpine:3.20
# busybox nc backs the compose healthcheck (a plain TCP probe on 50051); distroless has no shell.
RUN apk add --no-cache busybox-extras
COPY --from=build /out/airport_shim /airport_shim
EXPOSE 50051
ENTRYPOINT ["/airport_shim"]
