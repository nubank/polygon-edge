FROM golang:1.18-alpine AS builder

WORKDIR /polygon-edge

ADD go.mod go.sum ./
RUN go mod download

RUN apk add git

COPY . .

RUN go build -o polygon-edge -tags netgo -ldflags="\
		-s -w \
    	-X 'github.com/0xPolygon/polygon-edge/versioning.Version=$(git describe --tags --abbrev=0)' \
		-X 'github.com/0xPolygon/polygon-edge/versioning.Commit=$(git rev-parse HEAD)' \
		-X 'github.com/0xPolygon/polygon-edge/versioning.Branch=$(git rev-parse --abbrev-ref HEAD | tr -d '\040\011\012\015\n')' \
		-X 'github.com/0xPolygon/polygon-edge/versioning.BuildTime=$(date)'" \
	main.go

FROM alpine:latest AS runner

RUN apk --no-cache add ca-certificates jq

WORKDIR /polygon-edge

COPY --from=builder /polygon-edge/polygon-edge ./
COPY ./docker/local/polygon-edge.sh ./

# Expose json-rpc, libp2p and grpc ports
EXPOSE 8545 9632 1478 5001

ENTRYPOINT ["./polygon-edge.sh"]
