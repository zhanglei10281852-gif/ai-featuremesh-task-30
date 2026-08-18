FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/featuremesh-server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/featuremesh-server /featuremesh-server
EXPOSE 8080
ENTRYPOINT ["/featuremesh-server"]
