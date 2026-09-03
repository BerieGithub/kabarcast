# ---- build ----
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/kabarcast ./cmd/kabarcast

# ---- run ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kabarcast /kabarcast
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/kabarcast"]
