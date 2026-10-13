FROM golang:1.26.8@sha256:3c3e25a4da13fd0478eed2df1eb35a0e667094a7124d3993a6a1d30f71c17e79 AS build
WORKDIR /src
COPY test/fixtures/runtime/main.go main.go
RUN CGO_ENABLED=0 go build -tags=e2e -o /runtime main.go
FROM scratch
COPY --from=build /runtime /runtime
ENTRYPOINT ["/runtime"]
