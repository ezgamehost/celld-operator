FROM golang:1.26.9@sha256:d7722066f0b60ceccb6c0643cbed1f5f9e15506ac237146c95333504d7805d89 AS build
WORKDIR /src
COPY test/fixtures/runtime/main.go main.go
RUN CGO_ENABLED=0 go build -tags=e2e -o /runtime main.go
FROM scratch
COPY --from=build /runtime /runtime
ENTRYPOINT ["/runtime"]
