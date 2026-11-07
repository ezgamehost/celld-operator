FROM golang:1.26.10@sha256:cfa27959837144bf4649802ba38e59daffd0dd2a7994aa83daaf113285b0f551 AS build
WORKDIR /src
COPY test/fixtures/runtime/main.go main.go
RUN CGO_ENABLED=0 go build -tags=e2e -o /runtime main.go
FROM scratch
COPY --from=build /runtime /runtime
ENTRYPOINT ["/runtime"]
