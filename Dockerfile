FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/trusskv ./cmd/trusskv \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/trussctl ./cmd/trussctl

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/trusskv /trusskv
COPY --from=build /out/trussctl /trussctl
EXPOSE 7000
ENTRYPOINT ["/trusskv"]
