FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
COPY static static
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /apiary-site .

FROM scratch
COPY --from=build /apiary-site /apiary-site
ENV LISTEN=:8080 SIGNUPS_FILE=/data/signups.jsonl
EXPOSE 8080
VOLUME /data
ENTRYPOINT ["/apiary-site"]
