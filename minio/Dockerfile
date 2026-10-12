FROM golang:1.24 AS build
RUN git clone --depth 1 --branch RELEASE.2025-09-06T17-38-46Z https://github.com/minio/minio.git /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -o /minio .

FROM alpine:3.20
RUN apk add --no-cache curl ca-certificates
COPY --from=build /minio /usr/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]