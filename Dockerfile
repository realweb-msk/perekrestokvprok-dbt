FROM golang:1.13 as builder
WORKDIR /app
COPY invoke.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -v -o server

FROM fishtownanalytics/dbt:1.0.0
USER root
WORKDIR /perekrestokvprok-dbt
COPY --from=builder /app/server ./
COPY script.sh ./
COPY . ./

ENTRYPOINT "./server"