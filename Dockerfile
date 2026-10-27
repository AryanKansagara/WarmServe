FROM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod *.go ./
RUN CGO_ENABLED=0 go build -o /warmserve .

FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir torch==2.10.0 --index-url https://download.pytorch.org/whl/cpu
COPY --from=builder /warmserve /app/warmserve
COPY worker.py /app/worker.py
ENV PYTHONDONTWRITEBYTECODE=1 WARMSERVE_ADDR=0.0.0.0:8088
USER 10001:10001
EXPOSE 8088
CMD ["/app/warmserve"]
