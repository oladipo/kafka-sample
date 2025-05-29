# Kafka Producer and Consumer in Go

This project demonstrates how to use [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) to produce and consume messages with Apache Kafka in Go.

## Features

- **Producer:** Sends 1000 messages to a Kafka topic (`test-topic`).
- **Consumer:** Subscribes to the same topic and prints out all consumed messages.
- **Delivery Reports:** Prints delivery status for each produced message.

## Prerequisites

- [Go](https://golang.org/dl/) 1.18 or later
- [Apache Kafka](https://kafka.apache.org/) running and accessible at `localhost:9092`
- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) library

## How to Run

1. **Start Kafka**  
   Make sure Kafka is running on your machine at `localhost:9092`.

2. **Clone this repository and install dependencies**
   ```bash
   git clone <your-repo-url>
   cd kafka
   go mod tidy

3. Run the program

```bash
go run main.go

You should see output indicating messages are being delivered and then consumed.

Code Overview
Producer:
Connects to Kafka and sends 1000 "Hello from Go!" messages to test-topic.
Consumer:
Subscribes to test-topic and prints each message as it is consumed.
Example Output

Delivered to [test-topic][0]@123
...
Consumed message: Hello from Go! from test-topic[0]@123
...

Notes
The producer and consumer run sequentially in the same program for demonstration.
In production, you would typically run them as separate services.
