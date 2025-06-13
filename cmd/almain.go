package main

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"os"
// 	"os/signal"
// 	"syscall"
// 	"time"

// 	"github.com/confluentinc/confluent-kafka-go/kafka"
// )

// const (
// 	topic     = "test-topic"
// 	groupID   = "go-sample-group"
// 	bootstrap = "kafka0:29092"
// )

// func produce() {
// 	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
// 	if err != nil {
// 		log.Fatalf("Failed to create producer: %s\n", err)
// 	}
// 	defer p.Close()

// 	deliveryChan := make(chan kafka.Event)

// 	for i := 0; i < 10; i++ {
// 		value := fmt.Sprintf("Hello Kafka %d", i)
// 		err = p.Produce(&kafka.Message{
// 			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
// 			Value:          []byte(value),
// 		}, deliveryChan)

// 		e := <-deliveryChan
// 		m := e.(*kafka.Message)
// 		if m.TopicPartition.Error != nil {
// 			log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
// 		} else {
// 			log.Printf("Delivered message to %v\n", m.TopicPartition)
// 		}
// 		time.Sleep(500 * time.Millisecond)
// 	}

// 	close(deliveryChan)
// }

// func consume(ctx context.Context) {
// 	c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers": bootstrap,
// 		"group.id":          groupID,
// 		"auto.offset.reset": "earliest",
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create consumer: %s\n", err)
// 	}
// 	defer c.Close()

// 	err = c.SubscribeTopics([]string{topic}, nil)
// 	if err != nil {
// 		log.Fatalf("Failed to subscribe to topic: %s\n", err)
// 	}

// 	log.Println("Consumer started. Waiting for messages...")
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			log.Println("Shutting down consumer...")
// 			return
// 		default:
// 			msg, err := c.ReadMessage(1 * time.Second)
// 			if err == nil {
// 				log.Printf("Received message: %s from %s\n", string(msg.Value), msg.TopicPartition)
// 			} else if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() != kafka.ErrTimedOut {
// 				log.Printf("Consumer error: %v\n", err)
// 			}
// 		}
// 	}
// }

// func main() {
// 	// Graceful shutdown
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	sigs := make(chan os.Signal, 1)
// 	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
// 	go func() {
// 		<-sigs
// 		cancel()
// 	}()

// 	go produce()
// 	time.Sleep(2 * time.Second) // Ensure some messages are produced before consuming
// 	consume(ctx)
// }
