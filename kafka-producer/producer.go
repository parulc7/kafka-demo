package main

import (
	"fmt"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	// --
	// The topic is passed as a pointer to the Producer, so we can't
	// use a hard-coded literal. And a variable is a nicer way to do
	// it anyway ;-)
	topic := "test_topic"

	// --
	// Create Producer instance
	// Variable p holds the new Producer instance. There's a bunch of config that we _could_
	// specify here but the only essential one is the bootstrap server.

	// Kafka Configuration Map
	producerConfig := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	// Creating a new Producer using the above configuration
	p, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		// Convert to kafka.Error => Lets us see error codes and handle errors specifically
		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			// CASE: Invalid Argument => Wrong Configuration
			case kafka.ErrInvalidArg:
				fmt.Printf("Error in Kafka while creating the Producer...\nInvalid Configuration - Error Code = %v", ec)
			default:
				fmt.Printf("Error in Kafka while creating the Producer...\n Error Code = %v, Error = %v", ec, err)
			}
		} else {
			// Unable to convert to kafka.Error
			fmt.Printf("Generic Error creating the Producer - %v", err)
		}
	} else {
		fmt.Println("Producer was created successfully!!")

		// Delivery Channel to use for sending Messages
		deliveryChan := make(chan kafka.Event)

		// Channels to facilitate termination

		// From main to goroutine
		termChan := make(chan bool, 1)
		// From goroutine to main
		doneChan := make(chan bool)
		// Building the Message Object to be sent
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          []byte("Hello world"),
		}
		// Produce method executes asynchronously
		// Hence, there is no guarantee that it will always deliver the message
		// Additional error handling required

		go func() {
			doTerm := false
			for !doTerm {
				select {
				case ev := <-p.Events():
					switch ev.(type) {
					case *kafka.Message:
						km := ev.(*kafka.Message)
						if km.TopicPartition.Error != nil {
							fmt.Printf("Failed to send the message to topic %v\nError: %v", string(*km.TopicPartition.Topic), km.TopicPartition.Error)
						} else {
							fmt.Printf("Message = %v\nDelievered to topic %v : (partition %v at offset %v)", string(km.Value), string(*km.TopicPartition.Topic), string(km.TopicPartition.Partition), string(km.TopicPartition.Offset))
						}
					case kafka.Error:
						er := ev.(kafka.Error)
						fmt.Printf("Error encountered while sending the message: %v", er)
					default:
						fmt.Printf("Got an event that is neither error nor message : %v", ev)
					}
				case <-termChan:
					doTerm = true
				}
			}
		}()

		// Send a message using Produce() => Executes asynchronously
		//
		// takes the topic, partition, value and Delievery Channel
		//
		// Error Handling incorporated
		if err := p.Produce(&msg, deliveryChan); err != nil {
			fmt.Printf("Error Encountered while sending the message..\n%v", err)
		}

		// Flush the Producer queue
		t := 10000
		if r := p.Flush(t); r > 0 {
			fmt.Printf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r)
		} else {
			fmt.Println("\n--\n✨ All messages flushed from the queue")
		}

		// Stop listening for new events
		// Ready for termination

		termChan <- true
		<-doneChan
		// Close the producer
		p.Close()
	}
}
