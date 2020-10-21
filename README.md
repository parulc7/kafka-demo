# Kafka Producer and Consumer Boilerplate

## Steps -

1. Install Docker Desktop and docker-compose on your system.
2. Run ```docker-compose up``` in terminal to start the docker image.
3. In another terminal window, navigate to ```kafka-consumer``` and run the ```consumer.go``` file using ```go run consumer.go```
4. In another terminal window, navigate to ```kafka-producer``` and run the ```producer.go``` file using ```go run producer.go```
5. The producer will send the message to the consumer, and the message will get logged in the terminal side.

