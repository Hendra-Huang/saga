Saga pattern in Go
===========================

This project is just a POC and inspired by [Caitie](https://twitter.com/caitie) talks about Saga ([video](https://www.youtube.com/watch?v=xDuwrtwYHu8)). I suggest you to watch the talk if you haven't heard about saga.


## About the project

This project implements Saga for distributed transaction. In this project, I have 3 microservices. They are:
1. Transaction - for handling transaction, eg: transfer between user
2. User - for handling user and their balance
3. Statement - for handling statements of the user

## How it works

When a transaction happens, the `transaction` microservice will publish a `StartSaga` event to kafka. SEC (Saga Execution Coordinator) will subscribe for `StartSaga` event and process it. SEC will publish event(s) according to `StartSaga` specification. Corresponding microservices will subscribe to that particular event and do some logic. SEC will wait for the Ack (success or fail) from the corresponding microservices. If one of them is fail, SEC will do a rollback by publishing the compensate event from each published event. If everything is success, SEC will publish `EndSaga` event to mark the transaction has been fully processed.

## About the structure

`example` folders contain some microservices. `cmd/sec` contains entrypoint for SEC.

## Dependencies

- Kafka for event sourcing
- "github.com/Shopify/sarama" for connecting to kafka
