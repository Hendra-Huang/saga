package main

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/Hendra-Huang/saga"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

type Statement struct {
	ID        string
	EventID   string
	UserID    int64
	Amount    int64
	CreatedAt time.Time
}

var (
	brokers    = []string{"localhost:9092"}
	topic      = "saga"
	statements = []Statement{}
)

func newStatement() Statement {
	uuid4, _ := uuid.NewRandom()
	statement := Statement{}
	statement.ID = uuid4.String()
	statement.CreatedAt = time.Now()

	return statement
}

func main() {
	storageClient, err := saga.New(brokers, 1, 1)
	if err != nil {
		log.Fatalln(err.Error())
	}

	messages, err := storageClient.Consume(topic, sarama.OffsetNewest)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for message := range messages {
		var event saga.Event
		err := json.Unmarshal(message.Value, &event)
		if err != nil {
			log.Println(err.Error())
			continue
		}
		if event.Status != saga.StatusInit && event.Status != saga.StatusCompensate {
			continue
		}
		switch event.Type {
		case "DepositStatement":
			amount, err := strconv.ParseInt(event.Data["amount"], 10, 64)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			userID, err := strconv.ParseInt(event.Data["user_id"], 10, 64)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			switch event.Status {
			case saga.StatusInit:
				statement := newStatement()
				statement.Amount = amount
				statement.UserID = userID
				statement.EventID = event.ID

				statements = append(statements, statement)
				//successfulEvent := saga.GetAbortSagaEvent(event.SagaTransactionID, event.StartSagaOffset)
				//storageClient.Produce(topic, successfulEvent)
			case saga.StatusCompensate:
				for i, statement := range statements {
					if statement.UserID == userID && statement.EventID == event.ID {
						statements = append(statements[:i], statements[i+1:]...)
						break
					}
				}
				//successfulEvent := event.GetSuccessfulEvent()
				//storageClient.Produce(topic, successfulEvent)
			}
			successfulEvent := event.GetSuccessfulEvent()
			storageClient.Produce(topic, successfulEvent)
		case "WithdrawStatement":
			amount, err := strconv.ParseInt(event.Data["amount"], 10, 64)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			userID, err := strconv.ParseInt(event.Data["user_id"], 10, 64)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			switch event.Status {
			case saga.StatusInit:
				statement := newStatement()
				statement.Amount = amount * -1
				statement.UserID = userID
				statement.EventID = event.ID

				statements = append(statements, statement)
			case saga.StatusCompensate:
				for i, statement := range statements {
					if statement.UserID == userID && statement.EventID == event.ID {
						statements = append(statements[:i], statements[i+1:]...)
						break
					}
				}
			}
			successfulEvent := event.GetSuccessfulEvent()
			storageClient.Produce(topic, successfulEvent)
		}

		log.Printf("Total Count: %d\n", len(statements))
		log.Printf("%v\n", statements)
	}
}
