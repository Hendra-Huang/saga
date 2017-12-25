package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Hendra-Huang/saga"
	"github.com/Shopify/sarama"
)

var (
	brokers = []string{"localhost:9092"}
	topic   = "saga"
)

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
		case "Deposit":
			startBalance, err := strconv.ParseInt(event.Data["start_balance"], 10, 64)
			if err != nil {
				log.Println(err.Error())
				continue
			}
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
				endBalance := startBalance + amount
				setBalance(userID, endBalance)
			case saga.StatusCompensate:
				setBalance(userID, startBalance)
			}
			successfulEvent := event.GetSuccessfulEvent()
			storageClient.Produce(topic, successfulEvent)
		case "Withdraw":
			startBalance, err := strconv.ParseInt(event.Data["start_balance"], 10, 64)
			if err != nil {
				log.Println(err.Error())
				continue
			}
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
				endBalance := startBalance - amount
				setBalance(userID, endBalance)
			case saga.StatusCompensate:
				setBalance(userID, startBalance)
			}
			successfulEvent := event.GetSuccessfulEvent()
			storageClient.Produce(topic, successfulEvent)
		}
	}
}

func setBalance(id, balance int64) {
	_, err := http.PostForm("http://localhost:7778/setuserbalance", url.Values{
		"id":      []string{strconv.FormatInt(id, 10)},
		"balance": []string{strconv.FormatInt(balance, 10)},
	})
	if err != nil {
		log.Println(err.Error())
		return
	}
}
