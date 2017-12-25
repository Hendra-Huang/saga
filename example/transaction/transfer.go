package main

import (
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Hendra-Huang/saga"
)

var (
	ErrInsufficientBalance = errors.New("Insufficient balance")
)

func Transfer(storage saga.StorageClient, topic string, from, to, amount int64) error {
	fromBalance := getBalance(from)
	if fromBalance-amount < 0 {
		return ErrInsufficientBalance
	}

	withdrawEvent := saga.NewEvent()
	withdrawEvent.Type = "Withdraw"
	withdrawData := map[string]string{
		"user_id":       strconv.FormatInt(from, 10),
		"start_balance": strconv.FormatInt(fromBalance, 10),
		"amount":        strconv.FormatInt(amount, 10),
	}
	withdrawEvent.Data = withdrawData

	withdrawStatementEvent := saga.NewEvent()
	withdrawStatementEvent.Type = "WithdrawStatement"
	withdrawStatementData := map[string]string{
		"user_id":       strconv.FormatInt(from, 10),
		"start_balance": strconv.FormatInt(fromBalance, 10),
		"amount":        strconv.FormatInt(amount, 10),
	}
	withdrawStatementEvent.Data = withdrawStatementData

	toBalance := getBalance(to)
	depositEvent := saga.NewEvent()
	depositEvent.Type = "Deposit"
	depositData := map[string]string{
		"user_id":       strconv.FormatInt(to, 10),
		"start_balance": strconv.FormatInt(toBalance, 10),
		"amount":        strconv.FormatInt(amount, 10),
	}
	depositEvent.Data = depositData

	depositStatementEvent := saga.NewEvent()
	depositStatementEvent.Type = "DepositStatement"
	depositStatementData := map[string]string{
		"user_id":       strconv.FormatInt(to, 10),
		"start_balance": strconv.FormatInt(toBalance, 10),
		"amount":        strconv.FormatInt(amount, 10),
	}
	depositStatementEvent.Data = depositStatementData

	startSagaEvent := saga.GetStartSagaEvent([]saga.Event{withdrawEvent, withdrawStatementEvent, depositEvent, depositStatementEvent})
	_, _, err := storage.Produce(topic, startSagaEvent)

	return err
}

func getBalance(id int64) int64 {
	resp, err := http.PostForm("http://localhost:7778/getuserbalance", url.Values{
		"id": []string{strconv.FormatInt(id, 10)},
	})
	if err != nil {
		log.Println(err.Error())
		return 0
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err.Error())
		return 0
	}
	defer resp.Body.Close()

	balance, err := strconv.ParseInt(string(body), 10, 64)
	if err != nil {
		log.Println(err.Error())
		return 0
	}

	return balance
}
