package main

import (
	"log"
	"net/http"
	"strconv"

	"github.com/Hendra-Huang/saga"
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

	http.HandleFunc("/transfer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requestedAmount := r.FormValue("amount")
		if requestedAmount == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("amount is required"))
			return
		}
		requestedFrom := r.FormValue("from")
		if requestedFrom == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("from is required"))
			return
		}
		requestedTo := r.FormValue("to")
		if requestedTo == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("to is required"))
			return
		}

		amount, err := strconv.ParseInt(requestedAmount, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("amount is invalid"))
			return
		}
		from, err := strconv.ParseInt(requestedFrom, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("from is invalid"))
			return
		}
		to, err := strconv.ParseInt(requestedTo, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("to is invalid"))
			return
		}

		err = Transfer(storageClient, topic, from, to, amount)
		if err != nil {
			if err == ErrInsufficientBalance {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("insufficient balance"))
				return
			}
		}

		w.WriteHeader(http.StatusOK)
	})
	log.Fatal(http.ListenAndServe("localhost:7777", nil))
}
