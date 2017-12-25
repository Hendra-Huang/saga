package main

import (
	"log"
	"net/http"
	"strconv"
)

var storedBalances = map[int64]int64{
	1: 10000,
	2: 0,
}

func main() {
	http.HandleFunc("/getuserbalance", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requestedID := r.FormValue("id")
		if requestedID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("id is required"))
		}

		id, err := strconv.ParseInt(requestedID, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("id is invalid"))
		}

		balance := storedBalances[id]
		log.Printf("[GetBalance] 1: %d 2:%d\n", storedBalances[1], storedBalances[2])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(strconv.FormatInt(balance, 10)))
	})

	http.HandleFunc("/setuserbalance", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requestedID := r.FormValue("id")
		if requestedID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("id is required"))
		}
		requestedBalance := r.FormValue("balance")
		if requestedBalance == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("balance is required"))
		}

		id, err := strconv.ParseInt(requestedID, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("id is invalid"))
		}
		balance, err := strconv.ParseInt(requestedBalance, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("balance is invalid"))
		}

		storedBalances[id] = balance
		log.Printf("[SetBalance] 1: %d 2:%d\n", storedBalances[1], storedBalances[2])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(strconv.FormatInt(balance, 10)))
	})

	http.ListenAndServe("localhost:7778", nil)
}
