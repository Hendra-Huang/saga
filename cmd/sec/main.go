package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Hendra-Huang/saga"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: \n\t %s kafka-host topic\n", os.Args[0])
		os.Exit(1)
	}

	kafkaHost := os.Args[1]
	topic := os.Args[2]
	storageClient, err := saga.New([]string{kafkaHost}, 1, 1)
	if err != nil {
		log.Fatalln("Failed to start storage client. ", err)
	}
	err = saga.StartSEC(storageClient, topic)
	if err != nil {
		log.Fatalln("Failed to start SEC. ", err)
	}
}
