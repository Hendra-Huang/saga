package saga

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

type sagaTracker struct {
	subEvents                      []Event
	successfulInitEventTypes       map[string]bool
	successfulCompensateEventTypes map[string]bool
	aborted                        bool
}

func StartSEC(storage StorageClient, topic string) error {
	messageChan, err := storage.Consume(topic, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	killed := false
	killedChan := make(chan struct{})
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		killed = true
		killedChan <- struct{}{}
	}()

	onGoingSagas := make(map[string]*sagaTracker)
ConsumeMessageLoop:
	for !killed {
		select {
		case message := <-messageChan:
			var event Event
			err := json.Unmarshal(message.Value, &event)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			switch event.Type {
			case StartSagaEventType:
				subEvents := processStartSagaEvent(storage, topic, message, event)

				successfulInitEventTypes := make(map[string]bool)
				successfulCompensateEventTypes := make(map[string]bool)
				for _, subEvent := range subEvents {
					successfulInitEventTypes[subEvent.Type] = false
					successfulCompensateEventTypes[subEvent.Type] = false
				}
				onGoingSagas[event.ID] = &sagaTracker{
					subEvents:                      subEvents,
					successfulInitEventTypes:       successfulInitEventTypes,
					successfulCompensateEventTypes: successfulCompensateEventTypes,
					aborted: false,
				}
			case AbortSagaEventType:
				if tracker, ok := onGoingSagas[event.SagaTransactionID]; ok {
					processAbortSagaEvent(storage, topic, event.SagaTransactionID, event.StartSagaOffset, tracker.subEvents)
					tracker.aborted = true
				}
			case EndSagaEventType:
			default:
				if tracker, ok := onGoingSagas[event.SagaTransactionID]; ok {
					allEventCompleted := true
					if tracker.aborted {
						if _, ok := tracker.successfulCompensateEventTypes[event.Type]; event.Status == StatusCompensateSuccess && ok {
							tracker.successfulCompensateEventTypes[event.Type] = true
						}

						for _, successfulCompensateEventType := range tracker.successfulCompensateEventTypes {
							if !successfulCompensateEventType {
								allEventCompleted = false
								break
							}
						}
					} else {
						if _, ok := tracker.successfulInitEventTypes[event.Type]; event.Status == StatusInitSuccess && ok {
							tracker.successfulInitEventTypes[event.Type] = true
						}

						for _, successfulInitEventType := range tracker.successfulInitEventTypes {
							if !successfulInitEventType {
								allEventCompleted = false
								break
							}
						}
					}

					if allEventCompleted {
						endSagaEvent := GetEndSagaEvent(event.SagaTransactionID, event.StartSagaOffset)
						retry(func() error {
							_, _, err := storage.Produce(topic, endSagaEvent)
							return err
						})
					}
				}
			}
		case <-killedChan:
			break ConsumeMessageLoop
		}
	}
	log.Println("SEC exits gracefully ...")

	return nil
}

func processStartSagaEvent(storage StorageClient, topic string, message Message, startSagaEvent Event) []Event {
	startOffset := message.Offset
	var events []Event
	err := json.Unmarshal([]byte(startSagaEvent.Data["events"]), &events)
	if err != nil {
		log.Println(err.Error())
		return nil
	}

	subEvents := make([]Event, len(events))
	for i, event := range events {
		subEvent := startSagaEvent.NewSagaSubEvent(startOffset)
		subEvent.Data = event.Data
		subEvent.Type = event.Type
		retry(func() error {
			_, _, err := storage.Produce(topic, subEvent)
			return err
		})
		subEvents[i] = subEvent
	}

	return subEvents
}

func processAbortSagaEvent(storage StorageClient, topic string, transactionID string, startOffset int64, subEvents []Event) {
	for _, subEvent := range subEvents {
		subCompensateEvent := subEvent.GetCompensateEvent()
		retry(func() error {
			_, _, err := storage.Produce(topic, subCompensateEvent)
			return err
		})
	}
}

func retry(action func() error) {
	attemp := 0
	for {
		attemp++
		err := action()
		if err != nil {
			log.Println("Error on retrying. ", err.Error())
			delayDuration := time.Duration(math.Ceil(float64(attemp) / 5.0))
			time.Sleep(delayDuration * time.Second)
			continue
		}

		break
	}
}
