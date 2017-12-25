package saga

import (
	"encoding/json"
	"log"
)

var (
	StartSagaEventType = "StartSaga"
	EndSagaEventType   = "EndSaga"
	AbortSagaEventType = "AbortSaga"
)

func GetStartSagaEvent(events []Event) Event {
	e := NewEvent()
	e.Type = StartSagaEventType
	data := make(map[string]string)

	eventBytes, err := json.Marshal(events)
	if err != nil {
		log.Println("Error on marshaling event. ", err)
	}
	data["events"] = string(eventBytes)

	e.Data = data

	return e
}

func GetEndSagaEvent(sagaTransactionID string, startSagaOffset int64) Event {
	e := NewEvent()
	e.Type = EndSagaEventType
	e.SagaTransactionID = sagaTransactionID
	e.StartSagaOffset = startSagaOffset

	return e
}

func GetAbortSagaEvent(sagaTransactionID string, startSagaOffset int64) Event {
	e := NewEvent()
	e.Type = AbortSagaEventType
	e.SagaTransactionID = sagaTransactionID
	e.StartSagaOffset = startSagaOffset

	return e
}
