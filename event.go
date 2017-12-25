package saga

import (
	"time"

	"github.com/google/uuid"
)

const (
	StatusInit              = "init"
	StatusInitSuccess       = "init_success"
	StatusCompensate        = "compensate"
	StatusCompensateSuccess = "compensate_success"
)

type (
	Event struct {
		ID                string
		Type              string
		CreatedAt         time.Time
		Status            string
		Data              map[string]string
		SagaTransactionID string
		StartSagaOffset   int64
	}

	Eventable interface {
		GetSuccessfulEvent() Event
		GetCompensateEvent() Event
	}
)

func getSuccessfulStatus(e Event) string {
	switch e.Status {
	case StatusInit:
		return StatusInitSuccess
	case StatusCompensate:
		return StatusCompensateSuccess
	default:
		return ""
	}
}

func NewEvent() Event {
	event := Event{}
	event.CreatedAt = time.Now()
	event.Status = StatusInit
	uuid4, _ := uuid.NewRandom()
	event.ID = uuid4.String()

	return event
}

func (e Event) NewSagaSubEvent(startOffset int64) Event {
	event := NewEvent()
	event.SagaTransactionID = e.ID
	event.StartSagaOffset = startOffset

	return event
}

func (e Event) GetSuccessfulEvent() Event {
	successfulEvent := e
	successfulEvent.CreatedAt = time.Now()
	successfulEvent.Status = getSuccessfulStatus(e)

	return successfulEvent
}

func (e Event) GetCompensateEvent() Event {
	compensateEvent := e
	compensateEvent.CreatedAt = time.Now()
	compensateEvent.Status = StatusCompensate

	return compensateEvent
}
