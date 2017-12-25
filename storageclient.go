package saga

// StorageClient interface
type StorageClient interface {
	Produce(string, Event) (int32, int64, error)
	Consume(string, int64) (<-chan Message, error)
}
