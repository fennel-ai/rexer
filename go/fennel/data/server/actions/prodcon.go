package actions

import (
	"time"

	"fennel/data/lib"
)

type ActionProducer interface {
	LogAction(*lib.ProtoAction) error
	Flush(timeout time.Duration) int
}

type ActionConsumer interface {
	ReadActionMessage() (*lib.ProtoAction, error)
}

var _ ActionProducer = (*LocalActionProducer)(nil)
var _ ActionConsumer = (*LocalActionConsumer)(nil)

type LocalActionProducer struct {
	ch chan<- *lib.ProtoAction
}

func NewLocalActionProducer(ch chan<- *lib.ProtoAction) *LocalActionProducer {
	return &LocalActionProducer{ch}
}

func (lp *LocalActionProducer) LogAction(action *lib.ProtoAction) error {
	lp.ch <- action
	return nil
}

func (lp *LocalActionProducer) Flush(timeout time.Duration) int {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if len(lp.ch) == 0 {
				return 0
			}
		case <-timer.C:
			return len(lp.ch)
		}
	}
}

type LocalActionConsumer struct {
	ch <-chan *lib.ProtoAction
}

func NewLocalActionConsumer(ch <-chan *lib.ProtoAction) *LocalActionConsumer {
	return &LocalActionConsumer{ch}
}

func (lc *LocalActionConsumer) ReadActionMessage() (*lib.ProtoAction, error) {
	return <-lc.ch, nil
}
