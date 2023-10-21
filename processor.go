package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sourcegraph/conc/pool"

	"github.com/koolay/outbox/store/types"
)

const (
	defaultMsgBufferSize     = 10
	defaultProcess           = "default"
	defaultConcurrentWorkers = 10
)

type (
	MsgHandle func(ctx context.Context, msg types.Message) error
	Processor struct {
		name              string
		storage           types.Storager
		msgBufferSize     int
		msgHandle         MsgHandle
		concurrentWorkers int
	}
)

func NewProcessor(
	name string,
	msgHandle MsgHandle,
	db *sqlx.DB,
	options ...func(*Processor),
) *Processor {
	if name == "" {
		name = defaultProcess
	}

	storager := types.GetStorager()
	storager.Init(&types.Option{ProcessName: name, DB: db})

	return &Processor{
		name:          name,
		storage:       storager,
		msgBufferSize: defaultMsgBufferSize,
		msgHandle:     msgHandle,
	}
}

func WithMsgBufferSize(msgBufferSize int) func(*Processor) {
	return func(p *Processor) {
		if msgBufferSize <= 0 {
			p.msgBufferSize = defaultMsgBufferSize
			return
		}
		p.msgBufferSize = msgBufferSize
	}
}

func WithConcurrentWorkers(concurrentWorkers int) func(*Processor) {
	return func(p *Processor) {
		if concurrentWorkers <= 0 {
			p.concurrentWorkers = defaultConcurrentWorkers
			return
		}
		p.concurrentWorkers = concurrentWorkers
	}
}

func (p *Processor) getNextPostion(ctx context.Context, processName string) (int64, error) {
	sqlctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	position, err := p.storage.GetPositionWithLock(sqlctx, processName)
	if err != nil {
		return 0, fmt.Errorf("failed to get next position, error: %w", err)
	}

	return position, nil
}

func (p *Processor) setPosition(ctx context.Context, processName string, position int64) error {
	sqlctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err := p.storage.SetPosition(sqlctx, processName, position)
	if err != nil {
		return fmt.Errorf("failed to set next position, error: %w", err)
	}
	return nil
}

func (p *Processor) addMessage(ctx context.Context, tx *sqlx.Tx, message types.Message) error {
	sqlctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := p.storage.AddMessage(sqlctx, tx, message)
	if err != nil {
		return fmt.Errorf("failed to add message, error: %w", err)
	}
	return nil
}

func (p *Processor) getMessagesFromPos(
	ctx context.Context,
	processName string,
	pos int64,
) ([]types.Message, error) {
	sqlctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	messages, err := p.storage.GetMessagesFromPos(sqlctx, processName, pos)
	if err != nil {
		return nil, fmt.Errorf("failed to get messages, error: %w", err)
	}
	return messages, nil
}

func (p *Processor) Start(ctx context.Context) error {
	pl := pool.New().WithMaxGoroutines(10).WithContext(ctx).WithCancelOnError()

	var errRtv error
	for {
		position, err := p.getNextPostion(ctx, p.name)
		if err != nil {
			errRtv = err
			break
		}

		msgs, err := p.getMessagesFromPos(ctx, p.name, position)
		if err != nil {
			errRtv = err
			break
		}

		for _, msg := range msgs {
			msg := msg
			pl.Go(func(ctx context.Context) error {
				return p.msgHandle(ctx, msg)
			})
		}
	}

	if err := pl.Wait(); err != nil {
		return err
	}

	return errRtv
}

func (p *Processor) Shutdown(ctx context.Context) error {
	slog.Info("processor is shutdown")
	return nil
}
