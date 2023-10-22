package outbox

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sourcegraph/conc/pool"

	"github.com/koolay/outbox/store/types"
)

const (
	defaultMsgBufferSize       = 10
	defaultProcess             = "default"
	defaultConcurrentWorkers   = 10
	defaultIntervalMillseconds = 1000
	defaultLimitPerFetch       = 100

	NoRecord int64 = -1
)

type (
	MsgHandle func(ctx context.Context, msg types.Outbox) error
	Processor struct {
		isQuit              atomic.Bool
		chDone              chan struct{}
		name                string
		storage             types.Storager
		msgBufferSize       int
		msgHandle           MsgHandle
		concurrentWorkers   int
		intervalMillseconds int
		limitPerFetch       int
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
	storager.Init(&types.Option{DB: db})

	processor := &Processor{
		name:                name,
		storage:             storager,
		msgBufferSize:       defaultMsgBufferSize,
		intervalMillseconds: defaultIntervalMillseconds,
		concurrentWorkers:   defaultConcurrentWorkers,
		limitPerFetch:       defaultLimitPerFetch,
		msgHandle:           msgHandle,
		chDone:              make(chan struct{}, 1),
	}

	for _, option := range options {
		option(processor)
	}

	return processor
}

func WithLimitPerFetch(limitPerFetch int) func(*Processor) {
	return func(p *Processor) {
		if limitPerFetch <= 0 {
			p.limitPerFetch = defaultLimitPerFetch
			return
		}
		p.limitPerFetch = limitPerFetch
	}
}

func WithIntervalMillseconds(intervalMillseconds int) func(*Processor) {
	return func(p *Processor) {
		if intervalMillseconds <= 0 {
			p.intervalMillseconds = defaultIntervalMillseconds
			return
		}
		p.intervalMillseconds = intervalMillseconds
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
	if errors.Is(err, sql.ErrNoRows) {
		return NoRecord, nil
	}

	if err != nil {
		return 0, err
	}

	return position, nil
}

func (p *Processor) setPosition(ctx context.Context, processName string, position int64) error {
	sqlctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err := p.storage.SetPosition(sqlctx, processName, position)
	if err != nil {
		return err
	}
	return nil
}

func (p *Processor) addMessage(ctx context.Context, tx *sqlx.Tx, message types.Outbox) error {
	sqlctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := p.storage.AddMessage(sqlctx, tx, message)
	if err != nil {
		return err
	}
	return nil
}

func (p *Processor) getMessagesFromPos(
	ctx context.Context,
	processName string,
	pos int64,
) ([]types.Outbox, error) {
	sqlctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	messages, err := p.storage.GetMessagesFromPos(sqlctx, processName, pos, p.limitPerFetch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}
	return messages, nil
}

// InitCursor initializes cursor with a processor
func (p *Processor) InitCursor(ctx context.Context, position int64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.storage.UpsertCursor(ctx, types.Cursor{
		ProcessName: p.name,
		Position:    position,
	})
}

func (p *Processor) Start(ctx context.Context) error {
	var errRtv error

	var positionLock sync.Mutex
	for {
		pl := pool.New().WithMaxGoroutines(10).WithContext(ctx).WithCancelOnError()

		if p.isQuit.Load() {
			slog.Warn("processor has be request to quit", "process", p.name)
			break
		}

		position, err := p.getNextPostion(ctx, p.name)
		if err != nil {
			errRtv = err
			break
		}

		if position == NoRecord {
			slog.Debug("get next position, there is no processor", "process", p.name)
			time.Sleep(5 * time.Second)
			continue
		}

		msgs, err := p.getMessagesFromPos(ctx, p.name, position)
		if err != nil {
			errRtv = err
			break
		}

		if len(msgs) == 0 {
			slog.Debug("got messages", "count", len(msgs))
			time.Sleep(5 * time.Second)
		}

		var latestPosition int64

		for _, msg := range msgs {
			msg := msg
			pl.Go(func(ctx context.Context) error {
				if errHandle := p.msgHandle(ctx, msg); errHandle != nil {
					return errHandle
				}
				positionLock.Lock()
				if latestPosition < msg.ID {
					latestPosition = msg.ID
				}
				positionLock.Unlock()
				return nil
			})
		}

		if err := pl.Wait(); err != nil {
			return err
		}

		if latestPosition > 0 {
			if serr := p.setPosition(ctx, p.name, latestPosition); serr != nil {
				slog.Error("failed to set position", "err", serr, "position", latestPosition)
			}
		}

		time.Sleep(time.Duration(p.intervalMillseconds) * time.Millisecond)
	}

	p.chDone <- struct{}{}
	return errRtv
}

func (p *Processor) Shutdown(ctx context.Context) error {
	slog.Info("processor is shutdowning")
	p.isQuit.Store(true)
	select {
	case <-p.chDone:
		slog.Info("processor has shutdowned")
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("processor has shutdown timeout")
	}
}
