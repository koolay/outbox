package types

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type Message struct {
	ID          int64  `json:"id,omitempty"`
	ProcessName string `json:"process_name,omitempty"`
	Body        []byte `json:"body,omitempty"`
}

var CurrentStorage Storager

type Option struct {
	ProcessName string
	DB          *sqlx.DB
}

type Storager interface {
	Init(option *Option)
	AddMessage(ctx context.Context, tx *sqlx.Tx, message Message) error
	GetPositionWithLock(ctx context.Context, processName string) (int64, error)
	SetPosition(ctx context.Context, processName string, position int64) error
	GetMessagesFromPos(ctx context.Context, processName string, pos int64) ([]Message, error)
}

func GetStorager() Storager {
	return CurrentStorage
}
