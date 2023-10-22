package types

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

type Outbox struct {
	ID          int64     `json:"id,omitempty"           db:"id"`
	ProcessName string    `json:"process_name,omitempty" db:"process_name"`
	Body        []byte    `json:"body,omitempty"         db:"body"`
	CreatedAt   time.Time `json:"created_at,omitempty"   db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"   db:"updated_at"`
}

type Cursor struct {
	ProcessName string `json:"process_name,omitempty" db:"process_name"`
	Position    int64  `json:"position,omitempty"     db:"position"`
}

var CurrentStorage Storager

type Option struct {
	DB *sqlx.DB
}

type Storager interface {
	Init(option *Option)
	UpsertCursor(ctx context.Context, cursor Cursor) error
	AddMessage(ctx context.Context, tx *sqlx.Tx, message Outbox) error
	GetPositionWithLock(ctx context.Context, processName string) (int64, error)
	SetPosition(ctx context.Context, processName string, position int64) error
	GetMessagesFromPos(
		ctx context.Context,
		processName string,
		position int64,
		limit int,
	) ([]Outbox, error)
}

func GetStorager() Storager {
	return CurrentStorage
}
