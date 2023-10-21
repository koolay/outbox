package pg

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/koolay/outbox/store/types"
)

const (
	sqlGetPositionWithLockForPg = `SELECT position FROM "cursor" WHERE "process_name" = $1 LIMIT 1 FOR UPDATE SKIP LOCKED `
	sqlSetPositionForPg         = `UPDATE "cursor"  SET "position" = $1 WHERE "process_name" = $2`
	sqlInsertMessageForPg       = `INSERT INTO "outbox" ("body") VALUES ($1)`
	sqlGetMessagesFromPosForPg  = `SELECT * FROM "outbox"
    WHERE "created_at" > (
      SELECT "created_at" FROM "outbox"
      WHERE "id" = $1
      LIMIT 1
    ) AND "created_at" < (NOW() - INTERVAL '20 milliseconds') and process_name = $2
    ORDER BY "created_at" ASC`
)

func init() {
	types.CurrentStorage = &Postgres{}
}

type Postgres struct {
	name string
	sess *sqlx.DB
}

func (postgres *Postgres) Init(option *types.Option) {
	postgres.name = option.ProcessName
	postgres.sess = option.DB
}

func (postgres *Postgres) GetMessagesFromPos(
	ctx context.Context,
	processName string,
	pos int64,
) ([]types.Message, error) {
	var messages []types.Message
	err := postgres.sess.SelectContext(ctx, &messages, sqlGetMessagesFromPosForPg, processName, pos)
	if err != nil {
		return nil, fmt.Errorf("failed to get messages, error: %w", err)
	}

	return messages, nil
}

// AddMessage adds message to outbox
func (postgres *Postgres) AddMessage(
	ctx context.Context,
	tx *sqlx.Tx,
	message types.Message,
) error {
	_, err := tx.ExecContext(ctx, sqlInsertMessageForPg, message.Body)
	if err != nil {
		return fmt.Errorf("failed to add message, error: %w", err)
	}

	return nil
}

func NewPostgres(sess *sqlx.DB, name string) *Postgres {
	return &Postgres{
		name: name,
		sess: sess,
	}
}

func (c *Postgres) GetPositionWithLock(ctx context.Context, processName string) (int64, error) {
	var position int64
	err := c.sess.GetContext(ctx, &position, sqlGetPositionWithLockForPg, processName)
	if err != nil {
		return 0, fmt.Errorf("failed to get next position, error: %w", err)
	}

	return position, nil
}

func (c *Postgres) SetPosition(ctx context.Context, processName string, position int64) error {
	_, err := c.sess.ExecContext(ctx, sqlSetPositionForPg, position, c.name)
	if err != nil {
		return fmt.Errorf("failed to set next position, error: %w", err)
	}

	return nil
}

var _ types.Storager = (*Postgres)(nil)
