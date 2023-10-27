package pg

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"github.com/koolay/outbox/store/types"
)

const (
	sqlGetPositionWithLock = `SELECT position FROM "cursor" WHERE "process_name" = $1 LIMIT 1 FOR UPDATE SKIP LOCKED `
	sqlSetPosition         = `UPDATE "cursor"  SET "position" = $1 WHERE "process_name" = $2`
	sqlInsertMessage       = `INSERT INTO "outbox" ("process_name", "body") VALUES ($1, $2)`
	sqlGetMessagesFromPos  = `SELECT * FROM "outbox"
    WHERE "created_at" > (
      SELECT "created_at" FROM "outbox"
      WHERE "id" = $1
      LIMIT 1
    ) AND "created_at" < (NOW() - INTERVAL '20 milliseconds') and process_name = $2
    ORDER BY "created_at" ASC LIMIT $3`
	sqlUpsertCursor = `INSERT INTO cursor (process_name, position) VALUES ($1, $2) 
	ON CONFLICT (process_name) DO UPDATE 
  SET position = excluded.position;`
)

func init() {
	types.CurrentStorage = &Postgres{}
}

type Postgres struct {
	sess *sqlx.DB
}

func (pg *Postgres) Init(option *types.Option) {
	pg.sess = option.DB
}

func (pg *Postgres) UpsertCursor(ctx context.Context, cursor types.Cursor) error {
	if _, err := pg.sess.ExecContext(
		ctx,
		sqlUpsertCursor,
		cursor.ProcessName,
		cursor.Position,
	); err != nil {
		return errors.Wrap(err, "failed to upsert cursor")
	}
	return nil
}

func (pg *Postgres) GetMessagesFromPos(
	ctx context.Context,
	processName string,
	position int64,
	limit int,
) ([]types.Outbox, error) {
	var messages []types.Outbox
	err := pg.sess.SelectContext(
		ctx,
		&messages,
		sqlGetMessagesFromPos,
		position,
		processName,
		limit,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get messages")
	}

	return messages, nil
}

// AddMessage adds message to outbox
func (pg *Postgres) AddMessage(
	ctx context.Context,
	tx *sqlx.Tx,
	message types.Outbox,
) error {
	_, err := tx.ExecContext(ctx, sqlInsertMessage, message.ProcessName, message.Body)
	if err != nil {
		return errors.Wrap(err, "failed to add message")
	}

	return nil
}

func (pg *Postgres) GetPositionWithLock(ctx context.Context, processName string) (int64, error) {
	var position int64
	err := pg.sess.GetContext(ctx, &position, sqlGetPositionWithLock, processName)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get next position")
	}

	return position, nil
}

func (pg *Postgres) SetPosition(ctx context.Context, processName string, position int64) error {
	_, err := pg.sess.ExecContext(ctx, sqlSetPosition, position, processName)
	if err != nil {
		return errors.Wrap(err, "failed to set next position")
	}

	return nil
}

var _ types.Storager = (*Postgres)(nil)
