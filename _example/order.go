package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/koolay/outbox/store/types"
)

var quitCreating atomic.Bool

func createOrder(ctx context.Context, db *sqlx.DB) {
	storage := types.GetStorager()
	storage.Init(&types.Option{
		DB: db,
	})
	var i int
	for {
		if quitCreating.Load() {
			slog.Info("create order finished by signal")
			return
		}
		tx, err := db.Beginx()
		panicIf(err)

		if i >= totalTestMsgCount {
			slog.Info("test finished")
			return
		}

		i++
		// do main business
		msg := fmt.Sprintf("test: %v", i)
		_, err = tx.ExecContext(
			ctx,
			"insert into example (subject) values ($1)",
			msg,
		)
		if err != nil {
			slog.Error("insert message failed", "err", err)
			if errRoll := tx.Rollback(); errRoll != nil {
				slog.Error("rollback failed", "err", errRoll)
			}
			return
		}

		// send message to outbox
		if errAdd := storage.AddMessage(ctx, tx, types.Outbox{
			ProcessName: processName,
			Body:        []byte(msg),
		}); errAdd != nil {
			slog.Error("insert outbox failed", "err", errAdd)
			if errRoll := tx.Rollback(); errRoll != nil {
				slog.Error("rollback failed", "err", errRoll)
			}
			return
		}

		panicIf(tx.Commit())
		slog.Info("insert message success", "num", i)
		time.Sleep(500 * time.Millisecond)
	}
}
