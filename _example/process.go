package main

import (
	"context"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/koolay/outbox"
	"github.com/koolay/outbox/store/types"
)

func process(db *sqlx.DB, chQuit, chDone chan struct{}) {
	processor := outbox.NewProcessor(
		"order_processor",
		func(ctx context.Context, msg types.Outbox) error {
			_, err := db.ExecContext(
				ctx,
				"insert into sinked (subject) values ($1)",
				string(msg.Body),
			)
			slog.Info(
				"message received, and handle it",
				"body",
				string(msg.Body),
				"processor",
				msg.ProcessName,
			)
			return err
		},
		db,
		outbox.WithConcurrentWorkers(2),
		outbox.WithMsgBufferSize(3),
	)

	ctx := context.Background()
	slog.Info("init cursor")
	panicIf(processor.InitCursor(ctx, 1))

	slog.Info("start processor")
	go func() {
		panicIf(processor.Start(ctx))
	}()

	<-chQuit
	slog.Info("received signal to shutdown")
	panicIf(processor.Shutdown())
	chDone <- struct{}{}
}
