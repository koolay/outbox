package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/koolay/outbox"
	_ "github.com/koolay/outbox/store/pg"
	"github.com/koolay/outbox/store/types"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	cli "github.com/urfave/cli/v2"
)

var (
	processName       = "tester"
	totalTestMsgCount = 1000
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func initLog() {
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	})

	slog.SetDefault(slog.New(logHandler))
}

func connect() (*sqlx.DB, error) {
	slog.Info("start to connect to db")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	db, err := sqlx.ConnectContext(
		ctx,
		"postgres",
		"host=localhost port=5543 user=postgres password=postgres dbname=goutbox sslmode=disable",
	)
	return db, err
}

func startPgServer(quit <-chan struct{}) testcontainers.Container {
	dbName := "goutbox"
	dbUser := "postgres"
	dbPassword := "postgres"

	pgc, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("docker.io/postgres:15.2-alpine"),
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2*time.Minute)),
	)

	panicIf(err)
	// defer func() {
	// 	if err := pgc.Terminate(ctx); err != nil {
	// 		panic(err)
	// 	}
	// }()

	time.Sleep(1 * time.Second)

	db, err := connect()
	panicIf(err)

	slog.Info("start run migrations")

	goose.Up(db.DB, "migrations")
	return pgc
}

func initApp() *cli.App {
	app := cli.App{
		Commands: []*cli.Command{
			{
				Name:  "proc",
				Usage: "process the message from outbox",
				Action: func(c *cli.Context) error {
					ctx := c.Context
					chQuit := make(chan struct{})

					dbserver := startPgServer(chQuit)
					defer dbserver.Terminate(ctx)

					db, err := connect()
					panicIf(err)

					processor := outbox.NewProcessor(
						"order_processor",
						func(ctx context.Context, msg types.Message) error {
							slog.Info(
								"message received, and handle it",
								"body",
								string(msg.Body),
								"processor",
								msg.ProcessName,
							)
							return nil
						},
						db,
						outbox.WithConcurrentWorkers(2),
						outbox.WithMsgBufferSize(3),
					)

					chSignal := make(chan os.Signal, 1)
					signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)

					go panicIf(processor.Start(ctx))
					select {
					case <-chSignal:
						processor.Shutdown(ctx)
					}
					return nil
				},
			},
			{
				Name:  "main",
				Usage: "main service",
				Action: func(c *cli.Context) error {
					ctx := c.Context

					chQuit := make(chan struct{})
					dbserver := startPgServer(chQuit)
					defer dbserver.Terminate(ctx)
					db, err := connect()
					panicIf(err)

					chSignal := make(chan os.Signal, 1)
					signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
					go func() {
						storage := types.GetStorager()
						storage.Init(&types.Option{})
						var i int
						for {
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
								panicIf(tx.Rollback())
							}

							// send message to outbox
							if err := storage.AddMessage(ctx, tx, types.Message{
								ProcessName: processName,
								Body:        []byte(msg),
							}); err != nil {
								panicIf(tx.Rollback())
							}

							panicIf(tx.Commit())
							slog.Info("insert message success", "num", i)
							time.Sleep(500 * time.Millisecond)
						}
					}()
					select {
					case <-chSignal:
						slog.Info("main service is shutdown")
					}

					return nil
				},
			},
		},
	}
	return &app
}

func main() {
	app := initApp()
	panicIf(app.Run(os.Args))
}
