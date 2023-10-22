package main

import (
	"context"
	"fmt"
	"log"
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
	processName       = "order_processor"
	totalTestMsgCount = 1000
)

func panicIf(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func initLog() {
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	slog.SetDefault(slog.New(logHandler))
}

func connect(connStr string) (*sqlx.DB, error) {
	slog.Info("start to connect to db")
	db, err := sqlx.ConnectContext(
		context.Background(),
		"postgres",
		connStr,
	)
	return db, err
}

func startPgServer(ctx context.Context) (testcontainers.Container, *sqlx.DB) {
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

	connStr, err := pgc.ConnectionString(context.Background(), "sslmode=disable")
	slog.Info("started pg server", "conn", connStr)
	panicIf(err)

	db, err := connect(connStr)
	panicIf(err)

	slog.Info("start run migrations")
	err = goose.Up(db.DB, "../deploy/pg_sql", goose.WithNoVersioning())
	panicIf(err)
	return pgc, db
}

func initApp() *cli.App {
	app := cli.App{
		Commands: []*cli.Command{
			{
				Name:  "proc",
				Usage: "process the message from outbox",
				Action: func(c *cli.Context) error {
					ctx := c.Context

					dbserver, db := startPgServer(ctx)
					defer dbserver.Terminate(ctx)

					processor := outbox.NewProcessor(
						"order_processor",
						func(ctx context.Context, msg types.Outbox) error {
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

					slog.Info("init cursor")
					panicIf(processor.InitCursor(ctx, 1))

					chSignal := make(chan os.Signal, 1)
					signal.Notify(chSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

					slog.Info("start processor")
					go func() {
						panicIf(processor.Start(ctx))
					}()

					select {
					case <-chSignal:
						slog.Info("received signal to shutdown")
						panicIf(processor.Shutdown(ctx))
					}
					return nil
				},
			},
			{
				Name:  "main",
				Usage: "main service",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "conn",
						Usage: "connection string",
						Value: "postgres://postgres:postgres@localhost:5432/goutbox?sslmode=disable",
					},
				},
				Action: func(c *cli.Context) error {
					ctx := c.Context

					db, err := connect(c.String("conn"))
					panicIf(err)

					chSignal := make(chan os.Signal, 1)
					signal.Notify(chSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

					go func() {
						storage := types.GetStorager()
						storage.Init(&types.Option{
							DB: db,
						})
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
								slog.Error("insert message failed", "err", err)
								if errRoll := tx.Rollback(); errRoll != nil {
									slog.Error("rollback failed", "err", errRoll)
								}
								return
							}

							// send message to outbox
							if err := storage.AddMessage(ctx, tx, types.Outbox{
								ProcessName: processName,
								Body:        []byte(msg),
							}); err != nil {
								slog.Error("insert outbox failed", "err", err)
								if errRoll := tx.Rollback(); errRoll != nil {
									slog.Error("rollback failed", "err", errRoll)
								}
								return
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
	initLog()
	app := initApp()
	panicIf(app.Run(os.Args))
}
