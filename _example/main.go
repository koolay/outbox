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
	_ "github.com/koolay/outbox/store/pg"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

func setupAndstartPgServer(ctx context.Context) (testcontainers.Container, *sqlx.DB) {
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

func checkResult(db *sqlx.DB) error {
	ctx := context.Background()
	var source int
	var dest int
	err := db.GetContext(ctx, &source, "select count(*) from example")
	if err != nil {
		return fmt.Errorf("failed to get source count: %w", err)
	}

	err = db.GetContext(ctx, &dest, "select count(*) from sinked")
	if err != nil {
		return fmt.Errorf("failed to get dest count: %w", err)
	}

	slog.Info("check result", "source", source, "dest", dest)
	return nil
}

func main() {
	chQuit := make(chan struct{}, 1)
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	initLog()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbserver, db := setupAndstartPgServer(ctx)
	defer func() {
		panicIf(dbserver.Terminate(ctx))
	}()

	go createOrder(ctx, db)

	chDone := make(chan struct{}, 1)
	go process(db, chQuit, chDone)

	<-chSignal
	quitCreating.Store(true)
	close(chQuit)
	slog.Info("main service is shutdown, and wait for processor graceful shutdown")
	<-chDone
	panicIf(checkResult(db))
}
