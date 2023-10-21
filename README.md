# simple outbox pattern 

This is a library for simple [outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern.

## How to use

[example](https://github.com/koolay/outbox/tree/main/_example)

**Main service**

```go
import (
  "github.com/koolay/outbox/store/types"
  _ "github.com/koolay/outbox/store/pg"
  _ "github.com/lib/pq"
)

...

storage := types.GetStorager()
storage.Init(&types.Option{})

tx, err := db.Beginx()
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
	ProcessName: "order_processor",
	Body:        []byte(msg),
}); err != nil {
	panicIf(tx.Rollback())
}
panicIf(tx.Commit())

```

**Outbox processor**

```go

var db *sqlx.DB
processor := outbox.NewProcessor(
	"order_processor",
	func(ctx context.Context, msg types.Message) error {
	    // call third service or send message to queue
		return nil
	},
	db,
	outbox.WithConcurrentWorkers(2),
	outbox.WithMsgBufferSize(3),
)

processor.Start(ctx)

```
