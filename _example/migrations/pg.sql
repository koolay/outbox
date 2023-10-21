CREATE TABLE "cursor" (
     "process_name" TEXT NOT NULL PRIMARY KEY, 
     "position" BIGINT NOT NULL DEFAULT 0,
     "created_at" TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
     "updated_at" TIMESTAMP
);

CREATE TRIGGER cursor_update_trigger 
 BEFORE UPDATE ON "cursor" 
 FOR EACH ROW 
 EXECUTE FUNCTION set_updated_at();

CREATE TABLE "outbox" (
     "id" SERIAL NOT NULL PRIMARY KEY,
     "process_name" TEXT NOT NULL,
     "payload" JSONB NOT NULL,
     "created_at" TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
     "updated_at" TIMESTAMP
)
CREATE INDEX "outbox_process_name_idx" ON "outbox" ("process_name");
CREATE INDEX "outbox_created_at_idx" ON "outbox" ("created_at");

CREATE TABLE "example" (
     "id" SERIAL NOT NULL PRIMARY KEY,
     "subject" TEXT NOT NULL
)
