-- +goose Up
-- +goose StatementBegin
CREATE  FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';
-- +goose StatementEnd

CREATE TABLE cursor (
     process_name TEXT NOT NULL PRIMARY KEY, 
     position BIGINT NOT NULL DEFAULT 0,
     created_at TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
     updated_at TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

CREATE TRIGGER cursor_update_trigger 
 BEFORE UPDATE ON cursor 
 FOR EACH ROW 
 EXECUTE FUNCTION update_updated_at();

CREATE TABLE outbox (
     id SERIAL NOT NULL PRIMARY KEY,
     process_name TEXT NOT NULL,
     body bytea NOT NULL,
     created_at TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
     updated_at TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP()
);
CREATE INDEX outbox_process_name_idx ON outbox (process_name);
CREATE INDEX outbox_created_at_idx ON outbox (created_at);
CREATE TRIGGER outbox_update_trigger 
 BEFORE UPDATE ON outbox  
 FOR EACH ROW 
 EXECUTE FUNCTION update_updated_at();


CREATE TABLE example (
     id SERIAL NOT NULL PRIMARY KEY,
     subject TEXT NOT NULL
);

CREATE TABLE sinked (
     id SERIAL NOT NULL PRIMARY KEY,
     subject TEXT NOT NULL
);

