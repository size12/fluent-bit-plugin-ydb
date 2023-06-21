package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/size12/fluent-bit-plugin-ydb/config"
	"github.com/size12/fluent-bit-plugin-ydb/model"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	_ "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type YDB struct {
	db  *sql.DB
	cfg config.Config
}

func (s *YDB) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	db, err := sql.Open("ydb", s.cfg.ConnectionURL)

	if err != nil {
		log.Fatal(err)
		return err
	}

	_, err = db.ExecContext(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf(`CREATE TABLE %s (
    			eventTime Timestamp,
    			metadata   String,
    			message    Json,
    			PRIMARY KEY (
					eventTime
				));`, s.cfg.TableName))

	if err != nil {
		log.Fatalf("[ydb] failed create events table: %v\n", err)
		return err
	}

	s.db = db
	return nil
}

func (s *YDB) Write(events []*model.Event) error {
	data := make([]types.Value, 0, len(events))

	for _, event := range events {
		b, err := json.Marshal(event.Message)
		if err != nil {
			log.Printf("Failed marshal event %v: %v", event.Message, b)
			continue
		}

		data = append(data, types.StructValue(
			types.StructFieldValue("eventTime", types.TimestampValueFromTime(event.Timestamp)),
			types.StructFieldValue("metadata", types.StringValueFromString(event.Metadata)),
			types.StructFieldValue("message", types.JSONValueFromBytes(b)),
		))
	}

	_, err := s.db.ExecContext(ydb.WithQueryMode(context.Background(), ydb.DataQueryMode),

		fmt.Sprintf(`DECLARE $events AS  List<Struct<eventTime: Timestamp, metadata: String, message: Json>>;
			INSERT INTO %s SELECT eventTime, metadata, message FROM AS_TABLE($events);`, s.cfg.TableName),
		table.ValueParam("$events", types.ListValue(data...)),
	)
	if err != nil {
		log.Printf("[ydb] failed write events: %v\n", err)
		return err
	}

	return nil
}

func (s *YDB) Exit() error {
	return s.db.Close()
}
