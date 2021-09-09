package sqlstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-utility/env"
	"github.com/SKF/go-utility/v2/pgxcompat"
	"github.com/SKF/go-utility/v2/uuid"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func getConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s sslmode=%s",
		env.MustGetAsString("PGHOST"),
		env.MustGetAsString("PGPORT"),
		env.MustGetAsString("PGUSER"),
		env.MustGetAsString("PGPASSWORD"),
		env.MustGetAsString("PGSSLMODE"),
	)
}

func setupDB(t *testing.T) (*sql.DB, string) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping postgres e2e test")
	}

	db, err := sql.Open("postgres", getConnectionString())
	require.NoError(t, err, "Could not connect to db")

	tableName, query := createTableQuery()
	_, err = db.Exec(query)
	require.NoError(t, err, "Could not create table")

	return db, tableName
}

func setupDBPgx(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping postgres e2e test")
	}

	dbConfig, err := pgxpool.ParseConfig(getConnectionString())
	require.NoError(t, err, "Could not parse db connection string %s", getConnectionString())

	dbConfig.MaxConns = 150
	dbConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		conn.ConnInfo().RegisterDataType(pgtype.DataType{
			Value: &pgxcompat.UUID{}, // nolint:exhaustivestruct
			Name:  "uuid",
			OID:   pgtype.UUIDOID,
		})

		return nil
	}
	dbConfig.LazyConnect = true

	db, err := pgxpool.ConnectConfig(context.Background(), dbConfig)
	require.NoError(t, err, "Could not connect to db")

	tableName, query := createTableQuery()
	_, err = db.Exec(ctx, query)
	require.NoError(t, err, "Could not create table")

	return db, tableName
}

func cleanupDBGeneric(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()

	defer db.Close()
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	require.NoError(t, err, "Could not perform DB cleanup")
}

func cleanupDBPgx(t *testing.T, db *pgxpool.Pool, tableName string) {
	t.Helper()

	defer db.Close()
	_, err := db.Exec(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
	require.NoError(t, err, "Could not perform DB cleanup")
}

func randomTableName() string {
	numChars := 30
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	tableName := make([]rune, numChars)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numChars; i++ {
		tableName[i] = letters[rand.Intn(len(letters))] // nolint:gosec
	}

	return string(tableName)
}

func createTableQuery() (tableName, query string) {
	return tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			sequence_id character(26) PRIMARY KEY,
			aggregate_id uuid,
			user_id uuid,
			created_at bigint NOT NULL,
			type character varying(255),
			data bytea
		)`, randomTableName())
}

// createTestEvents - create some random test events in sequence.
func createTestEvents(store eventsource.Store, numberOfEvents int, eventTypeList []string, eventDataList [][]byte) (result []eventsource.Record, err error) {
	result = []eventsource.Record{}

	for i := 0; i < numberOfEvents; i++ {
		aggID := uuid.New()
		userID := uuid.New()
		eventType := fmt.Sprintf("TestEvent %d", i+1)

		if i < len(eventTypeList) {
			eventType = eventTypeList[i]
		}

		eventData := []byte(fmt.Sprintf("TestEventData %d", i+1))
		if i < len(eventDataList) {
			eventData = eventDataList[i]
		}

		event := eventsource.Record{
			AggregateID: aggID.String(),
			UserID:      userID.String(),
			SequenceID:  eventsource.NewULID(),
			Type:        eventType,
			Timestamp:   time.Now().UnixNano(),
			Data:        eventData,
		}

		ctx := context.Background()

		var tx eventsource.StoreTransaction

		if tx, err = store.NewTransaction(ctx, event); err != nil {
			return
		}

		if err = tx.Commit(); err != nil {
			return
		}

		var records []eventsource.Record

		records, err = store.LoadByAggregate(ctx, aggID.String())
		if err != nil {
			return
		}

		if len(records) != 1 {
			return result, fmt.Errorf("Expected one result from store, got %d", len(records)) // nolint:goerr113
		}

		event.SequenceID = records[0].SequenceID

		if !reflect.DeepEqual(event, records[0]) {
			return result, fmt.Errorf("Expected identical records, saved: %v  loaded: %v", event, records[0]) // nolint:goerr113
		}

		result = append(result, records[0])
	}

	return result, fmt.Errorf("failed to create events: %w", err)
}
