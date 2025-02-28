package sqlstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-utility/v2/env"
	"github.com/SKF/go-utility/v2/pgxcompat"
	"github.com/SKF/go-utility/v2/uuid"
)

func getConnectionString() string {
	user := env.MustGetAsString("PGUSER")

	return fmt.Sprintf("host=%s port=%s user=%s password=%s sslmode=%s dbname=%s",
		env.GetAsString("PGHOST", "localhost"),
		env.GetAsString("PGPORT", "5432"),
		user,
		env.MustGetAsString("PGPASSWORD"),
		env.MustGetAsString("PGSSLMODE"),
		env.GetAsString("PGDB", user),
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

	connStr := getConnectionString()
	dbConfig, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err, "Could not parse db connection string %s", connStr)

	dbConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		conn.ConnInfo().RegisterDataType(pgtype.DataType{
			Value: &pgxcompat.UUID{}, // nolint:exhaustivestruct
			Name:  "uuid",
			OID:   pgtype.UUIDOID,
		})

		return nil
	}

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
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	tableName := make([]rune, numChars)

	for i := range numChars {
		tableName[i] = letters[rand.Intn(len(letters))] // nolint:gosec
	}

	return string(tableName)
}

func createTableQuery() (tableName, query string) {
	tableName = randomTableName()

	return tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			sequence_id character(26) PRIMARY KEY,
			aggregate_id uuid,
			user_id uuid,
			created_at bigint NOT NULL,
			type character varying(255),
			data bytea
		)`, tableName)
}

// createTestEvents - create some random test events in sequence.
func createTestEvents(store eventsource.Store, numberOfEvents int, eventTypeList []string, eventDataList [][]byte) ([]eventsource.Record, error) {
	result := []eventsource.Record{}

	for i := range numberOfEvents {
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

		tx, err := store.NewTransaction(ctx, event)
		if err != nil {
			return result, err
		}

		if err = tx.Commit(); err != nil {
			return result, err
		}

		var records []eventsource.Record

		records, err = store.LoadByAggregate(ctx, aggID.String())
		if err != nil {
			return result, err
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

	return result, nil
}
