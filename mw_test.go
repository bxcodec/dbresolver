package dbresolver

import (
	"context"
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"testing"
)

func TestMultiWrite(t *testing.T) {

	noOfPrimaries := 2
	noOfReplicas := 4

	primaries := make([]*sql.DB, noOfPrimaries)
	replicas := make([]*sql.DB, noOfReplicas)

	mockPimaries := make([]sqlmock.Sqlmock, noOfPrimaries)
	mockReplicas := make([]sqlmock.Sqlmock, noOfReplicas)

	for i := 0; i < noOfPrimaries; i++ {

		db, mock, err := createMock()

		if err != nil {
			t.Fatal("creating of mock failed")
		}

		defer mock.ExpectClose()
		defer db.Close()

		primaries[i] = db
		mockPimaries[i] = mock

	}

	for i := 0; i < noOfReplicas; i++ {

		db, mock, err := createMock()

		if err != nil {
			t.Fatal("creating of mock failed")
		}

		defer mock.ExpectClose()
		defer db.Close()

		replicas[i] = db
		mockReplicas[i] = mock
	}

	resolver := New(WithPrimaryDBs(primaries...), WithReplicaDBs(replicas...)).(*sqlDB)

	t.Run("primary dbs", func(t *testing.T) {

		for i := 0; i < noOfPrimaries*5; i++ {
			robin := resolver.loadBalancer.Predict(noOfPrimaries)
			mock := mockPimaries[robin]

			switch i % 5 {

			case 0:
				query := "SET timezone TO 'Asia/Tokyo'"
				mock.ExpectExec(query)
				resolver.Exec(query)
				t.Log("exec")
			case 1:
				query := "SET timezone TO 'Asia/Tokyo'"
				mock.ExpectExec(query)
				resolver.ExecContext(context.TODO(), query)
				t.Log("exec context")
			case 2:
				mock.ExpectBegin()
				resolver.Begin()
				t.Log("begin")
			case 4:
				mock.ExpectBegin()
				resolver.BeginTx(context.TODO(), &sql.TxOptions{
					Isolation: sql.LevelDefault,
					ReadOnly:  false,
				})
				t.Log("begin transaction")

			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		}
	})

	t.Run("replica dbs", func(t *testing.T) {

		for i := 0; i < noOfReplicas*5; i++ {

			robin := resolver.loadBalancer.Predict(noOfReplicas)
			mock := mockReplicas[robin]

			switch i % 5 {

			case 0:
				query := "select 1'"
				mock.ExpectQuery(query)
				resolver.Query(query)
				t.Log("query")
			case 1:
				query := "select 1'"
				mock.ExpectQuery(query)
				resolver.QueryRow(query)
				t.Log("query row")
			case 2:
				query := "select 1'"
				mock.ExpectQuery(query)
				resolver.QueryContext(context.TODO(), query)
				t.Log("query context")
			case 4:
				query := "select 1'"
				mock.ExpectQuery(query)
				resolver.QueryRowContext(context.TODO(), query)
				t.Log("query row context")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		}
	})

	t.Run("prepare", func(t *testing.T) {
		query := "select 1"

		for _, mock := range mockPimaries {
			mock.ExpectPrepare(query)
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}(mock)
		}
		for _, mock := range mockReplicas {
			mock.ExpectPrepare(query)
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}(mock)
		}

		stmt, err := resolver.Prepare(query)
		if err != nil {
			t.Error("prepare failed")
			return
		}

		robin := resolver.stmtLoadBalancer.Predict(noOfPrimaries)
		mock := mockPimaries[robin]

		mock.ExpectExec(query)

		stmt.Exec()

	})

	t.Run("ping", func(t *testing.T) {
		for _, mock := range mockPimaries {
			mock.ExpectPing()
			mock.ExpectPing()
		}
		for _, mock := range mockReplicas {
			mock.ExpectPing()
			mock.ExpectPing()
		}

		resolver.Ping()
		resolver.PingContext(context.TODO())
		t.Log("ping")

	})

}

func createMock() (db *sql.DB, mock sqlmock.Sqlmock, err error) {
	db, mock, err = sqlmock.New(sqlmock.MonitorPingsOption(true))
	return
}
