package dbresolver

import (
	"context"
	"database/sql"
	"fmt"
	"go.uber.org/goleak"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type DBConfig struct {
	primaryDBCount uint8
	replicaDBCount uint8
	lbPolicy       LoadBalancerPolicy
}

var LoadBalancerPolicies = []LoadBalancerPolicy{
	RandomLB,
	RoundRobinLB,
}

func handleDBError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("db error: %s", err)
	}

}

func testMW(t *testing.T, config DBConfig) {

	noOfPrimaries, noOfReplicas := int(config.primaryDBCount), int(config.replicaDBCount)
	lbPolicy := config.lbPolicy

	primaries := make([]*sql.DB, noOfPrimaries)
	replicas := make([]*sql.DB, noOfReplicas)

	mockPrimaries := make([]sqlmock.Sqlmock, noOfPrimaries)
	mockReplicas := make([]sqlmock.Sqlmock, noOfReplicas)

	for i := 0; i < noOfPrimaries; i++ {
		db, mock, err := createMock()

		if err != nil {
			t.Fatal("creating of mock failed")
		}

		primaries[i] = db
		mockPrimaries[i] = mock
	}

	for i := 0; i < noOfReplicas; i++ {
		db, mock, err := createMock()
		if err != nil {
			t.Fatal("creating of mock failed")
		}

		replicas[i] = db
		mockReplicas[i] = mock
	}

	resolver := New(WithPrimaryDBs(primaries...), WithReplicaDBs(replicas...), WithLoadBalancer(lbPolicy)).(*sqlDB)

	t.Run("primary dbs", func(t *testing.T) {
		var err error

		for i := 0; i < noOfPrimaries*6; i++ {
			robin := resolver.loadBalancer.predict(noOfPrimaries)
			mock := mockPrimaries[robin]

			switch i % 6 {
			case 0:
				query := "SET timezone TO 'Asia/Tokyo'"
				mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))
				_, err = resolver.Exec(query)
			case 1:
				query := "CREATE DATABASE test; use test"
				mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0)).WillDelayFor(time.Millisecond * 50)
				_, err = resolver.ExecContext(context.Background(), query)
			case 2:
				t.Log("transactions:begin")

				mock.ExpectBegin()
				tx, err := resolver.Begin()
				handleDBError(t, err)

				query := `CREATE TABLE users (id serial PRIMARY KEY, name varchar(50) unique)`
				mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))

				_, err = tx.Exec(query)
				handleDBError(t, err)

				mock.ExpectCommit()
				tx.Commit()

			case 3:
				t.Log("tx: query-return clause")

				mock.ExpectBegin()
				tx, err1 := resolver.BeginTx(context.TODO(), &sql.TxOptions{
					Isolation: sql.LevelDefault,
					ReadOnly:  false,
				})
				handleDBError(t, err1)

				query := "INSERT INTO users(id,name) VALUES ($1,$2) RETURNING id"
				mock.ExpectQuery(query).
					WithArgs(1, "Hiro").
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

				_, err = tx.Query(query, 1, "Hiro")

				mock.ExpectCommit()
				tx.Commit()
			case 4:
				query := `UPDATE users SET name='Hiro' where id=1 RETURNING id,name`
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
				_, err = resolver.Query(query)

			case 5:
				query := `delete from users where id=1 returning id,name`
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
				resolver.QueryRow(query)
			default:
				t.Fatal("developer needs to work on the tests")
			}

			handleDBError(t, err)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Skipf("sqlmock:unmet expectations: %s", err)
			}
		}
	})

	t.Run("replica dbs", func(t *testing.T) {

		var query string

		for i := 0; i < noOfReplicas*5; i++ {
			robin := resolver.loadBalancer.predict(noOfReplicas)
			mock := mockReplicas[robin]

			switch i % 4 {
			case 0:
				query = "select 1'"
				mock.ExpectQuery(query)
				resolver.Query(query)
			case 1:
				query := "select 'row'"
				mock.ExpectQuery(query)
				resolver.QueryRow(query)
			case 2:
				query = "select 'query-ctx' "
				mock.ExpectQuery(query)
				resolver.QueryContext(context.TODO(), query)
			case 3:
				query = "select 'row'"
				mock.ExpectQuery(query)
				resolver.QueryRowContext(context.TODO(), query)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Logf("failed query-%s", query)
				t.Skipf("sqlmock:unmet expectations: %s", err)
			}
		}
	})

	t.Run("prepare", func(t *testing.T) {
		query := "select 1"

		for _, mock := range mockPrimaries {
			mock.ExpectPrepare(query)
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("sqlmock:unmet expectations: %s", err)
				}
			}(mock)
		}
		for _, mock := range mockReplicas {
			mock.ExpectPrepare(query)
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("sqlmock:unmet expectations: %s", err)
				}
			}(mock)
		}

		stmt, err := resolver.Prepare(query)
		if err != nil {
			t.Error("prepare failed")
			return
		}

		robin := resolver.stmtLoadBalancer.predict(noOfPrimaries)
		mock := mockPrimaries[robin]

		mock.ExpectExec(query)

		stmt.Exec()
	})

	t.Run("prepare tx", func(t *testing.T) {
		query := "select 1"

		for _, mock := range mockPrimaries {
			mock.ExpectPrepare(query)
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("sqlmock:unmet expectations: %s", err)
				}
			}(mock)
		}
		for _, mock := range mockReplicas {
			mock.ExpectPrepare(query)
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("sqlmock:unmet expectations: %s", err)
				}
			}(mock)
		}

		stmt, err := resolver.Prepare(query)
		if err != nil {
			t.Error("prepare failed")
			return
		}

		robin := resolver.loadBalancer.predict(noOfPrimaries)
		mock := mockPrimaries[robin]

		mock.ExpectBegin()

		tx, err := resolver.Begin()
		if err != nil {
			t.Error("begin failed", err)
			return
		}

		txstmt := tx.Stmt(stmt)

		mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))
		_, err = txstmt.Exec()
		if err != nil {
			t.Error("stmt exec failed", err)
			return
		}

		mock.ExpectCommit()
		tx.Commit()
	})

	t.Run("ping", func(t *testing.T) {
		for _, mock := range mockPrimaries {
			mock.ExpectPing()
			mock.ExpectPing()
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("sqlmock:unmet expectations: %s", err)
				}
			}(mock)
		}
		for _, mock := range mockReplicas {
			mock.ExpectPing()
			mock.ExpectPing()
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("sqlmock:unmet expectations: %s", err)
				}
			}(mock)
		}

		err := resolver.Ping()
		if err != nil {
			t.Errorf("ping failed %s", err)
		}
		err = resolver.PingContext(context.TODO())
		if err != nil {
			t.Errorf("ping failed %s", err)
		}
	})

	t.Run("close", func(t *testing.T) {
		for _, mock := range mockPrimaries {
			mock.ExpectClose()
		}
		for _, mock := range mockReplicas {
			mock.ExpectClose()
		}
		err := resolver.Close()
		handleDBError(t, err)

		t.Logf("closed:DB-CLUSTER-%dP%dR", noOfPrimaries, noOfReplicas)
	})

}

func TestMultiWrite(t *testing.T) {
	defer goleak.VerifyNone(t)

	loadBalancerPolices := []LoadBalancerPolicy{
		RoundRobinLB,
		RandomLB,
	}

	retrieveLoadBalancer := func() (loadBalancerPolicy LoadBalancerPolicy) {
		loadBalancerPolicy = loadBalancerPolices[0]
		loadBalancerPolices = loadBalancerPolices[1:]
		return
	}

BEGIN_TEST:
	loadBalancerPolicy := retrieveLoadBalancer()

	t.Logf("LoadBalancer-%s", loadBalancerPolicy)

	testCases := []DBConfig{
		{1, 0, ""},
		{1, 1, ""},
		{1, 2, ""},
		{1, 10, ""},
		{2, 0, ""},
		{2, 1, ""},
		{3, 0, ""},
		{3, 1, ""},
		{3, 2, ""},
		{3, 3, ""},
		{3, 6, ""},
		{5, 6, ""},
		{7, 20, ""},
		{10, 10, ""},
		{10, 20, ""},
	}

	retrieveTestCase := func() DBConfig {
		testCase := testCases[0]
		testCases = testCases[1:]
		return testCase
	}

BEGIN_TEST_CASE:
	if len(testCases) == 0 {
		if len(loadBalancerPolices) == 0 {
			return
		}
		goto BEGIN_TEST
	}

	dbConfig := retrieveTestCase()

	dbConfig.lbPolicy = loadBalancerPolicy

	t.Run(fmt.Sprintf("DBCluster P%dR%d", dbConfig.primaryDBCount, dbConfig.replicaDBCount), func(t *testing.T) {
		testMW(t, dbConfig)
	})

	if testing.Short() {
		return
	}

	goto BEGIN_TEST_CASE
}

func createMock() (db *sql.DB, mock sqlmock.Sqlmock, err error) {
	db, mock, err = sqlmock.New(sqlmock.MonitorPingsOption(true), sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	return
}

type QueryMatcher struct {
}

func (*QueryMatcher) Match(expectedSQL, actualSQL string) error {
	return nil
}
