package dbresolver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMultiWrite(t *testing.T) {
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

	testCases := [][2]uint{
		{1, 0},
		{1, 1},
		{1, 2},
		{1, 10},
		{2, 0},
		{2, 1},
		{2, 2},
		{3, 0},
		{3, 1},
		{3, 2},
		{3, 3},
		{3, 6},
		{5, 6},
		{7, 20},
		{10, 10},
		{10, 20},
	}

	retrieveTestCase := func() (int, int) {
		testCase := testCases[0]
		testCases = testCases[1:]
		return int(testCase[0]), int(testCase[1])
	}

BEGIN_TEST_CASE:
	if len(testCases) == 0 {
		if len(loadBalancerPolices) == 0 {
			return
		}
		goto BEGIN_TEST
	}

	noOfPrimaries, noOfReplicas := retrieveTestCase()

	primaries := make([]*sql.DB, noOfPrimaries)
	replicas := make([]*sql.DB, noOfReplicas)

	mockPrimaries := make([]sqlmock.Sqlmock, noOfPrimaries)
	mockReplicas := make([]sqlmock.Sqlmock, noOfReplicas)

	for i := 0; i < noOfPrimaries; i++ {
		db, mock, err := createMock()

		if err != nil {
			t.Fatal("creating of mock failed")
		}

		defer mock.ExpectClose()
		defer db.Close()

		primaries[i] = db
		mockPrimaries[i] = mock
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

	resolver := New(WithPrimaryDBs(primaries...), WithReplicaDBs(replicas...), WithLoadBalancer(loadBalancerPolicy)).(*sqlDB)

	t.Run("primary dbs", func(t *testing.T) {
		for i := 0; i < noOfPrimaries*5; i++ {
			robin := resolver.loadBalancer.predict(noOfPrimaries)
			mock := mockPrimaries[robin]

			// t.Log("case - ", i%4)

			switch i % 4 {
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
			case 3:
				mock.ExpectBegin()
				resolver.BeginTx(context.TODO(), &sql.TxOptions{
					Isolation: sql.LevelDefault,
					ReadOnly:  false,
				})
				t.Log("begin transaction")
			default:
				t.Fatal("developer needs to work on the tests")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		}
	})

	t.Run("replica dbs", func(t *testing.T) {
		for i := 0; i < noOfReplicas*5; i++ {
			robin := resolver.loadBalancer.predict(noOfReplicas)
			mock := mockReplicas[robin]

			// t.Log("case -", i%4)

			switch i % 4 {
			case 0:
				query := "select 1'"
				mock.ExpectQuery(query)
				resolver.Query(query)
				t.Log("query")
			case 1:
				query := "select 'row'"
				mock.ExpectQuery(query)
				resolver.QueryRow(query)
				t.Log("query row")
			case 2:
				query := "select 'query-ctx' "
				mock.ExpectQuery(query)
				resolver.QueryContext(context.TODO(), query)
				t.Log("query context")
			case 3:
				query := "select 'row'"
				mock.ExpectQuery(query)
				resolver.QueryRowContext(context.TODO(), query)
				t.Log("query row context")
			default:
				t.Fatal("developer needs to work on the tests")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("expect failed %s", err)
			}
		}
	})

	t.Run("prepare", func(t *testing.T) {
		query := "select 1"

		for i, mock := range mockPrimaries {
			mock.ExpectPrepare(query)
			defer func(i int, mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("P%d: %s", i, err)
				}
			}(i, mock)
		}
		for i, mock := range mockReplicas {
			mock.ExpectPrepare(query)
			defer func(i int, mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("R%d: %s", i, err)
				}
			}(i, mock)
		}

		stmt, err := resolver.Prepare(query)
		if err != nil {
			t.Fatalf("[prepare] failed %s", err)
		}

		var getMock = func(mockDBs []sqlmock.Sqlmock) (mock sqlmock.Sqlmock) {
			robin := resolver.stmtLoadBalancer.predict(len(mockDBs))
			mock = mockDBs[robin]
			return
		}

		mock := getMock(mockPrimaries)
		mock.ExpectExec(query)
		_, err = stmt.Exec()

		if err != nil {
			t.Logf("[prepare] %s", err)
		}

		t.Run("primary stmts", func(t *testing.T) {
			for i := 0; i < noOfPrimaries*5; i++ {
				mock := getMock(mockPrimaries)

				// t.Log("case - ", i%3)

				switch i % 4 {
				case 0:
					mock.ExpectExec(query)
					stmt.Exec()
					t.Log("exec")
				case 1:
					mock.ExpectExec(query)
					stmt.ExecContext(ctx)
					t.Log("exec context")
				case 2:
					if noOfReplicas == 0 {
						mock.ExpectQuery(query)
						stmt.Query()
						t.Log("query")
					}
				case 3:
					if noOfReplicas == 0 {
						mock.ExpectQuery(query)
						stmt.QueryRow()
						t.Log("query row")
					}
				default:
					t.Fatal("developer needs to work on the tests")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}
		})

		t.Run("replica stmts", func(t *testing.T) {
			for i := 0; i < noOfReplicas*5; i++ {
				mock := getMock(mockReplicas)

				// t.Log("case -", i%4)

				switch i % 4 {
				case 0:
					mock.ExpectQuery(query)
					stmt.Query()
					t.Log("query")
				case 1:
					mock.ExpectQuery(query)
					stmt.QueryContext(ctx)
					t.Log("query ctx")
				case 2:
					mock.ExpectQuery(query)
					stmt.QueryRow()
					t.Log("row")
				case 3:
					mock.ExpectQuery(query)
					stmt.QueryRowContext(ctx)
					t.Log("row context")
				default:
					t.Fatal("developer needs to work on the tests")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("expect failed %s", err)
				}
			}
		})

		if err := stmt.Close(); err != nil {
			t.Errorf("[close] %s", err)
		}
	})

	t.Run("ping", func(t *testing.T) {
		for _, mock := range mockPrimaries {
			mock.ExpectPing()
			mock.ExpectPing()
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}(mock)
		}
		for _, mock := range mockReplicas {
			mock.ExpectPing()
			mock.ExpectPing()
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}(mock)
		}

		if err := resolver.Ping(); err != nil {
			t.Errorf("ping failed %s for %dP%dR", err, noOfPrimaries, noOfReplicas)
		}
		if err := resolver.PingContext(context.TODO()); err != nil {
			t.Errorf("ping failed %s", err)
		}
	})

	t.Run("close", func(t *testing.T) {
		for _, mock := range mockPrimaries {
			mock.ExpectClose()
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}(mock)
		}
		for _, mock := range mockReplicas {
			mock.ExpectClose()
			defer func(mock sqlmock.Sqlmock) {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}(mock)
		}
		resolver.Close()

		t.Logf("%dP%dR", noOfPrimaries, noOfReplicas)
	})

	goto BEGIN_TEST_CASE
}

func createMock() (db *sql.DB, mock sqlmock.Sqlmock, err error) {
	db, mock, err = sqlmock.New(sqlmock.MonitorPingsOption(true))
	return
}
