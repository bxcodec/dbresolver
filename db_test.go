package dbresolver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	fuzz "github.com/google/gofuzz"
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

	mockPimaries := make([]sqlmock.Sqlmock, noOfPrimaries)
	mockReplicas := make([]sqlmock.Sqlmock, noOfReplicas)

	for i := 0; i < noOfPrimaries; i++ {
		db, mock, err := createMock()

		if err != nil {
			t.Fatal("creating of mock failed")
		}

		primaries[i] = db
		mockPimaries[i] = mock
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
			mock := mockPimaries[robin]

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
				tx, err := resolver.BeginTx(context.TODO(), &sql.TxOptions{
					Isolation: sql.LevelDefault,
					ReadOnly:  false,
				})
				handleDBError(t, err)

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
				t.Errorf("sqlmock:unmet expectations: %s", err)
				t.SkipNow() //FIXME: remove
			}
		}
	})

	t.Run("replica dbs", func(t *testing.T) {
		for i := 0; i < noOfReplicas*5; i++ {
			robin := resolver.loadBalancer.predict(noOfReplicas)
			mock := mockReplicas[robin]

			t.Log("case -", i%4)

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

		for _, mock := range mockPimaries {
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
		mock := mockPimaries[robin]

		mock.ExpectExec(query)

		stmt.Exec()
	})

	t.Run("ping", func(t *testing.T) {
		for _, mock := range mockPimaries {
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
		for _, mock := range mockPimaries {
			mock.ExpectClose()
		}
		for _, mock := range mockReplicas {
			mock.ExpectClose()
		}
		err := resolver.Close()
		handleDBError(t, err)

		t.Logf("DB-CLUSTER-%dP%dR", noOfPrimaries, noOfReplicas)
	})

}

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

	testMW(t, dbConfig)

	if testing.Short() {
		return
	}

	goto BEGIN_TEST_CASE
}

func FuzzMultiWrite(f *testing.F) {

	func() { // generate corpus

		seed := time.Now().UnixNano()
		rand.Seed(seed)

		f.Logf("[seed] %v", seed) // recreate the testcase using this seed

		for i := 0; i < 10; i++ { // Corpus of <i>
			fuzzer := fuzz.New()
			var rdbCount, wdbCount uint8
			fuzzer.Fuzz(&rdbCount)
			fuzzer.Fuzz(&wdbCount)

			lbPolicyID := rand.Uint32()

			// f.Add(uint(1), uint(2), uint8(lbPolicyID))
			f.Add(wdbCount, rdbCount, uint8(lbPolicyID))

			if testing.Short() {
				break // short circuiting with 1 testcase
			}
		}
	}()

	f.Fuzz(func(t *testing.T, wdbCount, rdbCount, lbPolicyID uint8) {

		policyID := lbPolicyID % uint8(len(LoadBalancerPolicies))

		t.Log("Policy", LoadBalancerPolicies[policyID])

		config := DBConfig{
			wdbCount, rdbCount, LoadBalancerPolicies[policyID],
		}

		if config.primaryDBCount == 0 {
			t.Skipf("skipping due to mising primary db")
		}

		t.Log("dbConf", config)

		t.Run(fmt.Sprintf("%v", config), func(t *testing.T) {

			dbConf := config

			t.Parallel()

			testMW(t, dbConf)
		})

	})
}

func createMock() (db *sql.DB, mock sqlmock.Sqlmock, err error) {
	db, mock, err = sqlmock.New(sqlmock.MonitorPingsOption(true), sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	return
}

type QueryMatcher struct {
}

func (*QueryMatcher) Match(expectedSQL string, actualSQL string) error {
	return nil
}
