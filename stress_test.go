package dbresolver

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestIssue44(t *testing.T) {
	noOfQueries := 19990

	config := DBConfig{
		1,
		0,
		RandomLB,
	}

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
		mock.MatchExpectationsInOrder(false)
	}

	resolver := New(WithPrimaryDBs(primaries...), WithReplicaDBs(replicas...), WithLoadBalancer(lbPolicy)).(*sqlDB)

	allMocks := append(mockPrimaries, mockReplicas...)
	allDBs := append(primaries, replicas...)

	query := `select id,name from users where id=1`
	var err error
	for i := 0; i < noOfQueries; i++ {
		t.Run(fmt.Sprintf("q%d", i), func(t *testing.T) {
			//t.Parallel() //TODO: not concurrent safe because of shared mocks

			for _, mock := range allMocks {
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
				mock.MatchExpectationsInOrder(false)
			}

			_, err = resolver.Query(query)
			if err != nil {
				t.Errorf("resolver error: %s", err)
			}

			queriedMock := -1
			failedMocks := 0
			for iM, mock := range allMocks {
				if err := mock.ExpectationsWereMet(); err == nil {
					queriedMock = iM
					t.Logf("found mock:%d for query:%d", iM, i)
				} else {
					//t.Errorf("expect mock:%d error: %s", iM, err)
					failedMocks++
					_, err = allDBs[iM].Query(query)
					if err != nil {
						t.Errorf("db error: %s", err)
					}
				}
			}
			if queriedMock == -1 {
				t.Errorf("failedMocks:%d", failedMocks)
				t.Errorf("no mock queried for query:%d", i)
			}
		})
	}
}

func TestConcurrencyRandomLBIssue44(t *testing.T) {
	noOfQueries := 19990

	config := DBConfig{
		1,
		2,
		RandomLB,
	}

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
		mock.MatchExpectationsInOrder(false)
	}

	resolver := New(WithPrimaryDBs(primaries...), WithReplicaDBs(replicas...), WithLoadBalancer(lbPolicy)).(*sqlDB)

	mocks := append(mockPrimaries, mockReplicas...)
	allDBs := append(primaries, replicas...)

	query := `select id,name from users where id=1`
	var err error

	mockLocks := make(map[int]*sync.Mutex, len(mocks))

	for i := 0; i < len(mocks); i++ {
		mockLocks[i] = &sync.Mutex{}
	}

	dbLocks := make(map[int]*sync.Mutex, len(allDBs))

	for i := 0; i < len(allDBs); i++ {
		dbLocks[i] = &sync.Mutex{}
	}

	for _, mock := range mocks {
		mock.MatchExpectationsInOrder(false)
	}

	lb := resolver.loadBalancer

	for i := 0; i < noOfQueries; i++ {
		t.Run(fmt.Sprintf("q%d", i), func(t *testing.T) {
			//t.Parallel()

			rnDB := lb.predict(len(allDBs))

			curMock := mocks[rnDB]

			curMock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

			_, err = resolver.Query(query)
			if err != nil {
				t.Logf("resolver error: %s", err)
			}

			if err := curMock.ExpectationsWereMet(); err != nil {
				t.Errorf("expect mock:%d error: %s", rnDB, err)
			}
		})
	}
}
