package dbresolver

import (
	"database/sql"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"testing"
)

func TestIssue44(t *testing.T) {
	t.Deadline()

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

	query := `UPDATE users SET name='Hiro' where id=1 RETURNING id,name`
	var err error
	for i := 0; i < noOfQueries; i++ {

		t.Run(fmt.Sprintf("q%d", i), func(t *testing.T) {
			//t.Parallel()

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
					failedMocks += 1
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
