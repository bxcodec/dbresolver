package dbresolver

import (
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"testing"
)

func TestIssue44(t *testing.T) {
	noOfQueries := 19990

	config := DBConfig{
		10,
		10,
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
	}

	resolver := New(WithPrimaryDBs(primaries...), WithReplicaDBs(replicas...), WithLoadBalancer(lbPolicy)).(*sqlDB)

	var err error
	for i := 0; i < noOfQueries; i++ {
		query := `UPDATE users SET name='Hiro' where id=1 RETURNING id,name`

		for _, mock := range append(mockPrimaries, mockReplicas...) {
			mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
		}

		_, err = resolver.Query(query)
		if err != nil {
			t.Errorf("db error: %s", err)
		}

	}

}
