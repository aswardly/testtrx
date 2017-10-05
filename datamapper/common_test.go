package datamapper_test

import (
	"fmt"
	"strings"
	"sync"

	"github.com/gocql/gocql"
)

//Adjust cluster info and keyspace name prior to running test
var clusterPool = []string{"127.0.0.1"}
var keyspaceName string = "user_test"
var databaseSession *gocql.Session
var initOnce sync.Once

func initTest() *gocql.Session {
	initOnce.Do(func() {
		initSession := createInitSession()

		err := initSession.Query(`DROP KEYSPACE IF EXISTS ` + keyspaceName).Exec()
		if err != nil {
			panic(fmt.Errorf("Unable to drop keyspace '%v' during init: %v", keyspaceName, err))
		}

		err = initSession.Query(`CREATE KEYSPACE IF NOT EXISTS ` + keyspaceName +
			` WITH replication = {
			'class' : 'SimpleStrategy',
			'replication_factor' : 1
		}`).Exec()

		if err != nil {
			panic(fmt.Errorf("Unable to create keyspace '%v' during init: %v", keyspaceName, err))
		}
		initSession = nil
	})
	databaseSession = createSession(keyspaceName)
	return databaseSession
}

func createSession(keyspaceName string) *gocql.Session {
	// connect to the cluster
	cluster := gocql.NewCluster(strings.Join(clusterPool, ","))
	cluster.Consistency = gocql.One
	cluster.Keyspace = keyspaceName
	session, err := cluster.CreateSession()
	if err != nil {
		panic(fmt.Errorf("Could not connect to cluster: %v", err))
	}
	return session
}

func createInitSession() *gocql.Session {
	// connect to the cluster
	cluster := gocql.NewCluster(strings.Join(clusterPool, ","))
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		panic(fmt.Errorf("Could not connect to cluster: %v", err))
	}
	return session
}

func teardownTest() {
	initSession := createInitSession()

	err := initSession.Query(`DROP KEYSPACE IF EXISTS ` + keyspaceName).Exec()
	if err != nil {
		panic(fmt.Errorf("Unable to drop keyspace '%v' during teardown: %v", keyspaceName, err))
	}
}
