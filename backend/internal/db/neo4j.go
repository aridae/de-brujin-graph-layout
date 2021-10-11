package db

import (
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// Из доков:
// Each Driver instance maintains a pool of connections inside,
// as a result, it is recommended to only use one driver per application.

// It is considerably cheap to create new sessions and transactions,
// as sessions and transactions do not create new connections as long as
// there are free connections available in the connection pool.

// The driver is thread-safe, while the session or the transaction is not thread-safe.

// в итоге - синглтон, коннекшны открытые в пуле уже
type Neo4jClient struct {
	driver *neo4j.Driver
	// Driver represents a pool(s) of connections to a neo4j server or cluster. It's
	// safe for concurrent use
	DB string
}

type Options struct {
	URI      string
	User     string
	Password string
	DB       string
}

var (
	neo4jClient *Neo4jClient
)

func GetNeo4jClient(options *Options) (*Neo4jClient, error) {
	if neo4jClient == nil {
		driver, err := initNeo4jClient(options)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize neo4j client with error: %s", err)
		}
		neo4jClient = &Neo4jClient{
			driver: driver,
			DB:     options.DB,
		}
		return neo4jClient, nil
	}

	return neo4jClient, nil
}

func (client *Neo4jClient) CloseNeo4jClient() {
	(*client.driver).Close()
}

func (client Neo4jClient) CreateSession() neo4j.Session {
	return (*client.driver).NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: client.DB,
	})
}

func initNeo4jClient(options *Options) (*neo4j.Driver, error) {
	driver, err := neo4j.NewDriver(
		options.URI,
		neo4j.BasicAuth(
			options.User,
			options.Password,
			"",
		),
	)
	return &driver, err
}
