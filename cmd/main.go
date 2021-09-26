package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// init is invoked before main()
func init() {
	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		log.Print("no .env file found")
	}
}

type Neo4jManager struct {
	driver   neo4j.Driver
	database string
}

func (dbm *Neo4jManager) OpenConnection(dbUri, dbUsr, dbPass, dbName string) error {

	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth(dbUsr, dbPass, ""))
	if err != nil {
		panic(err)
	}

	dbm.driver = driver
	dbm.database = dbName
	return nil
}

func (dbm *Neo4jManager) CloseConnection() {
	dbm.driver.Close()
}

type Genome struct {
	meta     string
	sequence string
}

type KMer string
type Read struct {
	prefix KMer
	suffix KMer
}

func NewRead(read string) *Read {
	return &Read{
		prefix: KMer(read[:len(read)-1]),
		suffix: KMer(read[1:]),
	}
}

// func HashSum(s string) uint32 {
// 	h := fnv.New32a()
// 	h.Write([]byte(s))
// 	return h.Sum32()
// }

// read - два соседних к-мера, так что подстроки находятся в отношении префикс-суффикс
// добавить рид == добавить два к-мера и связь между ними
func AddRead(dbm *Neo4jManager, r *Read, genome int) error {
	session := dbm.driver.NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: dbm.database,
	})
	defer session.Close()

	_, err := session.WriteTransaction(
		func(tx neo4j.Transaction) (interface{}, error) {

			// // calculate ids - hashsum of strings
			// prefId := HashSum(string(r.prefix))
			// suffId := HashSum(string(r.suffix))

			// merge - создать все что нужно создать если не было уже создано
			_, err := tx.Run(
				"MERGE (g:Genome { id: $gId}) MERGE (pref:KMer { value: $prefValue, real: 1 }) MERGE (suff:KMer { value: $suffValue, real: 1 }) MERGE (pref)-[rBP:Belongs]->(g) MERGE (suff)-[rBS:Belongs]->(g) MERGE (pref)-[r:Precedes { real: 1 }]->(suff)",
				map[string]interface{}{
					"gId":       genome,
					"prefValue": r.prefix,
					"suffValue": r.suffix,
				},
			)
			if err != nil {
				return nil, err
			}
			return nil, nil
		},
	)
	return err
}

// // read - два соседних к-мера, так что подстроки находятся в отношении префикс-суффикс
// // удалить рид == удалить связь и, если к-меры остались без связи, удалить к-меры
// func DeleteRead(dbm *Neo4jManager) {
// 	session := dbm.driver.NewSession(neo4j.SessionConfig{
// 		AccessMode:   neo4j.AccessModeWrite,
// 		DatabaseName: dbm.database,
// 	})
// 	defer session.Close()
// }

func BuilDGraphWithNeo4j(dbm *Neo4jManager, k int, sequences ...string) {
	for genome, sequence := range sequences {
		for i := 0; i < len(sequence)-k; i++ {
			r := NewRead(sequence[i : i+k+1])
			err := AddRead(dbm, r, genome)
			if err != nil {
				panic(err)
			}
		}
	}
}

func main() {

	dbUri, exists := os.LookupEnv("NEO4J_URI")
	if !exists {
		log.Fatal("database uri not found in .env")
	}

	dbUsr, exists := os.LookupEnv("NEO4J_USR")
	if !exists {
		log.Fatal("database username not found in .env")
	}

	dbPass, exists := os.LookupEnv("NEO4J_PASS")
	if !exists {
		log.Fatal("database password not found in .env")
	}

	dbName, exists := os.LookupEnv("NEO4J_DB")
	if !exists {
		log.Fatal("database password not found in .env")
	}

	var neo4jManager Neo4jManager
	neo4jManager.OpenConnection(dbUri, dbUsr, dbPass, dbName)
	defer neo4jManager.CloseConnection()

	BuilDGraphWithNeo4j(&neo4jManager, 3, "ACCACCACCTG", "ACCACCACCTT") //, "AACACAACCTG")
}
