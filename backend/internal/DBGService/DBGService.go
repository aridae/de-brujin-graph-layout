package DBGService

import (
	"sync"

	"github.com/aridae/de-brujin-search-layout/backend/internal/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var (
	mtx sync.Mutex = sync.Mutex{}
)

func merge(session neo4j.Session, sequence []byte, genomeID int) error {
	//fmt.Println("Pushing...", string(sequence[:len(sequence)-1]), string(sequence[1:]))

	mtx.Lock()
	defer mtx.Unlock()

	_, err := session.WriteTransaction(
		func(tx neo4j.Transaction) (interface{}, error) {
			_, err := tx.Run(
				"MERGE (g:Genome { id: $gId}) MERGE (pref:KMer { value: $prefValue, real: 1 }) MERGE (suff:KMer { value: $suffValue, real: 1 }) MERGE (pref)-[rBP:Belongs]->(g) MERGE (suff)-[rBS:Belongs]->(g) MERGE (pref)-[r:Precedes { real: 1 }]->(suff)",
				map[string]interface{}{
					"gId":       genomeID,
					"prefValue": string(sequence[:len(sequence)-1]),
					"suffValue": string(sequence[1:]),
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

func MergeSequence(neo4jClient *db.Neo4jClient, sequence []byte, genomeID int, k int) {
	session := neo4jClient.CreateSession()
	defer session.Close()

	for i := 0; i < len(sequence)-k; i++ {
		err := merge(session, sequence[i:i+k+1], genomeID)
		if err != nil {
			panic(err)
		}
	}
}
