package main

/*
Нужен пул воркеров, ввод через буфио, пул коннекшнов к бд(?)
*/

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"

	"github.com/aridae/de-brujin-search-layout/backend/internal/DBGService"
	"github.com/aridae/de-brujin-search-layout/backend/internal/chunkreader"
	"github.com/aridae/de-brujin-search-layout/backend/internal/db"
	"github.com/aridae/de-brujin-search-layout/backend/internal/workerspool"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("no .env file found")
	}
}

// // // read - два соседних к-мера, так что подстроки находятся в отношении префикс-суффикс
// // // добавить рид == добавить два к-мера и связь между ними
// func AddRead(dbm *Neo4jManager, r *Read, genome int) error {
// 	session := dbm.driver.NewSession(neo4j.SessionConfig{
// 		AccessMode:   neo4j.AccessModeWrite,
// 		DatabaseName: dbm.database,
// 	})
// 	defer session.Close()
// 	_, err := session.WriteTransaction(
// 		func(tx neo4j.Transaction) (interface{}, error) {
// 			// merge - создать все что нужно создать если не было уже создано
// 			_, err := tx.Run(
// 				"MERGE (g:Genome { id: $gId}) MERGE (pref:KMer { value: $prefValue, real: 1 }) MERGE (suff:KMer { value: $suffValue, real: 1 }) MERGE (pref)-[rBP:Belongs]->(g) MERGE (suff)-[rBS:Belongs]->(g) MERGE (pref)-[r:Precedes { real: 1 }]->(suff)",
// 				map[string]interface{}{
// 					"gId":       genome,
// 					"prefValue": r.prefix,
// 					"suffValue": r.suffix,
// 				},
// 			)
// 			if err != nil {
// 				return nil, err
// 			}
// 			return nil, nil
// 		},
// 	)
// 	return err
// }
// func BuildGraphWithNeo4j(dbm *Neo4jManager, k int, sequences ...string) {
// 	for genome, sequence := range sequences {
// 		for i := 0; i < len(sequence)-k; i++ {
// 			r := NewRead(sequence[i : i+k+1])
// 			err := AddRead(dbm, r, genome)
// 			if err != nil {
// 				panic(err)
// 			}
// 		}
// 	}
// }

// func main() {

// 	dbUri, exists := os.LookupEnv("NEO4J_URI")
// 	if !exists {
// 		log.Fatal("database uri not found in .env")
// 	}

// 	dbUsr, exists := os.LookupEnv("NEO4J_USR")
// 	if !exists {
// 		log.Fatal("database username not found in .env")
// 	}

// 	dbPass, exists := os.LookupEnv("NEO4J_PASS")
// 	if !exists {
// 		log.Fatal("database password not found in .env")
// 	}

// 	dbName, exists := os.LookupEnv("NEO4J_DB")
// 	if !exists {
// 		log.Fatal("database password not found in .env")
// 	}

// 	var neo4jManager Neo4jManager
// 	neo4jManager.OpenConnection(dbUri, dbUsr, dbPass, dbName)
// 	defer neo4jManager.CloseConnection()

// 	// так у нас есть пул воркеров, будет пул чанков по 4кб - 4 * 1024

// 	// create worker pool, create chunk pool

// 	// открыть фас файл, прочитать строку, построить граф
// 	data, err := ioutil.ReadFile("./cmd/NC_030850.1.fasta") // тут одна хромосома
// 	if err != nil {
// 		panic(err)
// 	}
// 	BuildGraphWithNeo4j(&neo4jManager, 3, string(data)) //, "ACCACCACCTT") //, "AACACAACCTG")
// }

// // решено пожертвовать слоем доступа к данным, чтобы
// // уменьшить накладные расходы на передачу указателей на подстроки туда обратно

// // у нас есть пулы ресурсов: чанков и коннекшнов
// // есть пулы воркеров, которые берут чанки и закидывают их в бд
// // есть ридер, который вычитывает чанки

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("необходимо указать путь к фасте")
		return
	}

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

	fasta, err := os.Open(args[0])
	if err != nil {
		fmt.Println(err)
		return
	}
	defer fasta.Close()

	// создаем ридера, пул воркеров и пул конекшнов
	reader := chunkreader.GetChunkReader(4*1024, 3, fasta)
	pool := workerspool.GetWorkersPool(8, 10)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.RunBackground()
	}()

	neo4jClient, err := db.GetNeo4jClient(&db.Options{
		URI:      dbUri,
		User:     dbUsr,
		Password: dbPass,
		DB:       dbName,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer neo4jClient.CloseNeo4jClient()

	data, err := reader.ReadChunk()
	for err == nil {
		//fmt.Println("read:", data)
		pool.AddTask(
			DBGService.NewTask(
				data,
				neo4jClient,
				reader,
			),
		)
		data, err = reader.ReadChunk()
	}
	if err != io.EOF {
		fmt.Println(err)
		return
	}

	pool.Finish()
	wg.Wait()
	fmt.Println("That's all, folks!")
}
