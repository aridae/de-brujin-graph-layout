package DBGService

import (
	"github.com/aridae/de-brujin-search-layout/backend/internal/chunkreader"
	"github.com/aridae/de-brujin-search-layout/backend/internal/db"
	"github.com/aridae/de-brujin-search-layout/backend/internal/workerspool"
)

// одна задача
type DBGTask struct {
	ID     int
	data   *[]byte                  // чанк записать
	client *db.Neo4jClient          // куда записать
	reader *chunkreader.ChunkReader // потом куда вернуть чанк
}

var (
	currentID = 0
)

func NewTask(data *[]byte, client *db.Neo4jClient, reader *chunkreader.ChunkReader) workerspool.Task {
	currentID++
	return &DBGTask{
		ID:     currentID,
		data:   data,
		client: client,
		reader: reader,
	}
}

func (task *DBGTask) GetID() int {
	return task.ID
}

func (task *DBGTask) Process() error {
	MergeSequence(task.client, *task.data, 0, 3)
	return nil
}

func (task *DBGTask) Cleanup() error {
	task.reader.FreeChunk(task.data)
	return nil
}
