package chunkreader

import (
	"sync"
)

type ChunksPool struct {
	pool      *sync.Pool
	chunkSize int
}

// синглтон
var (
	chunksPool *ChunksPool
)

// создать/вернуть пул кусков памяти
func GetChunksPool(chunkSize int) *ChunksPool {
	if chunksPool == nil {
		chunksPool = &ChunksPool{
			chunkSize: chunkSize,
			pool: &sync.Pool{
				New: func() interface{} {
					chunks := make([]byte, chunkSize)
					return &chunks
				},
			},
		}
	}
	return chunksPool
}

// взять кусочек памяти пожалуйста спасибо хорошего дня
func (p *ChunksPool) Get() []byte {
	return *p.pool.Get().(*[]byte)
}

// вернуть кусочек памяти
func (p *ChunksPool) Put(chunk *[]byte) {
	p.pool.Put(chunk)
}
