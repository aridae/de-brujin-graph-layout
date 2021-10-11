package chunkreader

import (
	"bufio"
	"fmt"
	"io"
)

// буферизованный ввод - читаем из файла по чанкам байт,
// чанки могут перекрываться
// ридер должен быть один, тк доступ к файлу неконкуррентобезопасный
// поэтому синглтон
var (
	reader *ChunkReader
)

type ChunkReader struct {
	startingPoint int
	reader        *bufio.Reader
	chunks        *ChunksPool
	overlapSize   int
	overlap       []byte // кусок последнего вычитанного чанка - нам нужен оверлэп для кмер
}

func GetChunkReader(chunkSize int, overlapSize int, file io.Reader) *ChunkReader {
	if reader == nil {
		reader = &ChunkReader{
			startingPoint: 0,
			overlapSize:   overlapSize,
			reader:        bufio.NewReader(file),
			chunks:        GetChunksPool(chunkSize + overlapSize),
			// нужно дополнительное место, чтобы добавлять перекрывающиеся байты
			overlap: make([]byte, overlapSize), // тк мы не можем возвращать байты в поток
			// просто будем хранить маленький кусочек прошлого чонка
		}
	}
	return reader
}

// в го все возвращается по значению, слайсы тоже,
// но слайс по значению == хедер, в котором указатель на массив данных
// так что сами данные не копируются, все ок по производительности
// upd: возвращаю по указателю посмотрим
func (r *ChunkReader) ReadChunk() (*[]byte, error) {
	buf := chunksPool.Get()
	n, err := r.reader.Read(buf[r.startingPoint*r.overlapSize:]) // читаем один чонк данных, оставляя место для оверлэпа
	if err != nil {
		if err != io.EOF {
			fmt.Println(err)
		}
		return nil, err
	}
	buf = buf[:r.startingPoint*r.overlapSize+n] // изменяет только лен, не копирует сами данные в другую область

	if r.startingPoint == 0 {
		copy(r.overlap, buf[len(buf)-r.overlapSize:])
		r.startingPoint = 1
		return &buf, nil
	}

	copy(buf, r.overlap)
	copy(r.overlap, buf[len(buf)-r.overlapSize:])
	return &buf, nil
}

func (r *ChunkReader) FreeChunk(chunk *[]byte) {
	r.chunks.pool.Put(chunk)
}
