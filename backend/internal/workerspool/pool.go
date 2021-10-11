package workerspool

import (
	"fmt"
	"sync"
)

/*
В чем мем?

какой-то сторонний источник, накидывет нам таски в очередь тасок
мы не знаем, сколько их будет, когда они начнут поступать и когда закончат

в какой-то момент сторонний источник дает сигнал о том,
что новых тасков больше не будет - нужно закончить все,
что сейчас есть в очереди и распустить команду работяг

производство-потреблнение, только у нас один производитель
и надо обязательно употребить все, что он записал в очередь
*/

type WorkersPool struct {
	workersNumber int
	tasksQueue    chan Task
}

var (
	pool *WorkersPool
)

func GetWorkersPool(workersNumber, taskQueueSize int) *WorkersPool {
	if pool == nil {
		pool = &WorkersPool{
			workersNumber: workersNumber,
			tasksQueue:    make(chan Task, taskQueueSize),
		}
	}
	return pool
}

func (p *WorkersPool) AddTask(task Task) {
	p.tasksQueue <- task
}

func (p *WorkersPool) Finish() {
	// мы закроем канал с тасками так, что работяги дочитают все, что есть и завершатся
	close(p.tasksQueue)
}

func (p *WorkersPool) RunBackground() {

	fmt.Println("Initing rabotyag")
	// до последнего работяги
	wg := &sync.WaitGroup{}
	wg.Add(p.workersNumber)

	// запускаем воркеров, которые будут
	// на фоне ждать таски
	// и вычитывать их их канала с тасками
	// закрытый канал значит, что новых таск не будет
	// следовательно, заканчиваем все в очереди и завершаемся, работяги
	for i := 1; i <= p.workersNumber; i++ {
		worker := NewWorker(p.tasksQueue, i)
		go func() {
			defer wg.Done()
			worker.RunBackground()
		}()
	}

	// считаем, что пул работает, пока работают работяги
	wg.Wait()
	fmt.Println("All workers done")
}
