package workerspool

type Worker struct {
	ID         int
	tasksQueue chan Task // работяга работает, пока есть работа :_)
}

func NewWorker(tasksQueue chan Task, ID int) *Worker {
	return &Worker{
		ID:         ID,
		tasksQueue: tasksQueue,
	}
}

func (w *Worker) RunBackground() {
	// fmt.Printf("Starting worker %d...\n", w.ID)

	for task := range w.tasksQueue {
		process(w.ID, task)
	}

	// fmt.Printf("Worker %d stopped\n", w.ID)
}
