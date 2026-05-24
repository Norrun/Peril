package todo

import (
	"errors"
	"log"
	"runtime"
)

type ToDefer interface {
	Do() error
	IsDone() bool
	TryDo() (bool, error)
	Later(func() error)
}

type todefer struct {
	done  bool
	tasks []func() error
}

// Later implements [ToDefer].
func (receiver *todefer) Later(task func() error) {
	receiver.tasks = append(receiver.tasks, task)
}

func NewToDefer(executeOnCleanup bool, msg string) ToDefer {
	res := &todefer{tasks: make([]func() error, 0, 1)}

	runtime.SetFinalizer(res, func(todo *todefer) {
		if res.done {
			return
		}
		if executeOnCleanup {
			err := todo.Do()
			if err != nil {
				log.Printf("ERROR(s) When, %s : %v", msg, err)
			}
		}
	})
	return res
}

func (receiver *todefer) TryDo() (bool, error) {
	if receiver.done {
		return false, nil
	}

	var err error
	for i := len(receiver.tasks) - 1; i >= 0; i-- {
		errors.Join(err, receiver.tasks[i]())
	}
	receiver.done = true
	return true, err
}

func (receiver *todefer) IsDone() bool {
	return receiver.done
}

func (receiver *todefer) Do() error {
	if did, err := receiver.TryDo(); did {
		return err
	}
	return errors.New("Already done")
}
