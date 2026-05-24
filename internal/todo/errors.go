package todo

import (
	"log"
	"runtime"
	"sync"
)

var unhandled sync.Map

func CleanupOnPanic() {
	r := recover()
	if r == nil {
		return
	}
	unhandled.Range(func(key, value any) bool {
		if f, ok := value.(func()); ok {
			f()
		}

		return true
	})
	panic(r)

}

type MustHandleError interface {
	error
	Unwrap() error
	Handle()
}

type mustHandleError struct {
	inner   error
	handled bool
}

func MustHandle(err error) MustHandleError {

	if mhe, ok := err.(*mustHandleError); ok {
		mhe.Handle()
	}

	result := &mustHandleError{inner: err}
	runtime.SetFinalizer(result, func(mhe *mustHandleError) {
		mhe.ignored()
		unhandled.Delete(mhe)
	})
	unhandled.Store(result, result.ignored)
	return result
}

func (receiver *mustHandleError) Error() string {
	receiver.Handle()
	return receiver.inner.Error()
}

func (receiver *mustHandleError) Handle() {
	receiver.handled = true
}
func (receiver *mustHandleError) Unwrap() error {
	receiver.Handle()
	return receiver.inner
}

func (mhe *mustHandleError) ignored() {
	if mhe.handled {
		return
	}

	log.Printf("ERROR IGNORED: %s\n", mhe.Error())
}
