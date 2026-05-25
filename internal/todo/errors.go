package todo

import (
	"errors"
	"fmt"
	"log"
	"weak"

	"runtime"
	"sync"
)

var unhandled sync.Map

func LogIgnoredErrorsOnPanic() {
	r := recover()
	if r == nil {
		return
	}
	log.Println(ReadResidualErrorsString())
	panic(r)
}

func ReadResidualErrorsString() string {
	result := ""

	unhandled.Range(func(key, value any) bool {
		if s, ok := value.(string); ok {
			result += s
		}
		if wp, ok := key.(weak.Pointer[mustHandleError]); ok && wp.Value() != nil {
			wp.Value().Handle()
		}

		return true
	})
	return result
}

func GetResidualErrors() []error {
	errs := make([]error, 0)
	unhandled.Range(func(key, value any) bool {

		if wp, ok := key.(weak.Pointer[mustHandleError]); ok && wp.Value() != nil {
			errs = append(errs, wp.Value())
		}

		return true
	})
	return errs
}

func GetResidualErrorsError() error {

	errs := GetResidualErrors()
	if len(errs) == 0 {
		return nil
	}
	err := errors.Join(errs...)
	return err
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
	if err != nil {
		return nil
	}

	if mhe, ok := errors.AsType[*mustHandleError](err); ok {
		mhe.Handle()
	}

	result := &mustHandleError{inner: err}
	runtime.SetFinalizer(result, func(mhe *mustHandleError) {
		log.Println(mhe.ignored())
		mhe.Handle()

	})
	unhandled.Store(result.toKey(), result.ignored())
	return result
}

func (receiver *mustHandleError) Error() string {
	receiver.Handle()
	return receiver.inner.Error()
}

func (receiver *mustHandleError) Handle() {
	receiver.handled = true
	unhandled.Delete(receiver.toKey())
}
func (receiver *mustHandleError) Unwrap() error {
	receiver.Handle()
	return receiver.inner
}

func (mhe *mustHandleError) ignored() string {

	return fmt.Sprintf("ERROR IGNORED: %s\n", mhe.inner.Error())
}

func (receiver *mustHandleError) toKey() weak.Pointer[mustHandleError] {
	return weak.Make(receiver)
}
func Handle(err error) bool {
	if mhe, ok := err.(MustHandleError); ok {
		mhe.Handle()
		return true
	}
	return false
}
