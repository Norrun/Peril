package todo

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"
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
	log.Println("Panicing")

	unhandled.Range(func(key, value any) bool {
		if s, ok := value.(unhandledEntry); ok {

			log.Println(s.msg)
		}
		return true
	})

	//log.Println(ReadResidualErrorsString())

	debug.PrintStack()

	panic(r)
}

func StartPeriodicDump(interval, maxAge time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			//fmt.Println("scanning")
			fresh := true
			unhandled.Range(func(key, value any) bool {
				remove := false
				if s, ok := value.(unhandledEntry); ok && time.Since(s.creation) > maxAge {
					if fresh {
						log.Println("Periodic scan")
						fresh = false
					}
					remove = true
					log.Println(s.msg)
				}
				if errp, ok := key.(weak.Pointer[mustHandleError]); ok && errp.Value() != nil && remove {
					errp.Value().Handle()

				}
				return true
			})
		}
	}()
}

func ReadResidualErrorsString() string {
	result := ""

	unhandled.Range(func(key, value any) bool {
		if s, ok := value.(unhandledEntry); ok {
			result += s.msg
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

type unhandledEntry struct {
	msg      string
	creation time.Time
}

func MustHandle(err error) MustHandleError {
	if err == nil {
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
	unhandled.Store(result.toKey(), unhandledEntry{msg: result.ignored(), creation: time.Now()})
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
