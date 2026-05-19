package nhelps

import (
	"fmt"
	"log"
)

func RunLogErr(operation func() error, fmsg string, v ...any) {
	err := operation()
	if err != nil {
		msg := fmt.Sprintf(fmsg, v...)
		log.Printf("%s:%v", msg, err)
	}
}
func RunLogErrArg[T any](operation func(T) error, arg T, fmsg string, v ...any) {
	err := operation(arg)
	if err != nil {
		msg := fmt.Sprintf(fmsg, v...)
		log.Printf("%s:%v:%v", msg, arg, err)
	}
}
