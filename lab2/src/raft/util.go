package raft

import "log"

// Debugging
const Debug = false
const Info = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}