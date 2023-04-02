package internal

import (
	"crypto/rand"
	"fmt"
)

const (
	defaultUIDLen      = 8
	defaultUIDCharlist = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

// NewUID generate default 8 byte unique id using math/rand.
func NewUID() string {
	var uid [defaultUIDLen]byte
	if _, err := rand.Read(uid[:]); err != nil {
		panic(fmt.Errorf("sqlog: random read error from math/rand: %w", err))
	}

	for i := 0; i < defaultUIDLen; i++ {
		uid[i] = defaultUIDCharlist[uid[i]&byte(len(defaultUIDCharlist)-1)]
	}

	return string(uid[:])
}
