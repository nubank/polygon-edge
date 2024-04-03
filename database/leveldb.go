package database

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"time"
)

func NewLevelDB(path string, name string) (db *leveldb.DB, err error) {
	db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		return
	}

	go meter(db, time.Second*5, name)

	return
}

func meter(db *leveldb.DB, refresh time.Duration, name string) {
	fmt.Printf("metering %s\n", name)
}
