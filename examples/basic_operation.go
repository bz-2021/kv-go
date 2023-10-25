package main

import (
	kv "KV-go"
	"fmt"
	"path/filepath"
)

func main() {
	opts := kv.DefaultOptions
	opts.DirPath = "/tmp/kv"
	fmt.Println(filepath.Abs(opts.DirPath))
	db, err := kv.Open(opts)
	if err != nil {
		panic(err)
	}

	//err = db.Put([]byte("name2"), []byte("bitcask2"))
	//if err != nil {
	//	panic(err)
	//}
	value, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))

	err = db.Delete([]byte("name"))
	//if err != nil {
	//	panic(err)
	//}
}
