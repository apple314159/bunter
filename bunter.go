package main

import (
	"fmt"
	"os"

	"github.com/tidwall/buntdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

var revision = "(revision)" // replaced at build time by hg changeset

func openBunt(dbName *string) *buntdb.DB {
	// Open the data.db file. It will be created if it doesn't exist.
	db, err := buntdb.Open(*dbName)
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}
	return db
}

func main() {
	var key string
	var value string
	var db *buntdb.DB

	// kingpin.Flag("bunter", "buntdb tool")
	kingpin.Version(revision)
	dbName := kingpin.Flag("db", "File name").String()

	cmdGet := kingpin.Command("get", "get value")
	cmdGet.Arg("key", "key").Required().StringVar(&key)

	cmdSet := kingpin.Command("set", "Set key value")
	cmdSet.Arg("key", "key").Required().StringVar(&key)
	cmdSet.Arg("value", "value").Required().StringVar(&value)

	cmdFind := kingpin.Command("find", "find")
	cmdFind.Arg("key", "key").Required().StringVar(&key)

	switch kingpin.Parse() {
	case "get":
		db = openBunt(dbName)
		defer db.Close()
		db.View(func(tx *buntdb.Tx) error {
			val, err := tx.Get(key)
			if err != nil {
				fmt.Printf("error - %v\n", err)
				return err
			}
			fmt.Printf("%s: %s\n", key, val)
			return nil
		})

	case "set":
		db = openBunt(dbName)
		defer db.Close()
		db.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, value, nil)
			if err != nil {
				fmt.Printf("error - %v\n", err)
				return err
			}
			return err
		})

	case "find":
		db = openBunt(dbName)
		defer db.Close()

		db.View(func(tx *buntdb.Tx) error {
			tx.AscendKeys(key, func(k, v string) bool {
				fmt.Printf("%s: %v\n", k, v)
				return true
			})
			return nil
		})
	}
}
