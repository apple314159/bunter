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
		fmt.Println(err)
		os.Exit(1)
	}
	return db
}

func main() {
	var key string
	var value string
	var db *buntdb.DB

	app := kingpin.New("bunter", "command line interface to buntdb")
	// kingpin.Flag("bunter", "buntdb tool")
	app.DefaultEnvars()
	app.Version(revision)
	dbName := app.Flag("db", "File name").String()

	cmdGet := app.Command("get", "get value")
	cmdGet.Arg("key", "key").Required().StringVar(&key)

	cmdSet := app.Command("set", "Set key value")
	cmdSet.Arg("key", "key").Required().StringVar(&key)
	cmdSet.Arg("value", "value").Required().StringVar(&value)

	cmdFind := app.Command("find", "find")
	cmdFind.Arg("key", "key").Required().StringVar(&key)

	cmdIndex := app.Command("index", "create index")
	cmdIndex.Arg("index", "index name").Required().StringVar(&key)
	cmdIndex.Arg("pattern", "pattern").Required().StringVar(&value)

	app.Command("indexes", "list indexes")

	cmdDelIndex := app.Command("delindex", "delete key")
	cmdDelIndex.Arg("index", "name").Required().StringVar(&key)

	cmdDel := app.Command("del", "delete key")
	cmdDel.Arg("key", "key").Required().StringVar(&key)

	cmd, err := app.Parse(os.Args[1:])
	kingpin.FatalIfError(err, "Argument error")
	db = openBunt(dbName)
	defer db.Close()

	switch cmd {
	case "get":
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
		db.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, value, nil)
			if err != nil {
				fmt.Printf("error - %v\n", err)
				return err
			}
			return err
		})

	case "del":
		db.Update(func(tx *buntdb.Tx) error {
			_, err := tx.Delete(key)
			if err != nil {
				fmt.Printf("error - %v\n", err)
				return err
			}
			return err
		})

	case "find":
		db.View(func(tx *buntdb.Tx) error {
			tx.AscendKeys(key, func(k, v string) bool {
				fmt.Printf("%s: %v\n", k, v)
				return true
			})
			return nil
		})

	case "index":
		db.Update(func(tx *buntdb.Tx) error {
			err := tx.CreateIndex(key, value, buntdb.IndexString)
			if err != nil {
				fmt.Printf("error - %v\n", err)
			}
			return err
		})

	case "delindex":
		db.Update(func(tx *buntdb.Tx) error {
			err := tx.DropIndex(key)
			if err != nil {
				fmt.Printf("drop index - %v\n", err)
			}
			return err
		})

	case "indexes":
		db.View(func(tx *buntdb.Tx) error {
			list, err := tx.Indexes()
			if err != nil {
				fmt.Printf("error - %v\n", err)
				return err
			}
			if len(list) == 0 {
				fmt.Print("No additional indexes are defined\n")
			} else {
				fmt.Printf("%v\n", list)
			}
			return nil
		})
	}
}
