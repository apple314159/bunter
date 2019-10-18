package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	//"github.com/chzyer/readline"

	"github.com/tidwall/buntdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

var revision = "(revision)" // replaced at build time by hg changeset

func openBunt(dbName string) *buntdb.DB {
	// Open the data.db file. It will be created if it doesn't exist.
	db, err := buntdb.Open(dbName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return db
}

var (
	dbName, key, value, itype, jarg string
	db                              *buntdb.DB
)

func main() {
	app := kingpin.New("bunter", "command line interface to buntdb")
	// kingpin.Flag("bunter", "buntdb tool")
	app.DefaultEnvars()
	app.Version(revision)
	app.Flag("db", "File name").StringVar(&dbName)

	cmdGet := app.Command("get", "get value")
	cmdGet.Arg("key", "key").Required().StringVar(&key)

	cmdSet := app.Command("set", "Set key value")
	cmdSet.Arg("key", "key").Required().StringVar(&key)
	cmdSet.Arg("value", "value").Required().StringVar(&value)

	cmdFind := app.Command("find", "find")
	cmdFind.Arg("key", "key").Required().StringVar(&key)

	cmdFindIndex := app.Command("findindex", "find using ndex")
	cmdFindIndex.Arg("key", "key").Required().StringVar(&key)

	cmdIndex := app.Command("index", "create index")
	cmdIndex.Arg("index", "index name").Required().StringVar(&key)
	cmdIndex.Arg("pattern", "pattern").Required().StringVar(&value)
	cmdIndex.Arg("type", "index type").Required().StringVar(&itype)
	cmdIndex.Arg("arg", "json field selection").StringVar(&jarg)

	app.Command("indexes", "list indexes")

	cmdDelIndex := app.Command("delindex", "delete key")
	cmdDelIndex.Arg("index", "name").Required().StringVar(&key)

	cmdDel := app.Command("del", "delete key")
	cmdDel.Arg("key", "key").Required().StringVar(&key)

	cmdFile := app.Command("commands", "process commands from file")
	cmdFile.Arg("file", "file").Required().StringVar(&value)

	process(app, os.Args[1:])
}

func process(app *kingpin.Application, args []string) {
	cmd, err := app.Parse(args)
	kingpin.FatalIfError(err, "Argument error")
	if db == nil {
		db = openBunt(dbName)
		defer db.Close()
	}

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

	case "findindex":
		db.View(func(tx *buntdb.Tx) error {
			tx.Ascend(key, func(k, v string) bool {
				fmt.Printf("%s: %v\n", k, v)
				return true
			})
			return nil
		})

	case "index":
		var indexType func(a, b string) bool
		switch itype {
		case "string":
			indexType = buntdb.IndexString
		case "int":
			indexType = buntdb.IndexInt
		case "uint":
			indexType = buntdb.IndexUint
		case "float":
			indexType = buntdb.IndexFloat
		case "json":
			indexType = buntdb.IndexJSON(jarg)
		default:
			fmt.Printf("bad index type - %s\n", itype)
			return
		}
		db.Update(func(tx *buntdb.Tx) error {
			err := tx.CreateIndex(key, value, indexType)
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
				//return err
			}
			if len(list) == 0 {
				fmt.Print("No additional indexes are defined\n")
			} else {
				fmt.Printf("%v\n", list)
			}
			return nil
		})
	case "commands":
		// read lines from file and call process on each one
		f, err := os.Open(value)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer f.Close()

		r := bufio.NewReader(f)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			args := strings.Fields(line)
			if len(args) == 0 {
				break
			}
			fmt.Printf("(%d) %v\n", len(args), args)
			//process
			process(app, args)
		}

	}
}
