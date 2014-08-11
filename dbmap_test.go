package dbmap_test

import (
	"code.google.com/p/dbmap"
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strings"
)

const dbFileStr = "data/example.db"

type recType struct {
	ID  int64  `db_primary:"*" db_table:"rec"`
	Str string `db:"*" db_index:"*"`
	Num int64  `db:"num" db_index:"*"`
}

var glRecDsc = dbmap.MustDescribe(recType{})

func errf(fmtStr string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, fmtStr, args...)
}

func hashStr(v int64) string {
	data := md5.Sum([]byte(fmt.Sprintf("*%d*", v)))
	return base64.StdEncoding.EncodeToString(data[:])
}

// This example demonstrates a simple use of dbmap. In typical scenarios, the
// record type would be defined, and MustDescribe() called, outside of the
// function in which database operations would be performed.
func ExampleDscType_01() {
	type recType struct {
		ID   int64  `db_primary:"*" db_table:"rec"`
		Name string `db:"*" db_index:"*"`
	}
	os.Remove(dbFileStr)
	hnd, err := sql.Open("sqlite3", dbFileStr)
	if err == nil {
		db := dbmap.MustDescribe(recType{}).Wrap(hnd)
		db.Create()
		db.InsertClear()
		var rec recType
		for _, rec = range []recType{{0, "Athos"}, {0, "Porthos"}, {0, "Aramis"}} {
			db.Insert(rec)
		}
		db.Query(&rec, "WHERE Name LIKE ? ORDER BY Name", "A%")
		for db.Next() {
			fmt.Println(rec.Name)
		}
		hnd.Close()
		err = db.Err()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// Aramis
	// Athos
}

// This example demonstrates dbmap with table creation, transactions,
// insertions, deletions and select queries. Typically, since an sql.DB
// instance is safe for multiple goroutines, opening and closing the database
// would occur outside of the function where dbmap functions are used.
func ExampleDscType_02() {
	var hnd *sql.DB
	var err error
	var j int64
	dbFileStr := "data/02.db"
	os.Remove(dbFileStr)
	hnd, err = sql.Open("sqlite3", dbFileStr)
	if err == nil {
		var rec recType
		db := glRecDsc.Wrap(hnd)
		db.TransactionBegin()
		db.Create()
		db.InsertClear()
		for j = 4100; j < 4200; j++ {
			rec.Num = j
			rec.Str = hashStr(j)
			db.InsertOrReplace(&rec)
		}
		db.TransactionEnd()
		db.TransactionBegin()
		db.Query(&rec, "WHERE num > ? AND num < ?", 4151, 4155)
		for db.Next() {
			rec.Str = "*" + rec.Str + "*"
			db.Update(rec, "Str")
		}
		db.TransactionEnd()
		db.Delete("WHERE num = ?", 4153)
		db.Query(&rec, "WHERE num > ? AND num < ?", 4150, 4156)
		for db.Next() {
			fmt.Printf("Got [%04d][%s]\n", rec.Num, rec.Str)
		}
		db.QueryRow(&rec, "WHERE num = ?", 4198)
		if db.OK() {
			fmt.Printf("Got [%04d][%s]\n", rec.Num, rec.Str)
		}
		db.TransactionBegin()
		db.QueryRow(&rec, "WHERE num = ?", 4199)
		if db.OK() {
			fmt.Printf("Got [%04d][%s]\n", rec.Num, rec.Str)
		}
		db.TransactionEnd()
		hnd.Close()
		err = db.Err()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// Got [4151][ff5EcTiTnmf0KZOJPM7CzA==]
	// Got [4152][*PJwbEyUPk33FwwnWLOLZfw==*]
	// Got [4154][*vZ83nzcp7LwW1F/Rm3km9w==*]
	// Got [4155][SiIpgI5rXRYy1RAjJBd7xw==]
	// Got [4198][gxAIo+DBmOT9eld3PLgi3Q==]
	// Got [4199][NGNAARg6cmq9n5rf2RoV1Q==]
}

// This example demonstrates creating and selecting from a view. Note that
// WrapJoin is called to associate the database handle, error condition and
// transaction handle with a previously instantiated WrapType variable. This
// allows transactions to manage multiples tables and facilitates error
// handling.
func ExampleDscType_03() {
	type authorType struct {
		ID   int64  `db_primary:"*" db_table:"author"`
		Name string `db:"*"`
	}
	type titleType struct {
		ID       int64  `db_primary:"*" db_table:"title"`
		AuthorID int64  `db:"*"`
		Name     string `db:"*"`
	}
	type authorTitleType struct {
		Author string `db:"*" db_table:"author_title"`
		Title  string `db:"*"`
	}
	var hnd *sql.DB
	var err error
	os.Remove(dbFileStr)
	hnd, err = sql.Open("sqlite3", dbFileStr)
	if err == nil {
		atDb := dbmap.MustDescribe(authorTitleType{}).Wrap(hnd)
		tDb := dbmap.MustDescribe(titleType{}).WrapJoin(atDb)
		aDb := dbmap.MustDescribe(authorType{}).WrapJoin(atDb)
		tDb.Create()
		aDb.Create()
		_, err = hnd.Exec("CREATE VIEW author_title AS SELECT a.Name AS Author, " +
			"t.Name as Title FROM author a, title t WHERE a.rowid = t.AuthorID")
		atDb.SetError(err)
		atDb.TransactionBegin()
		for _, str := range []string{"William Faulkner/Go Down, Moses/The Bear",
			"Mark Twain/Huckleberry Finn/Life on the Mississippi"} {
			list := strings.Split(str, "/")
			var author authorType
			var title titleType
			for j, str := range list {
				if j == 0 {
					author.Name = str
					aDb.Insert(&author)
				} else {
					title.AuthorID = author.ID
					title.Name = str
					tDb.Insert(&title)
				}
			}
		}
		atDb.TransactionEnd()
		var atRec authorTitleType
		atDb.Query(&atRec, "ORDER BY Author, Title")
		for atDb.Next() {
			fmt.Printf("%s / %s\n", atRec.Author, atRec.Title)
		}
		hnd.Close()
		err = atDb.Err()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// Mark Twain / Huckleberry Finn
	// Mark Twain / Life on the Mississippi
	// William Faulkner / Go Down, Moses
	// William Faulkner / The Bear
}

// This example demonstrates, ill-advisedly, the direct use of DscType
// functions. The need to check errors and manage statements in more detail
// results in much more code than when the wrapped functions are used.
func ExampleDscType_04() {
	var hnd *sql.DB
	var err error
	os.Remove(dbFileStr)
	hnd, err = sql.Open("sqlite3", dbFileStr)
	if err == nil {
		var dsc dbmap.DscType
		dsc, err = dbmap.Describe(recType{})
		if err == nil {
			var tx *sql.Tx
			tx, err = hnd.Begin()
			if err == nil {
				_, err = tx.Exec(dsc.CreateStr())
			}
			if err == nil {
				var rec recType
				var argList []interface{}
				var stInsert *sql.Stmt
				stInsert, err = tx.Prepare(dsc.InsertStr())
				if err == nil {
					var j int64
					for j = 3100; j < 3200; j++ {
						if err == nil {
							rec.Num = j
							rec.Str = fmt.Sprintf("%x", j)
							argList, _, err = dsc.InsertArg(&rec)
							if err == nil {
								// _, err = stInsert.Exec(rec.Str, rec.Num)
								_, err = stInsert.Exec(argList...)
							}
						}
					}
				}
			}
			if err == nil {
				tx.Commit()
			} else {
				tx.Rollback()
			}
			if err == nil {
				var rec recType
				var argList []interface{}
				var rows *sql.Rows
				rows, err = hnd.Query(dsc.SelectStr("WHERE num > ? AND num < ?"), 3120, 3125)
				if err == nil {
					argList, err = dsc.SelectArg(&rec)
					if err == nil {
						for rows.Next() {
							err = rows.Scan(argList...)
							if err == nil {
								fmt.Printf("Got [%s]\n", rec.Str)
							}
						}
						if err == nil {
							err = rows.Err()
						}
					}
				}
			}
		}
		hnd.Close()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// Got [c31]
	// Got [c32]
	// Got [c33]
	// Got [c34]
}

// This example is the bad code emporium, full of calls to exercise errant code
// paths to increase test coverage.
func ExampleDscType_05() {
	var hnd *sql.DB
	var db dbmap.WrapType
	var rec recType
	var err error

	describe := func(s string, r interface{}) {
		dsc, err := dbmap.Describe(r)
		if err == nil {
			fmt.Println(s, &dsc)
		} else {
			fmt.Println(s, err)
		}
	}

	func() {
		defer func() {
			res := recover()
			if res != nil {
				fmt.Println(res)
			}
		}()
		_ = dbmap.MustDescribe(struct{ A int }{})
	}()

	type aType struct {
		A int `db:"*"`
	}
	describe("a", aType{})
	type bType struct {
		ID int64 `db_table:"test"`
		A  int64
	}
	describe("b", bType{})
	type cType struct {
		ID  int64   `db_table:"test"`
		Hnd *sql.DB `db:"*"`
	}
	describe("c", cType{})
	type dType struct {
		ID int32 `db_table:"test" db_primary:"*"`
		A  int32 `db:"*"`
	}
	describe("d", dType{})
	type eType struct {
		A int64 `db_table:"test" db_primary:"*"`
		B int64 `db_primary:"*"`
	}
	describe("e", eType{})
	type fType struct {
		A int64 `db_table:"test" db_primary:"*"`
		B int64 `db_table:"test"`
	}
	describe("f", fType{})
	describe("g", describe)
	db = glRecDsc.Wrap(nil)
	os.Remove(dbFileStr)
	hnd, err = sql.Open("sqlite3", dbFileStr)
	if err == nil {
		db = glRecDsc.Wrap(hnd)
		fmt.Printf("%s, %s, %v, %v\n", &glRecDsc, &db, db.DB() != nil, db.OK())
		db.TransactionBegin()
		fmt.Printf("%v\n", db.Tx() != nil)
		db.TransactionRollback()
		db.TransactionBegin()
		db.TransactionBegin()
		fmt.Println(db.Err())
		db.ClearError()
		db.TransactionCommit()
		db.TransactionEnd()
		fmt.Println(db.Err())
		db.ClearError()
		db.Create()
		db.InsertClear()
		db.Insert(recType{Num: 1234, Str: "ABC"})
		res := db.Result()
		rec.Num, _ = res.RowsAffected()
		fmt.Printf("Rows affected: %d, OK %v\n", rec.Num, db.OK())
		db.Query(&rec, "WHERE num = ?", 1234)
		var updRec recType
		for db.Next() {
			updRec = rec
		}
		updRec.Str = "*" + updRec.Str + "*"
		db.Update(updRec, "Str")
		db.Update(&updRec, "Str")
		db.Update(updRec, "*")
		db.Update(updRec)
		db.SetErrorf("keyboard missing, press Escape to continue")
		db.SetError(db.Err())
		db.ClearError()
		_, err = glRecDsc.SelectArg(aType{})
		fmt.Println(err)
		_, err = glRecDsc.SelectArg(&aType{})
		fmt.Println(err)
		_ = glRecDsc.TruncateStr()
		_, _ = glRecDsc.UpdateArg(rec, "foo")
		_, _ = glRecDsc.UpdateArg(struct{ foo int }{})
		_, _, _ = glRecDsc.InsertArg(struct{ foo int }{})
		type simpleType struct {
			A string `db:"*" db_table:"simple"`
		}
		var simple simpleType
		dsc, _ := dbmap.Describe(simple)
		_, _ = dsc.UpdateArg(simple, "A")
		_ = dbmap.MustDescribe(&recType{})
		hnd.Close()
	}
	// Output:
	// at least one exported structure field must have "db" tag
	// a missing "db_table" tag
	// b at least one exported structure field must have "db" tag
	// c database does not support fields of type *sql.DB
	// d expecting int64 for id, got int32
	// e multiple occurrence of "db_primary" tag
	// f multiple occurrence of "db_table" tag
	// g specified address must be of structure with one or more fields that have a "db" tag
	// dbmap, dbmap/wrap, true, true
	// true
	// nested transactions not supported
	// no transaction to end
	// Rows affected: 1, OK true
	// passed-in value must be a structure pointer
	// passed in record (dbmap_test.aType) for select does not match descriptor (dbmap_test.recType)
}
