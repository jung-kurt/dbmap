/*
 * Copyright (c) 2014 Kurt Jung (Gmail: kurt.w.jung)
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package dbmap_test

import (
	"code.google.com/p/dbmap"
	"crypto/sha1"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"os"
)

// This example demonstrates a simple use of dbmap. In the call to Retrieve(),
// each question mark is replaced with a quoted and escaped expression result.
// The "db_index" tag is used to index the table by the associated field. Here,
// one index is based on the the Name field.
func ExampleDbType_01() {
	type recType struct {
		ID   int64  `db_primary:"*" db_table:"rec"`
		Name string `db:"*" db_index:"*"`
	}
	db := dbmap.DbCreate("data/example.db")
	db.TableCreate(&recType{})
	db.Insert([]recType{{0, "Athos"}, {0, "Porthos"}, {0, "Aramis"}})
	var list []recType
	db.Retrieve(&list, "WHERE Name LIKE ? ORDER BY Name", "A%")
	fmt.Println(db)
	for _, r := range list {
		fmt.Println(r.Name)
	}
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// dbmap
	// Aramis
	// Athos
}

// This example demonstrates the use of blobs in dbmap.
func ExampleDbType_02() {
	type recType struct {
		ID      int64  `db_primary:"*" db_table:"image"`
		Img     []byte `db:"*"`
		FileStr string `db:"*"`
	}
	var rec recType
	var err error
	rec.FileStr = "dbmap.jpg"
	rec.Img, err = ioutil.ReadFile(rec.FileStr)
	if err == nil {
		chksum := sha1.Sum(rec.Img)
		db := dbmap.DbCreate("data/example.db")
		db.TableCreate(&rec)
		db.Insert([]recType{rec})
		var list []recType
		db.Retrieve(&list, "WHERE FileStr = ?", rec.FileStr)
		if len(list) == 1 {
			if chksum == sha1.Sum(list[0].Img) {
				fmt.Printf("%s, SHA1: %v, length: %d\n",
					rec.FileStr, chksum, len(rec.Img))
			}
		}
		db.Close()
		err = db.Error()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// dbmap.jpg, SHA1: [25 171 91 242 2 67 216 162 57 97 225 97 3 26 37 177 213 194 45 59], length: 3780
}

// This example demonstrates the reopening of a db database.
//
// This example shows the optional provision of names in the "db" field tags.
// The tag values are used as field names by the db database. This feature is
// generally useful only if the db database is used by multiple applications.
// It is these names rather than the names in the Go structure that are used in
// expressions passed to Retrieve() and for limiting fields to be updated in
// Update().
//
// Notice that the type of the expressions for the ? parameters need to match
// the type of the group_num field (in this case int64).
//
// The Retrieve function appends selection results to the passed-in slice. If
// you wish to repopulate the slice, empty it prior to calling Retrieve by
// assigning nil to it.
func ExampleDbType_03() {
	dbFileStr := "data/example.db"
	type recType struct {
		ID   int64  `db_primary:"*" db_table:"rec"`
		Name string `db:"last_name"`
		Num  int64  `db:"group_num"`
		Val  int    // exported but not managed by db
		val  int    // not exported
	}
	db := dbmap.DbCreate(dbFileStr)
	db.TableCreate(&recType{})
	var list []recType
	for j := 0; j < 1024; j++ {
		list = append(list, recType{0, fmt.Sprintf("*** %4d ***", j),
			int64(j), j * 2, j * 4})
	}
	db.Insert(list)
	db.Close()
	if db.OK() {
		db := dbmap.DbOpen(dbFileStr)
		list = nil // Reuse the slice but empty it first
		db.Retrieve(&list, "WHERE group_num > ? AND group_num < ? ORDER BY group_num",
			int64(1000), int64(1004))
		for _, r := range list {
			fmt.Printf("%s %d %d %d\n", r.Name, r.Num, r.Val, r.val)
		}
		db.Close()
	}
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// *** 1001 *** 1001 0 0
	// *** 1002 *** 1002 0 0
	// *** 1003 *** 1003 0 0
}

// This example demonstrates a record update. In the first call to Update(),
// only fields B and C are updated. In the second call, all fields are updated.
func ExampleDbType_04() {
	type recType struct {
		ID      int64 `db_primary:"*" db_table:"rec"`
		A, B, C int64 `db:"*"`
	}
	var rec recType
	db := dbmap.DbCreate("data/example.db")
	db.TableCreate(&recType{})
	adjust := func() {
		rec.A += 1000
		rec.B += 1000
		rec.C += 1000
	}
	retrieve := func() {
		var rl []recType
		// fmt.Printf("Rec ID %d\n", rec.ID)
		db.Retrieve(&rl, "WHERE rowid = ?", rec.ID)
		if len(rl) > 0 {
			for _, r := range rl {
				fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
			}

		}
	}
	var list []recType
	for j := int64(0); j < 10; j++ {
		list = append(list, recType{0, j, j + 1, j + 2})
	}
	db.Insert(list)
	list = nil
	db.Retrieve(&list, "WHERE A = ?", int64(2))
	if len(list) > 0 {
		rec = list[0]
		adjust()
		db.Update(&rec, "B", "C") // Update only B and C in the database
		retrieve()
		adjust()
		db.Update(&rec, "*") // Update all fields
		retrieve()
	}
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// 2 1003 1004
	// 2002 2003 2004
}

// This example demonstrates record deletion.
func ExampleDbType_05() {
	type recType struct {
		ID      int64 `db_primary:"*" db_table:"rec"`
		A, B, C int64 `db:"*"`
	}
	db := dbmap.DbCreate("data/example.db")
	show := func(str string) {
		var rs []recType
		db.Retrieve(&rs, "ORDER BY A")
		fmt.Printf("%s\n", str)
		for _, r := range rs {
			fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
		}
	}
	db.TableCreate(&recType{})
	var list []recType
	for j := int64(0); j < 5; j++ {
		list = append(list, recType{0, j, j + 1, j + 2})
	}
	db.Insert(list)
	show("All records after Insert()")
	db.Delete(&recType{}, "WHERE A = ? OR A = ?", int64(0), int64(4))
	show("All records after Delete()")
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// All records after Insert()
	// 0 1 2
	// 1 2 3
	// 2 3 4
	// 3 4 5
	// 4 5 6
	// All records after Delete()
	// 1 2 3
	// 2 3 4
	// 3 4 5
}

// This example demonstrates table truncation.
func ExampleDbType_06() {
	type recType struct {
		ID      int64 `db_primary:"*" db_table:"rec"`
		A, B, C int64 `db:"*"`
	}
	db := dbmap.DbCreate("data/example.db")
	show := func(str string) {
		var rs []recType
		db.Retrieve(&rs, "ORDER BY A")
		fmt.Printf("%s\n", str)
		for _, r := range rs {
			fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
		}
	}
	db.TableCreate(&recType{})
	var list []recType
	for j := int64(0); j < 5; j++ {
		list = append(list, recType{0, j, j + 1, j + 2})
	}
	db.Insert(list)
	show("All records after Insert()")
	db.Truncate(&recType{})
	show("All records after Truncate()")
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// All records after Insert()
	// 0 1 2
	// 1 2 3
	// 2 3 4
	// 3 4 5
	// 4 5 6
	// All records after Truncate()
}

// This example demonstrates using db to open and close the database. This
// could be useful if the db database needs to be opened with special options.
func ExampleDbType_07() {
	dbFileStr := "data/example.db"
	type recType struct {
		ID      int64 `db_primary:"*" db_table:"sample"`
		A, B, C int64 `db:"*"`
	}
	os.Remove(dbFileStr)
	hnd, err := sql.Open("sqlite3", dbFileStr)
	if err == nil {
		db := dbmap.DbSetHandle(hnd)
		db.TableCreate(&recType{})
		var list []recType
		for j := int64(0); j < 3; j++ {
			list = append(list, recType{0, j, j + 1, j + 2})
		}
		db.Retrieve(&list, "ORDER BY A DESC")
		for _, r := range list {
			fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
		}
		hnd.Close()
		err = db.Error()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// 0 1 2
	// 1 2 3
	// 2 3 4
}

// This example is a menagerie of calls that exercise various failure code
// paths. It is a catchall of routines needed for complete test coverage using
// the go cover tool.
func ExampleDbType_08() {
	type recType struct {
		ID      int64 `db_primary:"*" db_table:"rec"`
		A, B, C int64 `db:"*"`
	}
	var db *dbmap.DbType
	report := func() {
		if db.Err() {
			fmt.Println(db.Error())
		}
		db.ClearError()
	}
	var rl []recType
	var rec recType
	db = dbmap.DbCreate("data/foo/bar/baz/example.db")
	db.Close()
	report()
	os.RemoveAll("data/foo")
	db = dbmap.DbCreate("data/example.db")
	db.TableCreate(&rec)
	db.TransactBegin()
	db.Retrieve(&rl, "")
	db.TransactRollback()
	db.SetErrorf("application %s", "error")
	err := db.Error()
	// The following several calls exercise the quick return on existing error
	// condition
	db.Exec("foo")
	db.Query("foo")
	db.TableCreate(&rec)
	db.Update(&rec)
	db.Retrieve(&rl, "")
	db.Insert(rl)
	db.Delete(&rec, "")
	db.Truncate(&rec)
	report()
	db.SetError(err)
	report()
	db.Update(&rec)
	report()
	db.Insert(&rec)
	report()
	db.Retrieve(&rec, "")
	report()
	db.Retrieve(rec, "")
	report()
	db.TransactCommit()
	report()
	db.TransactRollback()
	report()
	db.Trace(true)
	db.Exec("foo")
	db.Trace(false)
	report()
	type aType struct {
		ID  bool  `db_primary:"*" db_table:"a"`
		Val int64 `db:"*"`
	}
	db.TableCreate(&aType{})
	report()
	type bType struct {
		ID  int64 `db_primary:"*" db_table:"b"`
		Val int64
	}
	db.TableCreate(&bType{})
	report()
	var a int
	db.TableCreate(a)
	report()
	db.TableCreate(&a)
	report()
	type cType struct {
		ID  int64        `db_primary:"*" db_table:"b"`
		Hnd dbmap.DbType `db:"*"`
	}
	db.TableCreate(&cType{})
	report()
	type dType struct {
		ID1 int64 `db_primary:"*" db_table:"d1"`
		ID2 int64 `db_primary:"*" db_table:"d2"`
		Val int64 `db:"*"`
	}
	db.TableCreate(&dType{})
	report()
	type eType struct {
		Val int64 `db:"*"`
	}
	db.TableCreate(&eType{})
	report()
	type fType struct {
		ID  int64 `db_primary:"*" db_table:"f1"`
		Val int64 `db:"*" db_table:"f2"`
	}
	db.TableCreate(&fType{})
	report()
	type gType struct {
		Val1 int64 `db:"*" db_table:"g"`
		Val2 int64 `db:"*"`
	}
	g := gType{1, 2}
	db.TableCreate(&g)
	db.Insert([]gType{g})
	report()
	// Output:
	// application error
	// application error
	// at least one field name expected in function Update
	// function Insert requires slice as first argument
	// function Retrieve expecting pointer to slice, got pointer to struct
	// function Retrieve expecting pointer to slice, got struct
	// no transaction to commit
	// no transaction to rollback
	// dbmap [--E] foo
	// near "foo": syntax error
	// expecting int64 for id, got bool
	// no structure fields have "db" tag
	// expecting record pointer, got int
	// specified address must be of structure with one or more fields that have a "db" tag
	// database does not support fields of type dbmap.DbType
	// multiple occurrence of "db_primary" tag
	// missing "db_table" tag
	// multiple occurrence of "db_table" tag
	// primary key is required but is not present in structure
}
