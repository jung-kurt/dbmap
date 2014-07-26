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

package dbmap

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unsafe"
)

var typeMap = map[string]string{
	"[]uint8": "blob",
	"bool":    "integer",
	"byte":    "integer",
	"float":   "real",
	"float32": "real",
	"float64": "real",
	"int":     "integer",
	"int16":   "integer",
	"int32":   "integer",
	"int64":   "integer",
	"int8":    "integer",
	"rune":    "integer",
	"string":  "text",
	"uint":    "integer",
	"uint16":  "integer",
	"uint32":  "integer",
	"uint64":  "integer",
	"uint8":   "integer",
}

type idxType struct {
	nameStr string
	fldStr  string
}

type dscType struct {
	tblStr    string                         // Name of database table
	idPresent bool                           // Primary key is present in table
	idSf      reflect.StructField            // Descriptor for primary key
	recTp     reflect.Type                   // Record interface
	nameMap   map[string]reflect.StructField // {"num":@, "name":@, ...}
	create    struct {
		nameTypeStr string    // "num int32, name string, ..."
		idxList     []idxType // {{"fooID", "rowid"}, {"fooName", "Name"}, {"fooNum", "Num"}, ...}
	}
	insert struct {
		nameStr  string   // "num, name, ..."
		nameList []string // {"num", "name", ...}
		qmStr    string   // "?, ?, ..."
		sfList   []reflect.StructField
	}
	sel struct {
		nameStr     string                // "rowid, num, name, ..."
		sfList      []reflect.StructField // Includes ID
		typeStrList []string              // {"int64", "bigint", "string", ...}
	}
}

// DbType facilitates use of the database/sql API. Hnd is the exposed handle to
// the database instance.
type DbType struct {
	Hnd *sql.DB
	tx  *sql.Tx
	// Cache for table descriptors
	dscMap map[reflect.Type]dscType
	// Cache for executable commands
	stmtMap map[string]*sql.Stmt
	trace   bool
	err     error
	tested  bool
}

// OK returns true if no processing errors have occurred.
func (db *DbType) OK() bool {
	return db.err == nil
}

// Err returns true if a processing error has occurred.
func (db *DbType) Err() bool {
	return db.err != nil
}

// ClearError unsets the current error value.
func (db *DbType) ClearError() {
	db.err = nil
}

// SetError sets an error to halt database calls. This may facilitate error
// handling by application. A value of nil for err is ignored; use ClearError()
// to unset the error condition. See also OK(), Err() and Error().
func (db *DbType) SetError(err error) {
	if db.err == nil && err != nil {
		db.err = err
		if !db.tested {
			// Take the opportunity here to exercise some trivial code paths that cannot
			// be tested externally
			_ = prePad("")
			_ = db.dscFromType(reflect.TypeOf(err), true)
			db.tested = true
		}
	}
}

// SetErrorf sets the internal Db error with formatted text to halt database
// calls. This may facilitate error handling by application.
//
// See the documentation for printing in the standard fmt package for details
// about fmtStr and args.
func (db *DbType) SetErrorf(fmtStr string, args ...interface{}) {
	if db.err == nil {
		db.err = fmt.Errorf(fmtStr, args...)
	}
}

// Error returns the internal Db error; this will be nil if no error has occurred.
func (db *DbType) Error() error {
	return db.err
}

// String satisfies the fmt.Stringer interface and returns the library name
func (db *DbType) String() string {
	return "dbmap"
}

func (db *DbType) init() {
	if db.err == nil {
		db.dscMap = make(map[reflect.Type]dscType)
		db.stmtMap = make(map[string]*sql.Stmt)
	}
}

// DbSetHandle initializes the dbmap instance with a database/sql handle that
// is already open. This function can be used if the database needs to be
// opened with special options. Only one of DbSetHandle, DbOpen and DbCreate
// should be called to initialize the dbmap instance. Close() may be called to
// close the specified handle after use.
func DbSetHandle(hnd *sql.DB) (db *DbType) {
	db = new(DbType)
	db.Hnd = hnd
	db.init()
	return
}

// DbOpen opens a database with default options. Only one of DbSetHandle,
// DbOpen and DbCreate should be called to initialize the dbmap instance. After
// use, Close() should be called to free resources.
func DbOpen(dbFileStr string) (db *DbType) {
	db = new(DbType)
	db.Hnd, db.err = sql.Open("sqlite3", dbFileStr)
	db.init()
	return
}

// DbCreate creates a new database with default options or overwrites an
// existing one. The directory path to the file will be created if needed. Only
// one of DbSetHandle, DbOpen and DbCreate should be called to initialize the
// dbmap instance. After use, Close() should be called to free resources.
func DbCreate(dbFileStr string) (db *DbType) {
	var err error
	db = new(DbType)
	dir := filepath.Dir(dbFileStr)
	_, err = os.Stat(dir)
	if err != nil {
		db.err = os.MkdirAll(dir, 0755)
	}
	if db.err == nil {
		_, err := os.Stat(dbFileStr)
		if err == nil {
			db.err = os.Remove(dbFileStr)
		}
		if db.err == nil {
			db.Hnd, db.err = sql.Open("sqlite3", dbFileStr)
			db.init()
		}
	}
	return
}

// Close closes the dbmap instance.
func (db *DbType) Close() {
	if db.Hnd != nil {
		db.Hnd.Close()
		db.Hnd = nil
	}
}

// Trace sets or unsets trace mode in which commands are printed to standard
// out. Statements that are submitted for execution are printed with a three
// character flag indicating whether the command was cached (C), whether a
// transaction is pending (T), and whether an error has occurred (E).
func (db *DbType) Trace(on bool) {
	// why? if db.err == nil {
	db.trace = on
	// }
}

// TransactBegin begins a new transaction. This function is typically not
// needed by applications because transactions are managed by dbmap functions as
// required.
func (db *DbType) TransactBegin() {
	if db.err == nil {
		if db.tx == nil {
			db.tx, db.err = db.Hnd.Begin()
		}
	}
	return
}

func (db *DbType) transactEnd(ok bool) {
	if db.tx != nil {
		if ok {
			db.err = db.tx.Commit()
		} else {
			db.err = db.tx.Rollback()
		}
		db.tx = nil
	} else {
		if db.err == nil {
			db.SetErrorf("no transaction to %s", strIf(ok, "commit", "rollback"))
		}
	}
	return
}

// TransactCommit commits the pending transaction. This function is typically
// not needed by applications because transactions are managed by dbmap functions
// as required.
func (db *DbType) TransactCommit() {
	if db.err == nil {
		db.transactEnd(true)
	}
	return
}

// TransactRollback rolls back the pending transaction. This function is
// typically not needed by applications because transactions are managed by dbmap
// functions as required.
func (db *DbType) TransactRollback() {
	if db.err == nil {
		db.transactEnd(false)
	}
	return
}

func (db *DbType) statement(cmdStr string) (stmt *sql.Stmt, cached bool) {
	stmt, cached = db.stmtMap[cmdStr]
	if !cached {
		// Caveat: cached commands may become obsolete as different execution paths
		// result from changing database.
		stmt, db.err = db.Hnd.Prepare(cmdStr)
		if db.err == nil {
			db.stmtMap[cmdStr] = stmt
		}
	}
	return
}

func (db *DbType) report(cmdStr string, cached bool) {
	if db.trace {
		fmt.Printf("dbmap [%s%s%s] %s\n",
			strIf(cached, "C", "-"),
			strIf(db.tx != nil, "T", "-"),
			strIf(db.err != nil, "E", "-"),
			cmdStr)
	}

}

// Exec compiles (if not already cached) and executes a statement that does not
// return rows.
func (db *DbType) Exec(cmdStr string, prms ...interface{}) (res sql.Result) {
	if db.err != nil {
		return
	}
	stmt, cached := db.statement(cmdStr)
	if db.err == nil {
		if db.tx != nil {
			stmt = db.tx.Stmt(stmt)
		}
		res, db.err = stmt.Exec(prms...)
	}
	db.report(cmdStr, cached)
	return
}

// Query compiles (if not already cached) and executes a statement that return rows.
func (db *DbType) Query(cmdStr string, prms ...interface{}) (rows *sql.Rows) {
	if db.err != nil {
		return
	}
	stmt, cached := db.statement(cmdStr)
	if db.err == nil {
		if db.tx != nil {
			stmt = db.tx.Stmt(stmt)
		}
		rows, db.err = stmt.Query(prms...)
	}
	db.report(cmdStr, cached)
	return
}

func strIf(cond bool, aStr string, bStr string) (res string) {
	if cond {
		res = aStr
	} else {
		res = bStr
	}
	return
}

func prePad(str string) string {
	if len(str) > 0 {
		return " " + str
	}
	return str
}

func valueList(recVl reflect.Value, sfList []reflect.StructField) (list []reflect.Value) {
	addr := recVl.UnsafeAddr()
	var fldVl reflect.Value
	for _, sf := range sfList {
		fldVl = reflect.Indirect(reflect.NewAt(sf.Type, unsafe.Pointer(addr+sf.Offset)))
		list = append(list, fldVl)
	}
	return
}

func valueInterfaceList(recVl reflect.Value, sfList []reflect.StructField) (list []interface{}) {
	vlist := valueList(recVl, sfList)
	for _, v := range vlist {
		list = append(list, v.Interface())
	}
	return
}

func valueInterfaceAddrList(recVl reflect.Value, sfList []reflect.StructField) (list []interface{}) {
	vlist := valueList(recVl, sfList)
	for _, v := range vlist {
		list = append(list, v.Addr().Interface())
	}
	return
}

func idxListAppend(listPtr *[]idxType, nameStr, fldStr string) {
	*listPtr = append(*listPtr, idxType{nameStr, fldStr})
}

// dscFromType collects meta information, for example field types and SQL
// names, from the passed-in record.
func (db *DbType) dscFromType(recTp reflect.Type, primaryKeyRequired bool) (dsc dscType) {
	if db.err != nil {
		return
	}
	if recTp.Kind() == reflect.Struct {
		var typeOk, ok bool
		dsc, ok = db.dscMap[recTp]
		if !ok {
			dsc.recTp = recTp
			var sfList []reflect.StructField
			var primaryStr, sqlStr, tblStr, typeStr string
			var fldTp reflect.Type
			var selList, qmList, createList []string
			dsc.nameMap = make(map[string]reflect.StructField)
			for j := 0; j < recTp.NumField(); j++ {
				sfList = append(sfList, recTp.Field(j))
			}
			var indexed bool
			for _, sf := range sfList {
				if db.err == nil {
					indexed = len(sf.Tag.Get("db_index")) > 0
					// Note on indexes. In the future, if this package gains support for
					// multi-field indexes, the db_index tag can have strings such as "a+01",
					// "a-02", etc. Here, "a" will be the index, the sort order of the key
					// segment will be specified by "-" (descending) or "+" (ascending) and
					// the significance of the key will be determined by sorting the following
					// text (here, "01" and "02", but any text could be used).
					fldTp = sf.Type
					sqlStr = sf.Tag.Get("db")
					if len(sqlStr) > 0 {
						if sqlStr == "*" {
							sqlStr = sf.Name
						}
						// fmt.Printf("Processing field of type %s\n", fldTp.String())
						typeStr, typeOk = typeMap[fldTp.String()]
						if typeOk {
							dsc.nameMap[sqlStr] = sf
							strListAppend(&createList, "%s %s", sqlStr, typeStr)
							if indexed {
								idxListAppend(&dsc.create.idxList, sf.Name, sqlStr)
							}
							dsc.insert.sfList = append(dsc.insert.sfList, sf)
							strListAppend(&dsc.insert.nameList, "%s", sqlStr)
							strListAppend(&qmList, "?")
							strListAppend(&dsc.sel.typeStrList, "%s", typeStr)
							strListAppend(&selList, "%s", sqlStr)
							dsc.sel.sfList = append(dsc.sel.sfList, sf)
						} else {
							db.SetErrorf("database does not support fields of type %s", fldTp.String())
						}
					} else {
						primaryStr = sf.Tag.Get("db_primary")
						if len(primaryStr) > 0 {
							if !dsc.idPresent {
								if fldTp.Kind() == reflect.Int64 {
									strListAppend(&selList, "rowid") // Warning: SQLite3ism
									dsc.sel.sfList = append(dsc.sel.sfList, sf)
									strListAppend(&dsc.sel.typeStrList, "%v", sf.Type.Kind())
									dsc.idSf = sf
									dsc.idPresent = true
								} else {
									db.SetErrorf("expecting int64 for id, got %v", fldTp.Kind())
								}
							} else {
								db.SetErrorf(`multiple occurrence of "db_primary" tag`)
							}
						}
					}
					if db.err == nil {
						tblStr = sf.Tag.Get("db_table")
						if len(tblStr) > 0 {
							if len(dsc.tblStr) == 0 {
								dsc.tblStr = tblStr
							} else {
								db.SetErrorf(`multiple occurrence of "db_table" tag`)
							}
						}
					}
				}
			}
			if db.err == nil {
				if len(dsc.insert.sfList) == 0 {
					db.SetErrorf(`no structure fields have "db" tag`)
				} else if len(dsc.tblStr) == 0 {
					db.SetErrorf(`missing "db_table" tag`)
				} else {
					dsc.insert.qmStr = strings.Join(qmList, ", ")
					dsc.insert.nameStr = strings.Join(dsc.insert.nameList, ", ")
					dsc.create.nameTypeStr = strings.Join(createList, ", ")
					dsc.sel.nameStr = strings.Join(selList, ", ")
					db.dscMap[recTp] = dsc // cache
					// dump(dsc)
				}
			}
		}
		if primaryKeyRequired && !dsc.idPresent {
			db.SetErrorf(`primary key is required but is not present in structure`)
		}
	} else {
		db.SetErrorf(`specified address must be of structure with ` +
			`one or more fields that have a "db" tag`)
	}
	return
}

// Function dsc collects meta information, for example field types and SQL
// names, from the passed-in record.
func (db *DbType) dscFromPtr(recPtr interface{}, primaryKeyRequired bool) (dsc dscType) {
	ptrVl := reflect.ValueOf(recPtr)
	kd := ptrVl.Kind()
	if kd == reflect.Ptr {
		return db.dscFromType(ptrVl.Elem().Type(), primaryKeyRequired)
	}
	db.SetErrorf("expecting record pointer, got %v", kd)
	return
}

func strListAppend(listPtr *[]string, fmtStr string, args ...interface{}) {
	*listPtr = append(*listPtr, fmt.Sprintf(fmtStr, args...))
}

// TableCreate creates a table and its associated indexes based strictly on the
// "db", "db_table", and "db_index" tags in the type definition of the
// specified record. The table and indexes are overwritten if they already
// exist.
func (db *DbType) TableCreate(recPtr interface{}) {
	if db.err != nil {
		return
	}
	// DROP TABLE IF EXISTS foo
	// DROP INDEX IF EXISTS fooID;
	// DROP INDEX IF EXISTS fooDate;
	// CREATE TABLE foo (num int32, name string)
	// CREATE INDEX fooID ON foo (rowid);
	// CREATE INDEX fooDate ON foo (Date);
	var dsc dscType
	// fmt.Printf("Trace 1 %v\n", db.trace)
	dsc = db.dscFromPtr(recPtr, false)
	if db.err == nil {
		// fmt.Printf("Trace 2 %v\n", db.trace)
		// Consider supporting flag that controls how existing table is handled
		// (function fail or table overwritten)
		// db.TransactBegin()
		if db.err == nil {
			// fmt.Printf("Trace 3 %v\n", db.trace)
			cmd := fmt.Sprintf("DROP TABLE IF EXISTS %s;", dsc.tblStr)
			_ = db.Exec(cmd)
			// fmt.Printf("Trace 4 %v\n", db.trace)
			for _, idx := range dsc.create.idxList {
				cmd = fmt.Sprintf("DROP INDEX IF EXISTS %s%s;", dsc.tblStr, idx.nameStr)
				_ = db.Exec(cmd)
			}
			if db.err == nil {
				cmd = fmt.Sprintf("CREATE TABLE %s (%s);", dsc.tblStr, dsc.create.nameTypeStr)
				// fmt.Printf("dbmap [%s]\n", cmd)
				_ = db.Exec(cmd)
				for _, idx := range dsc.create.idxList {
					cmd = fmt.Sprintf("CREATE INDEX %s%s ON %s (%s);",
						dsc.tblStr, idx.nameStr, dsc.tblStr, idx.fldStr)
					// fmt.Printf("dbmap [%s]\n", cmd)
					_ = db.Exec(cmd)
				}
				// fmt.Printf("%s\n", strIf(db.OK(), "OK", "Not OK"))
			}
		}
		// db.transactEnd(db.err == nil)
	}
	return
}

// Update updates the specified record in the database. The ID field (tagged
// with "db_table" in the structure definition) is used to identify the record
// in the table. It must have the same value as it had when the record was
// retrieved from the database using Retrieve. fldNames specify the fields that
// will be updated. The field names are the ones used in the database, that is,
// the names identified with the "db" tag in the structure definition. If the
// first string is "*", all fields are updated. Unmatched field names result in
// an error.
func (db *DbType) Update(recPtr interface{}, fldNames ...string) {
	if db.err != nil {
		return
	}
	// UPDATE foo SET name = ?, num = ? WHERE rowid = ?;
	if len(fldNames) > 0 {
		var dsc dscType
		dsc = db.dscFromPtr(recPtr, true)
		if db.err == nil {
			recVl := reflect.ValueOf(recPtr).Elem()
			addr := recVl.UnsafeAddr()
			var args []interface{}
			var eqList []string
			var sf reflect.StructField
			if fldNames[0] == "*" {
				fldNames = dsc.insert.nameList
			}
			pos := 0
			for _, nm := range fldNames {
				// fmt.Printf("sf.Name [%s], %v\n", sf.Name, fldMap[sf.Name])
				pos++
				sf = dsc.nameMap[nm]
				strListAppend(&eqList, "%s = ?", nm)
				args = append(args, reflect.Indirect(
					reflect.NewAt(sf.Type, unsafe.Pointer(addr+sf.Offset))).Interface())
			}
			args = append(args, reflect.Indirect(
				reflect.NewAt(dsc.idSf.Type, unsafe.Pointer(addr+dsc.idSf.Offset))).Interface())
			db.TransactBegin()
			if db.err == nil {
				cmd := fmt.Sprintf("UPDATE %s SET %s WHERE rowid = ?;", dsc.tblStr,
					strings.Join(eqList, ", "))
				// fmt.Println(cmd)
				_ = db.Exec(cmd, args...)
			}
			db.transactEnd(db.err == nil)
		}
	} else {
		db.SetErrorf("at least one field name expected in function Update")
	}
	return
}

// Delete removes all records from the database that satisfy the specified tail
// clause and its arguments. For example, if tailStr is empty, all records from
// the table will be deleted.
func (db *DbType) Delete(recPtr interface{}, tailStr string, prms ...interface{}) {
	if db.err != nil {
		return
	}
	// DELETE FROM foo WHERE a > ? AND b < ?
	var dsc dscType
	dsc = db.dscFromPtr(recPtr, false)
	if db.err == nil {
		db.TransactBegin()
		if db.err == nil {
			cmd := fmt.Sprintf("DELETE FROM %s%s;", dsc.tblStr, prePad(tailStr))
			_ = db.Exec(cmd, prms...)
		}
		db.transactEnd(db.err == nil)
	}
}

// Truncate removes all records from the table in the database associated with
// the specified record pointer.
func (db *DbType) Truncate(recPtr interface{}) {
	if db.err != nil {
		return
	}
	// DELETE FROM foo;
	var dsc dscType
	dsc = db.dscFromPtr(recPtr, false)
	if db.err == nil {
		db.TransactBegin()
		if db.err == nil {
			cmd := fmt.Sprintf("DELETE FROM %s;", dsc.tblStr)
			_ = db.Exec(cmd)
		}
		db.transactEnd(db.err == nil)
	}
}

// Insert stores in the database the records included in the specified slice.
// The value of the ID field that is tagged with "db_table" is ignored. After
// this function returns, the ID field of each inserted record will contain the
// indentifier assigned by the database.
func (db *DbType) Insert(slice interface{}) {
	if db.err != nil {
		return
	}
	var dsc dscType
	var id int64
	var res sql.Result
	var vList []interface{}
	sliceVl := reflect.ValueOf(slice)
	sliceTp := sliceVl.Type()
	if sliceTp.Kind() == reflect.Slice {
		count := sliceVl.Len()
		recTp := sliceTp.Elem()
		dsc = db.dscFromType(recTp, true)
		if db.err == nil {
			cmdStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
				dsc.tblStr, dsc.insert.nameStr, dsc.insert.qmStr)
			// fmt.Printf("dbmap [%s]\n", cmdStr)
			var idVal, recVl reflect.Value
			db.TransactBegin()
			for recJ := 0; recJ < count && db.err == nil; recJ++ { // Record loop
				recVl = sliceVl.Index(recJ)
				vList = valueInterfaceList(recVl, dsc.insert.sfList)
				res = db.Exec(cmdStr, vList...)
				if db.err == nil {
					id, _ = res.LastInsertId()
					idVal = reflect.Indirect(reflect.NewAt(dsc.idSf.Type,
						unsafe.Pointer(recVl.UnsafeAddr()+dsc.idSf.Offset)))
					idVal.SetInt(id)
				}
			}
			db.transactEnd(db.err == nil)
		}
	} else {
		db.SetErrorf("function Insert requires slice as first argument")
	}
}

// Retrieve selects zero or more records of the type pointed to by slicePtr
// from the database. The retrieved records are appended to the slice. If the
// retrieved records are to repopulate the slice instead, assign nil to the
// slice prior to calling this function. tailStr is intended to include a WHERE
// clause. For every parameter token ("?", "?", etc) in the string, a
// suitable expression list (one-based) after the tail string should be passed.
func (db *DbType) Retrieve(slicePtr interface{}, tailStr string, prms ...interface{}) {
	if db.err != nil {
		return
	}
	var dsc dscType
	slicePtrVl := reflect.ValueOf(slicePtr)
	kd := slicePtrVl.Kind()
	if kd == reflect.Ptr {
		sliceVl := reflect.Indirect(slicePtrVl)
		kd = sliceVl.Kind()
		if kd == reflect.Slice {
			sliceTp := sliceVl.Type()
			recTp := sliceTp.Elem()
			dsc = db.dscFromType(recTp, false)
			if db.err == nil {
				cmdStr := fmt.Sprintf("SELECT %s FROM %s%s;",
					dsc.sel.nameStr, dsc.tblStr, prePad(tailStr))
				var rows *sql.Rows
				rows = db.Query(cmdStr, prms...)
				if db.err == nil {
					recVl := reflect.Indirect(reflect.New(recTp)) // Buffer
					vList := valueInterfaceAddrList(recVl, dsc.sel.sfList)
					for rows.Next() {
						// fmt.Printf("Next %s\n", strIf(db.OK(), "OK", "Not OK"))
						if db.err == nil {
							db.err = rows.Scan(vList...)
							// fmt.Printf("Scan %s\n", strIf(db.OK(), "OK", "Not OK"))
							if db.err == nil {
								sliceVl = reflect.Append(sliceVl, recVl)
							}
						}
					}
					if db.err == nil {
						// Assign sliceVl back to *slicePtr
						reflect.Indirect(slicePtrVl).Set(sliceVl)
					}
				}
			}
		} else {
			db.SetErrorf("function Retrieve expecting pointer to slice, got pointer to %v", kd)

		}
	} else {
		db.SetErrorf("function Retrieve expecting pointer to slice, got %v", kd)
	}
	return
}
