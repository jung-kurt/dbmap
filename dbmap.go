package dbmap

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

type tmType map[string]string

var typeMap = tmType{
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

type idxListType []idxType

type idxMapType map[string]idxListType

func (list *idxListType) append(nameStr, fldStr string) {
	*list = append(*list, idxType{nameStr, fldStr})
}

func (list idxListType) Len() int {
	return len(list)
}

func (list idxListType) Less(i, j int) bool {
	return list[i].nameStr < list[j].nameStr
}

func (list idxListType) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

type strListType []string

func (list *strListType) append(str string) {
	*list = append(*list, str)
}

func (list *strListType) appendf(fmtStr string, args ...interface{}) {
	list.append(fmt.Sprintf(fmtStr, args...))
}

func (list *strListType) join() string {
	return strings.Join(*list, ", ")
}

type sfListType []reflect.StructField

func (list *sfListType) append(sf reflect.StructField) {
	*list = append(*list, sf)
}

func prePad(str string) string {
	if len(str) > 0 {
		return " " + str
	}
	return str
}

// DscType contains meta information of a particular record structure. It
// facilitates the construction and organization of SQL calls. It is lock-free
// and is safe for concurrent use by goroutines. It is generally instantiated
// once (with a call to Describe() or MustDescribe()) and used for the duration
// of the program.
type DscType struct {
	// Name of database table
	tblStr string
	// Primary key is present in table
	idPresent bool
	// Descriptor for primary key if present
	idSf reflect.StructField
	// Record interface
	recTp reflect.Type
	// {"num":sfNum, "name":sfName, ...}
	nameMap map[string]reflect.StructField
	create  struct {
		// "num int32, name string, ..."
		nameTypeStr string
		// {{"fooID", "rowid"}, {"fooName", "Name"}, {"fooNum", "Num"}, ...}
		idxMap idxMapType
	}
	insert struct {
		// "num, name, ..."
		nameStr string
		// {"num", "name", ...}
		nameList strListType
		// "?, ?, ..."; one mark for each field
		qmStr  string
		sfList sfListType
	}
	sel struct {
		// "rowid, num, name, ..."
		nameStr string
		// Includes ID if present in structure
		sfList sfListType
		// {"int64", "bigint", "string", ...}
		typeStrList strListType
	}
}

var glIdxRe = regexp.MustCompile("^\\s*(\\D{1,})(\\S{1,})\\s*$")

// Tokenize index tag and store for later sorting and assembling
func processIndex(tagStr, fldStr string, idxMap map[string]idxListType) (err error) {
	// tagStr looks like ``, `db_index:"name1"` or `db_index:"loc5 name2"
	if len(tagStr) > 0 {
		var segList, pairList []string
		var sortStr string
		segList = strings.Split(tagStr, ",")
		for _, str := range segList {
			if err == nil {
				pairList = glIdxRe.FindStringSubmatch(str)
				if pairList != nil {
					sortStr = pairList[1]
					idxMap[sortStr] = append(idxMap[sortStr],
						idxType{nameStr: pairList[2], fldStr: fldStr})
				} else {
					err = fmt.Errorf("malformed index tag: %s", str)
				}
			}
		}
	}
	return
}

// describe collects meta information, for example field types and SQL
// names, from the passed-in record.
func describe(recTp reflect.Type) (dsc DscType, err error) {
	errorstr := func(str string) {
		err = errors.New(str)
	}
	errorf := func(fmtStr string, args ...interface{}) {
		err = fmt.Errorf(fmtStr, args...)
	}
	if recTp.Kind() == reflect.Struct {
		var typeOk bool
		dsc.recTp = recTp
		var sfList sfListType
		var primaryStr, sqlStr, tblStr, typeStr string
		var fldTp reflect.Type
		var selList, qmList, createList strListType
		dsc.create.idxMap = make(idxMapType)
		dsc.nameMap = make(map[string]reflect.StructField)
		for j := 0; j < recTp.NumField(); j++ {
			sfList.append(recTp.Field(j))
		}
		for _, sf := range sfList {
			if err == nil {
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
						createList.appendf("%s %s", sqlStr, typeStr)
						err = processIndex(sf.Tag.Get("db_index"), sf.Name, dsc.create.idxMap)
						if err == nil {
							dsc.insert.sfList.append(sf)
							dsc.insert.nameList.append(sqlStr)
							qmList.append("?")
							dsc.sel.typeStrList.append(typeStr)
							selList.append(sqlStr)
							dsc.sel.sfList.append(sf)
						}
					} else {
						errorf("database does not support fields of type %s", fldTp.String())
					}
				} else {
					primaryStr = sf.Tag.Get("db_primary")
					if len(primaryStr) > 0 {
						if !dsc.idPresent {
							if fldTp.Kind() == reflect.Int64 {
								selList.append("rowid") // Warning: SQLite3ism
								dsc.sel.sfList.append(sf)
								dsc.sel.typeStrList.appendf("%v", sf.Type.Kind())
								dsc.idSf = sf
								dsc.idPresent = true
							} else {
								errorf("expecting int64 for id, got %v", fldTp.Kind())
							}
						} else {
							errorstr(`multiple occurrence of "db_primary" tag`)
						}
					}
				}
				if err == nil {
					tblStr = sf.Tag.Get("db_table")
					if len(tblStr) > 0 {
						if len(dsc.tblStr) == 0 {
							dsc.tblStr = tblStr
						} else {
							errorstr(`multiple occurrence of "db_table" tag`)
						}
					}
				}
			}
		}
		if err == nil {
			if len(dsc.insert.sfList) == 0 {
				errorstr(`at least one exported structure field must have "db" tag`)
			} else if len(dsc.tblStr) == 0 {
				errorstr(`missing "db_table" tag`)
			} else {
				dsc.insert.qmStr = qmList.join()
				dsc.insert.nameStr = dsc.insert.nameList.join()
				dsc.create.nameTypeStr = createList.join()
				for _, v := range dsc.create.idxMap {
					sort.Sort(v)
					// fmt.Printf("%s %v\n", k, v)
				}
				dsc.sel.nameStr = selList.join()
				// dump(dsc)
			}
		}
	} else {
		errorstr(`specified address must be of structure with ` +
			`one or more fields that have a "db" tag`)
	}
	return
}

// SelectStr returns a command string suitable for retrieving records from the
// database table that is associated with the receiver. tailStr is any SQL that
// can follow the main select portion of the command. Parameters are indicated
// by a question mark and will be included, in the same order, in the call to
// SelectArg().
func (dsc DscType) SelectStr(tailStr string) string {
	return fmt.Sprintf("SELECT %s FROM %s%s;",
		dsc.sel.nameStr, dsc.tblStr, prePad(tailStr))
}

// SelectArg returns a slice of interface values, one for each table field,
// that can be expanded in an SQL query call. This function needs to be called
// once for each selected record variable. Consequently, this function can be
// called once with a record declared outside of a retrieval loop. Within the
// loop, calls to Scan() with the expanded interface slice will repeatedly
// update the same record.
func (dsc DscType) SelectArg(recPtr interface{}) (argList []interface{}, err error) {
	ptrVl := reflect.ValueOf(recPtr)
	kd := ptrVl.Kind()
	if kd == reflect.Ptr {
		recVl := ptrVl.Elem()
		if recVl.Type() == dsc.recTp {
			var sf reflect.StructField
			for _, sf = range dsc.sel.sfList {
				argList = append(argList, recVl.FieldByIndex(sf.Index).Addr().Interface())
			}
		} else {
			err = fmt.Errorf("passed in record (%s) for select does not match descriptor (%s)",
				recVl.Type().String(), dsc.recTp.String())
		}
	} else {
		err = errors.New("passed-in value must be a structure pointer")
	}
	return
}

// CreateStr returns a command string suitable for creating the database table
// that is associated with the receiver.
func (dsc DscType) CreateStr() (createStr string, idxStrList []string) {
	createStr = fmt.Sprintf("CREATE TABLE %s (%s);", dsc.tblStr, dsc.create.nameTypeStr)
	for k, v := range dsc.create.idxMap {
		var list strListType
		for _, idx := range v {
			list.append(idx.fldStr)
		}
		idxStrList = append(idxStrList, fmt.Sprintf("CREATE INDEX %s_%s ON %s (%s)",
			dsc.tblStr, k, dsc.tblStr, list.join()))
	}
	return
}

func (dsc DscType) updateNames(fldNames ...string) []string {
	if len(fldNames) == 0 {
		fldNames = dsc.insert.nameList
	} else if fldNames[0] == "*" {
		fldNames = dsc.insert.nameList
	}
	return fldNames
}

// UpdateStr returns a command string suitable for updating records into
// the table associated with the receiver.
func (dsc DscType) UpdateStr(fldNames ...string) string {
	fldNames = dsc.updateNames(fldNames...)
	var eqList strListType
	for _, nm := range fldNames {
		// fmt.Printf("sf.Name [%s], %v\n", sf.Name, fldMap[sf.Name])
		eqList.appendf("%s = ?", nm)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE rowid = ?;", dsc.tblStr, eqList.join())
}

// UpdateArg returns a slice of interface values that can be expanded in an SQL
// call. This function needs to be called once for each updated record. The
// passed-in val can be a properly tagged structure variable or a pointer to
// one. Additionally, the field names that are passed in must be the same ones,
// in the same order, as passed to UpdateStr(). The record to update is
// identified by the field tagged 'db_primary'. That field must contain the
// same identifier as it had when retrieved from the database.
func (dsc DscType) UpdateArg(rec interface{}, fldNames ...string) (argList []interface{}, err error) {
	if dsc.idPresent {
		vl := reflect.ValueOf(rec)
		if vl.Kind() == reflect.Ptr {
			vl = vl.Elem()
		}
		if vl.Type() == dsc.recTp {
			fldNames = dsc.updateNames(fldNames...)
			// var list sfListType
			var ok bool
			var sf reflect.StructField
			for _, nm := range fldNames {
				// See InsertArg for correct way of doing this
				if err == nil {
					// fmt.Printf("sf.Name [%s], %v\n", sf.Name, fldMap[sf.Name])
					sf, ok = dsc.nameMap[nm]
					if ok {
						argList = append(argList, vl.FieldByIndex(sf.Index).Interface())
						// list.append(sf)
					} else {
						err = fmt.Errorf("field name \"%s\" not in structure", nm)
					}
				}
			}
			if err == nil {
				argList = append(argList, vl.FieldByIndex(dsc.idSf.Index).Interface())
			}
		} else {
			err = fmt.Errorf("value passed into update must be a structure (or pointer to a structure) "+
				"of type %s", dsc.recTp.String())
		}
	} else {
		err = errors.New("update requires structure with primary ID")
	}
	return
}

// InsertStr returns a command string suitable for inserting new records into
// the table associated with the receiver.
func (dsc DscType) InsertStr() string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		dsc.tblStr, dsc.insert.nameStr, dsc.insert.qmStr)
}

// InsertOrReplaceStr returns a command string suitable for inserting (or
// replacing, if the insertion would violate a unique constraint) records into
// the table associated with the receiver.
func (dsc DscType) InsertOrReplaceStr() string {
	return fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s);",
		dsc.tblStr, dsc.insert.nameStr, dsc.insert.qmStr)
}

// InsertArg returns a slice of interface values that can be expanded in an SQL
// call. This function needs to be called once for each inserted record. rec
// can be a properly tagged structure variable or a pointer to one. If it is a
// pointer and the associated structure has an ID field tagged db_primary, this
// method also returns a function that can be called to set the record's ID
// field.
func (dsc DscType) InsertArg(rec interface{}) (argList []interface{}, setID func(int64), err error) {
	vl := reflect.ValueOf(rec)
	isPtr := vl.Kind() == reflect.Ptr
	if isPtr {
		vl = vl.Elem()
	}
	if vl.Type() == dsc.recTp {
		var sf reflect.StructField
		for _, sf = range dsc.insert.sfList {
			argList = append(argList, vl.FieldByIndex(sf.Index).Interface())
		}
		if dsc.idPresent && isPtr {
			vl = vl.FieldByIndex(dsc.idSf.Index)
			if vl.CanSet() {
				setID = func(id int64) {
					vl.SetInt(id)
				}
			}
		}

	} else {
		err = fmt.Errorf("value passed into insert must be a structure (or pointer to a structure) "+
			"of type %s", dsc.recTp.String())
	}
	return
}

// TruncateStr returns a command string that will remove all records from the
// table associated with the receiver.
func (dsc DscType) TruncateStr() string {
	return fmt.Sprintf("DELETE FROM %s;", dsc.tblStr)
}

// Describe generates a descriptor containing meta information of the passed-in
// record (or record pointed to by rec). See DscType for more information. An
// error occurs if the record stucture fails to meet the tag requirements as
// explained in the documentation.
func Describe(rec interface{}) (dsc DscType, err error) {
	vl := reflect.ValueOf(rec)
	kd := vl.Kind()
	if kd == reflect.Ptr {
		vl = vl.Elem()
	}
	dsc, err = describe(vl.Type())
	return
}

// MustDescribe calls Describe() and panics if an error occurs.
func MustDescribe(rec interface{}) (dsc DscType) {
	var err error
	dsc, err = Describe(rec)
	if err != nil {
		panic(err)
	}
	return
}

// String satisfies the fmt.Stringer interface and returns the library name
func (dsc *DscType) String() string {
	return "dbmap"
}
