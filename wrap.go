package dbmap

import (
	"database/sql"
	"errors"
	"fmt"
)

type shareType struct {
	hnd    *sql.DB
	tx     *sql.Tx
	errVal error
}

// WrapType facilitates the use of DscType. Since it is not safe for concurrent
// use, it is intended for self-contained database interactions that take place
// within a function where the WrapType variable is local.
type WrapType struct {
	sharePtr *shareType
	dsc      DscType
	res      sql.Result
	insert   struct {
		st     *sql.Stmt
		idAddr interface{}
	}
	sel struct {
		rows *sql.Rows
		args []interface{}
	}
}

// String satisfies the fmt.Stringer interface and returns the wrapper name.
func (w *WrapType) String() string {
	return "dbmap/wrap"
}

// Tx returns the currently registered SQL transaction. This is nil if no
// transaction is active.
func (w *WrapType) Tx() *sql.Tx {
	return w.sharePtr.tx
}

// DB returns the registered database instance.
func (w *WrapType) DB() *sql.DB {
	return w.sharePtr.hnd
}

// Err returns the currently stored error value. This is nil if no error has occurred.
func (w *WrapType) Err() error {
	return w.sharePtr.errVal
}

// OK return true if no error has occurred, false otherwise.
func (w *WrapType) OK() bool {
	return w.sharePtr.errVal == nil
}

// Wrap instantiates a variable to assist with database activities. The
// execution of this method is constant-time and fast. The returned instance is
// not safe for concurrent use.
func (dsc DscType) Wrap(hnd *sql.DB) (w WrapType) {
	w.sharePtr = new(shareType)
	w.sharePtr.hnd = hnd
	w.dsc = dsc
	// Exercise some error paths for test coverage purposes
	if hnd == nil {
		_ = prePad("")
	}
	return
}

// WrapJoin instantiates a variable to assist with database activities. It is
// used when multiple WrapType instances need to share the database handle,
// transactions and error handling. The execution of this method is
// constant-time and fast. The returned instance is not safe for concurrent
// use.
func (dsc DscType) WrapJoin(masterWrap WrapType) (w WrapType) {
	w.sharePtr = masterWrap.sharePtr
	w.dsc = dsc
	return
}

// TransactionBegin start a database transaction.
func (w *WrapType) TransactionBegin() {
	if w.sharePtr.errVal == nil {
		if w.sharePtr.tx == nil {
			w.sharePtr.tx, w.sharePtr.errVal = w.sharePtr.hnd.Begin()
		} else {
			w.sharePtr.errVal = errors.New("nested transactions not supported")
		}
	}
}

func (w *WrapType) transactionEnd(commit bool) {
	if w.sharePtr.tx != nil {
		if commit {
			w.sharePtr.tx.Commit()
		} else {
			w.sharePtr.tx.Rollback()
		}
		w.sharePtr.tx = nil
	} else if w.sharePtr.errVal == nil {
		w.sharePtr.errVal = errors.New("no transaction to end")
	}
}

// TransactionEnd completes a database transaction. If no error has occurred,
// the the transaction is committed, otherwise rolled back.
func (w *WrapType) TransactionEnd() {
	w.transactionEnd(w.sharePtr.errVal == nil)
}

// TransactionCommit commits the pending transaction.
func (w *WrapType) TransactionCommit() {
	w.transactionEnd(true)
}

// TransactionRollback rolls back the pending transaction.
func (w *WrapType) TransactionRollback() {
	w.transactionEnd(false)
}

// Result returns the result of the most recent database operation that does
// not return rows. The return value can be used to retrieve the number of
// affected rows and the most recently inserted ID.
func (w *WrapType) Result() sql.Result {
	return w.res
}

// InsertClear prepares the wrap instance for calls to Insert().
func (w *WrapType) InsertClear() {
	w.insert.st = nil
}

// Insert adds the record pointed to by recPtr to the database. If a unique
// constraint is violated by the insertion and replace is true, the record will
// be relaced.
func (w *WrapType) insertOrReplace(recPtr interface{}, replace bool) {
	if w.sharePtr.errVal == nil {
		if w.insert.st == nil {
			var cmdStr string
			if replace {
				cmdStr = w.dsc.InsertOrReplaceStr()
			} else {
				cmdStr = w.dsc.InsertStr()
			}
			if w.sharePtr.tx == nil {
				w.insert.st, w.sharePtr.errVal = w.sharePtr.hnd.Prepare(cmdStr)
			} else {
				w.insert.st, w.sharePtr.errVal = w.sharePtr.tx.Prepare(cmdStr)
			}
		}
		if w.sharePtr.errVal == nil {
			if w.insert.st != nil {
				var args []interface{}
				var idFnc func(int64)
				args, idFnc, w.sharePtr.errVal = w.dsc.InsertArg(recPtr)
				if w.sharePtr.errVal == nil {
					w.res, w.sharePtr.errVal = w.insert.st.Exec(args...)
					if w.sharePtr.errVal == nil {
						if idFnc != nil {
							var id int64
							id, w.sharePtr.errVal = w.res.LastInsertId()
							if w.sharePtr.errVal == nil {
								idFnc(id)
							}
						}
					}
				}
			}
		}
	}
}

// Insert adds the record pointed to by recPtr to the database. If the record
// structure contains an ID field tagged with db_primary, this field will be
// assigned an identifier by the database.
func (w *WrapType) Insert(recPtr interface{}) {
	w.insertOrReplace(recPtr, false)
}

// InsertOrReplace adds the record pointed to by recPtr to the database. If the
// insertion would violate a unique constraint on the table, the record will be
// replaced. If the record structure contains an ID field tagged with
// db_primary, this field will be assigned an identifier by the database.
func (w *WrapType) InsertOrReplace(recPtr interface{}) {
	w.insertOrReplace(recPtr, true)
}

// Update stores the passed-in value to the database. rec must be a properly
// tagged structure variable or a pointer to one. The structure must be one
// that has an ID field tagged with db_primary. Furthermore, this field must
// hold a valid ID, typically the same value the record had when it was
// retrieved with a call to Select(). fldNames is a list of names of the
// particular tagged fields to update. If the first name is "*", or the list is
// entirely missing, all tagged fields are stored.
func (w *WrapType) Update(rec interface{}, fldNames ...string) {
	if w.sharePtr.errVal == nil {
		cmdStr := w.dsc.UpdateStr(fldNames...)
		var st *sql.Stmt
		if w.sharePtr.tx == nil {
			st, w.sharePtr.errVal = w.sharePtr.hnd.Prepare(cmdStr)
		} else {
			st, w.sharePtr.errVal = w.sharePtr.tx.Prepare(cmdStr)
		}
		if w.sharePtr.errVal == nil {
			var args []interface{}
			args, w.sharePtr.errVal = w.dsc.UpdateArg(rec, fldNames...)
			if w.sharePtr.errVal == nil {
				w.res, w.sharePtr.errVal = st.Exec(args...)
			}
		}
	}
}

// Create adds a new table and indexes of the type associated with the receiver.
func (w *WrapType) Create() {
	if w.sharePtr.errVal == nil {
		cmdStr, idxList := w.dsc.CreateStr()
		if w.sharePtr.tx == nil {
			_, w.sharePtr.errVal = w.sharePtr.hnd.Exec(cmdStr)
			for _, cmdStr = range idxList {
				if w.sharePtr.errVal == nil {
					_, w.sharePtr.errVal = w.sharePtr.hnd.Exec(cmdStr)
				}
			}
		} else {
			_, w.sharePtr.errVal = w.sharePtr.tx.Exec(cmdStr)
			for _, cmdStr = range idxList {
				if w.sharePtr.errVal == nil {
					_, w.sharePtr.errVal = w.sharePtr.tx.Exec(cmdStr)
				}
			}
		}
	}
}

// Delete removes database rows that satisfy the WHERE clause in tailStr. For
// each question mark in tailStr, there must be an appropriate parameter in the
// args list. If tailStr is empty and args not passed, all records in the table
// will be deleted.
func (w *WrapType) Delete(tailStr string, args ...interface{}) {
	if w.sharePtr.errVal == nil {
		cmdStr := fmt.Sprintf("DELETE FROM %s%s;", w.dsc.tblStr, prePad(tailStr))
		w.res, w.sharePtr.errVal = w.sharePtr.hnd.Exec(cmdStr, args...)
	}
}

// QueryRow submits a SELECT command to the database. recPtr must be a pointer
// to a properly tagged structure variable. tailStr contains the portion of the
// SELECT command that filters and orders the results. tailStr should be
// constructed so that at most one row is returned. This may involve including
// a LIMIT clause in it. For each question mark in tailStr, there must be an
// appropriate parameter in the args list. This command is self-contained; it
// is an error to use it in conjunction with Next().
func (w *WrapType) QueryRow(recPtr interface{}, tailStr string, args ...interface{}) {
	if w.sharePtr.errVal == nil {
		var fldList []interface{}
		fldList, w.sharePtr.errVal = w.dsc.SelectArg(recPtr)
		if w.sharePtr.errVal == nil {
			cmdStr := w.dsc.SelectStr(tailStr)
			var row *sql.Row
			if w.sharePtr.tx == nil {
				row = w.sharePtr.hnd.QueryRow(cmdStr, args...)
			} else {
				row = w.sharePtr.tx.QueryRow(cmdStr, args...)
			}
			w.sharePtr.errVal = row.Scan(fldList...)
		}
	}
}

// Query submits a SELECT command to the database. recPtr must be a pointer to
// a properly tagged structure variable. tailStr contains the portion of the
// SELECT command that filters and orders the results. For each question mark
// in tailStr, there must be an appropriate parameter in the args list. If
// tailStr is empty and args not passed, all records in the table will be
// selected. This command works in conjunction with Next().
func (w *WrapType) Query(recPtr interface{}, tailStr string, args ...interface{}) {
	if w.sharePtr.errVal == nil {
		w.sel.args, w.sharePtr.errVal = w.dsc.SelectArg(recPtr)
		if w.sharePtr.errVal == nil {
			cmdStr := w.dsc.SelectStr(tailStr)
			if w.sharePtr.tx == nil {
				w.sel.rows, w.sharePtr.errVal = w.sharePtr.hnd.Query(cmdStr, args...)
			} else {
				w.sel.rows, w.sharePtr.errVal = w.sharePtr.tx.Query(cmdStr, args...)
			}
		}
	}
}

// Next retrieves the next row in the result set generated with a call to
// Query(). Each row in turn is copied to the record variable pointed to the
// recPtr argument in Query(). This method should be called repeatedly until it
// returns false. This happens when there are no more rows to retrieve or an
// error occurs.
func (w *WrapType) Next() bool {
	if w.sharePtr.errVal == nil {
		if w.sel.args != nil {
			if w.sel.rows != nil {
				if w.sel.rows.Next() {
					w.sharePtr.errVal = w.sel.rows.Scan(w.sel.args...)
					if w.sharePtr.errVal == nil {
						return true
					}
				} else if w.sharePtr.errVal == nil {
					w.sharePtr.errVal = w.sel.rows.Err()
					w.sel.args = nil
					w.sel.rows = nil
				}
			}
		}
	}
	return false
}

// ClearError unsets the current error value.
func (w *WrapType) ClearError() {
	w.sharePtr.errVal = nil
}

// SetError sets an error to halt database calls. This may facilitate error
// handling by application. A value of nil for err is ignored; use ClearError()
// to unset the error condition. See also OK(), Err() and Error().
func (w *WrapType) SetError(err error) {
	if err != nil {
		w.sharePtr.errVal = err
	}
}

// SetErrorf sets the internal Db error with formatted text to halt database
// calls. This may facilitate error handling by application.
//
// See the documentation for printing in the standard fmt package for details
// about fmtStr and args.
func (w *WrapType) SetErrorf(fmtStr string, args ...interface{}) {
	if w.sharePtr.errVal == nil {
		w.sharePtr.errVal = fmt.Errorf(fmtStr, args...)
	}
}
