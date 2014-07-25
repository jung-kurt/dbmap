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

/*
Package dbmap implements a simple, high-level wrapper for the database/sql
package. Currently, only sqlite3 (using github.com/mattn/go-sqlite3) has been
tested. Each table in the database is associated with an application-defined
structure in Go. These structures contain special tags that allow dbmap to
automatically manage basic database operations.

License

dbmap is copyrighted by Kurt Jung and is released under the MIT License.

Installation

To install the package on your system, run

	go get code.google.com/p/dbmap

Later, to receive updates, run

	go get -u code.google.com/p/dbmap

Quick Start

The following Go code demonstrates the creation of a database, the creation of
a table within that database, and subsequent operations.

	type recType struct {
		ID   int64  `db_table:"rec"`
		Name string `db:"*" db_index:"*"`
	}
	db := DbCreate("data/simple.db")
	db.TableCreate(&recType{})
	db.Insert([]recType{{0, "Athos"}, {0, "Porthos"}, {0, "Aramis"}})
	var list []recType
	db.Retrieve(&list, "WHERE Name[0:1] == ?1", "A")
	for _, r := range list {
		fmt.Println(r.Name)
	}
	db.Close()

In this example, the field tag "db_table" identifies the name of the table in
which to store records of this type. It is associated with the int64 ID that is
generated by the database. The "db" tags identify application fields that will
be stored in the database. The tag value is the name to use in the database; an
asterisk indicates that the field name itself will be used. The Name field is
indexed for fast record selection by virtue of the "db_index". All managed
fields must be exported, that is, their names must begin with an uppercase
letter.

See the tutorials in the dbmap_test.go file (shown as examples in this
documentation) for other operations.

Errors

If an error occurs in a dbmap method, an internal error field is set. After this
occurs, dbmap method calls typically return without performing any operations and
the error state is retained. This error management scheme facilitates database
operations since individual method calls do not need to be examined for
failure; it is generally sufficient to wait until after Close() is called. For
the same reason, if an error occurs in the calling application during the
database session, it may be desirable for the application to transfer the error
to the dbmap instance by calling the SetError() method or the SetErrorf() method.
At any time during the life cycle of the dbmap instance, the error state can be
determined with a call to OK() or Err(). The error itself can be retrieved with
a call to Error().

Limitations

This wrapper to database/sql does not currently support table joins or table
alterations.

*/
package dbmap
