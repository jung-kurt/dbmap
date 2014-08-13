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
	// Output:
	// Aramis
	// Athos

See the "Structure field tags" section for an explanation of the field tags.

See the tutorials in the dbmap_test.go file (shown as examples in this
documentation) for other operations.

Errors

This package exposes two method receiver types, DscType and WrapType. Of the
two, DscType performs lower level functions and its methods return errors
directly. Instances of this type are safe for use by multiple goroutines.

The methods of WrapType hide the details of using DscType and facilitate error
management. Instances of this type are not safe for concurrent use. If an error
occurs in a WrapType method, an internal error field is set. After this
happens, WrapType method calls typically return without performing any
operations and the error state is retained. This error management scheme
simplifies database operations since individual method calls do not need to be
examined for failure; it is generally sufficient to wait until after the last
WrapType method is called in a function. For the same reason, if an error
occurs in the calling application during the database session, it may be
desirable for the application to transfer the error to the WrapType instance by
calling the SetError() method or the SetErrorf() method. At any time during the
life cycle of the WrapType instance, the error state can be determined with a
call to OK(). The error itself can be retrieved with a call to Err().

Structure field tags

This mapping utility depends on tags to associate a Go application structure
with a database table. The fields that will be managed by this package need to
be exported, that is, have names that begin with an upper case letter. One and
only one of these fields needs to have a tag named "db_table" whose value is
the name of the database table or view.

If updates or insertions will be performed with a structure, it needs to have a
"db_primary" tag. This tag identifies an int64 field that corresponds with the
unique record identifier maintained by the database.

If a managed field does not have a "db_primary" tag, it must have a "db" tag
that identifies the column name used in the database. If the tag value is an
asterisk, the field name itself will be used.

A field with an optional "db_index" tag will be indexed. The form of this tag
is a comma-separated list of key segments. Each key segment is made of a name
portion and an integer sequence. For example `db_index="name1, num2" indicates
that the tagged field will be the first field in the index named 'name', and
the second field in the index named 'num'. Even if a field is the only member
of an index, it requires an integer suffix. The integer sequences for segments
within a given key do not necessarily need to be sequential but they should not
be duplicated.

Limitations

This wrapper to database/sql does not currently support table alterations. It
does not directly support table joins but it can read database views (which in
turn can include joins).

*/
package dbmap
