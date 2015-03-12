Package dbmap implements a simple, high-level wrapper for the database/sql
package. Currently, only sqlite3 (using github.com/mattn/go-sqlite3) has been
tested. Each table in the database is associated with an application-defined
structure in Go. These structures contain special tags that allow dbmap to
automatically manage basic database operations.

See the [documentation](http://godoc.org/code.google.com/p/dbmap) for more details.
