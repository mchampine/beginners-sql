# jjdbc

Stands for Java JDBC

This directory is a leiningen project that contains the clojure.java.jdbc version of examples for [A Beginner's Guide to SQL](https://www.sohamkamani.com/blog/2016/07/07/a-beginners-guide-to-sql/)

db/sqlitedb.db is a SQLite Database populated with all the data needed to run the example queries. You can use it as-is, or create your own by executing the create-table calls, then run the insert-multi! calls to populate it.

## Usage

Clone the repo and try out the queries in [src/jjdbc/core.clj](https://github.com/mchampine/beginners-sql/blob/master/jjdbc/src/jjdbc/core.clj)

## License

Copyright © 2020

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
