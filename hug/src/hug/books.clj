(ns hug.books
  (:require [clojure.java.jdbc :as jdbc]
            [hugsql.core :as hugsql]))

;;; this module creates the books database.

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "db/books.db"
   })

;; path relative to src
(hugsql/def-db-fns "hug/books.sql")
(hugsql/def-sqlvec-fns "hug/books.sql")

;; create tables
;; (create-books-table-sqlvec)
(create-books-table db)

;; (create-members-table-sqlvec)
(create-members-table db)

;; (create-borrowings-table-sqlvec)
(create-borrowings-table db)

;; populate books table
(insert-books
 db
 {:books
  [[1 "Scion of Ikshvaku" "Amish Tripathi" "06-22-2015" 2]
   [2 "The Lost Symbol" "Dan Brown" "07-22-2010" 3]
   [3 "Who Will Cry When You Die?" "Robin Sharma" "06-15-2006" 4] 
   [4 "Inferno" "Dan Brown" "05-05-2014" 3]
   [5 "The Fault in our Stars" "John Green" "01-03-2015" 3]]})

;; populate members table
(insert-members
 db
 {:members
  [[1 "Sue"   "Mason" ]
   [2 "Ellen" "Horton"]
   [3 "Henry" "Clarke"]
   [4 "Mike"  "Willis"]
   [5 "Lida"  "Tyler" ]]})

;; populate borrowings table
(insert-borrowings
 db
 {:borrowings
  [[1 1 3 "01-20-2016" "03-17-2016"]
   [2 2 4 "01-19-2016" "03-23-2016"]
   [3 1 1 "02-17-2016" "05-18-2016"]
   [4 4 2 "12-15-2015" "04-13-2016"]
   [5 2 2 "02-18-2016" "04-19-2016"]
   [6 3 5 "02-29-2016" "04-11-2016"]]})

;; result

(jdbc/query db "select * from books")
;; (clojure.pprint/print-table *1)
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      3 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      3 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |

(jdbc/query db "select * from members")
;; (clojure.pprint/print-table *1)
;; | :memberid | :firstname | :lastname |
;; |-----------+------------+-----------|
;; |         1 |        Sue |     Mason |
;; |         2 |      Ellen |    Horton |
;; |         3 |      Henry |    Clarke |
;; |         4 |       Mike |    Willis |
;; |         5 |       Lida |     Tyler |

(jdbc/query db "select * from borrowings")
;; (clojure.pprint/print-table *1)
;; | :id | :bookid | :memberid | :borrowdate | :returndate |
;; |-----+---------+-----------+-------------+-------------|
;; |   1 |       1 |         3 |  01-20-2016 |  03-17-2016 |
;; |   2 |       2 |         4 |  01-19-2016 |  03-23-2016 |
;; |   3 |       1 |         1 |  02-17-2016 |  05-18-2016 |
;; |   4 |       4 |         2 |  12-15-2015 |  04-13-2016 |
;; |   5 |       2 |         2 |  02-18-2016 |  04-19-2016 |
;; |   6 |       3 |         5 |  02-29-2016 |  04-11-2016 |
