(ns jjdbc.core
  (:require [clojure.java.jdbc :as jdbc]
            [jjdbc.utils :refer :all]))

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "db/sqlitedb.db"})

;;;; NOTE                                                        ;;;;
;;;; The "Setup Database" section can be skipped if you use      ;;;;
;;;; the existing database file: db/sqlitedb.db                  ;;;;
;;;; In that case skip to "Queries from Beginner's Guide to SQL" ;;;;

;;; Setup Database ;;;

;; define books table
(create-table
 db
 :books
 [[:bookid :integer :primary :key]
  [:title "varchar(50)"]
  [:author "varchar(50)"]
  [:published "varchar(50)"]
  [:stock :int]])

;; members table
(create-table
 db
 :members
 [[:memberid :integer :primary :key]
  [:firstname "varchar(50)"]
  [:lastname "varchar(50)"]])

;; borrowings table
(create-table
 db
 :borrowings
 [[:id :integer :primary :key]
  [:bookid :int]
  [:memberid :int]
  [:borrowdate "varchar(50)"]
  [:returndate "varchar(50)"]])

;; populate the books table
(jdbc/insert-multi!
 db
 :books
 [:bookid :title :author :published :stock]
 [[1 "Scion of Ikshvaku" "Amish Tripathi" "06-22-2015" 2]
  [2 "The Lost Symbol" "Dan Brown" "07-22-2010" 3]
  [3 "Who Will Cry When You Die?" "Robin Sharma" "06-15-2006" 4] 
  [4 "Inferno" "Dan Brown" "05-05-2014" 3]
  [5 "The Fault in our Stars" "John Green" "01-03-2015" 3]])

;; Note, insert-multi! also accepts maps instead of vectors, e.g.
;; {:title "Scion of Ikshvaku" :author "Amish Tripathi" :published "06-22-2015" :stock 2}

;; populate members table
(jdbc/insert-multi!
 db
 :members
 [{:firstname "Sue"  :lastname "Mason"}
  {:firstname "Ellen" :lastname "Horton"}
  {:firstname "Henry" :lastname "Clarke"}
  {:firstname "Mike" :lastname "Willis"}
  {:firstname "Lida" :lastname "Tyler"}])

;; populate borrowings table
(jdbc/insert-multi!
 db
 :borrowings
 [{:bookid 1 :memberid 3 :borrowdate "01-20-2016" :returndate "03-17-2016"}
  {:bookid 2 :memberid 4 :borrowdate "01-19-2016" :returndate "03-23-2016"}
  {:bookid 1 :memberid 1 :borrowdate "02-17-2016" :returndate "05-18-2016"}
  {:bookid 4 :memberid 2 :borrowdate "12-15-2015" :returndate "04-13-2016"}
  {:bookid 2 :memberid 2 :borrowdate "02-18-2016" :returndate "04-19-2016"}
  {:bookid 3 :memberid 5 :borrowdate "02-29-2016" :returndate "04-11-2016"}])

;;; End Setup Database ;;;


;; Queries from Beginner's Guide to SQL

;; 3. Simple Query

;; dan brown books
(jdbc/query db "SELECT bookid AS id, title FROM books WHERE author='Dan Brown'")
;; => ({:id 2, :title "The Lost Symbol"} {:id 4, :title "Inferno"})

;; my util function pretty-query makes it easier to see the query results
;; From here forward it will be used to show the query results or updated table
;; (unless the results are very simple)
(pretty-query db "SELECT bookid AS id, title FROM books WHERE author='Dan Brown'")
;; | :id |          :title |
;; |-----+-----------------|
;; |   2 | The Lost Symbol |
;; |   4 |         Inferno |

;; 4. Joins

;; all borrowed books written by dan brown
(jdbc/query db "SELECT books.title AS title, borrowings.returndate AS ReturnDate
FROM borrowings JOIN books ON borrowings.bookid=books.bookid
WHERE books.author='Dan Brown';")
;; => ({:title "The Lost Symbol", :returndate "03-23-2016"} {:title "Inferno", :returndate "04-13-2016"} {:title "The Lost Symbol", :returndate "04-19-2016"})
;; |          :title | :returndate |
;; |-----------------+-------------|
;; | The Lost Symbol |  03-23-2016 |
;; |         Inferno |  04-13-2016 |
;; | The Lost Symbol |  04-19-2016 |

;; firstand last name of everyone who has borrowed a book by dan brown
(jdbc/query db "SELECT members.firstname AS FirstName,members.lastname AS LastName
FROM borrowings
JOIN books ON borrowings.bookid=books.bookid
JOIN members ON members.memberid=borrowings.memberid
WHERE books.author='Dan Brown'")
;; => ({:firstname "Ellen", :lastname "Horton"} {:firstname "Ellen", :lastname "Horton"} {:firstname "Mike", :lastname "Willis"})
;; | :firstname | :lastname |
;; |------------+-----------|
;; |      Ellen |    Horton |
;; |      Ellen |    Horton |
;; |       Mike |    Willis |

;; 5. Aggregations

;; dan brown books borrowed per member
(jdbc/query db "SELECT
members.firstname AS FirstName,
members.lastname AS LastName,
count(*) AS Number_of_books_borrowed
FROM borrowings
JOIN books ON borrowings.bookid=books.bookid
JOIN members ON members.memberid=borrowings.memberid
WHERE books.author='Dan Brown'
GROUP BY members.firstname, members.lastname")
;; => ({:firstname "Ellen", :lastname "Horton", :number_of_books_borrowed 2}
;; => {:firstname "Mike", :lastname "Willis", :number_of_books_borrowed 1})
;; | :firstname | :lastname | :number_of_books_borrowed |
;; |------------+-----------+---------------------------|
;; |      Ellen |    Horton |                         2 |
;; |       Mike |    Willis |                         1 |

(jdbc/query db "SELECT author, sum(stock)
FROM books
GROUP BY author")
;; => ({:author "Dan Brown", :sum 6} {:author "John Green", :sum 3} {:author "Amish Tripathi", :sum 2} {:author "Robin Sharma", :sum 4})
;; |        :author | :sum |
;; |----------------+------|
;; |      Dan Brown |    6 |
;; |     John Green |    3 |
;; | Amish Tripathi |    2 |
;; |   Robin Sharma |    4 |


;; 6. Subqueries

(jdbc/query db "SELECT *
FROM (SELECT author, sum(stock)
  FROM books
  GROUP BY author) AS results
WHERE author='Robin Sharma'")
;; => ({:author "Robin Sharma", :sum 4})

;; for sqlite must add "AS ss" to FROM and change WHERE sum to WHERE ss
(jdbc/query db "SELECT author
FROM (SELECT author, sum(stock) AS ss
  FROM books
  GROUP BY author) AS results
WHERE ss > 3")
;; => ({:author "Dan Brown"} {:author "Robin Sharma"})

;; for sqlite must add "AS ss" to FROM and change WHERE sum to WHERE ss
(jdbc/query db "SELECT title, bookid
FROM books
WHERE author IN (SELECT author
  FROM (SELECT author, sum(stock) AS ss
  FROM books
  GROUP BY author) AS results
  WHERE ss > 3)")
;; => ({:title "The Lost Symbol", :bookid 2} {:title "Who Will Cry When You
;;Die?", :bookid 3} {:title "Inferno", :bookid 4})
;; |                     :title | :bookid |
;; |----------------------------+---------|
;; |            The Lost Symbol |       2 |
;; | Who Will Cry When You Die? |       3 |
;; |                    Inferno |       4 |

(jdbc/query db "SELECT title, bookid FROM books WHERE author IN ('Robin Sharma', 'Dan Brown')")
;; => ({:title "The Lost Symbol", :bookid 2} {:title "Who Will Cry When You Die?", :bookid 3} {:title "Inferno", :bookid 4})
;; |                     :title | :bookid |
;; |----------------------------+---------|
;; |            The Lost Symbol |       2 |
;; | Who Will Cry When You Die? |       3 |
;; |                    Inferno |       4 |


(jdbc/query db "select avg(stock) from books")
;; ({:avg(stock) 3.0})

(jdbc/query db  "SELECT * FROM books WHERE stock>(SELECT avg(stock) FROM books)")
;; ({:bookid 3, :title "Who Will Cry When You Die?", :author "Robin Sharma", :published "06-15-2006, :stock 4})

;; which is equivalent to writing:
(jdbc/query db "SELECT * FROM books WHERE stock>3.000")
;; ({:bookid 3, :title "Who Will Cry When You Die?", :author "Robin Sharma", :published "06-15-2006", :stock 4})

;; 7. Write Operations

;; update stock
(jdbc/update! db :books {:stock 0} ["author='Dan Brown'"])  ;; clojure.jdbc way
(pretty-query db "SELECT * from books")
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      0 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      0 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |

;; delete
(jdbc/delete! db :books ["author='Dan Brown'"]) ;; clojure.jdbc way
(pretty-query db "SELECT * from books")
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |

;; put the Dan Brown books back
(jdbc/insert-multi!  ;;the clojure.java.jdbc way
 db
 :books
 [:bookid :title :author :published :stock]
 [[2 "The Lost Symbol" "Dan Brown" "07-22-2010" 3]
  [4 "Inferno" "Dan Brown" "05-05-2014" 3]])


(pretty-query db "SELECT * from books")
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      3 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      3 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |


;; 8. Feedback

(jdbc/query db
  "SELECT members.firstname || ' ' || members.lastname AS 'Full Name'
    FROM borrowings
    JOIN members
    ON members.memberid=borrowings.memberid
    JOIN books
    ON books.bookid=borrowings.bookid
    WHERE borrowings.bookid IN 
     (SELECT bookid FROM books 
       WHERE stock>  (SELECT avg(stock) FROM books))
    GROUP BY members.firstname, members.lastname")

;; ({:full name "Lida Tyler"})

;; end
