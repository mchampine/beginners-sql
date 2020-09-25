(ns nextjdbc.core
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbcsql]
            [nextjdbc.util :as util]))

(defmacro pt 
  "pretty prints the last thing output as a table. This is
  equivalent to (print-table *1)."
  [] `(clojure.pprint/print-table *1))

(def db {:dbtype "sqlite" :dbname "db/sqlitedb.db"})
(def ds (jdbc/get-datasource db))

;; create and populate the database if needed
;; (util/mkdb ds)
;; (util/wipedb ds)

;; smoke test db
(jdbc/execute-one! ds ["select author from books"])
;; => #:books{:author "Amish Tripathi"}


;; Queries from Beginner's Guide to SQL

;; 3. Simple Query

;; dan brown books
(jdbc/execute! ds ["SELECT bookid AS id, title FROM books WHERE author='Dan Brown'"])
;; => [#:books{:id 2, :title "The Lost Symbol"}
;;     #:books{:id 4, :title "Inferno"}]

;; 4. Joins

;; all borrowed books written by dan brown
(jdbc/execute! ds ["
SELECT books.title AS title, borrowings.returndate AS ReturnDate
FROM borrowings JOIN books 
ON borrowings.bookid=books.bookid
WHERE books.author='Dan Brown';"])
;;=>[{:books/title "The Lost Symbol", :borrowings/ReturnDate "03-23-2016"} {:books/title "Inferno", :borrowings/ReturnDate "04-13-2016"} {:books/title "The Lost Symbol", :borrowings/ReturnDate "04-19-2016"}]
;; |          :title | :returndate |
;; |-----------------+-------------|
;; | The Lost Symbol |  03-23-2016 |
;; |         Inferno |  04-13-2016 |
;; | The Lost Symbol |  04-19-2016 |

;; firstand last name of everyone who has borrowed a book by dan brown
(jdbc/execute! ds ["SELECT members.firstname AS FirstName,members.lastname AS LastName
FROM borrowings
JOIN books ON borrowings.bookid=books.bookid
JOIN members ON members.memberid=borrowings.memberid
WHERE books.author='Dan Brown'"])
;;=> [#:members{:FirstName "Mike", :LastName "Willis"} #:members{:FirstName "Ellen", :LastName "Horton"} #:members{:FirstName "Ellen", :LastName "Horton"}]
;; | :firstname | :lastname |
;; |------------+-----------|
;; |       Mike |    Willis |
;; |      Ellen |    Horton |
;; |      Ellen |    Horton |


;; 5. Aggregations

;; dan brown books borrowed per member
(jdbc/execute! ds ["SELECT
members.firstname AS FirstName,
members.lastname AS LastName,
count(*) AS Number_of_books_borrowed
FROM borrowings
JOIN books ON borrowings.bookid=books.bookid
JOIN members ON members.memberid=borrowings.memberid
WHERE books.author='Dan Brown'
GROUP BY members.firstname, members.lastname"])
;; [{:members/FirstName "Ellen", :members/LastName "Horton", :Number_of_books_borrowed 2} {:members/FirstName "Mike", :members/LastName "Willis", :Number_of_books_borrowed 1}]
;; | :firstname | :lastname | :number_of_books_borrowed |
;; |------------+-----------+---------------------------|
;; |      Ellen |    Horton |                         2 |
;; |       Mike |    Willis |                         1 |

(jdbc/execute! ds ["SELECT author, sum(stock)
FROM books
GROUP BY author"])
;;=> [{:books/author "Amish Tripathi", :sum(stock) 2} {:books/author "Dan Brown", :sum(stock) 6} {:books/author "John Green", :sum(stock) 3} {:books/author "Robin Sharma", :sum(stock) 4}]
;; |        :author | :sum |
;; |----------------+------|
;; | Amish Tripathi |    2 |
;; |      Dan Brown |    6 |
;; |     John Green |    3 |
;; |   Robin Sharma |    4 |

;; 6. Subqueries

(jdbc/execute! ds ["SELECT *
FROM (SELECT author, sum(stock)
  FROM books
  GROUP BY author) AS results
WHERE author='Robin Sharma'"])
;;=> [{:books/author "Robin Sharma", :sum(stock) 4}]

;; for sqlite must add "AS ss" to FROM and change WHERE sum to WHERE ss
(jdbc/execute! ds ["SELECT author
FROM (SELECT author, sum(stock) AS ss
  FROM books
  GROUP BY author) AS results
WHERE ss > 3"])
;;=> [#:books{:author "Dan Brown"} #:books{:author "Robin Sharma"}]


;; for sqlite must add "AS ss" to FROM and change WHERE sum to WHERE ss
(jdbc/execute! ds ["SELECT title, bookid
FROM books
WHERE author IN (SELECT author
  FROM (SELECT author, sum(stock) AS ss
  FROM books
  GROUP BY author) AS results
  WHERE ss > 3)"])
;; => [#:books{:title "The Lost Symbol", :bookid 2}
;;     #:books{:title "Who Will Cry When You Die?", :bookid 3}
;;     #:books{:title "Inferno", :bookid 4}]
;; |                     :title | :bookid |
;; |----------------------------+---------|
;; |            The Lost Symbol |       2 |
;; | Who Will Cry When You Die? |       3 |
;; |                    Inferno |       4 |

(jdbc/execute! ds ["SELECT title, bookid FROM books WHERE author IN ('Robin Sharma', 'Dan Brown')"])
;; => [#:books{:title "The Lost Symbol", :bookid 2}
;;     #:books{:title "Who Will Cry When You Die?", :bookid 3}
;;     #:books{:title "Inferno", :bookid 4}]
;; |                     :title | :bookid |
;; |----------------------------+---------|
;; |            The Lost Symbol |       2 |
;; | Who Will Cry When You Die? |       3 |
;; |                    Inferno |       4 |


(jdbc/execute! ds ["select avg(stock) from books"])
;;=> [{:avg(stock) 3.0}]

(jdbc/execute! ds  ["SELECT * FROM books WHERE stock>(SELECT avg(stock) FROM books)"])
;; => [#:books{:bookid 3,
;;             :title "Who Will Cry When You Die?",
;;             :author "Robin Sharma",
;;             :published "06-15-2006",
;;             :stock 4}]

;; which is equivalent to writing:
(jdbc/execute! ds ["SELECT * FROM books WHERE stock>3.000"])
;; => [#:books{:bookid 3,
;;             :title "Who Will Cry When You Die?",
;;             :author "Robin Sharma",
;;             :published "06-15-2006",
;;             :stock 4}]

;; 7. Write Operations

;; update stock
(jdbcsql/update! ds :books {:stock 0} ["author='Dan Brown'"])

;; check it
(jdbc/execute! ds ["SELECT * from books"])
;; | :books/bookid |               :books/title |  :books/author | :books/published | :books/stock |
;; |---------------+----------------------------+----------------+------------------+--------------|
;; |             1 |          Scion of Ikshvaku | Amish Tripathi |       06-22-2015 |            2 |
;; |             2 |            The Lost Symbol |      Dan Brown |       07-22-2010 |            0 |
;; |             3 | Who Will Cry When You Die? |   Robin Sharma |       06-15-2006 |            4 |
;; |             4 |                    Inferno |      Dan Brown |       05-05-2014 |            0 |
;; |             5 |     The Fault in our Stars |     John Green |       01-03-2015 |            3 |

;; delete
(jdbcsql/delete! db :books ["author='Dan Brown'"]) ;; cloj
;; check it
(jdbc/execute! ds ["SELECT * from books"])
;; | :books/bookid |               :books/title |  :books/author | :books/published | :books/stock |
;; |---------------+----------------------------+----------------+------------------+--------------|
;; |             1 |          Scion of Ikshvaku | Amish Tripathi |       06-22-2015 |            2 |
;; |             3 | Who Will Cry When You Die? |   Robin Sharma |       06-15-2006 |            4 |
;; |             5 |     The Fault in our Stars |     John Green |       01-03-2015 |            3 |

;; put the Dan Brown books back
(jdbcsql/insert-multi!
 db
 :books
 [:bookid :title :author :published :stock]
 [[2 "The Lost Symbol" "Dan Brown" "07-22-2010" 3]
  [4 "Inferno" "Dan Brown" "05-05-2014" 3]])

;; check it
(jdbc/execute! ds ["SELECT * from books"])
;; | :books/bookid |               :books/title |  :books/author | :books/published | :books/stock |
;; |---------------+----------------------------+----------------+------------------+--------------|
;; |             1 |          Scion of Ikshvaku | Amish Tripathi |       06-22-2015 |            2 |
;; |             2 |            The Lost Symbol |      Dan Brown |       07-22-2010 |            3 |
;; |             3 | Who Will Cry When You Die? |   Robin Sharma |       06-15-2006 |            4 |
;; |             4 |                    Inferno |      Dan Brown |       05-05-2014 |            3 |
;; |             5 |     The Fault in our Stars |     John Green |       01-03-2015 |            3 |


;; 8. Feedback

(jdbc/execute!
 ds
 ["SELECT members.firstname || ' ' || members.lastname AS 'Full Name'
    FROM borrowings
    JOIN members
    ON members.memberid=borrowings.memberid
    JOIN books
    ON books.bookid=borrowings.bookid
    WHERE borrowings.bookid IN 
     (SELECT bookid FROM books 
       WHERE stock>  (SELECT avg(stock) FROM books))
    GROUP BY members.firstname, members.lastname"])

;; => [{:Full Name "Lida Tyler"}]

;; end
