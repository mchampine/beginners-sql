(ns honey.core
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :as helpers :refer [select from where join sset]]))

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "db/database.db"
   })

;; See the DB creation and examples in https://github.com/mchampine/beginners-sql/tree/master/jjdbc
;; query examples based on http://www.sohamkamani.com/blog/2016/07/07/a-beginners-guide-to-sql/


;; smoke test the database
(jdbc/query db "SELECT bookid AS id, title FROM books WHERE author='Dan Brown'")
;; => ({:id 2, :title "The Lost Symbol"} {:id 4, :title "Inferno"})

;; try it in honey
(def sqlmap {:select [[:bookid :id] :title]
             :from [:books]
             :where [:= :author "Dan Brown"]})

(sql/format sqlmap)
;; ["SELECT bookid AS id, title FROM books WHERE author = ?" "Dan Brown"]

(jdbc/query db (sql/format sqlmap))
;; => ({:id 2, :title "The Lost Symbol"} {:id 4, :title "Inferno"})  - same as above

;; util
(defn format-and-query [m] (jdbc/query db (sql/format m)))
(defn format-and-execute! [m] (jdbc/execute! db (sql/format m)))
(defn ppt [td] (clojure.pprint/print-table td))

;; honey helpers style
(-> (select [:bookid :id] :title)
    (from :books)
    (where [:= :author "Dan Brown"])
    format-and-query)
;; ({:id 2, :title "The Lost Symbol"} {:id 4, :title "Inferno"}) - same



;;;;;;;;;;;;; Translation of "THE BEGINNERS GUIDE TO SQL QUERIES" into HoneySQL ;;;;;;;;;;;;;;;;;;;;

;; dan brown books
(format-and-query
 {:select [[:bookid :id] :title]
  :from [:books]
  :where [:= :author "Dan Brown"]})
;; ["SELECT bookid AS id, title FROM books WHERE author = ?" "Dan Brown"] 
;; ({:id 2, :title "The Lost Symbol"} {:id 4, :title "Inferno"})


;; all borrowed books written by dan brown
(format-and-query
 {:select [[:books.title :title] [:borrowings.returndate :returnDate]]
  :from [:borrowings]
  :join [:books [:= :borrowings.bookid :books.bookid]]
  :where [:= :books.author "Dan Brown"]
  })
;; ["SELECT books.title AS title, borrowings.returndate AS returnDate FROM borrowings INNER JOIN books ON borrowings.bookid = books.bookid WHERE books.author = ?" "Dan Brown"]

;; => ({:title "The Lost Symbol", :returndate "03-23-2016"} {:title "Inferno", :returndate "04-13-2016"} {:title "The Lost Symbol", :returndate "04-19-2016"})


;; firstand last name of everyone who has borrowed a book by dan brown
(format-and-query ;sql/format
 {:select [[:members.firstname :FirstName] [:members.lastname :LastName]]
  :from [:borrowings]
  :join [:books   [:= :borrowings.bookid :books.bookid]
         :members [:= :members.memberid :borrowings.memberid]]
  :where [:= :books.author "Dan Brown"]
  })
;; ["SELECT members.firstname AS FirstName, members.lastname AS LastName FROM borrowings INNER JOIN books ON borrowings.bookid = books.bookid INNER JOIN members ON members.memberid = borrowings.memberid WHERE books.author = ?" "Dan Brown"] 

;; ({:firstname "Mike", :lastname "Willis"} {:firstname "Ellen", :lastname "Horton"} {:firstname "Ellen", :lastname "Horton"})
;; | :firstname | :lastname |
;; |------------+-----------|
;; |       Mike |    Willis |
;; |      Ellen |    Horton |
;; |      Ellen |    Horton |


;; dan brown books borrowed per member
(format-and-query ;sql/format
 {:select [[:members.firstname :FirstName]
           [:members.lastname :LastName]
           [:%count.* :Number_of_books_borrowed]]
  :from [:borrowings]
  :join [:books   [:= :borrowings.bookid :books.bookid]
         :members [:= :members.memberid :borrowings.memberid]]
  :where [:= :books.author "Dan Brown"]
  :group-by [:members.firstname :members.lastname]
  })
;; ["SELECT members.firstname AS FirstName, members.lastname AS LastName, count(*) AS Number_of_books_borrowed FROM borrowings INNER JOIN books ON borrowings.bookid = books.bookid INNER JOIN members ON members.memberid = borrowings.memberid WHERE books.author = ? GROUP BY members.firstname, members.lastname" "Dan Brown"]

;; ({:firstname "Ellen", :lastname "Horton", :number_of_books_borrowed 2}
;; {:firstname "Mike", :lastname "Willis", :number_of_books_borrowed 1})
;; | :firstname | :lastname | :number_of_books_borrowed |
;; |------------+-----------+---------------------------|
;; |      Ellen |    Horton |                         2 |
;; |       Mike |    Willis |                         1 |


;; total stock of all books written by each author
(format-and-query ;sql/format
 {:select [:author :%sum.stock]
  :from [:books]
  :group-by [:author]
  })
;; ["SELECT author, sum(stock) FROM books GROUP BY author"]
;; => ({:author "Dan Brown", :sum 6} {:author "John Green", :sum 3} {:author "Amish Tripathi", :sum 2} {:author "Robin Sharma", :sum 4})
;; |        :author | :sum(stock) |
;; |----------------+-------------|
;; | Amish Tripathi |           2 |
;; |      Dan Brown |           6 |
;; |     John Green |           3 |
;; |   Robin Sharma |           4 |


;; only the stock of books written by “Robin Sharma”
(format-and-query ;sql/format
 {:select [:*]
  :from
  [{:select [:author [:%sum.stock :sum]]
    :from [:books]
    :group-by [:author]}]
  :where [:= :author "Robin Sharma"]})
;; ["SELECT * FROM (SELECT author, sum(stock) AS sum FROM books GROUP BY author) WHERE author = ?" "Robin Sharma"]
;; ({:author "Robin Sharma", :sum 4})

;; titles and ids of all books written by an author, whose total stock of books is greater than 3
;; step 1, get authors with stock > 3.
(def step1q
  {:select [:author]
   :from
   [{:select [:author [:%sum.stock :ss]]
     :from [:books]
     :group-by [:author]}]
   :where [:> :ss 3]})

(format-and-query step1q)
;; ["SELECT author FROM (SELECT author, sum(stock) AS ss FROM books GROUP BY author) WHERE ss > ?" 3]
;; ({:author "Dan Brown"} {:author "Robin Sharma"})

;; step 2, get books and ids for the authors returned in step 1.
;; for sqlite must add "AS ss" to FROM and change WHERE sum to WHERE ss
(format-and-query
 {:select [:title :bookid]
  :from [:books]
  :where [:in :author step1q] })
;; ["SELECT title, bookid FROM books WHERE (author in (SELECT author FROM (SELECT author, sum(stock) AS ss FROM books GROUP BY author) WHERE ss > ?))" 3] 

;; => ({:title "The Lost Symbol", :bookid 2} {:title "Who Will Cry When You
;;Die?", :bookid 3} {:title "Inferno", :bookid 4})

;; |                     :title | :bookid |
;; |----------------------------+---------|
;; |            The Lost Symbol |       2 |
;; | Who Will Cry When You Die? |       3 |
;; |                    Inferno |       4 |


;; the above is equivalent to:
(jdbc/query db "SELECT title, bookid FROM books WHERE author IN ('Robin Sharma', 'Dan Brown')")
;; => ({:title "The Lost Symbol", :bookid 2} {:title "Who Will Cry When You Die?", :bookid 3} {:title "Inferno", :bookid 4})

;;;; Single Values

;; find all books having stock above the average stock of books present

;; step 1 average stock 
(def s1
  {:select [[:%avg.stock :avg]]
  :from [:books]})

(format-and-query s1)
;; ["SELECT avg(stock) AS avg FROM books"]
;; ({:avg(stock) 3.0})

(def sqo (format-and-query s1)) ;; ({:avg 3.0})

;; use s1 as subquery
(format-and-query
 {:select [:*]
  :from [:books]
  :where [:> :stock s1]})
;; ["SELECT * FROM books WHERE stock > (SELECT avg(stock) AS avg FROM books)"]
;; ({:bookid 3, :title "Who Will Cry When You Die?", :author "Robin Sharma", :published "06-15-2006", :stock 4})
;; | :bookid |                     :title |      :author | :published | :stock |
;; |---------+----------------------------+--------------+------------+--------|
;; |       3 | Who Will Cry When You Die? | Robin Sharma | 06-15-2006 |      4 |

;; same as
(format-and-query
 {:select [:*]
  :from [:books]
  :where [:> :stock (:avg (first sqo))]})
;; ["SELECT * FROM books WHERE stock > ?" 3.0]
;; ({:bookid 3, :title "Who Will Cry When You Die?", :author "Robin Sharma", :published "06-15-2006", :stock 4})


;; update stock of all dan brown books to 0
;; orig sql (jdbc/update! db :books {:stock 0} ["author='Dan Brown'"])  ;; clojure.jdbc way
;; or       (jdbc/execute! db "UPDATE books SET stock=0 WHERE author='Dan Brown'")

;; running this query fails on sqlite
(format-and-execute!
 {:update :books
  :set {:stock 0}
  :where [:= :author "Dan Brown"]})
;; ["UPDATE books SET stock = ? WHERE author = ?" 0 "Dan Brown"]
;; => (2)  ;; number of rows updated

;; check it
(format-and-query {:select [:*] :from [:books]})

;; notice :stock for Dan Brown now 0
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      0 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      0 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |


;; delete all dan brown books
;; orig sql (jdbc/delete! db :books ["author='Dan Brown'"]) ;; clojure.jdbc way
;; or       (jdbc/query db "DELETE from books WHERE author='Dan Brown'")
(format-and-execute!
 {:delete-from :books
  :where [:= :author "Dan Brown"]})
;; ["DELETE FROM books WHERE author = ?" "Dan Brown"]
;; => (2)  ;; number of rows deleted

;; check it
(format-and-query {:select [:*] :from [:books]})

;;;; Show query response as a table with (ppt *1)
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |

;; restore the deleted dan brown books
(format-and-execute!
 {:insert-into :books
  :columns [:bookid :title :author :published :stock]
  :values [[2 "The Lost Symbol" "Dan Brown" "07-22-2010" 3] 
           [4 "Inferno" "Dan Brown" "05-05-2014" 3]]})

;; check it
(format-and-query {:select [:*] :from [:books]})
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      3 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      3 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |


;; delete all the books
(format-and-execute!
 {:delete-from :books})
;; => [5]  5 rows deleted

;; insert  the entire books table
(format-and-execute!
 {:insert-into :books
  :columns [:bookid :title :author :published :stock]
  :values [[1 "Scion of Ikshvaku" "Amish Tripathi" "06-22-2015" 2]
           [2 "The Lost Symbol" "Dan Brown" "07-22-2010" 3] 
           [3 "Who Will Cry When You Die?" "Robin Sharma" "06-15-2006" 4] 
           [4 "Inferno" "Dan Brown" "05-05-2014" 3] 
           [5 "The Fault in our Stars" "John Green" "01-03-2015" 3]]})
;; ["INSERT INTO books (bookid, title, author, published, stock) VALUES (?, ?, ?, ?, ?)" 1 "Scion of Ikshvaku" "Amish Tripathi" "06-22-2015" 2]
;; etc.

;; or alternately
(sql/format
 {:insert-into :books
  :values [{:bookid 1 :title "Scion of Ikshvaku" :author "Amish Tripathi" :published "06-22-2015" :stock 2}
           {:bookid 2 :title "The Lost Symbol" :author "Dan Brown" :published "07-22-2010" :stock 3} 
           {:bookid 3 :title "Who Will Cry When You Die?" :author "Robin Sharma" :published "06-15-2006" :stock 4} 
           {:bookid 4 :title "Inferno" :author "Dan Brown" :published "05-05-2014" :stock 3} 
           {:bookid 5 :title "The Fault in our Stars" :author "John Green" :published "01-03-2015" :stock 3}]})

;;;; end 
