(ns trykorma.beginner
  (:refer-clojure :exclude [update])
  (:require [clojure.java.jdbc :as jdbc]
            [korma.db :refer :all]
            [korma.core :refer :all])
  (:require [clojure.pprint
             :refer [pprint print-table]
             :rename {pprint pp print-table pt}]))

;; Note: Korma appears to be mostly abandoned.
;; No DB creation SQL is provided.
;; Implements queries for https://www.sohamkamani.com/blog/2016/07/07/a-beginners-guide-to-sql/ in Korma

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "db/database.db"
   })

(defdb prod db)
;; {:pool {:classname "org.sqlite.JDBC", :subprotocol "sqlite", :subname "db/database.db"}, :options {:naming {:keys #function[clojure.core/identity], :fields #function[clojure.core/identity]}, :delimiters ["\"" "\""], :alias-delimiter " AS ", :subprotocol "sqlite"}}

;; See the DB creation and examples in https://github.com/mchampine/beginners-sql/blob/master/jjdbc/src/jjdbc
;; query examples based on http://www.sohamkamani.com/blog/2016/07/07/a-beginners-guide-to-sql/

;; smoke test the database using jdbc
(jdbc/query db "SELECT bookid AS id, title FROM books WHERE author='Dan Brown'")
;; => ({:id 2, :title "The Lost Symbol"} {:id 4, :title "Inferno"})


;; getting off the ground
(defentity books (pk :bookid)) ;; or members or borrowings
(select books)  ;; == ("SELECT * FROM books")
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      3 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      3 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |


;;;;;;;;;;;;; Translation of "THE BEGINNERS GUIDE TO SQL QUERIES" into SQLKorma ;;;;;;;;;;;;;;;;;;;;

;;;;;;;; dan brown books ;;;;;;;;

;; (jdbc/query db "SELECT bookid AS id, title FROM books WHERE author='Dan Brown'")
(select books
        (fields [:bookid :id] :title)
        (where {:author "Dan Brown"}))
;; ({:id 2, :title "The Lost Symbol"} {:id 4, :title "Inferno"})

;; Note: use clojure.pprint/print-table (pt *1) to get a readable table of of the last result.
;; (pt *1)
;; | :id |          :title |
;; |-----+-----------------|
;; |   2 | The Lost Symbol |
;; |   4 |         Inferno |


;;;;;;;; all borrowed books written by dan brown ;;;;;;;;

;; (jdbc/query db "SELECT books.title AS title, borrowings.returndate AS ReturnDate
;; FROM borrowings JOIN books ON borrowings.bookid=books.bookid
;; WHERE books.author='Dan Brown';")

(defentity borrowings)
(select books
        (fields :title :borrowings.returndate)
        (join borrowings (= :borrowings.bookid :bookid))
        (where {:books.author "Dan Brown"}))

;; |          :title | :returndate |
;; |-----------------+-------------|
;; | The Lost Symbol |  03-23-2016 |
;; | The Lost Symbol |  04-19-2016 |
;; |         Inferno |  04-13-2016 |



;;;;;;;; first and last name of everyone who has borrowed a book by dan brown ;;;;;;;;

;; (jdbc/query db "SELECT members.firstname AS FirstName,members.lastname AS LastName
;; FROM borrowings
;; JOIN books ON borrowings.bookid=books.bookid
;; JOIN members ON members.memberid=borrowings.memberid
;; WHERE books.author='Dan Brown'")

(defentity members (pk :memberid))
(select borrowings
        (fields :members.firstname :members.lastname)
        (join books (= :bookid :books.bookid))
        (join members (= :memberid :members.memberid))
        (where {:books.author "Dan Brown"}))

;; | :firstname | :lastname |
;; |------------+-----------|
;; |      Ellen |    Horton |
;; |      Ellen |    Horton |
;; |       Mike |    Willis |



;;;;;;;; dan brown books borrowed per member ;;;;;;;;

;; (jdbc/query db "SELECT
;; members.firstname AS FirstName,
;; members.lastname AS LastName,
;; count(*) AS Number_of_books_borrowed
;; FROM borrowings
;; JOIN books ON borrowings.bookid=books.bookid
;; JOIN members ON members.memberid=borrowings.memberid
;; WHERE books.author='Dan Brown'
;; GROUP BY members.firstname, members.lastname")

;; Note: Use of (count *) disagrees with Korma doc, which specifies
;; (count :*) which doesn't work
(select borrowings
        (fields :members.firstname :members.lastname)
        (aggregate (count *) :members.firstname :members.lastname)
        (join books (= :bookid :books.bookid))
        (join members (= :memberid :members.memberid))
        (where {:books.author "Dan Brown"})
        (group :members.firstname :members.lastname))

;; | :firstname | :lastname | :members.firstname |
;; |------------+-----------+--------------------|
;; |       Mike |    Willis |                  1 |
;; |      Ellen |    Horton |                  2 |


;;;;;;;; total stock of books per author ;;;;;;;;

;; (jdbc/query db "SELECT author, sum(stock)
;; FROM books
;; GROUP BY author")

(select books
        (fields :author)
        (aggregate (sum :books.stock) :sum :stock)
        (group :author))

;; |        :author | :sum |
;; |----------------+------|
;; |      Dan Brown |    6 |
;; |     John Green |    3 |
;; | Amish Tripathi |    2 |
;; |   Robin Sharma |    4 |



;;;;;;;; stock of books by a given author ;;;;;;;;

;; (jdbc/query db "SELECT *
;; FROM (SELECT author, sum(stock)
;;   FROM books
;;   GROUP BY author) AS results
;; WHERE author='Robin Sharma'")

(select books
        (fields :author)
        (aggregate (sum :books.stock) :sum :author)
        (where {:books.author "Robin Sharma"}))

;; => ({:author "Robin Sharma", :sum 4})



;;;;;;;; title and ID of books per author where stock > 3 ;;;;;;;;

;; FIRST STEP, get authors
;; (jdbc/query db "SELECT author
;; FROM (SELECT author, sum(stock)
;;   FROM books
;;   GROUP BY author) AS results
;; WHERE sum > 3")

;; method one - define an entity for the subquery
(defentity subsel
  (table 
   (subselect books
           (fields :author)
           (aggregate (sum :books.stock) :sum :author))
   :ttab))

(select subsel
        (fields :author)
        (where (> :sum 3)))
;; ({:author "Dan Brown"} {:author "Robin Sharma"})

;; method 2 - nested subquery
(select
 [(subselect books
             (fields :author)
             (aggregate (sum :books.stock) :sum :author))
  :tabalias]
 (fields :author)
 (where (> :sum 3)))
;; ({:author "Dan Brown"} {:author "Robin Sharma"})

;; method 3 - gross hack
(let [ba (select books
                 (fields :author)
                 (aggregate (sum :books.stock) :sum :stock)
                 (group :author))]
  (map #(do {:author (:author %)}) (filter #(> (:sum %) 3) ba)))

;; => ({:author "Dan Brown"} {:author "Robin Sharma"})


;; SECOND STEP - get titles and IDs from result above

;; (jdbc/query db "SELECT title, bookid
;; FROM books
;; WHERE author IN (SELECT author
;;   FROM (SELECT author, sum(stock)
;;   FROM books
;;   GROUP BY author) AS results
;;   WHERE sum > 3)")

;; using subsel defenity
(select books
        (fields :title :bookid)
        (where {:author [in (subselect subsel
                                       (fields :author)
                                       (where (> :sum 3)))]}))

;; using subselect twice
(select books
        (fields :title :bookid)
        (where {:author [in (subselect [(subselect books
                                                   (fields :author)
                                                   (aggregate (sum :books.stock) :sum :author))
                                        :tabalias]
                                       (fields :author)
                                       (where (> :sum 3)))]}))

;; this is equivalent to writing
(select books
        (fields :title :bookid)
        (where {:author [in '("Robin Sharma" "Dan Brown")]}))

;; |                     :title | :bookid |
;; |----------------------------+---------|
;; |            The Lost Symbol |       2 |
;; | Who Will Cry When You Die? |       3 |
;; |                    Inferno |       4 |



;;;;;;;; Books that have above average stock ;;;;;;;;

;;(jdbc/query db "select avg(stock) from books")
(select books (aggregate (avg :stock) :avg))
;; ({:avg 3.0})

;;(jdbc/query db  "SELECT * FROM books WHERE stock>(SELECT avg(stock) FROM books)")
(select books
        (where {:stock [> (subselect books
                                     (aggregate (avg :stock) :avg))]}))

;; | :bookid |                     :title |      :author | :published | :stock |
;; |---------+----------------------------+--------------+------------+--------|
;; |       3 | Who Will Cry When You Die? | Robin Sharma | 06-15-2006 |      4 |

;; this is equivalent to writing

;;(jdbc/query db "SELECT * FROM books WHERE stock>3.000")
(select books (where (> :stock 3)))
;; ({:bookid 3, :title "Who Will Cry When You Die?", :author "Robin Sharma", :published "06-15-2006", :stock 4})



;;;;;;;; update Dan Brown stock to zero ;;;;;;;;

;;(jdbc/update! db :books {:stock 0} ["author='Dan Brown'"])  ;; clojure.jdbc way
;;(jdbc/query db "UPDATE books SET stock=0 WHERE author='Dan Brown'")

;;;;;;;; show Dan Brown Stock ;;;;;;;;
(select books (fields :author :stock :bookid) (where (= :author "Dan Brown")))
;; ({:author "Dan Brown", :stock 3, :bookid 2} {:author "Dan Brown", :stock 3, :bookid 4})

;;;;;;;; zero all DB stock ;;;;;;;;
(update books
        (set-fields {:stock 0})
        (where (= :author "Dan Brown")))

(select books)
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      0 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      0 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |

;;;;;;;; restore them ;;;;;;;;
(update books (set-fields {:stock 3}) (where (= :bookid 2)))
(update books (set-fields {:stock 3}) (where (= :bookid 4)))
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      3 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      3 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |

;;;;;;;; delete Dan Brown books ;;;;;;;;

;;(jdbc/delete! db :books ["author='Dan Brown'"]) ;; clojure.jdbc way
;;(jdbc/query db "]DELETE from books WHERE author='Dan Brown'")

(delete books (where (= :author "Dan Brown")))

;; check books table with (pt (select books))
;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |


;;;;;;;; insert books ;;;;;;;;
;; (jdbc/query db "INSERT INTO books
;;   (bookid,title,author,published,stock)
;; VALUES
;;   (2,'The Lost Symbol','Dan Brown','07-22-2010',3),
;;   (4,'Inferno','Dan Brown','05-05-2014',3),

;;;;;;;; restore dan brown books ;;;;;;;;

(insert books
        (values [{:bookid 2 :title "The Lost Symbol" :author "Dan Brown" :published "07-22-2010" :stock 3}
                 {:bookid 4 :title "Inferno"         :author "Dan Brown" :published "05-05-2014" :stock 3}]))

;; | :bookid |                     :title |        :author | :published | :stock |
;; |---------+----------------------------+----------------+------------+--------|
;; |       1 |          Scion of Ikshvaku | Amish Tripathi | 06-22-2015 |      2 |
;; |       2 |            The Lost Symbol |      Dan Brown | 07-22-2010 |      3 |
;; |       3 | Who Will Cry When You Die? |   Robin Sharma | 06-15-2006 |      4 |
;; |       4 |                    Inferno |      Dan Brown | 05-05-2014 |      3 |
;; |       5 |     The Fault in our Stars |     John Green | 01-03-2015 |      3 |



;;;;;;;; members who borrowed any book with a total stock that was above average. ;;;;;;;;

;; (jdbc/query db "SELECT 
;; members.firstname || ' ' || members.lastname AS "Full Name"
;; FROM borrowings
;; JOIN members
;; ON members.memberid=borrowings.memberid
;; JOIN books
;; ON books.bookid=borrowings.bookid
;; WHERE borrowings.bookid IN (SELECT bookid FROM books WHERE stock>  (SELECT avg(stock) FROM books)  )
;; GROUP BY members.firstname, members.lastname;")
(select borrowings
        (fields (raw "members.firstname || ' ' || members.lastname AS FullName"))
        (join members (= :memberid :members.memberid))
        (join books (= :books.bookid :borrowings.bookid))
        (where {:borrowings.bookid [in (subselect books
                                       (fields :bookid)
                                       (where (> :stock (subselect books (aggregate (avg :stock) :avg)))))]})
        (group :members.firstname :members.lastname))

;; ({:FullName "Lida Tyler"})

;;;;;;;; fin
