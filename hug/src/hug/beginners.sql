-- src/hug/beginners.sql
-- The Beginners Guide to SQL


-- A ":result" value of ":*" specifies a vector of records
-- (as hashmaps) will be returned
-- :name all-books :? :*
-- :doc Get all books
select * from books

-- :name books-by-author :? :*
-- :doc Get books written by a given author
SELECT bookid AS id, title FROM books WHERE author = :author


-- :name borrowed-books-by-author :? :*
-- :doc Get all borrowed book written by a given author
SELECT books.title AS title, borrowings.returndate AS ReturnDate
  FROM borrowings JOIN books ON borrowings.bookid=books.bookid
  WHERE books.author = :author


-- :name who-borrowed-an-author :? :*
-- :doc Everyone who has borrowed a book by a given author
SELECT members.firstname AS FirstName,members.lastname AS LastName
  FROM borrowings
  JOIN books ON borrowings.bookid=books.bookid
  JOIN members ON members.memberid=borrowings.memberid
  WHERE books.author = :author


-- :name book-borrowings-by-author :? :*
-- :doc how many books where borrowed of a given author
SELECT
members.firstname AS FirstName,
members.lastname AS LastName,
count(*) AS Number_of_books_borrowed
FROM borrowings
JOIN books ON borrowings.bookid=books.bookid
JOIN members ON members.memberid=borrowings.memberid
WHERE books.author = :author
GROUP BY members.firstname, members.lastname

-- :name stock-of-books-per-author :? :*
-- :doc stock of all books written by each author
SELECT author, sum(stock)
  FROM books
  GROUP BY author

-- :name stock-of-books-by-author  :? :*
-- :doc stock of books written by an author
SELECT *
  FROM (SELECT author, sum(stock)
    FROM books
    GROUP BY author) AS results
  WHERE author = :author


-- :name authors-with-stock-gt :? :raw
-- :doc authors w/ stock greater than :ct
SELECT author
FROM (SELECT author, sum(stock) AS ss
  FROM books
  GROUP BY author) AS results
WHERE ss > :ct

-- :name title-stock-authors-stock-gt :?
-- :doc title and stock for authors w stock >3
SELECT title, bookid
FROM books
WHERE author IN (SELECT author
  FROM (SELECT author, sum(stock) AS ss
  FROM books
  GROUP BY author) AS results
  WHERE ss > :ct)

-- :name title-stock-authors-in :?
-- :doc title and stock for authors w stock >3
SELECT title, bookid
FROM books
WHERE author
IN (:v*:names)

-- :snip authors-gt3-snip
SELECT author
FROM (SELECT author, sum(stock) AS ss
  FROM books
  GROUP BY author) AS results
WHERE ss > 3

-- :name title-stock-authors-subq-in :?
-- :doc title and stock for authors w stock >3
SELECT title, bookid
FROM books
WHERE author
IN
(:snip:where)

-- :name average-book-stock :?
-- :doc average book stock
select avg(stock) from books

-- :snip avgstk-snip
select avg(stock) from books

-- :name books-with-above-avg-stock :?
-- :doc books with above average stock
SELECT * FROM books
 WHERE stock>(:snip:where)

-- :name zero-stock-by-author :! :n
-- :doc set author books to zero
UPDATE books SET stock=0 WHERE author = :author

-- :name delete-stock-by-author :!
-- :doc delete an author's books
DELETE from books WHERE author = :author


-- :name insert-books :!
-- :doc insert books
INSERT INTO books
  (bookid,title,author,published,stock)
VALUES
  :tuple*:authors

-- :name delete-all-books :!
-- :doc delete-all-books
DELETE from books
