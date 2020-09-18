-- src/hug/books.sql
-- Tables from The Beginners Guide to SQL


-- :name create-books-table :! :raw
-- :doc Create books table
-- could also use auto_increment and no bookid
create table books (
  bookid     integer       primary key,
  title      varchar(50),
  author     varchar(50),
  published  varchar(50),
  stock      integer
)

-- :name create-members-table :! :raw
-- :doc Create members table
create table members (
  memberid   integer       primary key,
  firstname  varchar(50),
  lastname   varchar(50)
)

-- :name create-borrowings-table :! :raw
-- :doc Create borrowings table
create table borrowings (
  id         integer      primary key,
  bookid     integer,
  memberid   integer,
  borrowdate varchar(50),
  returndate varchar(50)
)

-- :name insert-books :! :n
-- :doc Insert multiple books with :tuple* parameter type
insert into books (bookid, title, author, published, stock)
values :tuple*:books

-- :name insert-members :! :n
-- :doc Insert multiple members with :tuple* parameter type
insert into members (memberid, firstname, lastname)
values :tuple*:members

-- :name insert-borrowings :! :n
-- :doc Insert borrowings with :tuple* parameter type
insert into borrowings (id, bookid, memberid, borrowdate, returndate)
values :tuple*:borrowings
