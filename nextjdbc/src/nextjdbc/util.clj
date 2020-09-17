(ns nextjdbc.util
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbcsql]))

(defn mkdb
  "create 3 tables and populate them"
  [tds]
  (do
      ;; make books table
      (jdbc/execute! tds ["
  create table books (
  bookid integer primary key,
  title varchar(50),
  author varchar(50),
  published varchar(50),
  stock int)"])

      ;; populate books table
      (jdbc/execute! tds ["
 insert into books(title,author,published,stock)
 values
 ('Scion of Ikshvaku', 'Amish Tripathi', '06-22-2015', 2),
 ('The Lost Symbol', 'Dan Brown', '07-22-2010', 3),
 ('Who Will Cry When You Die?', 'Robin Sharma', '06-15-2006', 4),
 ('Inferno', 'Dan Brown', '05-05-2014', 3),
 ('The Fault in our Stars', 'John Green', '01-03-2015', 3)"])

      ;; make members table
      (jdbc/execute! tds ["
  create table members (
  memberid integer primary key,
  firstname varchar(50),
  lastname varchar(50))"])

      ;; populate members table
      (jdbc/execute! tds ["
 insert into members(firstname,lastname)
 values
 ('Sue', 'Mason'),
 ('Ellen', 'Horton'),
 ('Henry', 'Clarke'),
 ('Mike', 'Willis'),
 ('Lida', 'Tyler')"])

      ;; make borrowings table
      (jdbc/execute! tds ["
  create table borrowings (
  id integer primary key,
  bookid int,
  memberid int,
  borrowdate varchar(50),
  returndate varchar(50))"])

      ;; populate borrowings table
      (jdbc/execute! tds ["
 insert into borrowings(bookid,memberid,borrowdate,returndate)
 values
  (1, 3, '01-20-2016', '03-17-2016'),
  (2, 4, '01-19-2016', '03-23-2016'),
  (1, 1, '02-17-2016', '05-18-2016'),
  (4, 2, '12-15-2015', '04-13-2016'),
  (2, 2, '02-18-2016', '04-19-2016'),
  (3, 5, '02-29-2016', '04-11-2016')"])
      ))
;; end mkdb

(defn wipedb
  "delete books db tables"
  [tds]
  (do
    (jdbc/execute! tds ["drop table books"])
    (jdbc/execute! tds ["drop table members"])
    (jdbc/execute! tds ["drop table borrowings"])))
