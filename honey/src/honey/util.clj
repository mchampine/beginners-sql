(ns honey.util
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]))


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
      (->> {:insert-into :books
            :values [{:bookid 1 :title "Scion of Ikshvaku" :author "Amish Tripathi"
                      :published "06-22-2015" :stock 2}
                     {:bookid 2 :title "The Lost Symbol" :author "Dan Brown"
                      :published "07-22-2010" :stock 3} 
                     {:bookid 3 :title "Who Will Cry When You Die?" :author "Robin Sharma"
                      :published "06-15-2006" :stock 4} 
                     {:bookid 4 :title "Inferno" :author "Dan Brown"
                      :published "05-05-2014" :stock 3} 
                     {:bookid 5 :title "The Fault in our Stars" :author "John Green"
                      :published "01-03-2015" :stock 3}]}
          sql/format
          (jdbc/execute! tds))
      
      ;; make members table
      (jdbc/execute! tds ["
       create table members (
       memberid integer primary key,
       firstname varchar(50),
       lastname varchar(50))"])
     
      ;; populate members table
      (->> {:insert-into :members
            :values [{:memberid 1 :firstname "Sue" :lastname "Mason"}
                     {:memberid 2 :firstname "Ellen" :lastname "Horton"}
                     {:memberid 3 :firstname "Henry" :lastname "Clarke"}
                     {:memberid 4 :firstname "Mike" :lastname "Willis"}
                     {:memberid 5 :firstname "Lida" :lastname "Tyler"}]}
           sql/format
          (jdbc/execute! tds))

      ;; make borrowings table
      (jdbc/execute! tds ["
       create table borrowings (
       id integer primary key,
       bookid int,
       memberid int,
       borrowdate varchar(50),
       returndate varchar(50))"])
     
      ;; populate borrowings table
      (->> {:insert-into :borrowings
            :values [{:id 1 :bookid 1 :memberid 3
                      :borrowdate "01-20-2016", :returndate "03-17-2016"}
                     {:id 2 :bookid 2 :memberid 4
                      :borrowdate "01-19-2016", :returndate "03-23-2016"}
                     {:id 3 :bookid 1 :memberid 1
                      :borrowdate "02-17-2016", :returndate "05-18-2016"}
                     {:id 4 :bookid 4 :memberid 2
                      :borrowdate "12-15-2015", :returndate "04-13-2016"}
                     {:id 5 :bookid 2 :memberid 2
                      :borrowdate "02-18-2016", :returndate "04-19-2016"}
                     {:id 6 :bookid 3 :memberid 5
                      :borrowdate "02-29-2016", :returndate "04-11-2016"}]}
           sql/format
          (jdbc/execute! tds))

      ))
;; end mkdb

(defn wipedb
  "delete books db tables"
  [tds]
  (do
    (jdbc/execute! tds ["drop table books"])
    (jdbc/execute! tds ["drop table members"])
    (jdbc/execute! tds ["drop table borrowings"])))
