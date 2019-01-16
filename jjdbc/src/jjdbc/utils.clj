(ns jjdbc.utils
  (:require [clojure.java.jdbc :as jdbc]))

;; table create helper 
(defn create-table
  "db   - a database
   name - keyword name of table to be created
   cols - vector of kv pair vectors"
  [db name cols]
  (try
    (jdbc/db-do-commands
     db
     (jdbc/create-table-ddl name cols))
    (catch Exception e (println e))))

(defn drop-table
  "drop (delete) a table"
  [db name]
  (try
    (jdbc/db-do-commands
     db
     (jdbc/drop-table-ddl name))
    (catch Exception e (println e))))

;; utils
;(defn mkdt [s] (java.sql.Date/valueOf s)) ; for use with :published as :date

(defn pretty-query [db s]
  (let [r (jdbc/query db s)]
    (clojure.pprint/print-table r)))

(defn ptab
  "pretty print a whole table "
  [db t]
  (let [s (if (keyword t) (name t) t)]
    (pretty-query db (str "SELECT * FROM " s))))
