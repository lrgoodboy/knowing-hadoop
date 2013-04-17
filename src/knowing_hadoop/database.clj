(ns knowing-hadoop.database
  (:require [knowing-hadoop.util :as util]
            [clojure.java.jdbc :as sql]))

(defn get-db-spec []
  (let [db (util/get-config :database :knowing)]
    {:classname "com.mysql.jdbc.Driver"
     :subprotocol "mysql"
     :subname (format "//%s:%d/%s?useUnicode=true&characterEncoding=UTF-8"
                      (:host db) (:port db) (:database db))
     :user (:username db)
     :password (:password db)}))

(def db-spec (delay (get-db-spec)))

(defn add-chartdata-daily! [records]
  (sql/with-connection @db-spec
    (apply sql/insert-records "t_chartdata_daily" records)))

(defn delete-chartdata-daily! [keys]
  (sql/with-connection @db-spec
    (sql/delete-rows "t_chartdata_daily"
      (cons (str "id IN (" (clojure.string/join "," (repeat (count keys) "?")) ")")
            keys))))
