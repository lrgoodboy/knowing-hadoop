(ns knowing-hadoop.database-test
  (:use clojure.test
        knowing-hadoop.database))

(deftest get-db-spec-test
  (let [db-spec (get-db-spec)
        subname (:subname db-spec)
        matches (re-matches #"^//(.*?):(.*?)/(.*?)\?.*" subname)]
    (is (not (nil? (nth matches 2))))))

(deftest add-chartdata-daily!-test
  (let [result (add-chartdata-daily!
                 [{"f_ds_id" 0
                   "f_time" "2013-01-01"
                   "f_data" 100}
                  {"f_ds_id" 0
                   "f_time" "2013-01-02"
                   "f_data" 200}])]
    (is (= 2 (count result)))
    (is (pos? (:generated_key (first result))))
    (delete-chartdata-daily! (for [{v :generated_key} result] v))))
