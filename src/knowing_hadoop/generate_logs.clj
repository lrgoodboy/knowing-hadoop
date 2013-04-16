(ns knowing-hadoop.generate-logs
  (:require [knowing-hadoop.util :as util])
  (:use [clj-time.core :only [plus secs year month day hour minute sec]]
        [clj-time.format :only [formatter unparse-local]]))

(defn gen-log [dt]
  (clojure.string/join "\t" [(hour dt)
                             (year dt)
                             (day dt)
                             (minute dt)
                             "-"
                             "180.0"
                             "-"
                             "-"
                             "www.anjuke.com"
                             "-"
                             "/sale/"
                             "200"
                             "-"
                             "-"
                             "---Baiduspider---"
                             "-"
                             "-"
                             "-"
                             "-"
                             (sec dt)
                             (month dt)]))

(defn -main [& args]
  (let [now (util/current-minute)
        filename (str "test_logs/access_log_"
                      (unparse-local (formatter "yyyyMMddhhmm") now)
                      "_log")]
    (with-open [wtr (clojure.java.io/writer filename)]
      (doseq [i (range 0 60)]
        (let [dt (plus now (secs i))]
          (.write wtr (str (gen-log dt) "\n")))))
    (println (slurp filename))))
