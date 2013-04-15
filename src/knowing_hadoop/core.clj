(ns knowing-hadoop.core
  (:require [knowing-hadoop.rule :as rule])
  (:gen-class))

(defn -main [& args]
  (println @rule/rules))
