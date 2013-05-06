(ns knowing-hadoop.core-test
  (:require [clj-time.core])
  (:use clojure.test
        knowing-hadoop.core))

(deftest parse-accesslog-path-test
  (is (= "test" (parse-accesslog-path "test" nil)))
  (is (= (clojure.string/join "," (for [h (range 24)] (format "/y=2013/m=04/d=01/h=%02d/" h)))
         (parse-accesslog-path "/y={yyyy}/m={MM}/d={dd}/h={HH}/" (clj-time.core/local-date 2013 4 1))))
  (is (= "/y=2013/m=04/d=01/h=10/"
         (parse-accesslog-path "/y={yyyy}/m={MM}/d={dd}/h=10/" (clj-time.core/local-date 2013 4 1)))))

(deftest parse-soj-path-test
  (is (= "soj/20130401/" (parse-soj-path "soj/{yyyy}{MM}{dd}/" (clj-time.core/local-date 2013 4 1)))))
