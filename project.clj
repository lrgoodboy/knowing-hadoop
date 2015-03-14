(defproject knowing-hadoop "0.1.0-SNAPSHOT"
  :description "Hadoop Job for Knowing."
  :url "http://knowing.corp.anjuke.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [clj-yaml "0.4.0"]
                 [com.netflix.curator/curator-framework "1.3.3"]
                 [org.clojure/data.json "0.2.2"]
                 [clj-time "0.5.0"]
                 [clojure-hadoop "1.4.4-ANJUKE"
                  :exclusions [org.apache.hadoop/hadoop-common
                               org.apache.hadoop/hadoop-client]]
                 [org.clojure/java.jdbc "0.2.3"]
                 [mysql/mysql-connector-java "5.1.24"]
                 [org.clojure/tools.logging "0.2.6"]]
  :mirrors {"central" {:name "nexus"
                        :url "http://10.20.8.31:8081/nexus/content/groups/public"}
            #"clojars" {:name "nexus"
                        :url "http://10.20.8.31:8081/nexus/content/groups/public"}}
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-common "2.3.0"]
                                  [org.apache.hadoop/hadoop-client "2.3.0"]]}}
  :plugins [[lein2-eclipse "2.0.0"]]
  :main knowing-hadoop.core
  :aot [knowing-hadoop.core])
