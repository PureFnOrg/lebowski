(defproject org.purefn/lebowski "2.0.2"
  :description "A Couchbase implementation of the Bridges protocols."
  :url "https://github.com/PureFnOrg/lebowski"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.7.1"
  ;; :global-vars {*warn-on-reflection* true}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [com.stuartsierra/component "0.3.2"]
                 
                 [org.purefn/kurosawa.core "2.0.11"]
                 [org.purefn/kurosawa.log "2.0.11"]
                 [org.purefn/bridges "1.13.0"]

                 ;; silence noisy internal couchbase logs
                 [com.fzakaria/slf4j-timbre "0.3.5"]

                 [org.clojure/test.check "0.9.0"]
                 [com.gfredericks/test.chuck "0.2.7"]
                 [com.taoensso/timbre "4.10.0"]
                 [com.taoensso/nippy "2.13.0"]
                 [com.couchbase.client/java-client "2.4.5"]
                 [com.cemerick/url "0.1.1"]
                 [clj-http "3.5.0"]
                 [cheshire "5.7.1"]]
  
  :deploy-repositories
  [["releases" {:url "https://clojars.org/repo/" :creds :gpg}]]

   :profiles
  {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]]
         :jvm-opts ["-Xmx2g"]
         :source-paths ["dev"]
         :codeina {:sources ["src"]
                   :exclude [org.purefn.lebowski.version]
                   :reader :clojure
                   :target "doc/dist/latest/api"
                   :src-uri "http://github.com/PureFnOrg/lebowski/blob/master/"
                   :src-uri-prefix "#L"}
         :plugins [[funcool/codeina "0.4.0"
                    :exclusions [org.clojure/clojure]]
                   [lein-ancient "0.6.10"]]}}
    :aliases {"project-version" ["run" "-m" "org.purefn.lebowski.version"]})
