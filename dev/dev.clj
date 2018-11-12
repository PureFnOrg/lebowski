(ns dev
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.namespace.repl :refer [refresh refresh-all]]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.lebowski.core :as cb]
            [org.purefn.bridges.api :as kv]))

(defn default-system
  []
  (let [config {::cb/hosts [;;TODO
                            ]
                ::cb/bucket-passwords {;;TODO
                                       }
                ::cb/namespaces {"animals" {::cb/encoder :edn
                                            ::cb/bucket "edn"
                                            ::cb/key-sets "json"}
                                 "fruit" {::cb/encoder :nippy
                                          ::cb/bucket "nippy"}
                                 "bytes" {::cb/encoder :binary
                                          ::cb/bucket "binary"}
                                 "jobseeker-profile" {::cb/encoder :nippy
                                                      ::cb/bucket  "nippy"}}
                ::cb/initial-delay-ms 2
                ::cb/busy-delay-ms 500
                ::cb/busy-retries 3
                ::cb/max-retries 10}]
    (component/system-map
     :couch (cb/couchbase config))))

(def system
  "A Var containing an object representing the application under
  development."
  nil)

(defn init
  "Constructs the current development system."
  []
  (alter-var-root #'system
    (constantly (default-system))))

(defn start
  "Starts the current development system."
  []
  (alter-var-root #'system component/start))

(defn stop
  "Shuts down and destroys the current development system."
  []
  (alter-var-root #'system
    (fn [s] (when s (component/stop s)))))

(defn go
  "Initializes and starts the system running."
  []
  (init)
  #_(klog/init-dev-logging)         ;; high-level and low-level to console.
  (klog/init-dev-logging system)    ;; high-level only.
  (klog/set-level :debug)
  #_(klog/init-prod-logging system) ;; high-level to console, low-level to files.
  (start)
  :ready)

(defn reset
  "Stops the system, reloads modified source files, and restarts it."
  []
  (stop)
  (refresh :after `go))
