(ns dev
  (:require [clojure.tools.namespace.repl :refer [refresh refresh-all]]
            [com.stuartsierra.component :as component]
            [org.purefn.bridges.api :as kv]
            [org.purefn.kurosawa.health :as health]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.lebowski.core :as cb]))

(defn default-system
  []
  (let [config (assoc (cb/default-config "aws-couchbase")
                      ::cb/namespaces {"animals" {::cb/encoder :edn
                                                  ::cb/bucket "edn"
                                                  ::cb/key-sets "json"}
                                       "insects" {::cb/encoder :json
                                                  ::cb/bucket "json"} 
                                       "fruit" {::cb/encoder :nippy
                                                ::cb/bucket "nippy"}
                                       "insects-two" {::cb/encoder :json
                                                  ::cb/bucket "json"} 
                                       "fruit-two" {::cb/encoder :nippy
                                                ::cb/bucket "nippy"}
                                       "bytes" {::cb/encoder :binary
                                                ::cb/bucket "binary"}})
        ;;config (assoc-in config [::cb/bucket-passwords "json"] "foo")
        ]
;;    (println config)
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
  ;;(klog/set-level :debug)
  #_(klog/init-prod-logging system) ;; high-level to console, low-level to files.
  (start)
  :ready)

(defn reset
  "Stops the system, reloads modified source files, and restarts it."
  []
  (stop)
  (refresh :after `go))
