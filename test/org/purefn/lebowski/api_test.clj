(ns org.purefn.lebowski.api-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [taoensso.timbre :as log]
            [com.stuartsierra.component :as component]
            [clj-http.client :as client]
            [cemerick.url :as url]
            [taoensso.timbre :as log]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.kurosawa.result :refer :all]
            [org.purefn.kurosawa.k8s :as k8s]
            [org.purefn.bridges.api :refer :all]
            [org.purefn.lebowski.core :as cb]))

(stest/instrument
 [`set-create* `set-destroy* `set-add* `set-remove* `set-contents*
  `create* `delete* `lookup `store*
  `fetch* `destroy* `swap-in*
  `set-exists? `set-create `set-destroy `set-add `set-remove `set-contents
  `exists? `create `delete `lookup `store
  `fetch `destroy `swap-in])

;; TODO: Remove these `aws-overrides` when we fully move to k8s!
(def base-config
  (let [aws-overrides (when-not (k8s/kubernetes?)
                        {::cb/hosts [;;TODO
                                     ]})]
    (-> (cb/default-config)
        (dissoc ::cb/bucket-passwords ::cb/namespaces)
        (merge aws-overrides))))

;; TODO: Remove the non-k8s branch when we fully move to k8s!
(def admin-config
  (if (k8s/kubernetes?)
    (let [s (k8s/secrets "couchbase")]
      {:admin (get s "admin.username")
       :admin-password (get s "admin.password")})
    {:admin ;;FIXME
     :admin-password ;;FIXME
     }))

(klog/init-dev-logging)


;;------------------------------------------------------------------------------
;; Buckets. 
;;------------------------------------------------------------------------------

(def gen-bucket
  (gen/fmap (fn [[n pw]]
              (-> {:bucket-name n :bucket-password pw}
                  (merge admin-config)
                  (assoc :host (first (::cb/hosts base-config)))))
            (gen/tuple (s/gen cb/full-string?)
                       (s/gen cb/full-string?))))

(def gen-bucket-configs
  (let [f (fn [c suffix]
            (->> (str (get-in c [:bucket-name]) "-" suffix)
                 (assoc-in c [:bucket-name])))]
    (gen/bind (gen/vector gen-bucket 2)
              (fn [[e j]] (gen/return {:edn (f e "edn")
                                       :json (f j "json")})))))

(defn sample-bucket-configs
  []
  (->> (gen/sample gen-bucket-configs)
       (drop 5)
       (first)))

(defn create-bucket
  "Creates a test bucket on the Couchbase cluster based on the given 
   configuration.
   
   The map parameter should contain the following key/value pairs:
   - `:host` Fully qualified hostname of a Couchbase cluster node.
   - `:admin` The user name of the Couchbase administrator.
   - `:admin-password` The password of the Couchbase administrator.
   - `:bucket-name` The name of the created bucket.
   - `:bucket-password` The password for the created bucket."
  [{:keys [host admin admin-password bucket-name bucket-password]}]
  (log/info "Creating Bucket: " bucket-name)
  (client/post (str "http://" host ":8091/pools/default/buckets")
               {:basic-auth [admin admin-password]
                :content-type "application/x-www-form-urlencoded"
                :body (url/map->query {:name bucket-name
                                       :ramQuotaMB 128
                                       :authType "sasl"
                                       :saslPassword bucket-password
                                       :bucketType "couchbase"
                                       :replicaNumber 1})}))

(defn delete-bucket
  "Deletes an existing test bucket from the Couchbase cluster based on the 
   given configuration.
   
   The map parameter should contain the following key/value pairs:
   - `:host` Fully qualified hostname of a Couchbase cluster node.
   - `:admin` The user name of the Couchbase administrator.
   - `:admin-password` The password of the Couchbase administrator.
   - `:bucket-name` The name of the created bucket."
  [{:keys [host admin admin-password bucket-name]}]
  (client/delete (str "http://" host ":8091/pools/default/buckets/" bucket-name)
                 {:basic-auth [admin admin-password]}))


;;------------------------------------------------------------------------------
;; Components. 
;;------------------------------------------------------------------------------

(defn test-system
  [bucket-configs namespaces]
  (let [bs (->> bucket-configs
                (map (fn [[_ {:keys [:bucket-name :bucket-password]}]]
                       [bucket-name bucket-password]))
                (into {}))

        [edn-bucket json-bucket]
        (->> [:edn :json]
             (map (fn [k] (get-in bucket-configs [k :bucket-name]))))
        
        ns (->> namespaces
                (map (fn [[k n]]
                       [n {::cb/encoder k
                           ::cb/bucket (get-in bucket-configs [k :bucket-name])
                           ::cb/key-sets json-bucket}]))
                (into {}))
        config (-> base-config
                   (merge {::cb/bucket-passwords bs
                           ::cb/namespaces ns}))]
    (component/system-map
     :couch (cb/couchbase config))))
       

;;------------------------------------------------------------------------------
;; StringSetStore API.
;;------------------------------------------------------------------------------

(def gen-set-op
  "Generates a StringSetStore API operation."
  (let [snames (set (gen/sample (s/gen namespace?) 4))
        keys (set (gen/sample (s/gen key?) 15))]
    (gen/frequency
     [[1 (gen/tuple (gen/return :set-destroy)
                    (gen/elements snames))]
      [8 (gen/tuple (gen/return :set-create)
                    (gen/elements snames))]
      [32 (gen/tuple (gen/return :set-add)
                     (gen/elements snames)
                     (gen/elements keys))]
      [4 (gen/tuple (gen/return :set-remove)
                    (gen/elements snames)
                    (gen/elements keys))]
      [2 (gen/tuple (gen/elements #{:set-contents :set-exists?})
                    (gen/elements snames))]])))
                                  
(defn apply-set-op
  "Reduction function for the simple model of StringSetStore."
  [model [op sname key]]
  (let [s (get model sname)]
    (case op
      :set-destroy (dissoc model sname)
      :set-create  (if s
                     model
                     (assoc model sname #{}))
      :set-add (if s
                 (->> (conj s key)
                      (assoc model sname))
                 model)
      :set-remove (if s
                    (->> (disj s key)
                         (assoc model sname))
                    model)
      model)))

(defspec string-set-store
  1
  (prop/for-all [ops (gen/vector gen-set-op 1000)]
    (let [{:keys [json edn] :as bc} (sample-bucket-configs)]
      (log/info "BEGIN: StringSetStore Tests -----------------------------------")
      (create-bucket edn)
      (create-bucket json)
      (log/info "Pausing for 15 seconds to let buckets initialize...")
      (Thread/sleep 15000)
      (log/info "Starting Tests.")
      (let [bk-ns (->> ops
                       (filter #(= (first %) :set-add))
                       (map (comp (partial vector :edn) second))
                       (into #{}))
            system (-> (test-system bc bk-ns)
                       (component/start))
            cb (:couch system)
            model (reduce apply-set-op {} ops)]
        
        (doseq [[op sname key] ops]
          (let [res (case op
                      :set-destroy (set-destroy cb sname)
                      :set-create (set-create cb sname)
                      :set-add (set-add cb sname key)
                      :set-remove (set-remove cb sname key)
                      :set-contents (set-contents cb sname)
                      :set-exists? (set-exists? cb sname))]
            (log/info (format "(%s) = %s"
                              (->> [(name op) sname key]
                                   (filter some?)
                                   (clojure.string/join " "))
                              res))))
        
        (let [out (java.io.StringWriter.)]
          (doseq [[n m] model]
            (clojure.pprint/pprint {:set-name n
                                    :model-set m
                                    :couch-set (set-contents cb n)} out))
          (log/info (str "TEST RESULTS:\n" (.toString out))))
                
        (let [ok? (every? (fn [[n m]] (= m (set-contents cb n))) model)]
          (component/stop system)
          (delete-bucket edn)
          (delete-bucket json)
          (log/info "END: StringSetStore Tests -----------------------------------")
          ok?)))))

