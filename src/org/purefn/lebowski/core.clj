(ns org.purefn.lebowski.core
  "A Couchbase implementation of all Bridges protocols."
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as str]
            [com.gfredericks.test.chuck.generators :refer [string-from-regex]]
            [com.stuartsierra.component :as component]
            [org.purefn.bridges.api :as api]
            [org.purefn.bridges.cache.api :as cache]
            [org.purefn.bridges.protocol :as proto]
            [org.purefn.kurosawa.error :as error]
            [org.purefn.kurosawa.health :as health]
            [org.purefn.kurosawa.k8s :as k8s]
            [org.purefn.kurosawa.log.api :as log-api]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.kurosawa.log.protocol :as log-proto]
            [org.purefn.kurosawa.result :refer :all]
            [org.purefn.kurosawa.transform :as xform]
            [org.purefn.kurosawa.util :as util]
            [org.purefn.lebowski.encoder.api :as encoder]
            [org.purefn.lebowski.encoder.binary-encoder :refer [binary-encoder]]
            [org.purefn.lebowski.encoder.edn-encoder :refer [edn-encoder]]
            [org.purefn.lebowski.encoder.json-encoder :refer [json-encoder]]
            [org.purefn.lebowski.encoder.nippy-encoder :refer [nippy-encoder]]
            [taoensso.timbre :as log])
  (:import [com.couchbase.client.core
            BackpressureException]
           [com.couchbase.client.core.env
            CoreEnvironment]
           [com.couchbase.client.java
            CouchbaseCluster Bucket]
           [com.couchbase.client.java.document
            AbstractDocument StringDocument
            JsonDocument JsonArrayDocument]
           [com.couchbase.client.java.document.json
            JsonObject JsonArray]
           [com.couchbase.client.java.env
            CouchbaseEnvironment DefaultCouchbaseEnvironment]
           [com.couchbase.client.java.error
            DocumentDoesNotExistException DocumentAlreadyExistsException
            TemporaryFailureException CASMismatchException]
           [com.couchbase.client.java.subdoc
            MutateInBuilder LookupInBuilder DocumentFragment]
           [java.util UUID]
           [java.util.concurrent TimeoutException]))


;;------------------------------------------------------------------------------
;; Error Helpers.
;;------------------------------------------------------------------------------

(defn- reason
  "The high-level reason for the failure."
  [ex]
  (cond
    (instance? clojure.lang.ExceptionInfo ex) (::api/reason (ex-data ex)
                                                            ::api/fatal)
    (instance? CASMismatchException ex) ::api/cas-mismatch
    (instance? DocumentDoesNotExistException ex) ::api/doc-missing
    (instance? DocumentAlreadyExistsException ex) ::api/doc-exists
    (instance? ArithmeticException ex) ::api/arithmetic
    (or (and (instance? RuntimeException ex)
             (instance? TimeoutException (.getCause ^Exception ex)))
        (instance? BackpressureException ex)
        (instance? TemporaryFailureException ex)) ::api/server-busy
    :default ::api/fatal))

(def ^:private snafu
  (partial error/snafu reason ::api/reason ::api/fatal))

(defn- retry-generic
  "Retry the supplied no-arg `f` until it succeeds or exhausts the number of 
   retries. 

   The function `f` will be retried when the reason for the failure is one
   of the keys in the `recovery` map.  The value for the matching key is a 
   function which will produce the next delay given the current one.
   
   Returns the result of `f`, or a non-recoverable `Failure`."
  [couch f recovery]
  (let [{:keys [::initial-delay-ms ::max-retries]} (:config couch)]        
    (error/retry-generic reason ::api/reason
                         initial-delay-ms max-retries f recovery)))

(defn- retry
  "Retry the supplied no-arg `f` when the server is busy or when a CAS token
   mismatches.
   
   Returns the result of `f`, or a non-recoverable `Failure`."
  [couch f]
  (let [{busy-delay-ms ::busy-delay-ms} (:config couch)
        recovery {::api/server-busy (partial + busy-delay-ms)
                  ::api/cas-mismatch (partial * 2)}]
    (retry-generic couch f recovery)))

(defn- robust
  "Retry the supplied no-arg `f` when the server is busy.
   
   Returns the result of `f`, or a non-recoverable `Failure`."
  [couch f]
  (let [{busy-delay-ms ::busy-delay-ms} (:config couch)
        recovery {::api/server-busy (partial + busy-delay-ms)}]
    (retry-generic couch f recovery)))

(defn- success-or-throw
  "Unwraps a result, throwing if `Failure`, and returning the underlying value
  if `Success`."
  [result]
  (if-let [f (failure result)]
    (throw (or (and (instance? Throwable f) f)
               (ex-info "Unknown failure" {:failure f})))
    (success result)))

;;------------------------------------------------------------------------------
;; Couchbase Helpers.
;;------------------------------------------------------------------------------

(defn- bucket*
  [couch namespace tag]
  (let [{:keys [config buckets]} couch
        {nss ::namespaces} config]
    (if-let [bck (->> (get-in nss [namespace tag])
                      (get buckets))]
      (succeed bck)
      (fail (ex-info "Unable to locate Couchbase bucket for namespace!"
                     {::api/namespace namespace
                      ::api/reason ::api/doc-missing})))))

(defn- ns-bucket*
  "The Couchbase Bucket object configured as the backing storage for the given
   namespace."
  ^Bucket [couch namespace]
  (bucket* couch namespace ::bucket))

(defn- set-bucket*
  "The Couchbase Bucket object configured as the backing storage for set data
   related to the given namespace."
  ^Bucket [couch namespace]
  (bucket* couch namespace ::key-sets))

(defn- has-set?
  "Whether a Couchbase Bucket object has been configured as the backing storage 
   for set data related to the given namespace."
  [couch namespace]
  (-> (set-bucket* couch namespace)
      success?))

(defn- ns-key
  "The combined namespace/key used as the actual key for a valued stored in a
   Couchbase bucket."
  [namespace key]
  (str namespace "/" key))

(defn- set-key
  "The combined set/name used as the actual key for a set stored in a Couchbase
   bucket."
  [name]
  (str "sets/" name))

(defn- encoder*
  "The CheckAndSetEncoder configured to be used when read/writing docs to the
   given namespace."
  [couch namespace]
  (if-let [enc (get-in couch [:config ::namespaces namespace ::encoder])]
    (succeed (get-in couch [:encoders enc]))
    (fail (ex-info "Improperly configured encoder for namespace!" 
                   {::api/namespace namespace}))))

;; couchbase expiration is rather odd, when the value is small
;; it represents offset, yet when larger, unix epoch -- so here
;; we will normalize to always use epoch
;; https://docs.couchbase.com/server/4.1/developer-guide/expiry.html#expiry-value

(defn- ttl-epoch
  "Convert ttl to epoch-ttl when provided offset in seconds, yet also
   accept ttl provided as epoch"
   [ttl]
   (let [unix-ts (quot (System/currentTimeMillis) 1000)
         offset (try (Math/toIntExact ttl) (catch Exception e nil))]
     (cond
       (nil? offset) nil
       (> offset unix-ts) offset
       :else (+ unix-ts offset))))

;;------------------------------------------------------------------------------
;; Java Interop Helper Functions. 
;;------------------------------------------------------------------------------

;; Environment
(defn- shutdown-env
  [^CouchbaseEnvironment env]
  (.shutdown env))


;; Cluster
(defn- disconnect-cluster
  [^CouchbaseCluster cluster]
  (.disconnect cluster))

(defn- open-bucket
  [^CouchbaseCluster cluster ^String bucket-name ^String password]
  (.openBucket cluster bucket-name password))


;; Bucket
(defn- get-doc
  [^Bucket bucket ^AbstractDocument doc]
  (.get bucket doc))

(defn- insert-doc
  [^Bucket bucket ^AbstractDocument doc]
  (.insert bucket doc))

(defn- replace-doc
  [^Bucket bucket ^AbstractDocument doc]
  (.replace bucket doc))

(defn- remove-doc
  [^Bucket bucket ^AbstractDocument doc]
  (.remove bucket doc))

(defn- remove-doc-named
  [^Bucket bucket ^String s]
  (.remove bucket s))

(defn- mutate-in
  [^Bucket bucket ^String s]
  (.mutateIn bucket s))

(defn- lookup-in
  [^Bucket bucket ^String s]
  (.lookupIn bucket s))


;; MutateInBuilder
(defn- add-members
  [^MutateInBuilder builder ^String value]
  (.arrayAppend builder "members" (str "+" value) true))

(defn- remove-members
  [^MutateInBuilder builder ^String value]
  (.arrayAppend builder "members" (str "-" value) true))

(defn- mutate-exec
  [^MutateInBuilder builder]
  (.execute builder))


;; LookupInBuilder
(defn- get-frag
  [^LookupInBuilder builder]
  (.get builder (into-array String ["members"])))

(defn- lookup-exec
  [^LookupInBuilder builder]
  (.execute builder))


;; JsonDocument
(defn- json-create
  [^String id ^JsonObject jobj ^Long cas]
  (JsonDocument/create id jobj cas))


;; DocumentFragment
(defn frag-content
  [^DocumentFragment frag]
  (if-let [c (.content frag "members")]
    (.toList ^JsonArray c)))

(defn- frag-cas
  [^DocumentFragment frag]
  (.cas frag))

;; Expiration
(defn- touch
  "Set the `ttl` for the document with key `id` for `expiry` seconds."
  [^Bucket bucket ^Integer expiry ^String id]
  (.touch bucket id expiry))

;;------------------------------------------------------------------------------
;; Component. 
;;------------------------------------------------------------------------------

(defrecord Couchbase
    [config cluster buckets encoders health-keys]
  
  component/Lifecycle
  (start [this]
    (let [{:keys [config env cluster]} this
          {:keys [::hosts ::bucket-passwords ::namespaces]} config]
      (log/info "Starting Couchbase.")
      (if (or env cluster)
        (do
          (log/warn "Couchbase was already started.")
          this)
        (let [nenv (DefaultCouchbaseEnvironment/create)
              hs ^java.util.List (apply list hosts)
              ncluster (CouchbaseCluster/create nenv hs)
              _ (->> hosts
                     (map #(log/info (str "Connected to: " %)))
                     (doall))
              f (fn [[name pw]]
                  (let [errmsg "Failed to open bucket of Couchbase!"
                        bucket
                        (-> (robust
                             this
                             #(-> (attempt open-bucket ncluster name pw)
                                  (recover (snafu errmsg
                                                  {::bucket-name name
                                                   ::bucket-password pw}))))
                            (success))]
                    (if bucket
                      (log/info
                       (str "Opened bucket: " name))
                      (log/warn
                       (format "Bucket [%s], could not be opened!" name)))
                    [name bucket]))
              buckets (->> bucket-passwords
                           (map f)
                           (into {}))
              encoders {:edn (edn-encoder)
                        :nippy (nippy-encoder)
                        :binary (binary-encoder)
                        :json (json-encoder)}
              health-keys (->> (vec namespaces)
                               (xform/distinct-by (comp ::bucket val))
                               (map (juxt first
                                          (constantly (str (UUID/randomUUID))))))]

          (doseq [[nname {:keys [::bucket ::key-sets]}] namespaces]
            (let [bs (->> [bucket key-sets]
                          (filter some?)
                          (map (fn [bname]
                                 [bname (get buckets bname)])))]
              (if (every? second bs)
                (log/info
                 (str "Validated namespace: " nname))
                (do
                  (doseq [[bn bk] bs]
                    (when-not bk
                      (log/warn
                       (format
                        "Namespace [%s] referenced an invalid bucket [%s]!"
                        nname bn))))))))
          
          (log/info "Couchbase started.")
          (assoc this
                 :env nenv
                 :health-keys health-keys
                 :cluster ncluster
                 :buckets buckets
                 :encoders encoders)))))
  
  (stop [this]
    (let [{:keys [env cluster]} this]
      (log/info "Stopping Couchbase.")
      (if (and (nil? env)
               (nil? cluster))
        (do
          (log/warn "Couchbase was already stopped.")
          this)
        
        (do
          (when-not
              (-> (attempt disconnect-cluster cluster)
                  (recover (snafu "Failed to disconnect cluster from Couchbase!"
                                  {::env env ::cluster cluster}))
                  (success))
            (log/error "Unable to disconnect from Couchbase cluster!"))
          
          (when-not
              (-> (attempt shutdown-env env)
                  (recover (snafu "Failed to shutdown environment of Couchbase!"
                                  {::env env}))
                  (success))
            (log/error "Unable to shutdown the Couchbase environment!"))
          
          (log/info "Couchbase stopped.")
          (assoc this
                 :env nil
                 :cluster nil
                 :buckets nil)))))

  
  ;;----------------------------------------------------------------------------
  log-proto/Logging
  (log-namespaces [_]
    ["com.couchbase.client.*"])
  
  (log-configure [this dir]
    (klog/add-component-appender :couchbase (log-api/log-namespaces this)
                                 (str dir "/couchbase.log")))

  
  ;;----------------------------------------------------------------------------
  proto/UnsafeStringSetStore
  (set-create* [this name]
    (klog/fn-trace :set-create* {:name name})
    (-> (set-bucket* this name)
        (proceed insert-doc
                 (JsonDocument/create (set-key name) (JsonObject/create)))
        (branch (snafu "Failed to create set in Couchbase!"
                       {::api/set-name name})
                some?)))

  (set-destroy* [this name]
    (klog/fn-trace :set-destroy* {:name name})
    (-> (set-bucket* this name)
        (proceed remove-doc-named (set-key name))
        (branch (snafu "Failed to destroy set from Couchbase!"
                       {::api/set-name name})
                some?)))
  
  (set-add* [this name value]
    (klog/fn-trace :set-add* {:name name :value value})
    (-> (set-bucket* this name)
        (proceed mutate-in (set-key name))
        (proceed add-members value)
        (proceed mutate-exec)
        (branch (snafu "Failed to add a value to set in Couchbase!"
                       {::api/set-name name
                        ::api/set-value value})
                some?)))
  
  (set-remove* [this name value]
    (klog/fn-trace :set-remove* {:name name :value value})
    (-> (set-bucket* this name)
        (proceed mutate-in (set-key name))
        (proceed remove-members value)
        (proceed mutate-exec)
        (branch (snafu "Failed to remove a value from set in Couchbase!"
                       {::api/set-name name
                        ::api/set-value value})
                some?)))
    
  (set-contents* [this name]
    (klog/fn-trace :set-contents* {:name name})
    (let [bck (set-bucket* this name)
          ^String key (set-key name)
          frag (-> bck
                   (proceed lookup-in key)
                   (proceed get-frag)
                   (proceed lookup-exec))
          strip (fn [s] (str/join (drop 1 s)))
          replay (partial reduce
                          (fn [s e]
                            (cond
                              (str/starts-with? e "+") (conj s (strip e))
                              (str/starts-with? e "-") (disj s (strip e))
                              :else s))
                          #{})
          finish (fn [compact]
                   (let [jary (reduce (fn [^JsonArray a e] (.add a (str "+" e)))
                                      (JsonArray/empty) compact)
                         jobj (-> (JsonObject/create)
                                  (.put "members" jary))]
                     (-> (proceed frag frag-cas)
                         (proceed (partial json-create key jobj))
                         (proceed-all (fn [d b] (replace-doc b d)) bck))
                     compact))]                         
      (-> (proceed frag frag-content)
          (proceed replay)
          (branch (snafu "Failed to retrieve contents of set from Couchbase!"
                         {::api/name name})
                  finish))))

  
  ;;----------------------------------------------------------------------------
  proto/StringSetStore
  (set-exists? [this name]
    (klog/fn-trace :set-exists? {:name name})
    (-> (robust
         this
         #(-> (set-bucket* this name)
              (proceed get-doc
                       (encoder/named (get-in this [:encoders :json])
                                      (set-key name)))
              (branch (snafu "Failed to determine if set exists in Couchbase!"
                             {::api/set-name name
                              ::api/key key})
                      some?)))
        (success)))
  
  (set-create [this name]
    (klog/fn-trace :set-create {:name name})
    (-> (robust this #(proto/set-create* this name))
        (success?)))
  
  (set-destroy [this name]
    (klog/fn-trace :set-destroy {:name name})
    (-> (robust this #(proto/set-destroy* this name))
        (success?)))

  (set-add [this name value]
    (klog/fn-trace :set-add {:name name :value value})
    (-> (robust this #(proto/set-add* this name value))
        (success?)))

  (set-remove [this name value]
    (klog/fn-trace :set-remove {:name name :value value})
    (-> (robust this #(proto/set-remove* this name value))
        (success?)))

  (set-contents [this name]
    (klog/fn-trace :set-contents {:name name})
    (-> (robust this #(proto/set-contents* this name))
        (success)))

  
  ;;----------------------------------------------------------------------------
  proto/UnsafeCheckAndSetStore 
  (create* [this namespace key value]
    (klog/fn-trace :create* {:namespace namespace :key key :value value})
    (let [enc (encoder* this namespace)
          doc (proceed enc encoder/encode (ns-key namespace key) value)
          result (-> (ns-bucket* this namespace)
                     (proceed-all insert-doc doc)
                     (proceed-all (util/flip encoder/decode) enc)
                     (recover (snafu "Failed to create value in Couchbase!"
                                     {:namespace namespace
                                      :key key
                                      :value value})))]
      (when (and (success? result)
                 (has-set? this namespace))
        (robust this
                #(-> (if (proto/set-exists? this namespace)
                       (succeed true)
                       (proto/set-create* this namespace))
                     (proceed (fn [_] (proto/set-add this namespace key))))))
      result))
    
  (delete* [this namespace key cas]
    (klog/fn-trace :delete* {:namespace namespace :key key :cas cas})
    (let [doc (-> (encoder* this namespace)
                  (proceed encoder/versioned (ns-key namespace key) cas))
          result (-> (ns-bucket* this namespace)
                     (proceed-all remove-doc doc)
                     (branch (snafu "Failed to delete value from Couchbase!"
                                    {:namespace namespace
                                     :key key
                                     :cas cas})
                             some?))]
      (when (and (success? result)
                 (has-set? this namespace)
                 (proto/set-exists? this namespace))
        (retry this
               #(proto/set-remove* this namespace key)))
      result))

  (lookup* [this namespace key]
    (klog/fn-trace :lookup* {:namespace namespace :key key})
    (let [enc (encoder* this namespace)
          doc (proceed enc encoder/named (ns-key namespace key))]
      (-> (ns-bucket* this namespace)
          (proceed-all get-doc doc)
          (proceed-all (util/flip encoder/decode) enc)
          (recover (snafu "Failed to lookup value from Couchbase!"
                          {:namespace namespace
                           :key key})))))
  
  (store* [this namespace key value cas]
    (klog/fn-trace :store* {:namespace namespace :key key :cas cas})
    (let [enc (encoder* this namespace)
          doc (proceed enc encoder/encode-versioned
                       (ns-key namespace key) value cas)]
      (-> (ns-bucket* this namespace)
          (proceed-all replace-doc doc)
          (proceed-all (util/flip encoder/decode) enc)
          (recover (snafu "Failed to store value in Couchbase!"
                      {:namespace namespace
                       :key key
                       :value value
                       :cas cas})))))


  ;;----------------------------------------------------------------------------
  proto/CheckAndSetStore
  (exists? [this namespace key]
    (klog/fn-trace :exists? {:namespace namespace :key key})
    (-> (robust
         this
         #(let [doc (-> (encoder* this namespace)
                        (proceed encoder/named (ns-key namespace key)))]
            (-> (ns-bucket* this namespace)
                (proceed-all get-doc doc)
                (branch
                 (snafu "Failed to determine if value exists in Couchbase!"
                        {::api/namespace namespace
                         ::api/key key})
                 some?))))
        (success)))

  (create [this namespace key value]
    (klog/fn-trace :create {:namespace namespace :key key :value value})
    (-> (robust this #(proto/create* this namespace key value))
        (success)))

  (delete [this namespace key cas]
    (klog/fn-trace :delete {:namespace namespace :key key :cas cas})
    (-> (robust this #(proto/delete* this namespace key cas))
        (success)))  
  
  (lookup [this namespace key]
    (klog/fn-trace :lookup {:namespace namespace :key key})
    (-> (robust this #(proto/lookup* this namespace key))
        (success)))  
  
  (store [this namespace key value cas]
    (klog/fn-trace :store {:namespace namespace :key key :value value :cas cas})
    (-> (robust this #(proto/store* this namespace key value cas))
        (success)))

  
  ;;----------------------------------------------------------------------------
  proto/UnsafeKeyValueStore
  (fetch* [this namespace key]
    (klog/fn-trace :fetch* {:namespace namespace :key key})
    (some-> (proto/lookup* this namespace key)
            (proceed ::api/value)))
  
  (destroy* [this namespace key]
    (klog/fn-trace :destroy* {:namespace namespace :key key})
    (let [result (-> (ns-bucket* this namespace)
                     (proceed remove-doc-named (ns-key namespace key))
                     (branch (snafu "Failed to destroy value from Couchbase!"
                                    {:namespace namespace
                                     :key key})
                             some?))]
      (when (and (success? result)
                 (has-set? this namespace)
                 (proto/set-exists? this namespace))
        (retry this
               #(proto/set-remove* this namespace key)))
      result))

  (write* [this namespace key value]
    (klog/fn-trace :write* {:namespace namespace :key key})
    (proto/swap-in* this namespace key (constantly value)))

  (swap-in* [this namespace key f]
    (klog/fn-trace :swap-in* {:namespace namespace :key key :f f})
    (-> (let [{:keys [::api/value ::api/cas]}
              (proto/lookup this namespace key)]
          (if cas
            (proto/store* this namespace key (f value) cas)
            (proto/create* this namespace key (f nil))))
        (proceed ::api/value)))

  
  ;;----------------------------------------------------------------------------
  proto/KeyValueStore
  (fetch [this namespace key]
    (klog/fn-trace :fetch {:namespace namespace :key key})
    (-> (proto/fetch* this namespace key)
        (success-or-throw)))
  
  (destroy [this namespace key]
    (klog/fn-trace :destroy {:namespace namespace :key key})
    (-> (proto/destroy* this namespace key)
        (success-or-throw)))

  (write [this namespace key value]
    (klog/fn-trace :write {:namespace namespace :key key :value value})
    (proto/swap-in this namespace key (constantly value)))

  (swap-in [this namespace key f]
    (klog/fn-trace :swap-in {:namespace namespace :key key :f f})
    (-> (retry this #(proto/swap-in* this namespace key f))
        (success-or-throw)))


  ;;--------------------------------------------------------------------------------
  proto/Cache
  (expire [this namespace key ttl]
    (let [expiry (ttl-epoch ttl)]
      (-> (attempt ns-bucket* this namespace)
          (proceed-all touch expiry (ns-key namespace key))
          (branch (snafu "Failed to expire key in Couchbase!"
                         {:namespace namespace
                          :key key
                          :ttl ttl})
                  some?)
          (success))))

  ;;--------------------------------------------------------------------------------
  health/HealthCheck
  (healthy? [this]
    (->> (:health-keys this)
         (map (fn [[ns k]]
                (if (cache/swap-in
                     this ns k
                     (fn [_]
                       {:uuids (repeatedly (rand-int 5) (comp str #(UUID/randomUUID)))})
                     60)
                  true
                  (do (log/error "Health check failed!"
                                 {:namespace ns})
                      false))))
         (every? true?))))


;;------------------------------------------------------------------------------
;; Configuration 
;;------------------------------------------------------------------------------

(defn default-config
  "As much of the default configuration as can be determined from the current
   runtime environment.

   - `name` The root of the ConfigMap and Secrets directory.  Defaults to 
   `couchbase` if not provided.

   All environments will provide these configurations: 

   - `::namespaces`  This assumes that `json`, `nippy`, `binary`, `edn`, `test-json`,
   `test-nippy`, `test-binary` and `test-edn` buckets will be configured properly.
   - `::initial-delay-ms`
   - `::busy-delay-ms`
   - `::busy-retries`
   - `::max-retries`

   Under Kubernetes, these configs will also be provided:

   - `::hosts`
   - `::bucket-passwords`

   If not under Kubernetes, then only these configs are provided: 

   - `::hosts` Just a single `localhost` entry.

   The component system is still responsible for providing or overriding any 
   missing hostnames, bucket credentials and/or namespaces."
  ([name]
   (let [extra
         (when (k8s/kubernetes?)
           (let [bpw (->> (k8s/secrets name)
                          (map (fn [[k v]]
                                 (when (and (not= k "admin.password")
                                            (str/ends-with? k ".password"))
                                   [(-> (drop-last 9 k)
                                        (str/join)) v])))
                          (filter some?)
                          (into {}))]
             {::hosts (-> (k8s/config-map name)
                          (get "hosts")
                          (str/split #","))
              ::bucket-passwords bpw}))]
   (merge {::hosts ["localhost"]
           ::initial-delay-ms 2
           ::busy-delay-ms 500
           ::busy-retries 3
           ::max-retries 10}
          extra)))

  ([]
   (default-config "couchbase")))


;;------------------------------------------------------------------------------
;; Creation
;;------------------------------------------------------------------------------
     
(defn couchbase    
  [config]
  (map->Couchbase {:config config}))


;;------------------------------------------------------------------------------
;; Creation Spec
;;------------------------------------------------------------------------------

;; IP Addresses.
(def ip4-addr-regex #"^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$")

(def ip4-addr?
  (s/with-gen
    (s/and string? (partial re-matches ip4-addr-regex)
           (fn [s]
             (when-let [ss (str/split s #"\.")]
               (and (= (count ss) 4) ;; regex already does this
                    (every? #(try (<= 0 (Integer/parseInt %) 255)
                                  (catch Exception ex false))
                            ss)))))
    #(-> (gen/choose 0 255)
         (->> (gen/fmap str))
         (gen/vector 4)
         (->> (gen/fmap (partial str/join "."))))))


;; Fully Qualified Domain Names.
(def fqdn-regex #"[a-z][a-z0-9-]*[a-z0-9](\.[a-z][a-z0-9-]*[a-z0-9])*")

(def fqdn?
  (s/with-gen
    (s/and string? (partial re-matches fqdn-regex))
    #(-> (gen/string-alphanumeric)
         (->> (gen/fmap str/lower-case))
         (gen/not-empty)
         (gen/vector 1 2)
         (->> (gen/fmap (partial str/join "-")))
         (gen/vector 1 3)
         (->> (gen/fmap (partial str/join "."))))))


;; Host Identification.
(def host? (s/or :fqdn fqdn? :ip-addr ip4-addr?))
                                               
(s/def ::hosts (s/coll-of host?))


;; Buckets Authentication.
(def full-string?
  (s/spec (s/and string? (comp not empty?))
          :gen #(string-from-regex #"[a-z]+([a-zA-Z0-9])*")))

(s/def ::bucket-passwords (s/map-of full-string? full-string?))


;; Namespaces Configuration.
(s/def ::encoder #{:edn :json :nippy :binary})

(s/def ::bucket full-string?)

(s/def ::key-sets full-string?)

(def ns-conf? (s/keys :req [::encoder ::bucket] :opt [::key-sets]))

(s/def ::namespaces (s/map-of full-string? ns-conf?))
                         

;; Retry Parameters
(def whole-int? (s/and pos-int? (partial <= 0)))

(s/def ::initial-delay-ms pos-int?)

(s/def ::busy-delay-ms pos-int?)

(s/def ::busy-retries whole-int?)

(s/def ::max-retries whole-int?)


;; Whole Configuration.
(def config?
  (s/keys :req [::hosts ::bucket-passwords ::namespaces
                ::initial-delay-ms ::busy-delay-ms
                ::busy-retries ::max-retries]))


;; Creation.
(s/fdef couchbase
        :args (s/cat :config config?)
        :ret (s/and api/string-set-store?
                    api/unsafe-string-set-store?
                    api/check-and-set-store?
                    api/unsafe-check-and-set-store?
                    api/key-value-store?
                    api/unsafe-key-value-store?))
