(ns org.purefn.lebowski.encoder.edn-encoder
  "Converts between Clojure values stored as EDN encoded strings using the
   Couchbase StringDocument type."
  (:require [clojure.spec.alpha :as s]
            [clojure.edn :as edn]
            [org.purefn.bridges.api :as api]
            [org.purefn.lebowski.encoder.api :as enc]
            [org.purefn.lebowski.encoder.protocol :as proto])
  (:import [com.couchbase.client.java.document StringDocument]))

;;------------------------------------------------------------------------------
;; Component. 
;;------------------------------------------------------------------------------

(defrecord EdnEncoder []
  proto/CheckAndSetEncoder
  (named [_ id]
    (StringDocument/create id))

  (versioned [_ id cas]
    (StringDocument/create ^String id nil ^Long cas))
  
  (encode [_ id value]
    (StringDocument/create id (pr-str value)))

  (encode-versioned [_ id value cas]
    (StringDocument/create ^String id (pr-str value) ^Long cas))
  
  (decode [_ document]
    {::api/value (some-> ^StringDocument document
                         (.content)
                         (.toString)
                         (edn/read-string))
     ::api/cas (some-> ^StringDocument document
                       (.cas))}))


;;------------------------------------------------------------------------------
;; Creation
;;------------------------------------------------------------------------------

(defn edn-encoder
  "Create an EdnEncoder."
  []
  (->EdnEncoder))


;;------------------------------------------------------------------------------
;; Creation Spec
;;------------------------------------------------------------------------------

(s/fdef edn-encoder
        :ret enc/encoder?)
