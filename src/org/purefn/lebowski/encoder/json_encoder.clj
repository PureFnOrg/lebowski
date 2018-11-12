(ns org.purefn.lebowski.encoder.json-encoder
  "Converts between Clojure values stored as Couchbase JsonDocument type."
  (:require [clojure.spec.alpha :as s]
            [cheshire.core :as json]
            [org.purefn.bridges.api :as api]
            [org.purefn.lebowski.encoder.api :as enc]
            [org.purefn.lebowski.encoder.protocol :as proto])
  (:import [com.couchbase.client.java.document JsonDocument]
           [com.couchbase.client.java.document.json JsonObject]))

;;------------------------------------------------------------------------------
;; Helper Functions. 
;;------------------------------------------------------------------------------

(defn- as-json
  [value]
  (-> value
      (json/generate-string)
      (JsonObject/fromJson)))


;;------------------------------------------------------------------------------
;; Component. 
;;------------------------------------------------------------------------------

(defrecord JsonEncoder []
  proto/CheckAndSetEncoder
  (named [_ id]
    (JsonDocument/create id))

  (versioned [_ id cas]
    (JsonDocument/create ^String id nil ^Long cas))
  
  (encode [_ id value]
    (JsonDocument/create id (as-json value)))

  (encode-versioned [_ id value cas]
    (JsonDocument/create ^String id ^JsonObject (as-json value) ^Long cas))
  
  (decode [_ document]
    {::api/value (some-> ^JsonDocument document
                         (.content)
                         (.toString)
                         (json/parse-string true))                    
     ::api/cas (some-> ^JsonDocument document
                       (.cas))}))


;;------------------------------------------------------------------------------
;; Creation
;;------------------------------------------------------------------------------
     
(defn json-encoder
  "Create an JsonEncoder."
  []
  (->JsonEncoder))


;;------------------------------------------------------------------------------
;; Creation Spec
;;------------------------------------------------------------------------------

(s/fdef json-encoder
        :ret enc/encoder?)
