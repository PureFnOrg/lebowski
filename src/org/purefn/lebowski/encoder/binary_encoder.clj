(ns org.purefn.lebowski.encoder.binary-encoder
  "Converts between Clojure byte vectors and binary data stored using Couchbase 
   BinaryDocument type."
  (:require [clojure.spec.alpha :as s]
            [org.purefn.bridges.api :as api]
            [org.purefn.lebowski.encoder.api :as enc]
            [org.purefn.lebowski.encoder.protocol :as proto]
            [org.purefn.lebowski.encoder.common :as common])
  (:import [com.couchbase.client.java.document BinaryDocument]
           [com.couchbase.client.deps.io.netty.buffer Unpooled ByteBuf]
           [java.io Serializable ByteArrayOutputStream ObjectOutputStream]))

;;------------------------------------------------------------------------------
;; Helpers
;;------------------------------------------------------------------------------

(defn serializable? [v]
  (instance? Serializable v))

(defn serialize 
  "Serializes value, returns a byte array"
  [v]
  (let [buff (ByteArrayOutputStream. 1024)]
    (with-open [dos (ObjectOutputStream. buff)]
      (.writeObject dos v))
    (.toByteArray buff)))

(defn as-bytes
  [v]
  (cond
    (bytes? v) v
    (serializable? v) (serialize v)
    :else (throw (ex-info "Value does not implement Serializable"
                          {:value v
                           :type (type v)}))))

;;------------------------------------------------------------------------------
;; Component. 
;;------------------------------------------------------------------------------

(defrecord BinaryEncoder []
  proto/CheckAndSetEncoder
  (named [_ id]
    (BinaryDocument/create id))

  (versioned [_ id cas]
    (BinaryDocument/create ^String id nil ^Long cas))
  
  (encode [_ id value]
    (->> (as-bytes value)
         (Unpooled/copiedBuffer)
         (.retain)
         (BinaryDocument/create id)))
  
  (encode-versioned [_ id value cas]
    (->> (as-bytes value)
         (Unpooled/copiedBuffer)
         (.retain)
         ((fn [bs] (BinaryDocument/create ^String id
                                          ^ByteBuf bs
                                          ^Long cas)))))
  
  (decode [_ document]
    {::api/value (some-> ^BinaryDocument document
                         (common/binary-array))
     ::api/cas (some-> ^BinaryDocument document
                       (.cas))}))


;;------------------------------------------------------------------------------
;; Creation
;;------------------------------------------------------------------------------

(defn binary-encoder
  "Create an BinaryEncoder."
  []
  (->BinaryEncoder))


;;------------------------------------------------------------------------------
;; Creation Spec
;;------------------------------------------------------------------------------

(s/fdef binary-encoder
        :ret enc/encoder?)
