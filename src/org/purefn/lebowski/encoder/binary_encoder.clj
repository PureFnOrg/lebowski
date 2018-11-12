(ns org.purefn.lebowski.encoder.binary-encoder
  "Converts between Clojure byte vectors and binary data stored using Couchbase 
   BinaryDocument type."
  (:require [clojure.spec.alpha :as s]
            [org.purefn.bridges.api :as api]
            [org.purefn.lebowski.encoder.api :as enc]
            [org.purefn.lebowski.encoder.protocol :as proto]
            [org.purefn.lebowski.encoder.common :as common])
  (:import [com.couchbase.client.java.document BinaryDocument]
           [com.couchbase.client.deps.io.netty.buffer Unpooled ByteBuf]))

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
    (->> (Unpooled/copiedBuffer value)
         (.retain)
         (BinaryDocument/create id)))
  
  (encode-versioned [_ id value cas]
    (->> (Unpooled/copiedBuffer value)
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
