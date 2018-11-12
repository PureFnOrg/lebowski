(ns org.purefn.lebowski.encoder.nippy-encoder
  "Converts between Clojure values encoded with Nippy as binary data stored
   using Couchbase BinaryDocument type."
  (:require [clojure.spec.alpha :as s]
            [taoensso.nippy :as nippy]
            [org.purefn.bridges.api :as api]
            [org.purefn.lebowski.encoder.common :as common]
            [org.purefn.lebowski.encoder.api :as enc]
            [org.purefn.lebowski.encoder.protocol :as proto])
  (:import [com.couchbase.client.java.document BinaryDocument]
           [com.couchbase.client.deps.io.netty.buffer Unpooled ByteBuf]))

;;------------------------------------------------------------------------------
;; Component. 
;;------------------------------------------------------------------------------

(defrecord NippyEncoder []
  proto/CheckAndSetEncoder
  (named [_ id]
    (BinaryDocument/create id))

  (versioned [_ id cas]
    (BinaryDocument/create ^String id nil ^Long cas))
  
  (encode [_ id value]
    (->> (nippy/freeze value)
         (Unpooled/copiedBuffer)
         (.retain)
         (BinaryDocument/create id)))
  
  (encode-versioned [_ id value cas]
    (->> (nippy/freeze value)
         (Unpooled/copiedBuffer)
         (.retain)
         ((fn [bs] (BinaryDocument/create ^String id
                                          ^ByteBuf bs
                                          ^Long cas)))))
  
  (decode [_ document]
    {::api/value (some-> ^BinaryDocument document
                         (common/binary-array)
                         (nippy/thaw))
     ::api/cas (some-> ^BinaryDocument document
                       (.cas))}))


;;------------------------------------------------------------------------------
;; Creation
;;------------------------------------------------------------------------------

(defn nippy-encoder
  "Create an NippyEncoder."
  []
  (->NippyEncoder))


;;------------------------------------------------------------------------------
;; Creation Spec
;;------------------------------------------------------------------------------

(s/fdef nippy-encoder
        :ret enc/encoder?)
