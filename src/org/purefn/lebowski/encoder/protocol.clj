(ns org.purefn.lebowski.encoder.protocol
  "Protocol definitions for encoding data as Couchbase documents.")

(defprotocol CheckAndSetEncoder
   "Converts between documents usable with a key-value store with check-and-set
    semantics and Clojure values." 
   (named [this id])
   (versioned [this id cas])
   (encode [this id value])
   (encode-versioned [this id value cas])
   (decode [this document]))
