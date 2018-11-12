(ns org.purefn.lebowski.encoder.common
  (:import [com.couchbase.client.java.document BinaryDocument]
           [com.couchbase.client.deps.io.netty.buffer ByteBuf]))

(defn binary-array
  "Fetches the content of a BinaryDocument as a byte-array with proper
  resource de-allocation"
  [^BinaryDocument doc]
  (let [^ByteBuf content (.content doc)
        ary (byte-array (.readableBytes content))]
    (.readBytes content ary)
    (.release content)
    ary))
