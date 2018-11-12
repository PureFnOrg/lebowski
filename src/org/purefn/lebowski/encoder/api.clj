(ns org.purefn.lebowski.encoder.api
  (:require  [clojure.spec.alpha :as s]
             [clojure.spec.gen.alpha :as gen]
             [com.gfredericks.test.chuck.generators :refer [string-from-regex]]
             [org.purefn.bridges.api :as api]
             [org.purefn.lebowski.encoder.protocol :as proto])            
  (:import [com.couchbase.client.java.document AbstractDocument]))
  
;;------------------------------------------------------------------------------
;; API 
;;------------------------------------------------------------------------------

(defn named
  "Create an empty document with the given ID."
  [encoder id]
  (proto/named encoder id))

(defn versioned
  "Create an empty document with the given ID and check-and-set token."
  [encoder id cas]
  (proto/versioned encoder id cas))

(defn encode
  "Encode the value as a document with the given ID."
  [encoder id value]
  (proto/encode encoder id value))

(defn encode-versioned
  "Encode the value as a document with the given ID and check-and-set token."
  [encoder id value cas]
  (proto/encode-versioned encoder id value cas))

(defn decode
  "Decode the document into a value and check-and-set token."
  [encoder document]
  (proto/decode encoder document))


;;------------------------------------------------------------------------------
;; Data Specs 
;;------------------------------------------------------------------------------

(def encoder? (partial satisfies? proto/CheckAndSetEncoder))

(def id-regex #"^[a-z]+(-[a-z]+)*(/[a-z]+(-[a-z]+)*)*\p{Graph}+$")

(def id?
  (s/spec (s/and string?
                 (partial re-matches id-regex)
                 #(<= (count %) 250))
          :gen #(string-from-regex
                 #"[a-z]+(-[a-z]+){0,2}(/[a-z]+(-[a-z]+){0,2}){0,2}[a-zA-Z0-9#$%&\*\+-\.:;<=>\?@_]+")))

(def document? (partial instance? AbstractDocument))


;;------------------------------------------------------------------------------
;; Function Specs 
;;------------------------------------------------------------------------------

(s/fdef named
        :args (s/cat :encoder encoder?
                     :id id?)
        :ret document?)

(s/fdef versioned
        :args (s/cat :encoder encoder?
                     :id id?
                     :cas api/cas?)
        :ret document?)

(s/fdef encode
        :args (s/cat :encoder encoder?
                     :id id?
                     :value some?)
        :ret document?)

(s/fdef encode-versioned
        :args (s/cat :encoder encoder?
                     :id id?
                     :value some?
                     :cas api/cas?)
        :ret document?)

(s/fdef decode
        :arge (s/cat :encoder encoder?
                     :document document?)
        :ret api/doc?)
