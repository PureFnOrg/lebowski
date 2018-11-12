(ns org.purefn.lebowski.encoder.api-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [org.purefn.bridges.api :as api]
            [org.purefn.lebowski.encoder.api :refer :all]
            [org.purefn.lebowski.encoder.edn-encoder :refer [edn-encoder]]
            [org.purefn.lebowski.encoder.json-encoder :refer [json-encoder]]
            [org.purefn.lebowski.encoder.nippy-encoder :refer [nippy-encoder]])
  (:import [com.couchbase.client.java.document AbstractDocument]))

(stest/instrument [`named `versioned `encode `encode-versioned `decode])


(def gen-encoder
  (gen/elements [(edn-encoder) (json-encoder) (nippy-encoder)]))

(def gen-data 
  (s/gen (s/cat :id id?
                :value api/value?
                :cas api/cas?)))

(defspec named-encoding
  (prop/for-all [encoder gen-encoder
                 [id _ _] gen-data]
    (let [^AbstractDocument doc (named encoder id)]
      (and (= id (.id doc))
           (nil? (.content doc))
           (= 0 (.cas doc))))))
            
(defspec versioned-encoding
  (prop/for-all [encoder gen-encoder
                 [id _ cas] gen-data]
    (let [^AbstractDocument doc (versioned encoder id cas)]
      (and (= id (.id doc))
           (nil? (.content doc))
           (= cas (.cas doc))))))

(defspec encoding-decoding
  (prop/for-all [encoder gen-encoder
                 [id ovalue _] gen-data]
    (let [^AbstractDocument doc (encode encoder id ovalue)
          {:keys [::api/value ::api/cas]} (decode encoder doc)]
      (and (= id (.id doc))
           (= value ovalue)
           (= cas (.cas doc))))))

(defspec versioned-encoding-decoding
  (prop/for-all [encoder gen-encoder
                 [id ovalue ocas] gen-data]
    (let [^AbstractDocument doc (encode-versioned encoder id ovalue ocas)
          {:keys [::api/value ::api/cas]} (decode encoder doc)]
      (and (= id (.id doc))
           (= value ovalue)
           (= cas ocas)))))
