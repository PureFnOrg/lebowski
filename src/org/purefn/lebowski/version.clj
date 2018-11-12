(ns org.purefn.lebowski.version
  (:gen-class))

(defn -main
  []
  (println (System/getProperty "lebowski.version")))
