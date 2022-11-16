(ns com.breezeehr.edi-serde
  (:require [clojure.java.io :as io])
  (:import (io.xlate.edi.stream EDIInputFactory)))

(defn foo
  "I don't do a whole lot."
  [x]
  (prn x "Hello, World!"))


(comment
  (def fact  (EDIInputFactory/newFactory))
  (.setProperty fact EDIInputFactory/EDI_IGNORE_EXTRANEOUS_CHARACTERS true)
  (.setProperty fact EDIInputFactory/EDI_VALIDATE_CONTROL_STRUCTURE false)
  (let [r (.createEDIStreamReader fact (io/input-stream (io/resource  #_"271-3.edi"
                                                                      "simple_with_binary_segment.edi"
                                                          #_"sample837-original.edi")))]
    (loop []
      (if (.hasNext r)
        (let [next (.next r)]
          (when (.isError next)
            (prn (.getErrorType r)))
          (prn (str (.getLocation r)) (str next))
          (recur ))
        ))
    )
  )
