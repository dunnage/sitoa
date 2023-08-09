(ns dunnage.sitoa.schema
  (:require [clojure.java.io :as io])
  (:import (java.io Reader StringReader)
           (javax.xml.transform.stream StreamSource)
           (javax.xml.validation Schema SchemaFactory)))
(defn schema-validator [schema]
  (let [schemafactory ^SchemaFactory (SchemaFactory/newDefaultInstance)
        schema ^Schema (.newSchema schemafactory schema)
        validator (.newValidator schema)]
    (fn [data]
      (cond
        (instance? Reader data) (.validate validator (StreamSource. ^Reader data))
        (string? data)
        (.validate validator (StreamSource. (StringReader. data)))))))


(comment

  (def schemafactory (SchemaFactory/newDefaultInstance) )
  (def schema  (.newSchema schemafactory (io/resource "NCPDP_20170715/transport.xsd")))

  (def validator (.newValidator schema))

  (.validate validator (StreamSource. (StringReader. )))
  )