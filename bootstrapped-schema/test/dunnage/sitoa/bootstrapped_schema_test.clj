(ns dunnage.sitoa.bootstrapped-schema-test
  (:require [clojure.test :refer :all]
            [dunnage.sitoa.bootstrapped-schema :refer [xsd->schema xsd->registry]]
            [malli.core :as m]
            [malli.util :as mu]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [clojure.java.io :as io]
            [malli.generator :as mg]))

(comment
  (xsd->schema {:default-ns "xsd"} (io/resource "XMLSchema.xsd"))
  (xsd->schema {:default-ns "fop"} (io/resource "fop.xsd"))

  (def message-schema (m/schema (xsd->registry {:default-ns "script"} (io/resource "V20170715/transport.xsd"))
                                {:registry (merge
                                             (m/default-schemas)
                                             (mu/schemas)
                                             xml-primitives/xmlschema-custom)}

                                ))
  (->> (mg/generate message-schema) #_(m/explain message-schema))

  )
