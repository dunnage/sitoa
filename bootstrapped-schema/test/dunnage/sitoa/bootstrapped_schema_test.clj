(ns dunnage.sitoa.bootstrapped-schema-test
  (:require [clojure.test :refer :all]
            [dunnage.sitoa.bootstrapped-schema :refer [xsd->schema xsd->registry serialize-schema serialize-registry
                                                       raw-xsd->schema]]
            [malli.core :as m]
            [malli.util :as mu]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [clojure.java.io :as io]
            [malli.generator :as mg]))

(comment
  (set! *print-namespace-maps* false)
  (xsd->schema {:default-ns "xsd"} (io/resource "XMLSchema.xsd"))
  (with-open [writer (io/writer "resources/fop.edn")]
    (fipp.edn/pprint (m/form (xsd->schema {:default-ns "fop"} (io/resource "fop.xsd"))) {:writer writer}))

  (def message-schema (m/schema (xsd->registry {:default-ns "script"} (io/resource "NCPDP_2023011/transport.xsd"))
                                {:registry (merge
                                             (m/default-schemas)
                                             (mu/schemas)
                                             xml-primitives/xmlschema-custom)}

                                ))
  (->> (mg/generate message-schema) #_(m/explain message-schema))

  (serialize-registry (xsd->schema {:default-ns "script"} (io/resource "NCPDP_2023011/transport.xsd")) "script_registry.edn")
  (serialize-schema (xsd->schema {:default-ns "spl"} (io/resource "spl/spl.xsd")) "spl.edn")

  )
