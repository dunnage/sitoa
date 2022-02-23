(ns dunnage.sitoa.bootstrapped-schema-test
  (:require [clojure.test :refer :all]
            [dunnage.sitoa.bootstrapped-schema :refer :all]
            [malli.core :as m]
            [malli.util :as mu]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [clojure.java.io :as io]
            [malli.generator :as mg]))

(comment
  (xds->registry {:default-ns "script"} (io/resource "V2021011SCRIPT/ecl.xsd"))
  (xds->registry {:default-ns "script"} (io/resource "V2021011SCRIPT/datatypes.xsd"))
  (xds->registry {:default-ns "script"} (io/resource "V2021011SCRIPT/script.xsd"))
  (xds->registry {:default-ns "script"} (io/resource "V2021011SCRIPT/specialized.xsd"))
  (xds->registry {:default-ns "script"} (io/resource "V2021011SCRIPT/structures.xsd"))
  (xds->registry {:default-ns "script"} (io/resource "V2021011SCRIPT/transport.xsd"))
  (xds->registry {:default-ns "xsd"} (io/resource "XMLSchema.xsd"))
  (xds->registry {:default-ns "fop"} (io/resource "fop.xsd"))
  (xsd->schema  {:default-ns "script"} (io/resource "V2021011SCRIPT/transport.xsd"))
  (def message-schema (m/schema (xds->registry {:default-ns "script"} (io/resource "V20170715/transport.xsd"))
                                {:registry (merge
                                             (m/default-schemas)
                                             (mu/schemas)
                                             xml-primitives/xmlschema-custom)}

                                ))
  (->> (mg/generate message-schema) #_(m/explain message-schema))

  )
