(ns dunnage.sitoa.parser-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :as pprint]
            [dunnage.sitoa.bootstrapped-schema :as schema]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [malli.util :as mu]
            [malli.core :as m]
            [dunnage.sitoa.parser :refer :all]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn])
  (:import (java.io PushbackReader)))


(comment

  (def xsd-schema (m/schema (with-open [r (PushbackReader. (io/reader (io/resource "xsd-schema.edn")))]
                              (edn/read r))
                            xml-primitives/external-registry))

  (def xsd-parser (xml-parser xsd-schema))

  (def fop-schema (m/schema (with-open [r (PushbackReader. (io/reader (io/resource "fop-schema.edn")))]
                              (edn/read r))
                            xml-primitives/external-registry))

  (def fop-parser (xml-parser fop-schema))
  (def message-schema (m/schema (schema/xds->registry {:default-ns "script"} (io/resource "NCPDP_20170715/transport.xsd"))
                                {:registry xml-primitives/external-registry}))
  (m/options message-schema)

  (def p (xml-parser  message-schema))
  (with-open [s (StringReader. (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n  <Sig>\n  <SigText>Take 1 tablet oral route 3 per day as needed for headache</SigText>\n  <CodeSystem>\n  <SNOMEDVersion>2017_03_01</SNOMEDVersion>\n  <FMTVersion>16.03d</FMTVersion>\n  </CodeSystem>\n  <Instruction>\n  <DoseAdministration>\n  <DoseDeliveryMethod>\n  <Text>Take</Text>\n  <Qualifier>SNOMED</Qualifier>\n  <Code>419652001</Code>\n  </DoseDeliveryMethod>\n  <Dosage>\n  <DoseQuantity>1</DoseQuantity>\n  <DoseUnitOfMeasure>\n  <Text>tablet</Text>\n  <Qualifier>DoseUnitOfMeasure</Qualifier>\n  <Code>C48542</Code>\n  </DoseUnitOfMeasure>\n  </Dosage>\n  <RouteOfAdministration>\n  <Text>oral route</Text>\n  <Qualifier>SNOMED</Qualifier>\n  <Code>26643006</Code>\n  </RouteOfAdministration>\n  </DoseAdministration>\n  <TimingAndDuration>\n  <Frequency>\n  <FrequencyNumericValue>3</FrequencyNumericValue>\n  <FrequencyUnits>\n  <Text>day</Text>\n  <Qualifier>SNOMED</Qualifier>\n  <Code>258703001</Code>\n  </FrequencyUnits>\n  </Frequency>\n  </TimingAndDuration>\n  <IndicationForUse>\n  <IndicationPrecursor>\n  <Text>as needed for</Text>\n  <Qualifier>SNOMED</Qualifier>\n  <Code>420449005</Code>\n  </IndicationPrecursor>\n  <IndicationValue>\n  <Text>headache</Text>\n  <Qualifier>SNOMED</Qualifier>\n  <Code>25064002</Code>\n  </IndicationValue>\n  </IndicationForUse>\n  </Instruction>\n  </Sig>"))]
    (let [r ^XMLStreamReader (make-stream-reader {} s)]
      (p r)))

  (def parsed (with-open [s (io/reader (io/resource "NCPDP_20170715/Message-1621652001272.xml"))]
                (let [r ^XMLStreamReader (make-stream-reader {} s)]
                  (p r))))
  (def parsed (with-open [s (io/reader (io/resource "NCPDP_20170715/Message-sig1.xml"))]
                (let [r ^XMLStreamReader (make-stream-reader {} s)]
                  (p r))))
  (def parsed (with-open [s (io/reader (io/resource "NCPDP_20170715/renewalrequest.xml"))]
                (let [r ^XMLStreamReader (make-stream-reader {} s)]
                  (p r))))

  (def xsd-parser (xml-parser
                    (m/schema (schema/xds->registry {:default-ns "xs"} (io/resource "XMLSchema.xsd"))
                              {:registry xml-primitives/external-registry}

                              )))
  (def xsd-parser (let [xsd (m/schema (schema/xds->registry {:default-ns "xs"} (io/resource "XMLSchema.xsd"))
                                      {:registry xml-primitives/external-registry})]
                    (xml-parser
                      (m/-set-children xsd (-> xsd m/children first m/children first vector)))))
  (def parsed (with-open [s (io/reader (io/resource "XMLSchema.xsd"))]
                (let [r ^XMLStreamReader (make-stream-reader {} s)]
                  (xsd-parser r))))

  (def fopparsed (with-open [s (io/reader (io/resource "fopsample1.xml"))]
                   (let [r ^XMLStreamReader (make-stream-reader {} s)]
                     (fop-parser r))))
  (def fopparsed (with-open [s (io/reader (io/resource "table-borders.fo"))]
                   (let [r ^XMLStreamReader (make-stream-reader {} s)]
                     (fop-parser r))))
  (def fopparsed (with-open [s (io/reader (io/resource "table-borders-max-ram.fo"))]
                   (let [r ^XMLStreamReader (make-stream-reader {} s)]
                     (fop-parser r))))
  (select-keys (m/explain message-schema parsed) [:value :errors])
  )
