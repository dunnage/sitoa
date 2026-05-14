(ns dunnage.sitoa.script-roundtrip-test
  "Round-trip an EDN <Message> through unparser -> XML -> parser. Each
  deftest asserts the value that comes back equals the value that went
  in. Failing tests describe real data-loss / drift in the current
  pipeline against the SCRIPT 2023011 generated registry."
  (:require [clojure.set]
            [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.unparser :as unparser]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [malli.core :as m])
  (:import (java.io StringReader)
           (java.time OffsetDateTime)
           (javax.xml.stream XMLStreamReader)))

(def script-registry
  (delay (bs/xsd->registry {:default-ns "script"}
                           (bs/parse-xsd (io/resource "NCPDP_2023011/transport.xsd")))))

(defn- message-schema []
  (m/schema [:schema {:registry @script-registry :topElement "Message"}
             :script/MessageType]
            xml-primitives/external-registry))

(defn- round-trip [data]
  (let [s   (message-schema)
        xml ((unparser/xml-string-unparser s) data)
        p   (parser/xml-parser s)]
    (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. xml))]
      (p r))))

(def status-message
  {:DatatypesVersion   "20230101"
   :TransactionDomain  "SCRIPT"
   :TransactionVersion "20230101"
   :StructuresVersion  "20230101"
   :TransportVersion   "20230101"
   :ECLVersion         "20230101"
   :Header {:To             {:xml/value "RECEIVER123" :Qualifier "P"}
            :From           {:xml/value "SENDER789"   :Qualifier "D"}
            :MessageID      "MSG-0001"
            :SentTime       (OffsetDateTime/parse "2025-03-14T09:00:00-04:00")
            :SenderSoftware {:SenderSoftwareDeveloper      "AcmeRx"
                             :SenderSoftwareProduct        "RxSender"
                             :SenderSoftwareVersionRelease "1.0.0"}}
   :Body   [:Status {:Code            "010"
                     :DescriptionCode []
                     :Description     "approved\rfor dispense"}]})

(deftest header-sent-time-preserves-offset
  ;; HeaderType.SentTime is xsd:dateTime (UtcDateType). HeaderType is the
  ;; same shape in every SCRIPT body type, so this affects every message.
  (let [out (round-trip status-message)]
    (is (= (-> status-message :Header :SentTime)
           (-> out             :Header :SentTime)))))

(deftest status-empty-description-code-preserved
  ;; Status.DescriptionCode is minOccurs=0 maxOccurs=10 -> optional :sequential.
  ;; Empty vector input should not collapse to absent key on the way back.
  (let [out (round-trip status-message)]
    (is (= (-> status-message :Body second :DescriptionCode)
           (-> out             :Body second :DescriptionCode)))))

(deftest status-description-preserves-cr
  ;; Status.Description is xsd:string (an1..70). XML 1.0 sec 2.11 normalises
  ;; \r to \n on parse, so the round-trip is lossy for any \r-bearing string.
  (let [out (round-trip status-message)]
    (is (= (-> status-message :Body second :Description)
           (-> out             :Body second :Description)))))

(defn- scoped [type-kw element-name]
  (m/schema [:schema {:registry @script-registry :topElement element-name} type-kw]
            xml-primitives/external-registry))

(defn- round-trip-scoped [schema data]
  (let [xml ((unparser/xml-string-unparser schema) data)
        p   (parser/xml-parser schema)]
    (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. xml))]
      (p r))))

(deftest attachment-roundtrips
  ;; Attachment is xsd:cat containing a direct [:ref ClinicalInformation-seq].
  ;; The unparser's -ref-discriminator returns an arity-1 fn but is called
  ;; with (data pos) when inside a :cat -- "Wrong number of args (2)".
  ;; Used in ClinicalInfoResponse, ClinicalInfoRequest, PARequest, etc.
  (let [sample [[:AttachmentSource "EMR"]
                [:AttachmentData   "SGVsbG8="]
                [:CDA              {:TemplateID ["urn:hl7-org:ccda"]}]
                [:MIMEType         "image/png"]]]
    (is (= sample (round-trip-scoped (scoped :script/Attachment "Attachment") sample)))))

(deftest binary-data-roundtrips
  ;; BinaryDataType is <Binary LengthBytes="..." >base64...</Binary>.
  ;; LengthBytes is xsd:int as an attribute -> the attribute unparser path
  ;; casts the value to String and crashes for any non-String input.
  ;; Used in CFMonograph, CFAttachment.
  (let [sample {:LengthBytes 5 :xml/value "SGVsbG8="}]
    (is (= sample (round-trip-scoped (scoped :script/BinaryDataType "Binary") sample)))))
