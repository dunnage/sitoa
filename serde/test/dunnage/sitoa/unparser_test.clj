(ns dunnage.sitoa.unparser-test
  "Regression / reproduction tests for sitoa unparser issues."
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.generators :as gen]
   [dunnage.sitoa.unparser :as unparser]
   [dunnage.sitoa.xml-primitives :as xml-primitives]
   [malli.core :as m]
   [malli.generator :as mg]))

(defn- mini-unparser
  "Build an xml-string-unparser for a tiny schema [:map [:val body-type]]."
  [body-type]
  (-> (m/schema
       [:schema
        {:registry   {:test/Root [:map {:closed true} [:val {} body-type]]}
         :topElement "Root"}
        :test/Root]
       xml-primitives/external-registry)
      unparser/xml-string-unparser))

;; ---------- :Base64Binary (and friends) typed as :multi of :string ----------
;;
;; HISTORICAL CONTEXT
;; xmlschema-registry previously mapped several XSD types to :any:
;;   base64Binary, hexBinary, duration, dayTimeDuration, yearMonthDuration,
;;   gDay, gMonth, gMonthDay, gYear, gYearMonth.
;; The unparser dispatched :any to string-unparser, which calls
;; (.writeCharacters w data) — a direct cast to java.lang.String. malli's
;; default :any generator emits arbitrary values (Ratio, Vector, Map, Set,
;; nil, ...), and every non-String crashed with
;;
;;   class clojure.lang.<Foo> cannot be cast to class java.lang.String
;;
;; SURFACED BY
;; A generative XML round-trip test in the consuming furl project hit this
;; while exercising :script/Extension :xml/value, whose :Base64Data arm is
;; typed [:ref :org.w3.www.2001.XMLSchema/base64Binary].
;;
;; FIX (xml-primitives.clj)
;; Replaced :any with a shared schema:
;;
;;   (def xsd-textual-value
;;     [:multi {:dispatch class}
;;      [java.lang.String :string]])
;;
;; The :multi dispatches malli generation to the listed arm (String only,
;; for these textually-represented XSD types) and routes unparsing through
;; -multi-unparser → -string-unparser without the broken cast. Add more arms
;; if a real-world payload legitimately carries another Clojure type.

(deftest base64binary-typed-as-multi-of-string
  (testing "xmlschema-registry now uses xsd-textual-value (a :multi)"
    (is (= xml-primitives/xsd-textual-value
           (:org.w3.www.2001.XMLSchema/base64Binary xml-primitives/xmlschema-registry)))
    (is (= :multi (first xml-primitives/xsd-textual-value))))
  (testing "Unparsing a String body through xsd-textual-value works"
    (let [up (mini-unparser xml-primitives/xsd-textual-value)]
      (is (= "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root><val>hi</val></Root>"
             (up {:val "hi"})))))
  (testing "Generation from :base64Binary produces only Strings (no Ratio/Vector/Map)"
    (let [schema  (m/schema xml-primitives/xsd-textual-value xml-primitives/external-registry)
          samples (repeatedly 50 #(gen/generate (mg/generator schema) 10))]
      (is (every? string? samples)
          (str "Expected every sample to be String, got types: "
               (->> samples (map type) distinct))))))
