(ns dunnage.sitoa.unparser-test
  "Regression / reproduction tests for sitoa unparser issues."
  (:require
   [clojure.test :refer [deftest is testing]]
   [dunnage.sitoa.unparser :as unparser]
   [dunnage.sitoa.xml-primitives :as xml-primitives]
   [malli.core :as m]))

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

;; ---------- :any element body crashes on non-String data ----------
;;
;; CONTEXT
;; xml-primitives.clj/xmlschema-registry maps several XSD types to :any,
;; including :base64Binary, :hexBinary, :duration, :gDay, :gMonth, :gMonthDay,
;; :gYear, :gYearMonth. The unparser dispatch in unparser.clj routes :any to
;; string-unparser, which calls (.writeCharacters w data) — a direct cast to
;; java.lang.String. Any non-String body value crashes with ClassCastException.
;;
;; SURFACED BY
;; A generative XML round-trip test in the consuming furl project hit this
;; while exercising :script/Extension :xml/value, whose :Base64Data arm is
;; typed as :org.w3.www.2001.XMLSchema/base64Binary → :any. malli's default
;; generator for :any happily emits clojure.lang.Ratio (e.g. -1/2), which
;; then trips
;;
;;   class clojure.lang.Ratio cannot be cast to class java.lang.String
;;
;; PROPOSED FIX (unparser.clj, dispatch line for :any): route :any through a
;; coercing handler instead of string-unparser:
;;
;;   :any (any-unparser x in-regex?)
;;
;; where
;;
;;   (defn any-unparser [x in-regex?]
;;     (if in-regex?
;;       (fn [data pos ^XMLStreamWriter w]
;;         (.writeCharacters w (str data))
;;         (inc pos))
;;       (fn [data ^XMLStreamWriter w]
;;         (.writeCharacters w (str data))
;;         true)))
;;
;; After the fix, flip the (is (thrown? ...)) assertions below to assert the
;; expected XML output (e.g. <val>1/2</val>, <val>42</val>).

(deftest any-element-body-rejects-non-string-data
  (let [up (mini-unparser :any)]
    (testing "String body passes (baseline)"
      (is (= "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root><val>hi</val></Root>"
             (up {:val "hi"}))))
    (testing "Ratio body throws ClassCastException"
      (is (thrown-with-msg? ClassCastException #"Ratio cannot be cast"
                            (up {:val 1/2}))))
    (testing "Long body throws ClassCastException"
      (is (thrown-with-msg? ClassCastException #"Long cannot be cast"
                            (up {:val 42}))))))
