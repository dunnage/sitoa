(ns dunnage.sitoa.verify-offsetdatetime-test
  "Round-trip coverage for the :time/offset-date-time unparser fix.
  Helpers `tiny` and `round-trip` are ported from
  unparser_test.clj on the unparser-test-any-element-ratio branch."
  (:require [clojure.set]
            [clojure.test :refer [deftest is]]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.unparser :as unparser]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [malli.core :as m])
  (:import (java.io StringReader)
           (java.time OffsetDateTime Duration Period Year YearMonth MonthDay Month)
           (java.nio ByteBuffer)
           (javax.xml.stream XMLStreamReader)))

(defn- tiny [body-type]
  (m/schema [:schema {:registry {:test/Root [:map {:closed true} [:val {} body-type]]}
                      :topElement "Root"}
             :test/Root]
            xml-primitives/external-registry))

(defn- round-trip [schema data]
  (let [xml ((unparser/xml-string-unparser schema) data)
        p   (parser/xml-parser schema)]
    (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. xml))]
      (p r))))

(deftest offset-date-time-preserves-offset
  (let [in {:val (OffsetDateTime/parse "2025-03-14T09:00:00-04:00")}]
    (is (= in (round-trip (tiny :time/offset-date-time) in)))))

(deftest base64-binary-round-trip
  (let [in {:val (ByteBuffer/wrap (byte-array [1 2 3]))}]
    (is (= in (round-trip (tiny :xml/base64Binary) in)))))

(deftest hex-binary-round-trip
  (let [in {:val (ByteBuffer/wrap (byte-array [4 5 6]))}]
    (is (= in (round-trip (tiny :xml/hexBinary) in)))))

(deftest duration-round-trip
  (let [in {:val (Duration/ofMinutes 15)}]
    (is (= in (round-trip (tiny :time/duration) in)))))

(deftest period-round-trip
  (let [in {:val (Period/ofMonths 3)}]
    (is (= in (round-trip (tiny :time/period) in)))))

(deftest year-round-trip
  (let [in {:val (Year/of 2026)}]
    (is (= in (round-trip (tiny :time/year) in)))))

(deftest year-month-round-trip
  (let [in {:val (YearMonth/of 2026 5)}]
    (is (= in (round-trip (tiny :time/year-month) in)))))

(deftest month-day-round-trip
  (let [in {:val (MonthDay/of 5 20)}]
    (is (= in (round-trip (tiny :time/month-day) in)))))

(deftest month-round-trip
  (let [in {:val (Month/MAY)}]
    (is (= in (round-trip (tiny :time/month) in)))))

(deftest hiccup-round-trip
  (let [in1 {:val [:val {:attr1 "val1"} [:ChildTag "text"]]}
        in2 {:val [:val [:SomeTag {:attr1 "val1"} [:ChildTag "text"]]]}]
    (is (= in1 (round-trip (tiny :xml/hiccup) in1)))
    (is (= in2 (round-trip (tiny :xml/hiccup) in2)))))

(defn- value-wrapped-mixed []
  (m/schema
   [:schema {:registry
             {:test/Root
              [:map {:closed true}
               [:addr {}
                [:map {:closed true :xml/value-wrapped true}
                 [:use {:xml/attr true :optional true} :string]
                 [:xml/value :xml/hiccup]]]]}
             :topElement "Root"}
    :test/Root]
   xml-primitives/external-registry))

(deftest value-wrapped-hiccup-omits-outer-tag-and-attrs
  "Mixed value-wrapped :xml/hiccup must not repeat the parent element tag/attrs
  inside :xml/value — only content children (and text)."
  (let [schema (value-wrapped-mixed)
        in {:addr {:use "HP"
                   :xml/value [[:streetAddressLine "1000 Hospital Lane"]
                               [:city "Ann Arbor"]
                               [:state "MI"]
                               [:postalCode "99999"]
                               [:country "US"]]}}
        xml ((unparser/xml-string-unparser schema) in)
        p (parser/xml-parser schema)
        out (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. xml))]
              (p r))]
    (is (re-find #"use=\"HP\"" xml))
    (is (= 1 (count (re-seq #"use=\"HP\"" xml)))
        "attribute must appear once on wire, not duplicated from hiccup")
    (is (not (re-find #"<addr[^>]*>\s*<addr" xml))
        "outer addr tag must not reappear inside content")
    (is (= in out))
    ;; Content children only — no [:addr ...] wrapper.
    (is (vector? (get-in out [:addr :xml/value])))
    (is (not= :addr (first (get-in out [:addr :xml/value]))))
    (is (= :streetAddressLine (ffirst (get-in out [:addr :xml/value]))))))

(defn- qualified-address-schema []
  "SCRIPT Header To/From shape: attr Qualifier + text body."
  (m/schema
   [:schema {:registry
             {:test/Root
              [:map {:closed true}
               [:To {}
                [:map {:closed true :xml/value-wrapped true}
                 [:Qualifier {:xml/attr true :optional true} :string]
                 [:xml/value {} :string]]]]}
             :topElement "Root"}
    :test/Root]
   xml-primitives/external-registry))

(deftest value-wrapped-string-keeps-text-with-attr
  "safe-next-tag skips CHARACTERS; leaf value-wrapped content must not use it
  or <To Qualifier=\"P\">1655458</To> loses :xml/value."
  (let [schema (qualified-address-schema)
        xml "<?xml version=\"1.0\"?><Root><To Qualifier=\"P\">1655458</To></Root>"
        p (parser/xml-parser schema)
        out (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. xml))]
              (p r))]
    (is (= {:To {:Qualifier "P" :xml/value "1655458"}} out))
    (let [round (round-trip schema {:To {:Qualifier "D" :xml/value "5646633808001"}})]
      (is (= {:To {:Qualifier "D" :xml/value "5646633808001"}} round)))))

(deftest value-wrapped-element-body-attrs-only-still-empty
  "IVL/PQ-style: value is element seqex; self-closing / no children is attrs-only."
  (let [schema
        (m/schema
         [:schema {:registry
                   {:test/Root
                    [:map {:closed true}
                     [:when {}
                      [:map {:closed true :xml/value-wrapped true}
                       [:nullFlavor {:xml/attr true :optional true} :string]
                       [:xml/value {}
                        [:?
                         [:or
                          [:tuple {} [:enum :low] :string]
                          [:tuple {} [:enum :high] :string]]]]]]]}
                   :topElement "Root"}
          :test/Root]
         xml-primitives/external-registry)
        empty-xml "<?xml version=\"1.0\"?><Root><when nullFlavor=\"NA\"/></Root>"
        with-low "<?xml version=\"1.0\"?><Root><when><low>20130703</low></when></Root>"
        p (parser/xml-parser schema)
        empty-out (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. empty-xml))]
                    (p r))
        low-out (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. with-low))]
                  (p r))]
    (is (= {:when {:nullFlavor "NA"}} empty-out)
        "attrs-only must not invent :xml/value")
    (is (= {:when {:xml/value [[:low "20130703"]]}} low-out)
        "element-body :or under :? still parses child tags")))