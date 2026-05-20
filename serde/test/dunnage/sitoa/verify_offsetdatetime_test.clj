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
