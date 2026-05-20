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
           (java.time OffsetDateTime)
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
