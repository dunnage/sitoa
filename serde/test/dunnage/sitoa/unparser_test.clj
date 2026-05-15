(ns dunnage.sitoa.unparser-test
  "Synthetic-schema round-trips. Each deftest asserts the value coming
  back from unparse -> XML -> parse equals the value going in. Currently
  failing tests describe real bugs in the pipeline."
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
  ;; Unparser writes (str (.toInstant data)) which normalises to Z.
  (let [in {:val (OffsetDateTime/parse "2025-03-14T09:00:00-04:00")}]
    (is (= in (round-trip (tiny :time/offset-date-time) in)))))

(deftest empty-sequential-preserved
  ;; [] writes no children; parser then yields nil/absent key.
  (let [in {:val []}]
    (is (= in (round-trip (tiny [:sequential :string]) in)))))

(deftest string-cr-preserved
  ;; XML 1.0 sec 2.11 EOL normalisation collapses \r -> \n on parse.
  (let [in {:val "a\rb"}]
    (is (= in (round-trip (tiny :string) in)))))

(def cat-with-bare-ref-schema
  (m/schema [:schema {:registry {:test/Inner [:tuple [:enum :Middle] :string]
                                 :test/Root [:cat
                                             [:tuple [:enum :First] :string]
                                             [:ref :test/Inner]
                                             [:tuple [:enum :Last]  :string]]}
                      :topElement "Root"}
             :test/Root]
            xml-primitives/external-registry))

(deftest cat-with-bare-ref-child-unparses
  ;; -ref-discriminator returns an arity-1 fn but :cat invokes it as
  ;; (data pos) -- ArityException.
  (let [in [[:First "a"] [:Middle "b"] [:Last "c"]]]
    (is (= in (round-trip cat-with-bare-ref-schema in)))))

(def map-with-int-attr-schema
  (m/schema [:schema {:registry {:test/Root [:map {:closed true :xml/value-wrapped true}
                                             [:n {:xml/attr true} :int]
                                             [:xml/value {} :string]]}
                      :topElement "Root"}
             :test/Root]
            xml-primitives/external-registry))

(deftest map-with-int-attribute-unparses
  ;; The attribute unparser casts the value to String -- Long crashes
  ;; with ClassCastException.
  (let [in {:n 5 :xml/value "hello"}]
    (is (= in (round-trip map-with-int-attr-schema in)))))
