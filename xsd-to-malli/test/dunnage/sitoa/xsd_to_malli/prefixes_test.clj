(ns dunnage.sitoa.xsd-to-malli.prefixes-test
  "The parse drops xmlns declarations, so prefix bindings come from a pre-pass
  that also has to prove root bindings are enough for the document at hand."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.xsd-to-malli.prefixes :as prefixes]
            [dunnage.sitoa.xsd-to-malli.resolver :as resolver])
  (:import (java.io StringReader)))

(defn- xsd [name] (io/file ".." "bootstrapped-schema" "dev-resources" name))

(defn- scan-file [f]
  (with-open [rd (io/reader f)]
    (prefixes/scan-prefixes (resolver/->uri f) rd)))

(defn- scan-string [s]
  (with-open [rd (StringReader. s)]
    (prefixes/scan-prefixes "memory:test" rd)))

(deftest root-bindings-are-read-off-the-root-element
  (testing "fop.xsd line 2: a default binding plus fo and xs"
    (is (= {:prefix->uri {"fo" "http://www.w3.org/1999/XSL/Format"
                          "xs" "http://www.w3.org/2001/XMLSchema"}
            :default-uri "http://www.w3.org/2001/XMLSchema"}
           (scan-file (xsd "fop.xsd")))))
  (testing "a document with no default binding"
    (is (= {:prefix->uri {"xs" "http://www.w3.org/2001/XMLSchema"
                          "t" "urn:example:types"
                          "o" "urn:example:other"}
            :default-uri nil}
           (scan-file (io/file "dev-resources/multifile/main.xsd"))))))

(deftest declarations-inside-annotations-are-ignored
  ;; fop.xsd carries eight xmlns="" declarations inside xs:documentation prose.
  ;; A naive "no non-root xmlns" rule would reject the file outright.
  (is (some? (scan-file (xsd "fop.xsd"))))
  (is (some? (scan-file (xsd "XMLSchema.xsd"))))
  (is (some? (scan-file (xsd "JUnit.xsd"))))
  (is (some? (scan-file (io/file "dev-resources/catalog/xml.xsd")))))

(deftest a-rebound-prefix-fails-with-a-location
  (let [e (try
            (scan-file (io/file "dev-resources/multifile/evil-xmlns.xsd"))
            nil
            (catch clojure.lang.ExceptionInfo e e))
        data (ex-data e)]
    (is (some? e))
    (is (= :xsd-to-malli/non-root-xmlns (:type data)))
    (is (= "p" (:prefix data)))
    (is (= "type" (:attr data)))
    (is (= "p:codeType" (:value data)))
    (is (= "urn:example:types" (:root-uri data)))
    (is (= "urn:example:other" (:in-scope-uri data)))
    (testing "the failure points at the element that would be misresolved"
      (is (= 6 (:line data)))
      (is (pos? (:column data))))))

(deftest rebinding-a-prefix-to-the-same-namespace-is-not-a-conflict
  ;; Only a binding that would CHANGE a resolution is fatal; re-declaring the
  ;; same URI deeper in the document resolves identically either way.
  (is (some? (scan-string (str "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                               " xmlns:t='urn:example:types' targetNamespace='urn:example:types'>"
                               "<xs:complexType name='Fine'>"
                               "<xs:sequence xmlns:t='urn:example:types'>"
                               "<xs:element name='x' type='t:codeType'/>"
                               "</xs:sequence></xs:complexType></xs:schema>")))))

(deftest a-newly-bound-prefix-on-a-nested-element-is-fatal
  (let [data (try
               (scan-string (str "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                 " targetNamespace='urn:example:types'>"
                                 "<xs:complexType name='Evil'>"
                                 "<xs:sequence xmlns:q='urn:example:other'>"
                                 "<xs:element name='x' type='q:codeType'/>"
                                 "</xs:sequence></xs:complexType></xs:schema>"))
               nil
               (catch clojure.lang.ExceptionInfo e (ex-data e)))]
    (is (= :xsd-to-malli/non-root-xmlns (:type data)))
    (is (nil? (:root-uri data)))
    (is (= "urn:example:other" (:in-scope-uri data)))))

(deftest a-rebound-default-namespace-is-fatal
  ;; Rebinding the default namespace moves the element out of the XSD
  ;; namespace. The parser keys on local names and would read it as a schema
  ;; component anyway, resolving base="string" against the wrong namespace, so
  ;; foreign markup outside an annotation is fatal in its own right.
  (let [data (try
               (scan-string (str "<schema xmlns='http://www.w3.org/2001/XMLSchema'"
                                 " targetNamespace='urn:example:types'>"
                                 "<simpleType name='Evil'>"
                                 "<restriction xmlns='urn:example:other' base='string'/>"
                                 "</simpleType></schema>"))
               nil
               (catch clojure.lang.ExceptionInfo e (ex-data e)))]
    (is (= :xsd-to-malli/foreign-element (:type data)))
    (is (= "restriction" (:element data)))
    (is (= "urn:example:other" (:element-namespace data)))))
