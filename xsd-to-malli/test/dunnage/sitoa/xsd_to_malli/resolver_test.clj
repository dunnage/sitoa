(ns dunnage.sitoa.xsd-to-malli.resolver-test
  "The resolver replaces XSOM's own schemaLocation handling, including the part
  that went to the network. These tests pin both the resolution rules and the
  absence of any network path."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.xsd-to-malli.resolver :as resolver]))

(def ^:private xml-xsd (io/file "dev-resources/catalog/xml.xsd"))

(def ^:private catalog
  {"http://www.w3.org/2001/xml.xsd" xml-xsd
   "http://www.w3.org/XML/1998/namespace" xml-xsd})

(defn- main-uri []
  (resolver/->uri (io/file "dev-resources/multifile/main.xsd")))

(defn- ex-type [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:type (ex-data e)))))

(deftest relative-locations-resolve-against-the-including-document
  (let [r (resolver/catalog-resolver catalog)
        {:keys [uri open]} (resolver/resolve-schema r (main-uri) nil "types.xsd")]
    (is (str/ends-with? (str uri) "/dev-resources/multifile/types.xsd"))
    (testing "the source is readable, and readable more than once"
      (is (str/includes? (with-open [rd (open)] (slurp rd)) "BaseRecord"))
      (is (str/includes? (with-open [rd (open)] (slurp rd)) "BaseRecord")))
    (testing "a location with path segments normalizes"
      (is (= uri (:uri (resolver/resolve-schema r (main-uri) nil "./sub/../types.xsd")))))))

(deftest absolute-urls-come-from-the-catalog
  (let [r (resolver/catalog-resolver catalog)
        {:keys [uri open]} (resolver/resolve-schema
                            r (main-uri)
                            "http://www.w3.org/XML/1998/namespace"
                            "http://www.w3.org/2001/xml.xsd")]
    (is (= (resolver/->uri xml-xsd) uri))
    (is (str/includes? (with-open [rd (open)] (slurp rd)) "http://www.w3.org/XML/1998/namespace"))))

(deftest an-uncatalogued-url-fails-instead-of-reaching-the-network
  (let [r (resolver/catalog-resolver catalog)]
    (is (= :xsd-to-malli/no-network
           (ex-type #(resolver/resolve-schema r (main-uri) nil "http://example.com/other.xsd"))))
    (testing "an empty catalog rejects every absolute URL"
      (is (= :xsd-to-malli/no-network
             (ex-type #(resolver/resolve-schema (resolver/catalog-resolver) (main-uri)
                                                nil "http://www.w3.org/2001/xml.xsd")))))))

(deftest an-import-without-a-location-resolves-through-the-namespace
  (let [r (resolver/catalog-resolver catalog)]
    (is (= (resolver/->uri xml-xsd)
           (:uri (resolver/resolve-schema r (main-uri)
                                          "http://www.w3.org/XML/1998/namespace" nil))))
    (is (= :xsd-to-malli/unresolvable-import
           (ex-type #(resolver/resolve-schema r (main-uri) "urn:example:nowhere" nil))))))

(deftest a-missing-document-is-named-in-the-failure
  (let [r (resolver/catalog-resolver catalog)]
    (is (= :xsd-to-malli/missing-schema-document
           (ex-type #(resolver/resolve-schema r (main-uri) nil "absent.xsd"))))
    (is (= :xsd-to-malli/missing-schema-document
           (ex-type #(resolver/source (io/file "dev-resources/multifile/absent.xsd")))))))

(deftest a-load-cannot-start-from-the-network
  (is (= :xsd-to-malli/no-network
         (ex-type #(resolver/source "http://www.w3.org/2001/xml.xsd")))))
