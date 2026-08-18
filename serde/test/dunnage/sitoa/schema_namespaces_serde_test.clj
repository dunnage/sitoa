(ns dunnage.sitoa.schema-namespaces-serde-test
  "MUST-HOLD property for dunnage.sitoa.schema-namespaces: a schema assembled
  from generated namespaces drives the streaming parser and unparser exactly
  like the legacy in-memory schema does.

  Generated output goes under target/ only. XSD inputs are referenced by file
  path: dev-resources are not on a consumer's classpath across :local/root."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.schema-namespaces :as sn]
            [dunnage.sitoa.unparser :as unparser]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [malli.core :as m])
  (:import (java.io StringReader)))

(def out-root "target/schema-namespaces-test")

(defn- xsd [name] (io/file ".." "bootstrapped-schema" "dev-resources" name))

(defn- canon-arms [top-type]
  (update-vals (into {} (drop 2 top-type)) sn/canonicalize-form))

(defn- parse-doc [parse f]
  (with-open [s (io/reader f)]
    (parse (parser/make-stream-reader {} s))))

(defn- start-schema
  "Consumer-style start type: pick the top-type arm for a document root and
  build a schema for its body. Whole-multi unparse is broken independently of
  this generator, and real consumers (furl) always pick a start type first."
  [registry top-type root-tag]
  (let [arm (some (fn [[tag arm]] (when (= tag root-tag) arm)) (drop 2 top-type))]
    ;; The arm is [:tuple [:enum tag] type-ref], with properties when the
    ;; emitter kept any, so the type reference is the last child either way.
    (m/schema [:schema {:registry registry :topElement (name root-tag)} (peek arm)]
              xml-primitives/external-registry)))

(defn- round-trip [registry top-type parse f]
  (let [parsed (parse-doc parse f)
        root-tag (first parsed)
        emit (unparser/xml-string-unparser (start-schema registry top-type root-tag))
        reparsed (with-open [s (StringReader. (emit (nth parsed 1)))]
                   (parse (parser/make-stream-reader {} s)))]
    {:parsed parsed :reparsed reparsed}))

;; ---------------------------------------------------------------------------
;; fop.xsd
;; ---------------------------------------------------------------------------

(def fop-run
  (delay
    (let [out (str out-root "/fop/src")
          result (sn/xsd->namespaces! {:default-ns "fop"
                                       :out-dir out
                                       :entry-ns 'dunnage.sitoa.gen.fop}
                                      (xsd "fop.xsd"))
          xsom (bs/parse-xsd (xsd "fop.xsd"))
          legacy-registry (bs/xsd->registry {:default-ns "fop"} xsom)
          legacy-top (bs/xsd->top-type {:default-ns "fop"} xsom)]
      (sn/ensure-on-classpath! out)
      (require 'dunnage.sitoa.gen.fop)
      (let [registry @(resolve 'dunnage.sitoa.gen.fop/registry)
            top-type @(resolve 'dunnage.sitoa.gen.fop/top-type)
            schema ((resolve 'dunnage.sitoa.gen.fop/make-schema))]
        (assoc result
               :registry registry
               :top-type top-type
               :schema schema
               :parse (parser/xml-parser schema)
               :legacy-registry legacy-registry
               :legacy-top legacy-top
               :legacy-parse (parser/xml-parser
                              (xml-primitives/make-schema legacy-registry legacy-top)))))))

(def fop-documents ["dev-resources/fopsample1.xml" "dev-resources/table-borders.fo"])

(deftest fop-generated-registry-equals-the-legacy-registry
  (let [{:keys [registry top-type legacy-registry legacy-top]} @fop-run]
    (is (= (sn/canonicalize-registry legacy-registry) registry))
    (is (= (canon-arms legacy-top) (canon-arms top-type)))))

(deftest fop-generated-schema-parses-like-the-legacy-schema
  (let [{:keys [parse legacy-parse]} @fop-run]
    (doseq [doc fop-documents]
      (testing doc
        (is (= (parse-doc legacy-parse (io/file doc))
               (parse-doc parse (io/file doc))))))))

(deftest fop-generated-schema-round-trips-to-a-fixpoint
  (let [{:keys [registry top-type parse]} @fop-run]
    (doseq [doc fop-documents]
      (testing doc
        (let [{:keys [parsed reparsed]} (round-trip registry top-type parse (io/file doc))]
          (is (= parsed reparsed)))))))

(deftest fop-assembled-schema-supports-start-type-selection
  (let [{:keys [schema]} @fop-run]
    (is (some? (xml-primitives/update-start-type
                schema :org.w3.www.1999.XSL.Format/block_List)))
    (is (some? (xml-primitives/closed-update-start-type
                schema :org.w3.www.1999.XSL.Format/block_List)))))

;; ---------------------------------------------------------------------------
;; XMLSchema.xsd
;;
;; The schema for schemas is the hostile input: its targetNamespace IS the
;; builtin XSD namespace, its type graph is heavily cyclic, and it is its own
;; sample instance. It runs the same MUST-HOLD as fop.xsd.
;; ---------------------------------------------------------------------------

(def xmlschema-run
  (delay
    (let [out (str out-root "/xsd/src")
          result (sn/xsd->namespaces! {:default-ns "xsd"
                                       :out-dir out
                                       :entry-ns 'dunnage.sitoa.gen.xsd}
                                      (xsd "XMLSchema.xsd"))
          xsom (bs/parse-xsd (xsd "XMLSchema.xsd"))
          legacy-registry (bs/xsd->registry {:default-ns "xsd"} xsom)
          legacy-top (bs/xsd->top-type {:default-ns "xsd"} xsom)]
      (sn/ensure-on-classpath! out)
      (require 'dunnage.sitoa.gen.xsd)
      (let [registry @(resolve 'dunnage.sitoa.gen.xsd/registry)
            top-type @(resolve 'dunnage.sitoa.gen.xsd/top-type)
            schema ((resolve 'dunnage.sitoa.gen.xsd/make-schema))]
        (assoc result
               :registry registry
               :top-type top-type
               :schema schema
               :parse (parser/xml-parser schema)
               :legacy-registry legacy-registry
               :legacy-top legacy-top
               :legacy-parse (parser/xml-parser
                              (xml-primitives/make-schema legacy-registry legacy-top)))))))

(deftest xmlschema-cyclic-builtin-namespaced-registry-emits-and-loads
  (let [{:keys [legacy-registry registry top-type legacy-top schema included]} @xmlschema-run]
    (testing "every own key lives in the builtin XSD namespace"
      (is (pos? (count included)))
      (is (every? #(= "org.w3.www.2001.XMLSchema" (namespace %)) included)))
    (testing "the type graph really is cyclic, so per-type requires could not work"
      (is (contains? (sn/form-refs (set (keys legacy-registry))
                                   (get legacy-registry :org.w3.www.2001.XMLSchema/all))
                     :org.w3.www.2001.XMLSchema/allModel)))
    (testing "loaded namespaces reassemble the legacy registry exactly"
      (is (= (sn/canonicalize-registry legacy-registry) registry))
      (is (= (canon-arms legacy-top) (canon-arms top-type))))
    (testing "own entries shadow nothing and builtins survive the merge"
      (is (= :string (get registry :org.w3.www.2001.XMLSchema/string))))
    (testing "the assembled schema builds and retargets"
      (is (some? schema))
      (is (some? (xml-primitives/update-start-type
                  schema :org.w3.www.2001.XMLSchema/topLevelElement))))))

(deftest xmlschema-generated-schema-parses-like-the-legacy-schema
  ;; XMLSchema.xsd is its own sample instance.
  (let [{:keys [parse legacy-parse]} @xmlschema-run
        parsed (parse-doc parse (xsd "XMLSchema.xsd"))]
    (is (= (parse-doc legacy-parse (xsd "XMLSchema.xsd")) parsed))
    (testing "the parse is substantial, not an empty shell"
      (is (= :schema (first parsed)))
      (is (< 5000 (count (tree-seq coll? seq parsed)))))))

(deftest xmlschema-generated-schema-round-trips-to-a-fixpoint
  (let [{:keys [registry top-type parse]} @xmlschema-run
        {:keys [parsed reparsed]} (round-trip registry top-type parse (xsd "XMLSchema.xsd"))]
    (is (= parsed reparsed))))
