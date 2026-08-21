(ns dunnage.sitoa.xsd-to-malli.equivalence-test
  "MUST-HOLD property: a schema assembled from xsd-to-malli's generated
  namespaces drives sitoa's streaming parser and unparser exactly like the XSOM
  pipeline's in-memory schema does.

  Derivation-modelled forms differ structurally from flattened ones, so raw
  form equality is not the bar. The bar is behavioural, per document:

    a) the generated schema parses to the same value as the oracle schema,
    b) unparse -> reparse is a fixpoint,
    c) m/validate agrees.

  Where the fixpoint does not hold it does not hold on the ORACLE side either -
  an upstream unparser limitation, not a difference between the two paths - so
  the test asserts the fixpoint where the oracle reaches one and asserts that
  both paths unparse to the same thing everywhere."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [dunnage.sitoa.xsd-to-malli.compiler :as compiler]
            [dunnage.sitoa.xsd-to-malli.emit :as emit]
            [dunnage.sitoa.xsd-to-malli.support :as support]
            [malli.core :as m]))

(defn- fixture-name [fixture] (:name @fixture))

;; ---------------------------------------------------------------------------
;; a) parse equality
;; ---------------------------------------------------------------------------

(deftest generated-schemas-parse-like-the-oracle-schema
  (doseq [fixture support/all-fixtures
          doc (:documents @fixture)]
    (testing (str (fixture-name fixture) " " (.getName doc))
      (let [{:keys [parse oracle]} @fixture
            expected (support/parse-doc (:parse oracle) doc)
            actual (support/parse-doc parse doc)]
        (is (= expected actual))
        (testing "and the parse really read the document"
          (is (vector? actual))
          (is (keyword? (first actual)))
          (is (some? (nth actual 1))))))))

(deftest the-xmlschema-parse-is-substantial
  (let [{:keys [parse]} @support/xmlschema
        parsed (support/parse-doc parse (first (:documents @support/xmlschema)))]
    (is (= :schema (first parsed)))
    (is (< 5000 (count (tree-seq coll? seq parsed))))))

;; ---------------------------------------------------------------------------
;; b) unparse -> reparse
;; ---------------------------------------------------------------------------

(deftest generated-schemas-round-trip-exactly-as-the-oracle-does
  (doseq [fixture support/all-fixtures
          doc (:documents @fixture)]
    (testing (str (fixture-name fixture) " " (.getName doc))
      (let [{:keys [registry top-type parse oracle]} @fixture
            generated (support/round-trip registry top-type parse doc)
            expected (support/round-trip (:registry oracle) (:top-type oracle) (:parse oracle) doc)]
        (testing "both paths unparse and reparse to the same value"
          (is (= (:reparsed expected) (:reparsed generated))))
        (when (= (:parsed expected) (:reparsed expected))
          (testing "and where the oracle reaches a fixpoint, so does the generated schema"
            (is (= (:parsed generated) (:reparsed generated)))))))))

(deftest the-fixpoint-holds-for-every-document-the-unparser-supports
  (testing "the documents where a fixpoint is expected really do reach one"
    (doseq [[fixture docs] [[support/fop (:documents @support/fop)]
                            [support/multifile (:documents @support/multifile)]
                            [support/xmlschema (:documents @support/xmlschema)]
                            [support/junit [(first (:documents @support/junit))]]]
            doc docs]
      (testing (str (fixture-name fixture) " " (.getName doc))
        (let [{:keys [registry top-type parse]} @fixture
              {:keys [parsed reparsed]} (support/round-trip registry top-type parse doc)]
          (is (= parsed reparsed))))))

  (testing "the one document that does not is an unparser limitation, not a difference"
    (let [doc (second (:documents @support/junit))
          {:keys [registry top-type parse oracle]} @support/junit
          generated (support/round-trip registry top-type parse doc)
          expected (support/round-trip (:registry oracle) (:top-type oracle) (:parse oracle) doc)]
      (is (not= (:parsed expected) (:reparsed expected))
          "the oracle drops the same child")
      (is (= (:reparsed expected) (:reparsed generated))))))

;; ---------------------------------------------------------------------------
;; c) validation
;; ---------------------------------------------------------------------------

(deftest validation-agrees-with-the-oracle
  (doseq [fixture support/all-fixtures
          doc (:documents @fixture)]
    (testing (str (fixture-name fixture) " " (.getName doc))
      (let [{:keys [registry top-type parse oracle]} @fixture
            parsed (support/parse-doc parse doc)
            root (first parsed)
            value (nth parsed 1)]
        (is (= (support/validation (support/start-schema (:registry oracle) (:top-type oracle) root) value)
               (support/validation (support/start-schema registry top-type root) value)))))))

(deftest documents-the-parser-decodes-completely-really-do-validate
  ;; The streaming parser decodes element content through the value transformer
  ;; but leaves ATTRIBUTE values as raw strings, so any document with a
  ;; non-string-typed attribute validates false on both sides. These four have
  ;; none, so plain validity is asserted rather than only agreement.
  (let [{:keys [registry top-type parse]} @support/multifile]
    (doseq [name ["strict" "price" "score" "records"]
            :let [doc (io/file (str "dev-resources/multifile/" name ".xml"))
                  parsed (support/parse-doc parse doc)]]
      (testing name
        (is (true? (m/validate (support/start-schema registry top-type (first parsed))
                               (nth parsed 1))))))))

;; ---------------------------------------------------------------------------
;; Assembled schemas behave like the v1 ones
;; ---------------------------------------------------------------------------

(deftest assembled-schemas-support-start-type-selection
  (testing "an open registry retargets, exactly as the v1 test asserts for fop"
    (is (some? (xml-primitives/update-start-type
                (:schema @support/fop) :org.w3.www.1999.XSL.Format/block_List)))
    (is (some? (xml-primitives/closed-update-start-type
                (:schema @support/fop) :org.w3.www.1999.XSL.Format/block_List)))
    (is (some? (xml-primitives/update-start-type
                (:schema @support/xmlschema) :org.w3.www.2001.XMLSchema/topLevelElement))))

  (testing "a registry holding derived types closes through the entry namespace"
    (let [closed (:closed-schema @support/multifile)
          parse (parser/xml-parser closed)]
      (is (some? closed))
      (doseq [doc (:documents @support/multifile)]
        (testing (.getName doc)
          (is (= (support/parse-doc (:parse @support/multifile) doc)
                 (support/parse-doc parse doc))))))))

;; ---------------------------------------------------------------------------
;; The new emitter's XMLSchema tree, on a classpath it cannot be shadowed on
;; ---------------------------------------------------------------------------

(deftest the-new-emitters-xmlschema-tree-holds-on-a-clean-classpath
  ;; The in-process xmlschema fixture requires per-type namespaces named
  ;; org.w3.www.2001.XMLSchema.*, and the checked-in meta-schema under
  ;; generated-src wins that resolution on the parent classpath. Those tests
  ;; therefore pin the CHECKED-IN META-SCHEMA against the oracle. The emitter's
  ;; own XMLSchema output can only be exercised in a JVM whose :paths replace
  ;; generated-src with the freshly emitted tree, so this test emits the tree
  ;; and runs dev-resources/xmlschema-clean-harness.clj (which re-asserts the
  ;; parse-equality / fixpoint / validate-agreement bar and fails loudly if the
  ;; classpath would shadow) in a child JVM. The tree path is shared with the
  ;; harness; keep them in sync.
  ;;
  ;; The same JVM is where XMLSchema's 70 derived types get their old-vs-new
  ;; m/form parity check (derived-parity-test covers the other fixtures). The
  ;; child cannot compile the XSD itself - the loader reads .xsd documents with
  ;; the meta-schema under generated-src, which is exactly what this classpath
  ;; replaces - so the plans travel as EDN.
  (let [tree "target/xsd-to-malli-test/xmlschema-clean/src"
        plans-file "target/xsd-to-malli-test/xmlschema-clean/derived-plans.edn"
        hosts-file "target/xsd-to-malli-test/xmlschema-clean/embedded-hosts.edn"
        result (support/emit-into! (io/file ".." "bootstrapped-schema" "dev-resources" "XMLSchema.xsd")
                                   "xsd" tree 'dunnage.sitoa.gen.xmlschema)
        registry (:registry (:compiled result))
        plans (into (sorted-map)
                    (comp (filter (fn [[_ v]] (compiler/derived? (:emit v))))
                          (map (fn [[k v]] [k (:plan (emit/canonicalize-form (:emit v)))])))
                    registry)
        ;; values that are not themselves derived but embed an anonymous
        ;; derived type, so the child can check those chains in place too
        hosts (into (sorted-map)
                    (comp (remove (fn [[_ v]] (compiler/derived? (:emit v))))
                          (filter (fn [[_ v]] (some compiler/derived?
                                                    (tree-seq coll? seq (:emit v)))))
                          (map (fn [[k v]] [k (emit/canonicalize-form (:emit v))])))
                    registry)]
    (is (= 70 (count plans)))
    (is (= 18 (count hosts)))
    (spit (io/file plans-file) (pr-str plans))
    (spit (io/file hosts-file) (pr-str hosts))
    (is (= plans (read-string (slurp plans-file)))
        "the plans survive the trip to the child JVM")
    (is (= hosts (read-string (slurp hosts-file))))
    (let [deps (str "{:aliases {:clean {:replace-paths [\"src\" \"resources\" \"" tree "\"]}}}")
          pb (ProcessBuilder.
              ["clojure" "-Sdeps" deps "-M:clean:test"
               "-i" "dev-resources/xmlschema-clean-harness.clj"])
          _ (.redirectErrorStream pb true)
          proc (.start pb)
          out (slurp (.getInputStream proc))
          exit (.waitFor proc)]
      (is (zero? exit) out)
      (is (.contains ^String out "DERIVED-PARITY: 70/70") out)
      (is (.contains ^String out "EMBEDDED-HOST-PARITY: 18/18") out)
      (is (.contains ^String out "XMLSCHEMA-CLEAN-CLASSPATH: PASS") out))))
