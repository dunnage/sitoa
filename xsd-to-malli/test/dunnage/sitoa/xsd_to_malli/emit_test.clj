(ns dunnage.sitoa.xsd-to-malli.emit-test
  "The emitted tree has the shape the v1 emitter established, with derivation
  as the only new kind of edge.

  emit.clj copies six helpers out of dunnage.sitoa.schema-namespaces because
  requiring that namespace would drag XSOM onto the generator's runtime
  classpath. Copies drift, so each one is asserted against its original here,
  on the meta-schema registry rather than on toy input."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.schema-namespaces :as sn]
            [dunnage.sitoa.xsd-to-malli.compiler :as compiler]
            [dunnage.sitoa.xsd-to-malli.emit :as emit]
            [dunnage.sitoa.xsd-to-malli.support :as support]
            [dunnage.sitoa.xsd-meta :as xsd-meta]))

(defn- ex-type [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))

;; ---------------------------------------------------------------------------
;; The copies still agree with their originals
;; ---------------------------------------------------------------------------

(deftest copied-helpers-agree-with-the-v1-emitter
  (let [registry (:registry (:compiled @support/xmlschema))
        forms (update-vals registry :form)]
    (testing "canonicalize-form and canonicalize-registry"
      (is (= (sn/canonicalize-registry forms) (emit/canonicalize-registry forms)))
      (doseq [[_ form] (sort-by key forms)]
        (is (= (sn/canonicalize-form form) (emit/canonicalize-form form)))))

    (testing "form-refs"
      (let [ks (set (keys forms))]
        (doseq [[_ form] (sort-by key forms)]
          (is (= (sn/form-refs ks form) (emit/form-refs ks form))))))

    (testing "reachable-keys"
      (let [seeds [:org.w3.www.2001.XMLSchema/topLevelComplexType
                   :org.w3.www.2001.XMLSchema/topLevelElement]]
        (is (= (sn/reachable-keys forms seeds) (emit/reachable-keys forms seeds)))))

    (testing "kw->ns-sym and ns-sym->path"
      (doseq [k (concat (keys forms)
                        [:a.b/C.d :urn.example/Foo-seq :x/y-z])]
        (is (= (sn/kw->ns-sym k) (emit/kw->ns-sym k)))
        (is (= (sn/ns-sym->path (sn/kw->ns-sym k))
               (emit/ns-sym->path (emit/kw->ns-sym k))))))))

(deftest the-checked-in-meta-schema-is-what-the-v1-emitter-writes
  (testing "the tree the loader parses .xsd documents with is still loadable"
    (is (= 159 (count xsd-meta/registry))
        "114 own keys plus the 45 built-in datatypes merged in")
    (is (some? (xsd-meta/make-schema)))))

;; ---------------------------------------------------------------------------
;; File shapes
;; ---------------------------------------------------------------------------

(defn- read-file [fixture path]
  (slurp (io/file (:out @fixture) path)))

(deftest a-type-that-derives-from-nothing-is-plain-data
  (let [src (read-file support/multifile "types/example/BaseRecord.cljc")]
    (is (str/includes? src "(ns\n types.example.BaseRecord"))
    (is (not (str/includes? src ":require")))
    (is (not (str/includes? src "reify")))
    (is (re-find #"\(def\s+deps" src))
    (is (re-find #"\(def\s+sch\s" src))
    (is (re-find #"\(def\s+sch-seq" src))))

(deftest a-derived-type-requires-its-base-and-builds-on-its-schema
  (let [src (read-file support/multifile "types/example/ExtendedRecord.cljc")]
    (is (str/includes? src "[types.example.BaseRecord]"))
    (is (str/includes? src "[dunnage.sitoa.xsd-to-malli.runtime :as rt]"))
    (is (str/includes? src "[malli.core :as m]"))
    (is (str/includes? src "reify\n  m/IntoSchema"))
    (is (str/includes? src "rt/derive-complex"))
    (testing "the base's rows are read from its schema, never copied in"
      (is (str/includes? src "types.example.BaseRecord/sch"))
      (is (not (str/includes? src ":createdBy"))))))

(deftest a-restriction-folds-to-plain-data-with-inheritance-resolved
  ;; Broad rule: restriction restates, extension derives. StrictRecord is a
  ;; complexContent restriction of BaseRecord, so its file is plain data with
  ;; the inherited attribute row copied in statically - no reify, no requires.
  (let [src (read-file support/multifile "types/example/StrictRecord.cljc")]
    (is (not (str/includes? src "reify")))
    (is (not (str/includes? src ":require")))
    (is (str/includes? src ":createdBy"))
    (is (re-find #"\(def\s+sch\s" src))
    (is (re-find #"\(def\s+sch-seq" src))))

(deftest the-entry-namespace-assembles-everything
  (let [src (read-file support/multifile "dunnage/sitoa/gen/multifile.cljc")]
    (is (re-find #"\(def\s+registry" src))
    (is (str/includes? src "merge\n  xml-primitives/xmlschema-registry"))
    (is (str/includes? src ":types.example/ExtendedRecord types.example.ExtendedRecord/sch"))
    (is (str/includes? src ":types.example/ExtendedRecord-seq types.example.ExtendedRecord/sch-seq"))
    (is (re-find #"\(def\s+top-type" src))
    (is (re-find #"\(defn\s+make-schema" src))
    (testing "and closes over a realized registry, which reified values need"
      (is (str/includes? src "rt/realize-registry")))))

(deftest an-anonymous-derived-type-lands-inline-in-its-owner
  (let [src (read-file support/junit "dunnage/sitoa/gen/junit.cljc")]
    (testing "JUnit's testsuites arm nests an extension of testsuite"
      (is (str/includes? src "[junit.testsuite]"))
      (is (str/includes? src "junit.testsuite/sch"))
      (is (str/includes? src "rt/derive-complex")))))

(deftest every-generated-namespace-loads-and-serves-its-registry-key
  (doseq [fixture support/all-fixtures]
    (testing (:name @fixture)
      (let [{:keys [registry compiled included]} @fixture]
        (is (= (set (keys (:registry compiled))) included))
        (is (every? #(contains? registry %) included))
        (is (some? (:schema @fixture)))))))

;; ---------------------------------------------------------------------------
;; Reachability trimming
;; ---------------------------------------------------------------------------

(deftest top-types-trim-the-entry-registry-but-not-the-files
  (let [out (str support/out-root "/trimmed/src")
        result (emit/compile-xsd->namespaces!
                {:resolver (support/resolver)
                 :default-ns "multi"
                 :out-dir out
                 :entry-ns 'dunnage.sitoa.gen.trimmed
                 :top-types [:types.example/Price]}
                support/multifile-xsd)]
    (is (= #{:types.example/Price :types.example/codeType} (:included result)))
    (testing "unreachable types still get a file"
      (is (.exists (io/file out "types/example/StrictRecord.cljc"))))
    (testing "a seed that is not a registry key is refused"
      (is (= :xsd-to-malli/unknown-top-type
             (ex-type #(emit/compile-xsd->namespaces!
                        {:resolver (support/resolver)
                         :default-ns "multi"
                         :out-dir (str support/out-root "/trimmed-bad/src")
                         :entry-ns 'dunnage.sitoa.gen.trimmed-bad
                         :top-types [:types.example/Absent]}
                        support/multifile-xsd)))))))

;; ---------------------------------------------------------------------------
;; The require graph
;; ---------------------------------------------------------------------------

(deftest the-require-graph-is-exactly-the-derivation-graph
  (doseq [fixture support/all-fixtures]
    (testing (:name @fixture)
      (let [registry (:registry (:compiled @fixture))
            derived (into #{}
                          (comp (filter (fn [[_ v]] (compiler/derived? (:emit v)))) (map key))
                          registry)
            with-requires (into #{}
                                (comp (filter (fn [[_ v]] (seq (compiler/form-deps (:emit v)))))
                                      (map key))
                                registry)]
        (testing "every derived key really is emitted as code"
          (is (every? #(compiler/derived? (:emit (get registry %))) derived)))
        (is (<= (count derived) (count with-requires)))))))

(deftest a-require-cycle-is-fatal
  ;; Derivation cycles are already refused when the symbol table is built, and
  ;; an anonymous derived type can only name a global base, so this cannot
  ;; arise from an XSD. The check is a tripwire, and a tripwire nothing can
  ;; trip is worth nothing - so it is tripped directly.
  (let [node (fn [requires] (compiler/->Derived {} #{} requires))
        groups (sorted-map :a/A {:sch (node '#{b.B})}
                           :b/B {:sch (node '#{a.A})})]
    (is (= :xsd-to-malli/require-cycle
           (ex-type #(@#'emit/check-require-acyclic! groups))))
    (is (some? (@#'emit/check-require-acyclic!
                (sorted-map :a/A {:sch (node '#{b.B})} :b/B {:sch []}))))))
