(ns dunnage.sitoa.schema-namespaces-test
  "Generation-level tests for dunnage.sitoa.schema-namespaces.

  Output goes under target/ only. XSD inputs are referenced by file path
  because dev-resources lives on the :dev alias, not on the module classpath."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.schema-namespaces :as sn]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [malli.core :as m])
  (:import (java.io File)
           (java.nio.file Files)
           (java.security MessageDigest)))

(def out-root "target/schema-namespaces-test")

(def builtin-keys (set (keys xml-primitives/xmlschema-registry)))

(defn- xsd [name] (io/file "dev-resources" name))

(defn- legacy [default-ns f]
  (let [xsom (bs/parse-xsd f)
        context {:default-ns default-ns}]
    {:registry (bs/xsd->registry context xsom)
     :top-type (bs/xsd->top-type context xsom)}))

(defn- canon-arms [top-type]
  (update-vals (into {} (drop 2 top-type)) sn/canonicalize-form))

(defn- sha256 [^File f]
  (->> (Files/readAllBytes (.toPath f))
       (.digest (MessageDigest/getInstance "SHA-256"))
       (map #(format "%02x" %))
       (str/join)))

(defn- tree-digest [root]
  (let [root-file (io/file root)
        prefix (inc (count (.getPath root-file)))]
    (into (sorted-map)
          (comp (filter #(.isFile ^File %))
                (map (fn [^File f] [(subs (.getPath f) prefix) (sha256 f)])))
          (file-seq root-file))))

;; ---------------------------------------------------------------------------
;; Unit-level behaviour
;; ---------------------------------------------------------------------------

(deftest canonicalize-form-moves-only-attribute-rows
  (testing "attribute rows are sorted among themselves at the indexes they held"
    (is (= [:map {:closed true}
            [:a {:xml/attr true} :string]
            [:zz {} :string]
            [:b {:xml/attr true} :string]]
           (sn/canonicalize-form
            [:map {:closed true}
             [:b {:xml/attr true} :string]
             [:zz {} :string]
             [:a {:xml/attr true} :string]]))))
  (testing "element row order is load-bearing and is never touched"
    (is (= [:map {} [:z {} :string] [:a {} :string]]
           (sn/canonicalize-form [:map {} [:z {} :string] [:a {} :string]]))))
  (testing "seqex children keep their order"
    (is (= [:cat {} [:z {} :string] [:a {} :string]]
           (sn/canonicalize-form [:cat {} [:z {} :string] [:a {} :string]]))))
  (testing "nested maps are canonicalized too"
    (is (= [:cat {} [:map {} [:a {:xml/attr true} :string] [:b {:xml/attr true} :string]]]
           (sn/canonicalize-form
            [:cat {} [:map {} [:b {:xml/attr true} :string] [:a {:xml/attr true} :string]]])))))

(deftest form-refs-collects-both-reference-shapes
  (let [registry-keys #{:ns/A :ns/B :org.w3.www.2001.XMLSchema/string}]
    (is (= #{:ns/A :ns/B :org.w3.www.2001.XMLSchema/string}
           (sn/form-refs registry-keys
                         [:map {} [:x {} [:ref :ns/A]]
                          [:y {} [:and [:re "x"] :ns/B]]
                          [:z {} [:ref :org.w3.www.2001.XMLSchema/string]]])))
    (is (= #{} (sn/form-refs registry-keys [:map {} [:x {} :string]])))))

(deftest naming-is-clojure-compatible
  (testing "a dot inside the local name would create a bogus namespace segment"
    (is (= 'fop.border-start-width_length (sn/kw->ns-sym :fop/border-start-width.length)))
    (is (= "fop/border_start_width_length.cljc"
           (sn/ns-sym->path (sn/kw->ns-sym :fop/border-start-width.length)))))
  (testing "dashes stay in the symbol and become underscores in the path"
    (is (= 'junit.pre-string (sn/kw->ns-sym :junit/pre-string)))
    (is (= "junit/pre_string.cljc" (sn/ns-sym->path 'junit.pre-string)))
    (is (= "org/w3/www/1999/XSL/Format/block_List.cljc"
           (sn/ns-sym->path 'org.w3.www.1999.XSL.Format.block_List)))))

(deftest bare-keys-and-path-collisions-fail-fast
  (testing "registry keys without a namespace cannot be mapped to files"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"without a namespace"
         (sn/emit-namespaces! {:out-dir (str out-root "/guard/src") :entry-ns 'gen.guard}
                              {:Foo :string}
                              [:multi {:dispatch first}]))))
  (testing "two keys that munge onto the same file are rejected before writing"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"collide"
         (sn/emit-namespaces! {:out-dir (str out-root "/guard/src") :entry-ns 'gen.guard}
                              {:a/b-c :string :a/b_c :string}
                              [:multi {:dispatch first}])))))

(deftest seq-name-collision-fails-fast
  (let [f (io/file out-root "seq-collision.xsd")]
    (io/make-parents f)
    (spit f (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                 "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">\n"
                 "  <xs:complexType name=\"Foo\">\n"
                 "    <xs:sequence><xs:element name=\"a\" type=\"xs:string\"/></xs:sequence>\n"
                 "  </xs:complexType>\n"
                 "  <xs:complexType name=\"Foo-seq\">\n"
                 "    <xs:sequence><xs:element name=\"b\" type=\"xs:string\"/></xs:sequence>\n"
                 "  </xs:complexType>\n"
                 "  <xs:element name=\"root\" type=\"Foo\"/>\n"
                 "</xs:schema>\n"))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Foo-seq"
         (sn/xsd->namespaces! {:default-ns "coll"
                               :out-dir (str out-root "/seq-collision/src")
                               :entry-ns 'gen.seq-collision}
                              f)))))

;; ---------------------------------------------------------------------------
;; JUnit.xsd: generate, load, compare
;; ---------------------------------------------------------------------------

(def junit-run
  (delay
    (let [out (str out-root "/junit/src")
          result (sn/xsd->namespaces! {:default-ns "junit"
                                       :out-dir out
                                       :entry-ns 'dunnage.sitoa.gen.junit}
                                      (xsd "JUnit.xsd"))]
      (sn/ensure-on-classpath! out)
      (require 'dunnage.sitoa.gen.junit)
      (assoc result :out out :legacy (legacy "junit" (xsd "JUnit.xsd"))))))

(deftest junit-namespaces-are-equivalent-to-the-legacy-registry
  (let [{:keys [out included legacy]} @junit-run]
    (testing "the -seq dual lives in its base type's file"
      (is (contains? included :junit/testsuite))
      (is (contains? included :junit/testsuite-seq))
      (is (.exists (io/file out "junit/testsuite.cljc")))
      (is (not (.exists (io/file out "junit/testsuite_seq.cljc")))))
    (testing "the assembled registry is data-equal to the canonicalized legacy one"
      (is (= (sn/canonicalize-registry (:registry legacy))
             @(resolve 'dunnage.sitoa.gen.junit/registry))))
    (testing "top-type arms are equal"
      (is (= (canon-arms (:top-type legacy))
             (canon-arms @(resolve 'dunnage.sitoa.gen.junit/top-type)))))
    (testing "the entry namespace builds a schema consumers can retarget"
      (let [schema ((resolve 'dunnage.sitoa.gen.junit/make-schema))]
        (is (some? schema))
        (is (some? (xml-primitives/update-start-type schema :junit/testsuite)))
        (is (some? (xml-primitives/closed-update-start-type schema :junit/testsuite)))))))

(deftest generated-type-files-have-no-requires
  (testing "cross-type edges stay data, so a cyclic XSD graph cannot cycle requires"
    (let [files (->> (file-seq (io/file (:out @junit-run) "junit"))
                     (filter #(.isFile ^File %)))]
      (is (seq files))
      (doseq [^File f files]
        (is (not (str/includes? (slurp f) ":require"))
            (str (.getPath f) " should not require anything"))))))

;; ---------------------------------------------------------------------------
;; fop.xsd: determinism and trim parity
;; ---------------------------------------------------------------------------

;; fop.xsd is the only input in the repo big enough to exercise attribute-order
;; instability. Parsing it twice is the perturbation: XSOM iterates attribute
;; uses in an identity-hash sensitive order, so a second parse in the same JVM
;; reaches the generator with a different row order.
(def fop-runs
  (delay
    (let [a-dir (str out-root "/fop-a/src")
          a (sn/xsd->namespaces! {:default-ns "fop" :out-dir a-dir
                                  :entry-ns 'dunnage.sitoa.gen.fop-det}
                                 (xsd "fop.xsd"))
          _ (dotimes [_ 200000] (Object.))
          b-dir (str out-root "/fop-b/src")
          b (sn/xsd->namespaces! {:default-ns "fop" :out-dir b-dir
                                  :entry-ns 'dunnage.sitoa.gen.fop-det}
                                 (xsd "fop.xsd"))]
      {:a a :a-dir a-dir :b b :b-dir b-dir})))

(deftest fop-emission-is-deterministic
  (let [{:keys [a-dir b-dir]} @fop-runs]
    (is (< 200 (count (tree-digest a-dir))))
    (is (= (tree-digest a-dir) (tree-digest b-dir)))))

(deftest reachability-matches-trim-registry-for-top-types
  (let [{:keys [registry top-type]} (legacy "fop" (xsd "fop.xsd"))
        schema (xml-primitives/make-schema registry top-type)
        seeds [:org.w3.www.1999.XSL.Format/block_List
               :org.w3.www.1999.XSL.Format/marker_List]
        trimmed (bs/trim-registry-for-top-types (-> schema m/properties :registry) seeds)]
    (is (= (set (keys trimmed)) (sn/reachable-keys registry seeds)))
    (testing ":top-types restricts the entry registry to the same closure"
      (let [result (sn/emit-namespaces! {:out-dir (str out-root "/fop-trim/src")
                                         :entry-ns 'dunnage.sitoa.gen.fop-trim
                                         :top-types seeds}
                                        registry top-type)
            own-trimmed (into #{} (remove builtin-keys) (keys trimmed))
            own-all (into #{} (remove builtin-keys) (keys registry))]
        (is (= own-trimmed (:included result)))
        (testing "unreachable types still get a file"
          (is (< (count own-trimmed) (count own-all))))))
    (testing "an unknown seed fails fast"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"absent from registry"
                            (sn/reachable-keys registry [:fop/nope]))))))
