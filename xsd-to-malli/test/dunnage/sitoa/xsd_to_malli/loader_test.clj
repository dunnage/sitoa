(ns dunnage.sitoa.xsd-to-malli.loader-test
  "Loading a schema set: the checked-in meta-schema parses .xsd documents, and
  the include/import graph terminates, coerces chameleons and rejects the
  namespace mismatches XSOM used to reject.

  Fixtures for the failure paths are written under target/ at test time; only
  the well-formed multi-file fixture is checked in."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.xsd-meta :as xsd-meta]
            [dunnage.sitoa.xsd-to-malli.ast :as ast]
            [dunnage.sitoa.xsd-to-malli.loader :as loader]
            [dunnage.sitoa.xsd-to-malli.resolver :as resolver]))

(def ^:private out-root "target/loader-test")

(def ^:private catalog
  {"http://www.w3.org/2001/xml.xsd" (io/file "dev-resources/catalog/xml.xsd")
   "http://www.w3.org/XML/1998/namespace" (io/file "dev-resources/catalog/xml.xsd")})

(defn- opts []
  {:resolver (resolver/catalog-resolver catalog) :default-ns "test"})

(defn- xsd [name] (io/file ".." "bootstrapped-schema" "dev-resources" name))

(defn- multifile [name] (io/file "dev-resources/multifile" name))

(defn- write-fixture!
  "Write a throwaway .xsd under target/ and return its File."
  [dir name content]
  (let [f (io/file out-root dir name)]
    (io/make-parents f)
    (spit f content)
    f))

(defn- ex-type [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:type (ex-data e)))))

(defn- basenames [order]
  (mapv (fn [[uri ns]] [(last (str/split uri #"/")) ns]) order))

;; ---------------------------------------------------------------------------
;; The checked-in meta-schema
;; ---------------------------------------------------------------------------

(def ^:private meta-parse
  (delay (parser/xml-parser (xsd-meta/make-schema))))

(defn- parse-instance [f]
  (with-open [s (io/reader f)]
    (@meta-parse (parser/make-stream-reader {} s))))

(deftest the-checked-in-meta-schema-parses-schema-documents
  (doseq [[f expected-tns components]
          [[(xsd "fop.xsd") "http://www.w3.org/1999/XSL/Format" 355]
           [(xsd "XMLSchema.xsd") "http://www.w3.org/2001/XMLSchema" 127]
           [(xsd "JUnit.xsd") nil 6]
           [(io/file "dev-resources/catalog/xml.xsd") "http://www.w3.org/XML/1998/namespace" 9]]]
    (testing (str f)
      (let [[tag props] (parse-instance f)]
        (is (= :schema tag))
        (is (= expected-tns (:targetNamespace props)))
        (is (= components (count (:xml/value props))))))))

;; ---------------------------------------------------------------------------
;; The include/import graph
;; ---------------------------------------------------------------------------

(deftest the-multi-file-fixture-loads-into-four-documents
  (let [{:keys [documents order root]} (loader/load-documents (opts) (multifile "main.xsd"))]
    (is (= 4 (count documents)))
    (is (= 4 (count order)))
    (testing "every document is keyed by [uri namespace]"
      (is (= [["main.xsd" "urn:example:types"]
              ["types.xsd" "urn:example:types"]
              ["other.xsd" "urn:example:other"]
              ["common.xsd" "urn:example:types"]]
             (basenames order))))
    (testing "the root document is the one the load started from"
      (is (str/ends-with? (first root) "/main.xsd")))
    (testing "the chameleon include coerces common.xsd, which declares no namespace of its own"
      (let [common (get documents (first (filter #(str/ends-with? (first %) "/common.xsd") order)))]
        (is (nil? (:target-namespace common)))
        (is (= "urn:example:types" (:coerced-namespace common)))))))

(deftest a-loaded-document-carries-its-bindings-directives-and-components
  (let [{:keys [documents order]} (loader/load-documents (opts) (multifile "main.xsd"))
        main (get documents (first order))]
    (is (= "urn:example:types" (:target-namespace main)))
    (is (= "qualified" (:element-form-default main)))
    (is (= "unqualified" (:attribute-form-default main)))
    (is (= {"xs" "http://www.w3.org/2001/XMLSchema"
            "t" "urn:example:types"
            "o" "urn:example:other"}
           (:prefix->uri main)))
    (is (= [{:location "types.xsd"}] (:includes main)))
    (is (= [{:namespace "urn:example:other" :location "other.xsd"}] (:imports main)))
    (testing "include and import directives are not components"
      (is (= [:element :element :element :element :element]
             (mapv :kind (:components main)))))))

(deftest annotations-become-documentation-and-nothing-else
  (let [{:keys [documents order]} (loader/load-documents (opts) (xsd "JUnit.xsd"))
        junit (get documents (first order))
        testsuite (first (filter #(and (= :complexType (:kind %))
                                       (= "testsuite" (get-in % [:attrs :name])))
                                 (:components junit)))]
    (testing "the document-level annotation is not a component"
      (is (every? #{:element :complexType :simpleType} (map :kind (:components junit)))))
    (testing "a declaration keeps the first documentation string"
      (is (str/starts-with? (:doc testsuite) "Contains the results")))
    (testing "annotation subtrees never become children"
      (is (empty? (filter #(= :annotation (:kind %))
                          (tree-seq :children :children testsuite)))))))

(deftest a-single-file-load-needs-no-resolver
  (let [{:keys [order]} (loader/load-documents {} (xsd "fop.xsd"))]
    (is (= 1 (count order)))))

(deftest an-import-is-resolved-through-the-catalog
  (let [{:keys [order]} (loader/load-documents (opts) (xsd "XMLSchema.xsd"))]
    (is (= [["XMLSchema.xsd" "http://www.w3.org/2001/XMLSchema"]
            ["xml.xsd" "http://www.w3.org/XML/1998/namespace"]]
           (basenames order)))))

(deftest mutually-including-documents-terminate
  (let [a (write-fixture! "mutual" "a.xsd"
                          (str "<?xml version='1.0'?>"
                               "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                               " targetNamespace='urn:example:loop'>"
                               "<xs:include schemaLocation='b.xsd'/>"
                               "<xs:simpleType name='A'><xs:restriction base='xs:string'/></xs:simpleType>"
                               "</xs:schema>"))]
    (write-fixture! "mutual" "b.xsd"
                    (str "<?xml version='1.0'?>"
                         "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                         " targetNamespace='urn:example:loop'>"
                         "<xs:include schemaLocation='a.xsd'/>"
                         "<xs:simpleType name='B'><xs:restriction base='xs:string'/></xs:simpleType>"
                         "</xs:schema>"))
    (let [{:keys [order]} (loader/load-documents (opts) a)]
      (is (= [["a.xsd" "urn:example:loop"] ["b.xsd" "urn:example:loop"]]
             (basenames order))))))

(deftest a-diamond-import-loads-the-shared-document-once
  (let [main (write-fixture! "diamond" "main.xsd"
                             (str "<?xml version='1.0'?>"
                                  "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                  " targetNamespace='urn:example:main'>"
                                  "<xs:import namespace='urn:example:left' schemaLocation='left.xsd'/>"
                                  "<xs:import namespace='urn:example:right' schemaLocation='right.xsd'/>"
                                  "</xs:schema>"))]
    (doseq [side ["left" "right"]]
      (write-fixture! "diamond" (str side ".xsd")
                      (str "<?xml version='1.0'?>"
                           "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                           " targetNamespace='urn:example:" side "'>"
                           "<xs:import namespace='urn:example:shared' schemaLocation='shared.xsd'/>"
                           "</xs:schema>")))
    (write-fixture! "diamond" "shared.xsd"
                    (str "<?xml version='1.0'?>"
                         "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                         " targetNamespace='urn:example:shared'>"
                         "<xs:simpleType name='S'><xs:restriction base='xs:string'/></xs:simpleType>"
                         "</xs:schema>"))
    (let [{:keys [order]} (loader/load-documents (opts) main)]
      (is (= 4 (count order)))
      (is (= 1 (count (filter #(str/ends-with? (first %) "/shared.xsd") order)))))))

(deftest one-chameleon-included-from-two-namespaces-is-two-component-sets
  (let [main (write-fixture! "twice" "main.xsd"
                             (str "<?xml version='1.0'?>"
                                  "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                  " targetNamespace='urn:example:one'>"
                                  "<xs:include schemaLocation='shared.xsd'/>"
                                  "<xs:import namespace='urn:example:two' schemaLocation='other.xsd'/>"
                                  "</xs:schema>"))]
    (write-fixture! "twice" "other.xsd"
                    (str "<?xml version='1.0'?>"
                         "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                         " targetNamespace='urn:example:two'>"
                         "<xs:include schemaLocation='shared.xsd'/>"
                         "</xs:schema>"))
    (write-fixture! "twice" "shared.xsd"
                    (str "<?xml version='1.0'?>"
                         "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'>"
                         "<xs:simpleType name='S'><xs:restriction base='xs:string'/></xs:simpleType>"
                         "</xs:schema>"))
    (let [{:keys [order]} (loader/load-documents (opts) main)]
      (is (= #{["shared.xsd" "urn:example:one"] ["shared.xsd" "urn:example:two"]}
             (set (filter #(= "shared.xsd" (first %)) (basenames order))))))))

;; ---------------------------------------------------------------------------
;; AST normalization
;; ---------------------------------------------------------------------------

(defn- find-node [components pred]
  (first (filter pred (mapcat #(tree-seq :children :children %) components))))

(defn- components-of [f]
  (let [{:keys [documents order]} (loader/load-documents (opts) f)]
    (:components (get documents (first order)))))

(deftest keyed-children-are-restored-to-nodes
  ;; xs:attribute, xs:list and xs:union carry inline simple types as keyed map
  ;; entries rather than in an :xml/value stream. The AST must not care.
  (testing "an inline simpleType under xs:attribute (JUnit.xsd)"
    (let [attribute (find-node (components-of (xsd "JUnit.xsd"))
                               #(and (= :attribute (:kind %)) (seq (:children %))))]
      (is (= [:simpleType] (mapv :kind (:children attribute))))))
  (testing "an inline simpleType under xs:list (XMLSchema.xsd)"
    (let [list-node (find-node (components-of (xsd "XMLSchema.xsd"))
                               #(and (= :list (:kind %)) (seq (:children %))))]
      (is (= [:simpleType] (mapv :kind (:children list-node))))))
  (testing "a union's members (fop.xsd)"
    (let [union (find-node (components-of (xsd "fop.xsd")) #(= :union (:kind %)))]
      (is (seq (get-in union [:attrs :memberTypes]))))))

(deftest a-map-arm-inside-a-value-stream-becomes-children
  ;; fop.xsd's clip_Type restriction holds an annotation and a pattern; the
  ;; annotation arrives as a bare props map item inside the :xml/value stream,
  ;; a map-mode arm of a sequence expression.
  (let [{:keys [documents order]} (loader/load-documents {} (xsd "fop.xsd"))
        components (:components (get documents (first order)))
        clip (find-node components #(= "clip_Type" (get-in % [:attrs :name])))
        restriction (first (:children clip))]
    (is (= :restriction (:kind restriction)))
    (is (= "length_Type{1,2}" (:doc restriction)))
    (is (= [:pattern] (mapv :kind (:children restriction))))))

(deftest structure-the-ast-does-not-recognize-is-fatal
  (testing "an element outside the XML Schema vocabulary"
    (is (= :xsd-to-malli/unknown-element-kind
           (ex-type #(ast/node "memory:test" [:widget {:name "x"}])))))
  (testing "a child value that is neither a props map nor a vector of them"
    (is (= :xsd-to-malli/unknown-child-shape
           (ex-type #(ast/node "memory:test" [:element {:name "x" :simpleType 42}]))))
    (is (= :xsd-to-malli/unknown-child-shape
           (ex-type #(ast/node "memory:test" [:sequence {:xml/value ["text"]}])))))
  (testing "the shapes the parse really produces are accepted"
    (is (= {:kind :attribute
            :attrs {:name "x"}
            :children [{:kind :simpleType :attrs {} :children []}]}
           (ast/node "memory:test" [:attribute {:name "x" :simpleType {}}])))
    (is (= {:kind :union
            :attrs {}
            :children [{:kind :simpleType :attrs {:id "a"} :children []}
                       {:kind :simpleType :attrs {:id "b"} :children []}]}
           (ast/node "memory:test" [:union {:simpleType [{:id "a"} {:id "b"}]}])))))

;; ---------------------------------------------------------------------------
;; Failure paths
;; ---------------------------------------------------------------------------

(deftest an-include-of-a-different-namespace-is-fatal
  (let [main (write-fixture! "include-mismatch" "main.xsd"
                             (str "<?xml version='1.0'?>"
                                  "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                  " targetNamespace='urn:example:one'>"
                                  "<xs:include schemaLocation='other.xsd'/>"
                                  "</xs:schema>"))]
    (write-fixture! "include-mismatch" "other.xsd"
                    (str "<?xml version='1.0'?>"
                         "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                         " targetNamespace='urn:example:two'/>"))
    (is (= :xsd-to-malli/include-namespace-mismatch
           (ex-type #(loader/load-documents (opts) main))))))

(deftest an-import-of-the-wrong-namespace-is-fatal
  (let [main (write-fixture! "import-mismatch" "main.xsd"
                             (str "<?xml version='1.0'?>"
                                  "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                  " targetNamespace='urn:example:one'>"
                                  "<xs:import namespace='urn:example:two' schemaLocation='other.xsd'/>"
                                  "</xs:schema>"))]
    (write-fixture! "import-mismatch" "other.xsd"
                    (str "<?xml version='1.0'?>"
                         "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                         " targetNamespace='urn:example:three'/>"))
    (is (= :xsd-to-malli/import-namespace-mismatch
           (ex-type #(loader/load-documents (opts) main))))))

(deftest importing-the-xml-schema-namespace-is-refused
  (let [main (write-fixture! "xsd-import" "main.xsd"
                             (str "<?xml version='1.0'?>"
                                  "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                  " targetNamespace='urn:example:one'>"
                                  "<xs:import namespace='http://www.w3.org/2001/XMLSchema'"
                                  " schemaLocation='nowhere.xsd'/>"
                                  "</xs:schema>"))]
    (is (= :xsd-to-malli/xsd-namespace-import
           (ex-type #(loader/load-documents (opts) main))))))

(deftest redefine-is-refused-rather-than-half-supported
  (let [main (write-fixture! "redefine" "main.xsd"
                             (str "<?xml version='1.0'?>"
                                  "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                  " targetNamespace='urn:example:one'>"
                                  "<xs:redefine schemaLocation='other.xsd'/>"
                                  "</xs:schema>"))]
    (is (= :xsd-to-malli/unsupported
           (ex-type #(loader/load-documents (opts) main))))))

(deftest a-document-that-is-not-a-schema-is-refused
  (let [f (write-fixture! "not-a-schema" "doc.xml" "<?xml version='1.0'?><record><id>1</id></record>")]
    (is (= :xsd-to-malli/foreign-element (ex-type #(loader/load-documents (opts) f))))))

(deftest following-a-reference-without-a-resolver-is-refused
  (is (= :xsd-to-malli/no-resolver
         (ex-type #(loader/load-documents {} (multifile "main.xsd"))))))
