(ns dunnage.sitoa.xsd-to-malli.symbols-test
  "The symbol table has to agree with XSOM's XSSchemaSet name for name, because
  the compiler that comes next must produce the same registry keys as the
  bootstrapped-schema oracle. The oracle is only on the TEST classpath: nothing
  under src may require it, or the generator would drag XSOM along."
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [dunnage.sitoa.xsd-to-malli.loader :as loader]
            [dunnage.sitoa.xsd-to-malli.resolver :as resolver]
            [dunnage.sitoa.xsd-to-malli.symbols :as symbols])
  (:import (com.sun.xml.xsom XSModelGroupDecl XSType)
           (com.sun.xml.xsom.parser XSOMParser)
           (java.io File)
           (javax.xml.parsers SAXParserFactory)
           (org.xml.sax EntityResolver ErrorHandler InputSource SAXParseException)))

(def ^:private out-root "target/symbols-test")

(def ^:private xml-xsd (io/file "dev-resources/catalog/xml.xsd"))

(def ^:private catalog
  {"http://www.w3.org/2001/xml.xsd" xml-xsd
   "http://www.w3.org/XML/1998/namespace" xml-xsd})

(defn- xsd [name] (io/file ".." "bootstrapped-schema" "dev-resources" name))

(defn- load-symbols [f default-ns]
  (:symbols (loader/load-schemas {:resolver (resolver/catalog-resolver catalog)
                                  :default-ns default-ns}
                                 f)))

(defn- write-fixture! [dir name content]
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

;; ---------------------------------------------------------------------------
;; Oracle side, offline
;; ---------------------------------------------------------------------------

(defn- parse-xsd-offline
  "bs/parse-xsd resolves XMLSchema.xsd's xs:import over the network. The same
  parse with an entity resolver pointed at the checked-in catalog copy keeps
  this suite offline; bootstrapped-schema itself is untouched."
  [^File f]
  (let [parser (XSOMParser. (SAXParserFactory/newDefaultInstance))]
    (.setErrorHandler parser (reify ErrorHandler
                               (^void warning [_ ^SAXParseException x] (prn x))
                               (^void error [_ ^SAXParseException x] (prn x))
                               (^void fatalError [_ ^SAXParseException x] (prn x))))
    (.setEntityResolver parser (reify EntityResolver
                                 (resolveEntity [_ _public-id system-id]
                                   (when (and system-id
                                              (.endsWith ^String system-id "/2001/xml.xsd"))
                                     (InputSource. (io/input-stream xml-xsd))))))
    (.parse parser f)
    (.getResult parser)))

(defn- oracle-names [f default-ns]
  (let [schema-set (parse-xsd-offline f)]
    {:types (into (sorted-set)
                  (comp (remove (fn [^XSType x] (some-> x .asSimpleType .isPrimitive)))
                        (remove #(bs/xsd-builtin-decl? % default-ns))
                        (map #(bs/->nskw % default-ns)))
                  (iterator-seq (.iterateTypes schema-set)))
     :groups (into (sorted-set)
                   (comp (filter (fn [^XSModelGroupDecl x] (.isGlobal x)))
                         (remove #(bs/xsd-builtin-decl? % default-ns))
                         (map #(bs/->nskw % default-ns)))
                   (iterator-seq (.iterateModelGroupDecls schema-set)))
     :elements (into (sorted-set)
                     (map #(bs/->nskw % default-ns))
                     (iterator-seq (.iterateElementDecls schema-set)))}))

;; ---------------------------------------------------------------------------
;; uri->ns
;; ---------------------------------------------------------------------------

(def ^:private fixture-namespace-uris
  ["http://www.w3.org/2001/XMLSchema"
   "http://www.w3.org/1999/XSL/Format"
   "http://www.w3.org/XML/1998/namespace"
   "urn:example:types"
   "urn:example:other"
   "http://www.example.com"
   "http://example.com/a/b/c"
   "urn:hl7-org:v3"])

(deftest uri-to-namespace-matches-the-oracle
  (doseq [uri fixture-namespace-uris]
    (testing uri
      (is (= (bs/uri->ns uri) (symbols/uri->ns uri))))))

;; ---------------------------------------------------------------------------
;; The multi-file fixture
;; ---------------------------------------------------------------------------

(deftest the-multi-file-fixture-declares-exactly-these-globals
  (let [table (load-symbols (io/file "dev-resources/multifile/main.xsd") "mf")]
    (is (= #{:types.example/BaseRecord
             :types.example/ExtendedRecord
             :types.example/Price
             :types.example/RecordList
             :types.example/StrictRecord
             :types.example/UsdPrice
             :types.example/codeType
             :other.example/scoreType}
           (set (symbols/own-keys table :types))))
    (is (= #{:types.example/price
             :types.example/record
             :types.example/records
             :types.example/score
             :types.example/strict}
           (set (symbols/own-keys table :elements))))
    (is (= #{:types.example/auditAttrs} (set (symbols/own-keys table :attribute-groups))))
    (is (empty? (symbols/own-keys table :groups)))
    (testing "the chameleon include lands codeType in the including namespace"
      (let [entry (symbols/lookup table :types :types.example/codeType)]
        (is (= "codeType" (:local entry)))
        (is (= "urn:example:types" (:coerced-namespace (:doc entry))))
        (is (nil? (:target-namespace (:doc entry))))))))

(deftest builtin-datatypes-are-seeded
  (let [table (load-symbols (io/file "dev-resources/multifile/main.xsd") "mf")]
    (testing "every modelled builtin is present and marked as such"
      (is (every? (fn [k] (= k (:builtin (symbols/lookup table :types k))))
                  (keys xml-primitives/xmlschema-registry)))
      (is (:modeled? (symbols/lookup table :types :org.w3.www.2001.XMLSchema/string))))
    (testing "the builtins xml-primitives models nothing for are seeded too"
      (doseq [n symbols/unmodeled-builtin-names]
        (let [entry (symbols/lookup table :types (keyword symbols/xmlschema-ns n))]
          (is (some? entry))
          (is (false? (:modeled? entry))))))
    (testing "a seed is not a declaration"
      (is (nil? (:node (symbols/lookup table :types :org.w3.www.2001.XMLSchema/string)))))))

(deftest derivation-edges-are-recorded
  (let [table (load-symbols (io/file "dev-resources/multifile/main.xsd") "mf")
        derivation (fn [k] (:derivation (symbols/lookup table :types k)))]
    (is (= {:method :extension
            :base :types.example/BaseRecord
            :base-qname "t:BaseRecord"
            :content :complex}
           (derivation :types.example/ExtendedRecord)))
    (is (= {:method :restriction
            :base :types.example/BaseRecord
            :base-qname "t:BaseRecord"
            :content :complex}
           (derivation :types.example/StrictRecord)))
    (is (= {:method :extension
            :base :org.w3.www.2001.XMLSchema/decimal
            :base-qname "xs:decimal"
            :content :simple}
           (derivation :types.example/Price)))
    (is (= {:method :restriction
            :base :types.example/Price
            :base-qname "t:Price"
            :content :simple}
           (derivation :types.example/UsdPrice)))
    (is (= {:method :restriction
            :base :org.w3.www.2001.XMLSchema/token
            :base-qname "xs:token"
            :content :simple-type}
           (derivation :types.example/codeType)))
    (testing "a type with no base has no edge"
      (is (nil? (derivation :types.example/BaseRecord)))
      (is (nil? (derivation :types.example/RecordList))))))

(deftest deriving-from-anytype-is-not-derivation
  (let [f (write-fixture! "anytype" "main.xsd"
                          (str "<?xml version='1.0'?>"
                               "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                               " targetNamespace='urn:example:one'>"
                               "<xs:complexType name='Open'>"
                               "<xs:complexContent><xs:extension base='xs:anyType'>"
                               "<xs:sequence><xs:element name='x' type='xs:string'/></xs:sequence>"
                               "</xs:extension></xs:complexContent></xs:complexType>"
                               "</xs:schema>"))
        table (load-symbols f "one")]
    (is (nil? (:derivation (symbols/lookup table :types :one.example/Open))))))

;; ---------------------------------------------------------------------------
;; QName resolution
;; ---------------------------------------------------------------------------

(deftest qnames-resolve-through-the-documents-own-bindings
  (let [{:keys [documents order]} (loader/load-documents
                                   {:resolver (resolver/catalog-resolver catalog)}
                                   (io/file "dev-resources/multifile/main.xsd"))
        by-name (fn [suffix]
                  (get documents (first (filter #(clojure.string/ends-with? (first %) suffix) order))))
        main (by-name "/main.xsd")
        common (by-name "/common.xsd")
        context {:default-ns "mf"}
        resolve-in (fn [doc q] (symbols/resolve-qname context doc q))]
    (testing "a prefix bound on the root"
      (is (= :types.example/ExtendedRecord (:kw (resolve-in main "t:ExtendedRecord"))))
      (is (= :other.example/scoreType (:kw (resolve-in main "o:scoreType")))))
    (testing "an XSD builtin is marked, and one xml-primitives does not model is marked too"
      (let [s (resolve-in main "xs:string")]
        (is (= :org.w3.www.2001.XMLSchema/string (:kw s)))
        (is (:builtin? s))
        (is (:modeled? s)))
      (let [a (resolve-in main "xs:anyType")]
        (is (:builtin? a))
        (is (not (:modeled? a)))))
    (testing "inside a coerced chameleon an unprefixed name takes the coerced namespace"
      (is (= :types.example/codeType (:kw (resolve-in common "codeType")))))
    (testing "an unbound prefix is fatal"
      (is (= :xsd-to-malli/unbound-prefix (ex-type #(resolve-in main "nope:Thing")))))
    (testing "the xml prefix needs no declaration"
      (is (= :org.w3.www.XML.1998.namespace/lang (:kw (resolve-in main "xml:lang")))))))

(deftest a-default-xmlns-resolves-unprefixed-qnames
  ;; fop.xsd writes <xs:restriction base="string"> and means xs:string, because
  ;; its default xmlns is the XML Schema namespace.
  (let [{:keys [documents order]} (loader/load-documents {} (xsd "fop.xsd"))
        fop (get documents (first order))]
    (is (= :org.w3.www.2001.XMLSchema/string
           (:kw (symbols/resolve-qname {:default-ns "fop"} fop "string"))))
    (is (= :org.w3.www.1999.XSL.Format/length_Type
           (:kw (symbols/resolve-qname {:default-ns "fop"} fop "fo:length_Type"))))))

(deftest a-document-without-a-target-namespace-needs-a-default-ns
  (is (= :xsd-to-malli/missing-default-ns
         (ex-type #(loader/load-schemas {} (xsd "JUnit.xsd")))))
  (is (= #{:junit/ISO8601_DATETIME_PATTERN :junit/pre-string :junit/testsuite}
         (set (symbols/own-keys (load-symbols (xsd "JUnit.xsd") "junit") :types)))))

;; ---------------------------------------------------------------------------
;; Failure paths
;; ---------------------------------------------------------------------------

(deftest two-documents-declaring-one-global-name-is-fatal
  (let [main (write-fixture! "duplicate" "main.xsd"
                             (str "<?xml version='1.0'?>"
                                  "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                                  " targetNamespace='urn:example:one'>"
                                  "<xs:include schemaLocation='other.xsd'/>"
                                  "<xs:simpleType name='Same'><xs:restriction base='xs:string'/></xs:simpleType>"
                                  "</xs:schema>"))]
    (write-fixture! "duplicate" "other.xsd"
                    (str "<?xml version='1.0'?>"
                         "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                         " targetNamespace='urn:example:one'>"
                         "<xs:simpleType name='Same'><xs:restriction base='xs:token'/></xs:simpleType>"
                         "</xs:schema>"))
    (is (= :xsd-to-malli/duplicate-global
           (ex-type #(loader/load-schemas {:resolver (resolver/catalog-resolver catalog)
                                           :default-ns "one"}
                                          main))))))

(deftest a-derivation-cycle-is-fatal
  (let [f (write-fixture! "cycle" "main.xsd"
                          (str "<?xml version='1.0'?>"
                               "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                               " xmlns:t='urn:example:one' targetNamespace='urn:example:one'>"
                               "<xs:complexType name='A'><xs:complexContent>"
                               "<xs:extension base='t:B'/></xs:complexContent></xs:complexType>"
                               "<xs:complexType name='B'><xs:complexContent>"
                               "<xs:extension base='t:A'/></xs:complexContent></xs:complexType>"
                               "</xs:schema>"))
        e (try
            (load-symbols f "one")
            nil
            (catch clojure.lang.ExceptionInfo e e))]
    (is (= :xsd-to-malli/derivation-cycle (:type (ex-data e))))
    (is (= [:one.example/A :one.example/B :one.example/A] (:cycle (ex-data e))))))

(deftest a-type-declaring-a-builtin-name-is-dropped-like-the-oracle-drops-it
  ;; XSOM synthesizes the builtin datatypes from its own bundled datatypes.xsd,
  ;; so a document that redeclares one contributes nothing.
  (let [f (write-fixture! "builtin" "main.xsd"
                          (str "<?xml version='1.0'?>"
                               "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'"
                               " targetNamespace='http://www.w3.org/2001/XMLSchema'>"
                               "<xs:simpleType name='string'><xs:restriction base='xs:token'/></xs:simpleType>"
                               "<xs:simpleType name='formChoice'><xs:restriction base='xs:token'/></xs:simpleType>"
                               "</xs:schema>"))
        table (load-symbols f "xsd")]
    (is (= #{:org.w3.www.2001.XMLSchema/formChoice} (set (symbols/own-keys table :types))))
    (is (nil? (:node (symbols/lookup table :types :org.w3.www.2001.XMLSchema/string))))))

;; ---------------------------------------------------------------------------
;; Every reference resolves, with no further I/O
;; ---------------------------------------------------------------------------

(def ^:private qname-reference-sections
  "QName-valued attribute -> the symbol table section it points into. xs:keyref
  refer is left out: identity constraints are not modelled."
  {[:element :type] :types
   [:attribute :type] :types
   [:restriction :base] :types
   [:extension :base] :types
   [:list :itemType] :types
   [:element :ref] :elements
   [:attribute :ref] :attributes
   [:group :ref] :groups
   [:attributeGroup :ref] :attribute-groups
   [:element :substitutionGroup] :elements})

(defn- unresolved-references
  "Every QName in every component that does not resolve to a table entry or a
  builtin. This is the M1 contract the compiler will build on."
  [f default-ns]
  (let [{:keys [documents order symbols]}
        (loader/load-schemas {:resolver (resolver/catalog-resolver catalog)
                              :default-ns default-ns}
                             f)
        context {:default-ns default-ns}
        resolvable? (fn [doc section value]
                      (let [{:keys [kw builtin?]} (symbols/resolve-qname context doc value)]
                        (or builtin? (some? (symbols/lookup symbols section kw)))))]
    (into []
          (mapcat
           (fn [doc-key]
             (let [doc (get documents doc-key)]
               (for [node (mapcat #(tree-seq :children :children %) (:components doc))
                     [value section]
                     (concat (keep (fn [[[kind attr] section]]
                                     (when (= kind (:kind node))
                                       (when-some [v (get-in node [:attrs attr])]
                                         [v section])))
                                   qname-reference-sections)
                             (when-some [members (get-in node [:attrs :memberTypes])]
                               (for [part (clojure.string/split (clojure.string/trim members) #"\s+")
                                     :when (seq part)]
                                 [part :types])))
                     :when (not (resolvable? doc section value))]
                 {:uri (str (:uri doc)) :element (:kind node) :qname value :section section}))))
          order)))

(deftest every-reference-resolves-against-the-table
  (doseq [[label f default-ns]
          [["fop.xsd" (xsd "fop.xsd") "fop"]
           ["JUnit.xsd" (xsd "JUnit.xsd") "junit"]
           ["XMLSchema.xsd" (xsd "XMLSchema.xsd") "xsd"]
           ["xml.xsd" xml-xsd "xml"]
           ["multifile" (io/file "dev-resources/multifile/main.xsd") "mf"]]]
    (testing label
      (is (= [] (unresolved-references f default-ns))))))

;; ---------------------------------------------------------------------------
;; Parity with XSOM
;; ---------------------------------------------------------------------------

(deftest global-declarations-match-the-oracle
  (doseq [[label f default-ns]
          [["fop.xsd" (xsd "fop.xsd") "fop"]
           ["JUnit.xsd" (xsd "JUnit.xsd") "junit"]
           ["XMLSchema.xsd" (xsd "XMLSchema.xsd") "xsd"]
           ["multifile" (io/file "dev-resources/multifile/main.xsd") "mf"]]]
    (testing label
      (let [oracle (oracle-names f default-ns)
            table (load-symbols f default-ns)]
        (doseq [[section oracle-keys] [[:types (:types oracle)]
                                       [:groups (:groups oracle)]
                                       [:elements (:elements oracle)]]]
          (let [mine (symbols/own-keys table section)]
            (is (= oracle-keys mine)
                (str label " " section
                     " only-oracle=" (pr-str (set/difference oracle-keys mine))
                     " only-mine=" (pr-str (set/difference mine oracle-keys))))))))))
