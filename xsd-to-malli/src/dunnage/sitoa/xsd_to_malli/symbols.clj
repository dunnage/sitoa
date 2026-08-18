(ns dunnage.sitoa.xsd-to-malli.symbols
  "Global declarations of a loaded schema set, keyed by registry keyword.

  This is what XSOM's XSSchemaSet provided: one lookup surface over every
  document in the include/import graph, with QName references resolved against
  the in-scope prefix bindings of the document that spelled them, and the XSD
  builtin datatypes seeded so a reference to xs:string resolves like any other.

  Registry keywords are produced exactly the way the XSOM pipeline produces
  them, because the two must agree key for key."
  (:require [clojure.string :as str]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [dunnage.sitoa.xsd-to-malli.ast :as ast]
            [dunnage.sitoa.xsd-to-malli.prefixes :as prefixes])
  (:import (java.net URI)))

(def xsd-namespace "http://www.w3.org/2001/XMLSchema")

(def xmlschema-ns
  "Registry namespace of the XML Schema namespace, i.e. (uri->ns xsd-namespace)."
  "org.w3.www.2001.XMLSchema")

(defn uri->ns
  "Registry namespace for a namespace URI.

  Reimplemented from dunnage.sitoa.bootstrapped-schema/uri->ns
  (bootstrapped_schema.clj lines 31-42): urn URIs reverse their colon
  segments, everything else reverses the host labels and appends the path
  segments. Requiring the original would drag XSOM onto this project's runtime
  classpath, so it is copied; symbols-test asserts parity against the original
  for every namespace URI the fixtures use."
  [^String x]
  (let [uri (URI. x)]
    (case (.getScheme uri)
      "urn" (-> []
                (into (reverse (str/split (str (.getSchemeSpecificPart uri)) #":")))
                (->> (str/join ".")))
      (-> []
          (into (reverse (str/split (.getHost uri) #"\.")))
          (into (remove empty?) (str/split (.getPath uri) #"\/"))
          (->> (str/join "."))))))

(def unmodeled-builtin-names
  "XSD builtin datatypes xml-primitives models nothing for. They are builtins
  all the same, so a document may not redeclare them (bootstrapped_schema.clj
  lines 137-150)."
  #{"anyType" "IDREFS" "NMTOKENS" "ENTITIES"})

(def xsd-builtin-names
  "Local names that belong to the XSD builtin datatype hierarchy. Anything else
  in the XML Schema namespace can only come from a document that declares it -
  the schema for schemas declares element, complexType and friends there - so
  it is a real declaration."
  (into unmodeled-builtin-names (map name) (keys xml-primitives/xmlschema-registry)))

(defn builtin-kw?
  "True for a keyword naming an XSD builtin datatype."
  [k]
  (and (some? k)
       (= xmlschema-ns (namespace k))
       (contains? xsd-builtin-names (name k))))

(defn document-namespace
  "Namespace a document's declarations belong to: its targetNamespace, or the
  namespace a chameleon include coerced it into."
  [doc]
  (or (:target-namespace doc) (:coerced-namespace doc)))

(defn- registry-kw [{:keys [default-ns]} uri local detail]
  (cond
    (seq uri) (keyword (uri->ns uri) local)
    default-ns (keyword default-ns local)
    :else (throw (ex-info ":default-ns is required: a name has no namespace to map to"
                          (merge {:type :xsd-to-malli/missing-default-ns
                                  :local local}
                                 detail)))))

(defn- qname-parts [^String value]
  (let [idx (.indexOf value ":")]
    (if (pos? idx)
      [(subs value 0 idx) (subs value (inc idx))]
      [nil value])))

(defn resolve-qname
  "Resolve a QName attribute value found in `doc` to a registry keyword.

  Returns {:qname :prefix :local :uri :kw :builtin? :modeled?}. An unprefixed
  name takes the document's default xmlns, and failing that the no-namespace
  symbol space, which a chameleon include has coerced into the including
  document's target namespace. An unbound prefix is fatal: guessing would
  silently point a reference at the wrong type."
  [context doc value]
  (let [value (str/trim value)
        [prefix local] (qname-parts value)
        uri (if prefix
              (or (get (:prefix->uri doc) prefix)
                  (when (= "xml" prefix) prefixes/xml-namespace)
                  (throw (ex-info "QName uses a prefix the document never bound"
                                  {:type :xsd-to-malli/unbound-prefix
                                   :uri (str (:uri doc))
                                   :qname value
                                   :prefix prefix
                                   :bound (vec (sort (keys (:prefix->uri doc))))})))
              (or (:default-uri doc) (:coerced-namespace doc)))
        builtin? (and (= xsd-namespace uri) (contains? xsd-builtin-names local))
        kw (registry-kw context uri local {:uri (str (:uri doc)) :qname value})]
    {:qname value
     :prefix prefix
     :local local
     :uri uri
     :kw kw
     :builtin? builtin?
     :modeled? (and builtin? (contains? xml-primitives/xmlschema-registry kw))}))

(defn declaration-kw
  "Registry keyword for a global declaration named `local` in `doc`."
  [context doc local]
  (registry-kw context (document-namespace doc) local
               {:uri (str (:uri doc)) :name local}))

(def declaration-sections
  "Top-level element kind -> symbol table section."
  {:simpleType     :types
   :complexType    :types
   :element        :elements
   :group          :groups
   :attributeGroup :attribute-groups
   :attribute      :attributes})

(def ^:private empty-table
  {:types {} :elements {} :groups {} :attribute-groups {} :attributes {}})

(defn builtin-seed
  "Symbol table :types entries for the XSD builtin datatypes. Entries carry no
  :node - they are not declarations - so a lookup can tell a builtin from
  something a document wrote."
  []
  (into (into {}
              (map (fn [k] [k {:builtin k :modeled? true :local (name k)}]))
              (keys xml-primitives/xmlschema-registry))
        (map (fn [n] [(keyword xmlschema-ns n)
                      {:builtin (keyword xmlschema-ns n) :modeled? false :local n}]))
        unmodeled-builtin-names))

(defn- derivation-of
  "Base-type edge of a type declaration, or nil.

  complexContent and simpleContent carry the edge on their extension or
  restriction child. A simpleType restriction is an edge too: the compiler
  walks it to find the primitive ancestor, and a cycle there would not
  terminate either. Deriving from xs:anyType is not derivation - anyType
  constrains nothing - so it is reported as no edge at all."
  [context doc node]
  (case (:kind node)
    :complexType
    (when-some [content (ast/child node #{:simpleContent :complexContent})]
      (when-some [step (ast/child content #{:extension :restriction})]
        (when-some [base (ast/attr step :base)]
          (let [{:keys [kw builtin? local]} (resolve-qname context doc base)]
            (when-not (and builtin? (= "anyType" local))
              {:method (:kind step)
               :base kw
               :base-qname base
               :content (if (= :simpleContent (:kind content)) :simple :complex)})))))

    :simpleType
    (when-some [step (ast/child node :restriction)]
      (when-some [base (ast/attr step :base)]
        (let [{:keys [kw]} (resolve-qname context doc base)]
          {:method :restriction
           :base kw
           :base-qname base
           :content :simple-type})))

    nil))

(defn- builtin-declaration?
  "True for a document declaration that merely re-states a builtin datatype.
  XSOM synthesizes those from its own bundled datatypes.xsd, so the oracle
  drops them; anything else in the XML Schema namespace is a real declaration."
  [doc section local]
  (and (contains? #{:types :groups} section)
       (= xsd-namespace (document-namespace doc))
       (contains? xsd-builtin-names local)))

(defn- add-declaration [context table doc node]
  (let [section (declaration-sections (:kind node))
        local (ast/attr node :name)]
    (if (or (nil? section) (nil? local) (builtin-declaration? doc section local))
      table
      (let [kw (declaration-kw context doc local)
            existing (get-in table [section kw])
            derivation (derivation-of context doc node)]
        (when (:node existing)
          (throw (ex-info "two documents declare the same global name"
                          {:type :xsd-to-malli/duplicate-global
                           :section section
                           :kw kw
                           :name local
                           :first (str (:uri (:doc existing)))
                           :second (str (:uri doc))})))
        (assoc-in table [section kw]
                  (cond-> {:kw kw
                           :section section
                           :kind (:kind node)
                           :local local
                           :node node
                           :doc doc}
                    (:doc node) (assoc :documentation (:doc node))
                    derivation (assoc :derivation derivation)))))))

(defn check-derivation-acyclic!
  "Assert that base-type edges form a DAG. Generated namespaces require the
  namespace of their base type, so a derivation cycle would emit a require
  cycle; catching it here names the types instead of failing at load."
  [types]
  (let [state (volatile! {})]
    (letfn [(visit [k path]
              (case (get @state k)
                :done nil
                :open (throw (ex-info "types derive from each other in a cycle"
                                      {:type :xsd-to-malli/derivation-cycle
                                       :cycle (vec (conj path k))}))
                (do (vswap! state assoc k :open)
                    (when-some [base (get-in types [k :derivation :base])]
                      (when (contains? types base)
                        (visit base (conj path k))))
                    (vswap! state assoc k :done))))]
      (doseq [k (sort (keys types))]
        (visit k []))))
  types)

(defn symbol-table
  "Build the symbol table for a loaded document set.

  `documents` maps graph key -> schema document; `order` fixes the iteration
  so that error reporting and later emission are deterministic."
  [context documents order]
  (let [table (reduce (fn [table doc-key]
                        (let [doc (get documents doc-key)]
                          (when (and (nil? (document-namespace doc))
                                     (nil? (:default-ns context)))
                            (throw (ex-info ":default-ns is required: a loaded document has no targetNamespace"
                                            {:type :xsd-to-malli/missing-default-ns
                                             :uri (str (:uri doc))})))
                          (reduce (fn [table node] (add-declaration context table doc node))
                                  table
                                  (:components doc))))
                      (assoc empty-table :types (builtin-seed))
                      order)]
    (check-derivation-acyclic! (:types table))
    table))

(defn own-keys
  "Registry keywords of a table section that documents actually declared,
  sorted. The builtin seeds of :types are not own keys."
  [table section]
  (into (sorted-set)
        (comp (filter (fn [[_ entry]] (:node entry))) (map key))
        (get table section)))

(defn lookup
  "Symbol table entry for a registry keyword in `section`, or nil."
  [table section kw]
  (get-in table [section kw]))
