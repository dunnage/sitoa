;; REGENERATION of generated-src (the XMLSchema meta-schema)
;;
;; generated-src holds the malli namespaces this loader parses .xsd documents
;; with. They are generated ONCE, at development time, by the XSOM-based v1
;; emitter, and checked in; nothing at run time or test time regenerates them.
;;
;; 1. rm -rf sitoa/xsd-to-malli/generated-src   (the emitter overwrites but
;;    never deletes, schema_namespaces.clj lines 272-274)
;; 2. cd sitoa/bootstrapped-schema
;; 3. clojure -M:dev -e '
;;      (require (quote [dunnage.sitoa.schema-namespaces :as sn])
;;               (quote [clojure.java.io :as io]))
;;      (def res (sn/xsd->namespaces! {:default-ns "xsd"
;;                                     :out-dir "../xsd-to-malli/generated-src"
;;                                     :entry-ns (quote dunnage.sitoa.xsd-meta)}
;;                                    (io/file "dev-resources/XMLSchema.xsd")))
;;      (println "files:" (count (:files res)) "included:" (count (:included res)))
;;      (System/exit 0)'
;;    expected: files: 66 included: 114
;; 4. git diff --stat ../xsd-to-malli/generated-src - regeneration is
;;    byte-identical for an unchanged XMLSchema.xsd and emitter.
;;
;; Regeneration needs NETWORK access: XSOM resolves XMLSchema.xsd's
;; <xs:import schemaLocation="http://www.w3.org/2001/xml.xsd"> itself, and the
;; resulting declarations are baked into the generated tree. Tests never do -
;; they go through the catalog in dev-resources/catalog. After regenerating,
;; diff the document W3C serves against dev-resources/catalog/xml.xsd and
;; update that copy and its README if W3C published a new revision.
;;
;; The same recipe is recorded in dev-resources/catalog/README.md; keep both
;; in sync.

(ns dunnage.sitoa.xsd-to-malli.loader
  "Load an .xsd document and everything it includes or imports.

  This is the layer XSOM used to be: read schema documents, follow the
  include/import graph, apply chameleon coercion, and hand back one symbol
  table. Documents are read with sitoa's own streaming parser against the
  checked-in meta-schema, so no XSD-specific parser exists in this project -
  a schema document is just an XML instance of the schema for schemas."
  (:require [clojure.java.io :as io]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.xsd-meta :as xsd-meta]
            [dunnage.sitoa.xsd-to-malli.ast :as ast]
            [dunnage.sitoa.xsd-to-malli.prefixes :as prefixes]
            [dunnage.sitoa.xsd-to-malli.resolver :as resolver]
            [dunnage.sitoa.xsd-to-malli.symbols :as symbols])
  (:import (java.net URI)))

(def ^:private meta-parse
  (delay (parser/xml-parser (xsd-meta/make-schema))))

(def ^:private graph-kinds
  "Top-level elements that direct the load rather than declare a component."
  #{:include :import :redefine :override})

(def ^:private non-component-kinds
  "Top-level elements that are not declarations. Document-level annotations
  belong to the document, not to any component, and the oracle ignores them."
  (conj graph-kinds :annotation))

(defn parse-schema-document
  "Parse one {:uri :open} source into a schema document.

  The source is read twice: once by the prefix pre-pass, which recovers the
  bindings the parse drops, and once by the parse itself. The stream reader
  is built from a Reader with no system id on purpose - a DOCTYPE's external
  subset is then never fetched, which is what lets XMLSchema.xsd parse from
  any working directory."
  [{:keys [uri open]}]
  (let [bindings (with-open [r (open)] (prefixes/scan-prefixes uri r))
        parsed (with-open [r (open)] (@meta-parse (parser/make-stream-reader {} r)))
        [tag props] parsed]
    (when-not (= :schema tag)
      (throw (ex-info "document root is not xs:schema"
                      {:type :xsd-to-malli/not-a-schema-document
                       :uri (str uri)
                       :root tag})))
    (let [components (mapv (partial ast/node uri) (:xml/value props))
          directives (group-by :kind (filterv (comp graph-kinds :kind) components))]
      (when-some [unsupported (first (concat (:redefine directives) (:override directives)))]
        (throw (ex-info "xs:redefine and xs:override are not supported"
                        {:type :xsd-to-malli/unsupported
                         :uri (str uri)
                         :element (:kind unsupported)})))
      {:uri uri
       :target-namespace (not-empty (:targetNamespace props))
       :coerced-namespace nil
       :prefix->uri (:prefix->uri bindings)
       :default-uri (:default-uri bindings)
       :element-form-default (or (:elementFormDefault props) "unqualified")
       :attribute-form-default (or (:attributeFormDefault props) "unqualified")
       :includes (mapv (fn [node] {:location (ast/attr node :schemaLocation)})
                       (get directives :include))
       :imports (mapv (fn [node] {:namespace (not-empty (ast/attr node :namespace))
                                  :location (ast/attr node :schemaLocation)})
                      (get directives :import))
       :components (into [] (remove (comp non-component-kinds :kind)) components)})))

(defn- coerce-chameleon
  "Apply the effect an xs:include has on the document it pulled in.

  An included document without a targetNamespace takes on the includer's, and
  so do the no-namespace QNames inside it. The same file included from two
  namespaces is two different component sets, which is why the graph is keyed
  by [uri namespace] rather than by uri alone."
  [doc includer-ns]
  (let [tns (:target-namespace doc)]
    (cond
      (= tns includer-ns) doc
      (nil? tns) (assoc doc :coerced-namespace includer-ns)
      :else (throw (ex-info "xs:include pulled in a document with a different targetNamespace"
                            {:type :xsd-to-malli/include-namespace-mismatch
                             :uri (str (:uri doc))
                             :target-namespace tns
                             :including-namespace includer-ns})))))

(defn- check-import! [doc expected-ns]
  (let [tns (:target-namespace doc)]
    (when (not= tns expected-ns)
      (throw (ex-info "xs:import pulled in a document whose targetNamespace is not the imported one"
                      {:type :xsd-to-malli/import-namespace-mismatch
                       :uri (str (:uri doc))
                       :target-namespace tns
                       :imported-namespace expected-ns})))
    doc))

(defn- pending-includes [doc]
  (let [ns (symbols/document-namespace doc)]
    (mapv (fn [{:keys [location]}]
            {:base-uri (:uri doc)
             :location location
             :namespace nil
             :via :include
             :coerce ns})
          (:includes doc))))

(defn- pending-imports [doc]
  (when (some (comp #{symbols/xsd-namespace} :namespace) (:imports doc))
    (throw (ex-info "importing the XML Schema namespace would shadow the builtin datatypes"
                    {:type :xsd-to-malli/xsd-namespace-import
                     :uri (str (:uri doc))})))
  (mapv (fn [{:keys [namespace location]}]
          {:base-uri (:uri doc)
           :location location
           :namespace namespace
           :via :import
           :expect namespace})
        (:imports doc)))

(defn load-documents
  "Load `root` and, transitively, every document it includes or imports.

  Options:
    :resolver  a resolver/SchemaResolver, required once anything is included
               or imported.

  Returns {:documents {[uri namespace] doc}, :order [key ...], :root key}.
  Keys pair the canonical document URI with the namespace its declarations
  land in, so a chameleon file included from two namespaces appears twice and
  a diamond import appears once. A cycle terminates: a source already seen
  under the same coercion is never read again."
  [{:keys [resolver]} root]
  (let [root-source (if (and (map? root) (:open root) (:uri root))
                      root
                      (resolver/source root))]
    (loop [queue (conj clojure.lang.PersistentQueue/EMPTY
                       {:source root-source :coerce nil :via :root})
           seen #{}
           documents {}
           order []
           root-key nil]
      (if-some [{:keys [source coerce via expect namespace location base-uri]} (peek queue)]
        (let [queue (pop queue)
              source (or source
                         (do (when (nil? resolver)
                               (throw (ex-info "a resolver is required to follow xs:include and xs:import"
                                               {:type :xsd-to-malli/no-resolver
                                                :base-uri (str base-uri)
                                                :location location})))
                             (resolver/resolve-schema resolver base-uri namespace location)))
              source-key [(str (:uri source)) coerce via expect]]
          (if (contains? seen source-key)
            (recur queue seen documents order root-key)
            (let [doc (cond-> (parse-schema-document source)
                        (= :include via) (coerce-chameleon coerce)
                        (= :import via) (check-import! expect))
                  doc-key [(str (:uri source)) (symbols/document-namespace doc)]
                  known? (contains? documents doc-key)]
              (recur (if known?
                       queue
                       (into queue (concat (pending-includes doc) (pending-imports doc))))
                     (conj seen source-key)
                     (if known? documents (assoc documents doc-key doc))
                     (if known? order (conj order doc-key))
                     (or root-key doc-key)))))
        {:documents documents :order order :root root-key}))))

(defn load-schemas
  "Load a schema set and build its symbol table.

  Options:
    :resolver    a resolver/SchemaResolver (see load-documents)
    :default-ns  registry namespace for declarations without a
                 targetNamespace; required as soon as any loaded document
                 lacks one, exactly like the XSOM pipeline's :default-ns.

  Returns {:documents :order :root :symbols}."
  [{:keys [default-ns] :as opts} root]
  (let [{:keys [documents order root]} (load-documents opts root)]
    {:documents documents
     :order order
     :root root
     :symbols (symbols/symbol-table {:default-ns default-ns} documents order)}))
