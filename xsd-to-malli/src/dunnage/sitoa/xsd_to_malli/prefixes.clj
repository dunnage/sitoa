(ns dunnage.sitoa.xsd-to-malli.prefixes
  "Prefix bindings for an .xsd document, recovered by a StAX pre-pass.

  The streaming parser drops xmlns declarations: StAX excludes them from the
  attribute axis and the parser's attribute readers walk that axis only, so a
  parsed schema document carries no prefix bindings at all while its QName
  attribute VALUES still spell prefixes out. This pre-pass reads the raw bytes
  once more and hands the loader the root element's bindings.

  Using root bindings for the whole document is an assumption, so it is
  CHECKED rather than trusted: every QName-valued attribute on every XSD
  element outside an annotation subtree must resolve identically against the
  root bindings and against the live in-scope context. A document that would
  need scoped resolution fails loudly, with a line and column, instead of
  being silently misresolved.

  Annotation subtrees are exempt because their content is arbitrary foreign
  markup - fop.xsd carries eight xmlns=\"\" declarations inside
  <xs:documentation> prose - and nothing in there contributes components.
  Outside an annotation the reverse holds: every element must be in the XML
  Schema namespace, because the parser keys elements on their local name and
  would otherwise read foreign markup as a schema component."
  (:require [clojure.string :as str]
            [dunnage.sitoa.parser :as parser])
  (:import (java.io Reader)
           (javax.xml.stream XMLStreamReader)))

(def xsd-namespace "http://www.w3.org/2001/XMLSchema")

(def xml-namespace
  "The prefix xml is bound to this namespace by the XML specification itself
  and never needs a declaration."
  "http://www.w3.org/XML/1998/namespace")

(def qname-attributes
  "XSD attributes whose value is a single QName."
  #{"type" "ref" "base" "itemType" "refer"})

(def qname-list-attributes
  "XSD attributes whose value is a whitespace-separated list of QNames.
  substitutionGroup is single-valued in XSD 1.0 and a list in 1.1; splitting
  on whitespace covers both."
  #{"memberTypes" "substitutionGroup"})

(defn- blank->nil [s]
  (when-not (str/blank? s) s))

(defn- root-bindings [^XMLStreamReader r]
  (let [cnt (.getNamespaceCount r)]
    (loop [i 0 prefixes {} default nil]
      (if (< i cnt)
        (let [p (.getNamespacePrefix r i)
              u (blank->nil (.getNamespaceURI r i))]
          (if (str/blank? p)
            (recur (inc i) prefixes u)
            (recur (inc i) (assoc prefixes p u) default)))
        {:prefix->uri prefixes :default-uri default}))))

(defn- qname-prefix [^String value]
  (let [idx (.indexOf value ":")]
    (when (pos? idx)
      (subs value 0 idx))))

(defn- root-uri-for [{:keys [prefix->uri default-uri]} prefix]
  (cond
    (nil? prefix) default-uri
    (= "xml" prefix) (or (get prefix->uri prefix) xml-namespace)
    :else (get prefix->uri prefix)))

(defn- check-qname! [uri ^XMLStreamReader r bindings attr value]
  (let [prefix (qname-prefix value)
        root (root-uri-for bindings prefix)
        live (blank->nil (.getNamespaceURI (.getNamespaceContext r) (or prefix "")))]
    (when (not= root live)
      (let [loc (.getLocation r)]
        (throw (ex-info "prefix bindings differ between the root element and this element; scoped prefix resolution is not supported"
                        {:type :xsd-to-malli/non-root-xmlns
                         :uri (str uri)
                         :element (.getLocalName r)
                         :attr attr
                         :value value
                         :prefix prefix
                         :root-uri root
                         :in-scope-uri live
                         :line (.getLineNumber loc)
                         :column (.getColumnNumber loc)}))))))

(defn- check-foreign! [uri ^XMLStreamReader r]
  (let [loc (.getLocation r)]
    (throw (ex-info "element outside an annotation is not in the XML Schema namespace; the parser keys on local names and would read it as a schema component"
                    {:type :xsd-to-malli/foreign-element
                     :uri (str uri)
                     :element (.getLocalName r)
                     :element-namespace (.getNamespaceURI r)
                     :line (.getLineNumber loc)
                     :column (.getColumnNumber loc)}))))

(defn- check-element! [uri ^XMLStreamReader r bindings]
  (let [cnt (.getAttributeCount r)]
    (dotimes [i cnt]
      ;; XSD's own attributes are unqualified; anything namespaced is foreign.
      (when (str/blank? (.getAttributeNamespace r i))
        (let [attr (.getAttributeLocalName r i)
              value (.getAttributeValue r i)]
          (cond
            (contains? qname-attributes attr)
            (check-qname! uri r bindings attr value)

            (contains? qname-list-attributes attr)
            (doseq [part (str/split (str/trim value) #"\s+")
                    :when (seq part)]
              (check-qname! uri r bindings attr part))))))))

(defn scan-prefixes
  "Prefix bindings declared on the root element of `rdr`, after checking that
  they are the only ones any component QName needs.

  Returns {:prefix->uri {prefix uri}, :default-uri uri-or-nil}. Throws
  ex-info {:type :xsd-to-malli/non-root-xmlns} on a document that resolves a
  component QName through a non-root binding."
  [uri ^Reader rdr]
  (let [r (parser/make-stream-reader {} rdr)]
    (loop [bindings nil
           depth 0
           annotation-depth nil]
      (if (.hasNext r)
        (case (.next r)
          1                                                 ;START_ELEMENT
          (let [depth (inc depth)
                bindings (or bindings (root-bindings r))
                xsd? (= xsd-namespace (.getNamespaceURI r))
                annotation-depth (if (and (nil? annotation-depth)
                                          xsd?
                                          (= "annotation" (.getLocalName r)))
                                   depth
                                   annotation-depth)]
            (when (nil? annotation-depth)
              (if xsd?
                (check-element! uri r bindings)
                (check-foreign! uri r)))
            (recur bindings depth annotation-depth))

          2                                                 ;END_ELEMENT
          (recur bindings
                 (dec depth)
                 (when-not (= annotation-depth depth) annotation-depth))

          (recur bindings depth annotation-depth))
        (or bindings
            (throw (ex-info "document has no root element"
                            {:type :xsd-to-malli/empty-document :uri (str uri)})))))))
