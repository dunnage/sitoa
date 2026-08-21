(ns dunnage.sitoa.xsd-to-malli.ast
  "Normalize the parsed form of an .xsd document into a uniform AST.

  The streaming parser delivers children in modes the meta-schema fixes per
  XSD element, and a single element mixes them:

    - an ordered :xml/value stream, whose items are [tag props] tuples;
    - KEYED map entries whose value is a props map (single child) or a vector
      of props maps (repeated child), with the tag implied by the key -
      xs:attribute carries its inline simpleType this way, xs:union carries a
      vector of them;
    - a props map appearing as an ITEM of an :xml/value stream, which is a
      map-mode arm inside a sequence expression: its entries are keyed
      children occupying that position (fop.xsd's
      <xs:restriction> holding nothing but an annotation is one).

  Everything downstream sees one node shape instead:

    {:kind :complexType, :attrs {:name \"Foo\"}, :children [node ...], :doc \"..\"}

  Attribute values stay raw strings - QName resolution happens later, against
  the document's prefix bindings.

  Unrecognized structure is fatal. A regenerated meta-schema that flipped an
  element from one child mode to another would otherwise change the parse
  silently, so anything outside those modes raises ex-info naming the element,
  the key and the source URI."
  (:require [clojure.string :as str]))

(def xsd-element-kinds
  "Every element name the XML Schema namespace defines, XSD 1.0 and 1.1.

  Membership is the tripwire for a document that is not a schema document, or
  for a parse that produced something other than schema components. Kinds this
  project does not model - identity constraints, notations, assertions - are
  listed so they reach the AST and get ignored deliberately rather than
  crashing the loader."
  #{:all :alternative :annotation :any :anyAttribute :appinfo :assert :assertion
    :attribute :attributeGroup :choice :complexContent :complexType
    :defaultOpenContent :documentation :element :enumeration :explicitTimezone
    :extension :field :fractionDigits :group :import :include :key :keyref
    :length :list :maxExclusive :maxInclusive :maxLength :minExclusive
    :minInclusive :minLength :notation :openContent :override :pattern
    :redefine :restriction :schema :selector :sequence :simpleContent
    :simpleType :totalDigits :union :unique :whiteSpace})

(def ^:private keyed-child-order
  "Content-model order for the keyed children of an element kind. Keyed
  children arrive in a map, which has no order of its own; kinds absent from
  this table are emitted sorted by key, which puts annotation first and is the
  content-model order for every kind the parse actually delivers keyed
  children for."
  {:key    [:annotation :selector :field]
   :keyref [:annotation :selector :field]
   :unique [:annotation :selector :field]})

(defn- props-map? [x]
  (and (map? x) (not (record? x))))

(defn- props-vector? [x]
  (and (vector? x) (every? props-map? x)))

(defn- unknown-shape! [uri kind k v]
  (throw (ex-info "unrecognized child shape in the parsed schema document; the meta-schema may have drifted"
                  {:type :xsd-to-malli/unknown-child-shape
                   :uri (str uri)
                   :element kind
                   :key k
                   :value-class (class v)})))

(defn- text-of
  "First non-blank string of an xs:documentation body. The body is mixed
  content, so it is a hiccup vector of strings and elements, or a bare string
  when the element holds nothing but text."
  [props]
  (let [value (:xml/value props)]
    (cond
      (string? value) (when-not (str/blank? value) (str/trim value))
      (sequential? value) (some #(when (and (string? %) (not (str/blank? %))) (str/trim %))
                                value))))

(defn- documentation-text
  "First xs:documentation string inside an annotation's props, matching the
  single documentation string the oracle attaches to a declaration."
  [props]
  (some (fn [child]
          (when (and (vector? child) (= :documentation (first child)))
            (text-of (second child))))
        (:xml/value props)))

(declare node)

(defn- keyed-tuples
  "Keyed children of a props map as [tag props] tuples, in content-model
  order. Keys carrying attribute strings and :xml/value are not children."
  [uri kind props]
  (let [keyed (reduce-kv (fn [acc k v]
                           (cond
                             (= :xml/value k) acc
                             (string? v) acc
                             (props-map? v) (assoc acc k [v])
                             (props-vector? v) (assoc acc k (vec v))
                             :else (unknown-shape! uri kind k v)))
                         {} props)
        declared (get keyed-child-order kind [])
        order (into (vec declared) (sort (remove (set declared) (keys keyed))))]
    (into [] (mapcat (fn [k] (map (fn [p] [k p]) (get keyed k)))) order)))

(defn- string-attrs
  "Props entries carrying a raw attribute value."
  [props]
  (reduce-kv (fn [acc k v] (if (string? v) (assoc acc k v) acc)) {} props))

(defn- split-props
  "Split a props map into [attrs keyed-children doc]."
  [uri kind props]
  (let [attrs (string-attrs props)
        keyed (keyed-tuples uri kind props)]
    [attrs
     keyed
     (some (fn [[tag child-props]]
             (when (= :annotation tag) (documentation-text child-props)))
           keyed)]))

(defn- value-children
  "Children of an :xml/value stream as [tag props] tuples. A tuple item is one
  child; a props map item is a map-mode arm whose entries are children of that
  position."
  [uri kind props]
  (let [value (:xml/value props)]
    (when (some? value)
      (when-not (sequential? value)
        (unknown-shape! uri kind :xml/value value))
      (into []
            (mapcat (fn [item]
                      (cond
                        (and (vector? item) (keyword? (first item)) (props-map? (second item)))
                        [item]

                        (props-map? item)
                        (keyed-tuples uri kind item)

                        :else
                        (unknown-shape! uri kind :xml/value item))))
            value))))

(defn- annotation-node
  "An annotation's body is arbitrary foreign markup delivered as hiccup, not
  schema components, so normalization stops at the documentation string."
  [kind props]
  (let [doc (documentation-text props)]
    (cond-> {:kind kind :attrs (string-attrs props) :children []}
      (some? doc) (assoc :doc doc))))

(defn node
  "AST node for one [tag props] component of a parsed schema document."
  [uri [kind props]]
  (when-not (contains? xsd-element-kinds kind)
    (throw (ex-info "element is not part of the XML Schema vocabulary"
                    {:type :xsd-to-malli/unknown-element-kind
                     :uri (str uri)
                     :element kind})))
  (when-not (props-map? props)
    (unknown-shape! uri kind :props props))
  (if (= :annotation kind)
    (annotation-node kind props)
    (let [[attrs keyed doc] (split-props uri kind props)
          value (value-children uri kind props)
          children (into []
                         (comp (remove (fn [[tag _]] (= :annotation tag)))
                               (map (partial node uri)))
                         (concat keyed value))
          doc (or doc
                  (some (fn [[tag child-props]]
                          (when (= :annotation tag) (documentation-text child-props)))
                        value))]
      (cond-> {:kind kind :attrs attrs :children children}
        (some? doc) (assoc :doc doc)))))

(defn attr
  "Raw string value of an XSD attribute on a node."
  [node k]
  (get (:attrs node) k))

(defn children-of
  "Child nodes of `node` whose kind is in `kinds`."
  [node kinds]
  (let [kinds (if (keyword? kinds) #{kinds} (set kinds))]
    (filterv (comp kinds :kind) (:children node))))

(defn child
  "First child node of `node` whose kind is in `kinds`, or nil."
  [node kinds]
  (first (children-of node kinds)))
