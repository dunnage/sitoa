(ns dunnage.sitoa.xsd-to-malli.derive
  "The vocabulary generated namespaces derive complex types with.

  A derived type's file rebuilds itself from its base type's schema as an
  ordinary `->` chain of malli.util operations - `mu/assoc` a declared
  attribute, `mu/dissoc` a prohibited one - over the pieces the four functions
  here pull out of the base. Nothing about the base is copied into the
  generated source, so the derivation stays modelled rather than flattened, and
  a reader sees what the derivation does instead of a plan only an interpreter
  understands.

  malli.util addresses map entries by KEY; \"the attribute part\" and \"the
  content part\" of a compiled complex type are POSITIONAL concepts decided by
  the type's shape, so those two extractions, the flattening `:merge` and the
  value wrap are the four things mu cannot express. Everything else in a
  generated chain is a plain mu call.

  The form accessors and the choice promotion are shared verbatim with
  dunnage.sitoa.xsd-to-malli.runtime, which is where they were ported from
  dunnage.sitoa.bootstrapped-schema; runtime aliases them back so the compiler
  and its tests keep one name for each. This namespace requires only malli and
  the primitives registry, so a generated tree loads no compiler or interpreter
  code at all."
  (:require [malli.core :as m]
            [dunnage.sitoa.xml-primitives :as xml-primitives]))

;; ---------------------------------------------------------------------------
;; Form access
;;
;; Forms reach these functions from two directions: the compiler builds them
;; with an explicit properties map, and m/form hands them back without one when
;; the properties are empty. Every accessor tolerates both.
;; ---------------------------------------------------------------------------

(defn form-tag [form]
  (when (and (vector? form) (seq form)) (nth form 0)))

(defn form-props [form]
  (when (and (vector? form) (< 1 (count form)) (map? (nth form 1)))
    (nth form 1)))

(defn form-children [form]
  (let [form (vec form)]
    (if (form-props form) (subvec form 2) (subvec form 1))))

(defn attr-row? [row]
  (and (vector? row) (map? (second row)) (:xml/attr (second row))))

(defn value-row? [row]
  (and (vector? row) (= :xml/value (nth row 0))))

;; ---------------------------------------------------------------------------
;; Complex type shapes
;;
;; A compiled complex type is one of five shapes, decided by whether it has
;; attributes and what its content is (bootstrapped_schema.clj lines 650-679).
;; Derivation needs to pull the base's attribute rows and content back out, and
;; the shape says where they are.
;; ---------------------------------------------------------------------------

(defn shape-of [form]
  (cond
    (not (vector? form)) :content-only

    (= :merge (form-tag form))
    (let [first-part (first (form-children form))
          rows (when (and (vector? first-part) (= :map (form-tag first-part)))
                 (form-children first-part))]
      ;; The attribute map is always the first part of the :merge and holds
      ;; nothing but attribute rows; a :merge of content maps never does.
      (if (and (seq rows) (every? attr-row? rows)) :merge :content-only))

    (= :map (form-tag form))
    (let [props (or (form-props form) {})
          rows (form-children form)]
      (cond
        (:empty props) :empty
        (:xml/value-wrapped props) :value-wrapped
        (and (seq rows) (every? attr-row? rows)) :attrs-only
        :else :content-only))

    :else :content-only))

(defn attrs-of
  "Attribute rows of a compiled complex type form."
  [form shape]
  (case shape
    :merge (vec (form-children (first (form-children form))))
    :value-wrapped (into [] (remove value-row?) (form-children form))
    :attrs-only (vec (form-children form))
    []))

(defn content-of
  "Content form of a compiled complex type, or nil when it has none."
  [form shape]
  (case shape
    :merge (let [parts (vec (form-children form))
                 content (subvec parts 1)]
             (if (= 1 (count content)) (first content) (into [:merge {}] content)))
    :value-wrapped (some (fn [row] (when (value-row? row) (peek row))) (form-children form))
    :attrs-only nil
    :empty nil
    form))

(defn promote-value-choice-to-alt
  "Port of bootstrapped_schema.clj lines 567-584."
  [form]
  (cond
    (and (vector? form) (= :or (first form)))
    (assoc form 0 :alt)
    (and (vector? form) (#{:? :* :+ :repeat} (first form)))
    (let [idx (dec (count form))]
      (assoc form idx (promote-value-choice-to-alt (nth form idx))))
    (and (vector? form) (= :sequential (first form)))
    (assoc form (dec (count form))
           (promote-value-choice-to-alt (last form)))
    :else form))

;; ---------------------------------------------------------------------------
;; The derivation vocabulary
;;
;; Each one takes schemas and returns a schema built with the same options, so
;; a generated chain never has to thread `options` through anything but its
;; own m/schema calls.
;; ---------------------------------------------------------------------------

(defn attrs
  "The attribute rows of a complex type's schema, as a fresh closed map schema.

  The starting point of every derived type's chain: mu/assoc adds the rows this
  type declares, mu/dissoc removes the ones it prohibits or redeclares."
  [schema]
  (let [form (m/form schema)]
    (m/schema (into [:map {:closed true}] (attrs-of form (shape-of form)))
              (m/options schema))))

(defn content
  "The content model of a complex type's schema, as a schema; nil when it has
  none."
  [schema]
  (let [form (m/form schema)]
    (when-some [c (content-of form (shape-of form))]
      (m/schema c (m/options schema)))))

(defn- merge-parts [x]
  (if (= :merge (m/type x)) (vec (m/children x)) [x]))

(defn entries-merge
  "A `:merge` of the given schemas, splicing every `:merge` argument's parts in
  flat.

  mu/merge would deref the parts into one `:map`; a compiled complex type keeps
  its `[:merge attribute-map content-part ...]` structure, and the oracle's
  field combination flattens rather than nests. The first argument is usually
  the derived attribute map, a `:map` the flattening leaves alone, but it is
  the base's extracted content in the attribute-less case, where it can be a
  `:merge` that has to flatten too."
  [part & parts]
  (m/schema (into [:merge] (mapcat merge-parts) (cons part parts))
            (m/options part)))

(defn value-wrapped
  "An attribute map plus a value: the shape a simple-content or seqex-content
  complex type takes.

  `value` is a schema, a form, or the `:xml/hiccup` keyword mixed content
  collapses to. The row is appended without a properties map because m/form
  keeps empty entry properties, and a choice at the top of the value is
  promoted to `:alt` exactly as the oracle promotes it."
  [attrs value]
  (let [v (if (m/schema? value) (m/form value) value)]
    (m/schema (-> (m/form attrs)
                  (update 1 assoc :xml/value-wrapped true)
                  (conj [:xml/value (promote-value-choice-to-alt v)]))
              (m/options attrs))))

;; ---------------------------------------------------------------------------
;; Registry realization
;; ---------------------------------------------------------------------------

(defn realize-registry
  "Compile every registry value against the whole registry.

  xml-primitives/closed-make-schema maps mu/closed-schema over raw registry
  VALUES, which works for literal forms but not for the IntoSchema values a
  generated namespace exports: compiled outside the registry, their [:ref ...]
  children have nothing to resolve against. Realizing first hands
  closed-make-schema plain schemas."
  [registry]
  (persistent!
   (reduce-kv
    (fn [acc k _]
      (assoc! acc k (m/deref (m/schema [:schema {:registry registry} k]
                                       xml-primitives/external-registry))))
    (transient {})
    registry)))
