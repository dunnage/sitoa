(ns dunnage.sitoa.xsd-to-malli.runtime
  "Schema assembly shared by the compiler and by the code it generates.

  Everything here is a pure function over malli FORMS. The compiler runs these
  functions over the compiled forms at generation time, which is what makes the
  generated schema and the XSOM pipeline agree, and `derive-complex` runs the
  same rules over a derivation plan - the executable specification the emitted
  `->` chains are checked against.

  The assembly rules are ports of dunnage.sitoa.bootstrapped-schema, cited per
  function.

  The form accessors and the choice promotion live in
  dunnage.sitoa.xsd-to-malli.derive, which generated code requires and which
  therefore may not depend on anything here; they are aliased back below so
  every caller keeps one name for each."
  (:require [malli.core :as m]
            [dunnage.sitoa.xsd-to-malli.derive :as derive]))

;; ---------------------------------------------------------------------------
;; Form access and complex type shapes, aliased from the generated-code
;; namespace that owns them
;; ---------------------------------------------------------------------------

(def form-tag derive/form-tag)
(def form-props derive/form-props)
(def form-children derive/form-children)
(def attr-row? derive/attr-row?)
(def value-row? derive/value-row?)
(def shape-of derive/shape-of)
(def attrs-of derive/attrs-of)
(def content-of derive/content-of)

;; ---------------------------------------------------------------------------
;; Particle assembly (ports)
;; ---------------------------------------------------------------------------

(defn simplify-fields
  "Reducing function that folds map-mode fields into :map / :merge chains.

  Port of bootstrapped_schema.clj lines 203-236. The :x marker distinguishes a
  map this function opened, and so may keep filling, from one handed to it."
  [props]
  (fn
    ([] [:map (assoc props :x :x)])
    ([acc]
     (if (= :map (nth acc 0))
       (update acc 1 dissoc :x)
       acc))
    ([acc val]
     (case (nth acc 0)
       :map
       (case (form-tag val)
         :map (if (= (count acc) 2)
                [:merge {} val]
                [:merge {} (update acc 1 dissoc :x) val])
         :merge (if (= (count acc) 2)
                  val
                  (into [:merge {} acc] (form-children val)))
         (conj acc val))
       :merge
       (case (form-tag val)
         :map (conj acc val)
         :merge (into acc (form-children val))
         (let [last-index (dec (count acc))]
           (if (-> acc (get last-index) second :x)
             (update acc last-index conj val)
             (conj acc [:map (assoc props :x :x) val]))))))))

(defn combine-fields
  "simplify-fields over a sequence of already-compiled fields."
  [props vals]
  (let [rf (simplify-fields props)]
    (rf (reduce rf (rf) vals))))

(defn mark-map-in-seq-ex
  "Port of bootstrapped_schema.clj lines 299-308."
  [msch]
  (if (and (vector? msch) (= :map (first msch)))
    (let [props (second msch)
          props (if (map? props) props {})]
      (assoc msch 1 (assoc props :xml/in-seq-ex true)))
    msch))

(defn regex-occurrence
  "Port of bootstrapped_schema.clj lines 310-322."
  [min-occurs max-occurs msch]
  (let [can-be-empty? (= 0 min-occurs)
        unbounded? (= max-occurs -1)
        repeated? (or (> max-occurs 1) (= max-occurs -1))]
    (cond
      (and (not can-be-empty?) (not repeated?)) msch
      (and can-be-empty? (not repeated?)) [:? (mark-map-in-seq-ex msch)]
      (and (not can-be-empty?) repeated? unbounded?) [:+ (mark-map-in-seq-ex msch)]
      (and can-be-empty? repeated? unbounded?) [:* (mark-map-in-seq-ex msch)]
      :else [:repeat {:min min-occurs :max max-occurs} (mark-map-in-seq-ex msch)])))

(defn all-maps?
  "Port of bootstrapped_schema.clj lines 419-432, bug included.

  The original compares the SET #{:map :merge} against a child's head keyword,
  which is never equal, so the :cat post-collapse it guards never fires. The
  behaviour is load-bearing - collapsing would change parse results - so it is
  reproduced rather than corrected."
  [x]
  (transduce
   (drop 2)
   (fn ([acc] (if (nil? acc) false acc))
     ([_ nv]
      (if (and (vector? nv) (= #{:map :merge} (nth nv 0)))
        true
        (reduced false))))
   nil
   x))

(def promote-value-choice-to-alt derive/promote-value-choice-to-alt)

(defn value-wrap
  "Port of bootstrapped_schema.clj lines 586-591."
  [attr-map content]
  (-> attr-map
      (update 1 assoc :xml/value-wrapped true)
      (conj [:xml/value {} (promote-value-choice-to-alt content)])))

(defn assemble-complex
  "Combine a complex type's attributes and content into one form.

  Port of the XSComplexType -mtype cond, bootstrapped_schema.clj lines 650-679.
  `attrs` is the sequence of attribute rows; `content` the compiled content
  particle; `simple` the compiled simple content; `empty?` true when the
  content model is empty."
  [{:keys [attrs simple mixed? content empty?]}]
  (let [attr-map (when (seq attrs) (into [:map {:closed true}] attrs))]
    (cond
      (and attr-map simple) (value-wrap attr-map simple)
      (and attr-map mixed?) (value-wrap attr-map :xml/hiccup)
      (and attr-map content) (case (form-tag content)
                               :map [:merge {} attr-map content]
                               :merge (into [:merge {} attr-map] (form-children content))
                               (value-wrap attr-map content))
      (and attr-map (nil? content)) attr-map
      simple simple
      content content
      empty? (or attr-map [:map {:empty true}]))))

;; ---------------------------------------------------------------------------
;; Derivation
;; ---------------------------------------------------------------------------

(defn form-of
  "Form of a value a generated namespace exports as `sch`: either a literal
  form already, or an IntoSchema that has to be built first."
  [x options]
  (if (or (vector? x) (keyword? x))
    x
    (m/form (m/schema x options))))

(defn drop-empty-props
  "Drop empty property maps, which malli treats as absent.

  m/form normalizes them away for schemas but keeps them on map entries, so an
  assembled form is put in the same shape the emitted literals are before it
  becomes a schema. Only vectors headed by a keyword or symbol are touched, so
  :enum values are left alone."
  [form]
  (if (vector? form)
    (let [form (mapv drop-empty-props form)]
      (if (and (< 1 (count form))
               (= {} (nth form 1))
               (or (keyword? (nth form 0)) (symbol? (nth form 0))))
        (into (subvec form 0 1) (subvec form 2))
        form))
    form))

(defn derive-complex
  "Build a derived complex type from its base type's schema.

  The executable specification of what a derived type's emitted chain has to
  produce: derived_parity_test compares the m/form of every derived registry
  value against what this function makes of the same plan. Generated code does
  not call it - it carries its derivation as malli.util operations instead -
  so this is the compiler's and the tests' entry point only.

  `plan` is the compiler's description of what one derivation does; `:base` and
  `:content-source` hold the base namespace's exported schema values, which is
  the only place base rows come from.

    :base            base type value supplying inherited attribute rows
    :base-shape      shape-of the base type's form
    :content-source  base value supplying inherited content, when any
    :content-shape   shape-of that value's form
    :mode            :base | :own | :splice-map | :splice-cat | :none
    :own-content     content compiled from this type's own particle
    :splice-props    simplify-fields properties for :splice-map
    :attrs           attribute rows this type declares
    :drop-attrs      attribute keys this type prohibits
    :simple          compiled simple content, or :from-base
    :mixed?          effective mixed=\"true\"
    :empty?          effective content model is empty

  A plan also carries :base-attr-keys and :content-head, which only the emitter
  reads: it decides statically what this function decides from the base's form
  at build time."
  [{:keys [base base-shape content-source content-shape mode own-content
           splice-props attrs drop-attrs simple mixed? empty?]}
   options]
  (let [base-form (form-of base options)
        redeclared (into (set drop-attrs) (map first) attrs)
        inherited (into [] (remove (fn [row] (contains? redeclared (first row))))
                        (attrs-of base-form base-shape))
        content-form (when content-source
                       (content-of (form-of content-source options) content-shape))
        content (case mode
                  :base content-form
                  :own own-content
                  :splice-map (combine-fields splice-props [content-form own-content])
                  ;; both slots already carry their occurrence wrappers: the
                  ;; base's -seq content is wrapped by the same rule the splice
                  ;; would apply, and the own slot is compiled with it
                  :splice-cat [:cat {} content-form own-content]
                  :none nil)]
    (m/schema
     (drop-empty-props
      (assemble-complex {:attrs (into inherited attrs)
                         :simple (if (= :from-base simple)
                                   (content-of base-form base-shape)
                                   simple)
                         :mixed? mixed?
                         :content content
                         :empty? empty?}))
     options)))

;; ---------------------------------------------------------------------------
;; Registry realization
;; ---------------------------------------------------------------------------

(def realize-registry derive/realize-registry)
