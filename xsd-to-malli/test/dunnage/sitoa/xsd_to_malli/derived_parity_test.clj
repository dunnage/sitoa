(ns dunnage.sitoa.xsd-to-malli.derived-parity-test
  "A derived type's emitted `->` chain builds exactly what the interpreter
  built.

  runtime/derive-complex is the executable specification of a derivation plan;
  emit/derived-chain compiles the same plan into malli.util operations. The two
  have to agree on the nose, so every derived registry key is built both ways
  against the SAME loaded tree and the two m/forms are compared. m/form
  equality is row-order sensitive and entry-property sensitive, so a chain that
  reorders attribute rows, drops :optional, or leaks an empty property map
  fails on the key it happens to.

  XMLSchema.xsd's 70 derived keys cannot be checked here - the checked-in
  meta-schema under generated-src shadows the emitted tree in-process - so the
  same comparison runs in the child JVM of
  equivalence-test/the-new-emitters-xmlschema-tree-holds-on-a-clean-classpath.

  The synthetic plans at the bottom pin the branches no fixture reaches."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.walk :as walk]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [dunnage.sitoa.xsd-to-malli.compiler :as compiler]
            [dunnage.sitoa.xsd-to-malli.emit :as emit]
            [dunnage.sitoa.xsd-to-malli.runtime :as rt]
            [dunnage.sitoa.xsd-to-malli.support :as support]
            [malli.core :as m]))

;; ---------------------------------------------------------------------------
;; The old side: the same plans, run through the interpreter
;; ---------------------------------------------------------------------------

(declare plan->interp)

(defn- resolve-value
  "Replace every derivation node in a value by an interpreter-backed
  IntoSchema, which is what the emitter replaces them by chains."
  [x]
  (cond
    (compiler/derived? x) (plan->interp (:plan x))
    (map? x) (into (empty x) (map (fn [[k v]] [k (resolve-value v)])) x)
    (vector? x) (mapv resolve-value x)
    (set? x) (into (empty x) (map resolve-value) x)
    :else x))

(defn- resolve-plan
  "The plan the interpreter wants: base symbols resolved against the loaded
  tree, nested derivation nodes made interpreter-backed too."
  [plan]
  (let [p (resolve-value plan)]
    (cond-> p
      (symbol? (:base p)) (assoc :base @(requiring-resolve (:base p)))
      (symbol? (:content-source p)) (assoc :content-source
                                           @(requiring-resolve (:content-source p))))))

(defn- plan->interp [plan]
  (reify m/IntoSchema
    (-into-schema [_ _ _ options] (rt/derive-complex (resolve-plan plan) options))))

;; ---------------------------------------------------------------------------
;; Building both sides
;; ---------------------------------------------------------------------------

(def ^:private probe ::probe)

(defn- key-form
  "The form a registry key builds to, with the registry in scope so [:ref ...]
  children resolve. Two derefs: the first lands on the pointer m/schema makes
  for a registry keyword, the second on the value itself."
  [registry k]
  (m/form (m/deref (m/deref (m/schema [:schema {:registry registry} k]
                                      xml-primitives/external-registry)))))

(defn- value-form
  "The form an arbitrary registry value builds to, in a registry it can see."
  [registry v]
  (key-form (assoc registry probe v) probe))

(defn- canonical-emit [fixture k]
  (emit/canonicalize-form (:emit (get (:registry (:compiled @fixture)) k))))

(defn- derived-keys [fixture]
  (into (sorted-set)
        (comp (filter (fn [[_ v]] (compiler/derived? (:emit v)))) (map key))
        (:registry (:compiled @fixture))))

(defn- embedded-host-keys
  "Registry keys that are not themselves derived but whose value embeds an
  anonymous derived type."
  [fixture]
  (into (sorted-set)
        (comp (remove (fn [[_ v]] (compiler/derived? (:emit v))))
              (filter (fn [[_ v]] (some compiler/derived? (tree-seq coll? seq (:emit v)))))
              (map key))
        (:registry (:compiled @fixture))))

;; ---------------------------------------------------------------------------
;; Fixture parity
;; ---------------------------------------------------------------------------

(deftest every-derived-key-builds-what-the-interpreter-builds
  (doseq [fixture [support/multifile support/junit support/fop]]
    (testing (:name @fixture)
      (let [registry (:registry @fixture)
            ks (derived-keys fixture)]
        (doseq [k ks]
          (let [plan (:plan (canonical-emit fixture k))
                old (key-form (assoc registry k (plan->interp plan)) k)
                new (key-form registry k)]
            (testing (str k)
              (is (vector? old) "non-vacuity: the comparison sees a built form")
              (is (= old new)))))))))

(deftest the-fixtures-cover-the-derivations-they-are-here-for
  (is (= 6 (count (derived-keys support/multifile))))
  (is (= #{:none :own :splice-map}
         (into #{} (map #(:mode (:plan (canonical-emit support/multifile %))))
               (derived-keys support/multifile))))
  (testing "junit and fop derive nothing at registry level - junit's one
            derivation is anonymous, inside a top-type arm"
    (is (= 0 (count (derived-keys support/junit))))
    (is (= 0 (count (derived-keys support/fop))))))

(deftest a-value-embedding-an-anonymous-derived-type-builds-the-same-too
  (doseq [fixture [support/multifile support/junit support/fop]]
    (testing (:name @fixture)
      (let [registry (:registry @fixture)]
        (doseq [k (embedded-host-keys fixture)]
          (testing (str k)
            (is (= (key-form (assoc registry k (resolve-value (canonical-emit fixture k))) k)
                   (key-form registry k)))))))))

(deftest junits-anonymous-extension-inside-the-top-type-builds-the-same
  (let [{:keys [registry top-type compiled]} @support/junit
        arm-type (fn [tt tag] (peek (some (fn [[t arm]] (when (= t tag) arm)) (drop 2 tt))))
        old (resolve-value (emit/canonicalize-form (arm-type (:top-type compiled) :testsuites)))
        new (arm-type top-type :testsuites)]
    (is (some? old))
    (is (= (value-form registry old) (value-form registry new)))
    (testing "and the arm really does carry a derivation"
      (is (some compiler/derived?
                (tree-seq coll? seq (arm-type (:top-type compiled) :testsuites)))))))

;; ---------------------------------------------------------------------------
;; Synthetic plans: the branches no fixture reaches
;; ---------------------------------------------------------------------------

(def attrs-map
  [:map {:closed true}
   [:createdBy {:xml/attr true :optional true} :string]
   [:version {:xml/attr true :optional true} :string]])

(def content-map [:map {:closed true} [:id :string]])
(def other-map [:map {:closed true} [:note {:optional true} :string]])

(def merge-base [:merge {} attrs-map content-map])
(def multipart-content-base [:merge {} content-map other-map])
(def value-wrapped-base
  [:map {:closed true :xml/value-wrapped true}
   [:currency {:xml/attr true} :string]
   [:xml/value [:cat [:tuple [:enum :a] :string]]]])
(def seqex-base [:cat [:tuple [:enum :a] :string]])

(def ^:private this-ns "dunnage.sitoa.xsd-to-malli.derived-parity-test")

(defn- sym [n] (symbol this-ns n))

(def ^:private alias->ns
  {"m" "malli.core"
   "mu" "malli.util"
   "xd" "dunnage.sitoa.xsd-to-malli.derive"})

(defn- qualify
  "The emitter writes chains against the aliases a generated ns establishes;
  evaluating one here needs them spelled out."
  [code]
  (walk/postwalk
   (fn [x]
     (if (and (symbol? x) (alias->ns (namespace x)))
       (symbol (alias->ns (namespace x)) (name x))
       x))
   code))

(defn- chain-schema
  "Build the emitter's chain for a plan, with the hoisted payloads let-bound
  where a generated file would def them."
  [plan]
  (let [{:keys [hoists body]} (#'emit/derived-chain 'sch plan)]
    (eval (qualify (list 'let (into [] cat hoists)
                         (list 'reify 'm/IntoSchema
                               (list '-into-schema '[_ _ _ options] body)))))))

(defn- chain-code [plan] (#'emit/derived-chain 'sch plan))

(def ^:private synthetic-registry
  (merge xml-primitives/xmlschema-registry
         {:t/Base merge-base
          :t/MultiPart multipart-content-base
          :t/Value value-wrapped-base
          :t/Seqex seqex-base}))

(defn- synthetic-parity [label plan]
  (testing label
    (let [old (value-form synthetic-registry (plan->interp plan))
          new (value-form synthetic-registry (chain-schema plan))]
      (is (= old new)))))

(deftest mixed-content-beats-the-content-particle-in-every-mode
  ;; assemble-complex checks attributes+mixed? before attributes+content, so a
  ;; mixed complexContent derivation value-wraps :xml/hiccup and IGNORES what
  ;; its mode would otherwise have built.
  (let [own-plan {:base (sym "merge-base") :base-shape :merge
                  :base-attr-keys (sorted-set :createdBy :version)
                  :attrs [[:extra {:xml/attr true} :string]] :drop-attrs #{}
                  :mixed? true :mode :own :own-content content-map :empty? false}
        base-plan {:base (sym "merge-base") :base-shape :merge
                   :base-attr-keys (sorted-set :createdBy :version)
                   :attrs [[:extra {:xml/attr true} :string]] :drop-attrs #{}
                   :mixed? true :mode :base
                   :content-source (sym "merge-base") :content-shape :merge
                   :content-head :map :empty? false}]
    (synthetic-parity "mode :own" own-plan)
    (synthetic-parity "mode :base" base-plan)
    (testing "and the content it discards is not emitted at all"
      (is (empty? (:hoists (chain-code own-plan))))
      (is (= '(xd/value-wrapped :xml/hiccup) (last (:body (chain-code own-plan))))))))

(deftest a-splice-over-multi-part-base-content-stays-flat
  ;; combine-fields concatenates; nesting the base's own :merge inside the
  ;; result would be a different schema.
  (synthetic-parity
   "no attributes, base content extracts as a multi-part :merge"
   {:base (sym "multipart-content-base") :base-shape :content-only
    :base-attr-keys (sorted-set)
    :attrs [] :drop-attrs #{} :mixed? false :mode :splice-map
    :content-source (sym "multipart-content-base") :content-shape :content-only
    :own-content [:map {:closed true} [:detail :string]]
    :splice-props {:closed true}}))

(deftest prohibiting-every-inherited-attribute-leaves-no-attribute-map
  (synthetic-parity
   "drop-attrs empties the base's rows"
   {:base (sym "merge-base") :base-shape :merge
    :base-attr-keys (sorted-set :createdBy :version)
    :attrs [] :drop-attrs (sorted-set :createdBy :version)
    :mixed? false :mode :own :own-content content-map :empty? false}))

(deftest a-narrowed-simple-value-is-promoted-only-under-an-attribute-map
  (synthetic-parity
   "with attributes: value-wrapped, :or promoted to :alt"
   {:base (sym "value-wrapped-base") :base-shape :value-wrapped
    :base-attr-keys (sorted-set :currency)
    :attrs [[:currency {:xml/attr true} :string]] :drop-attrs #{}
    :mixed? false :mode :none :simple [:or :string [:enum "x"]]})
  (synthetic-parity
   "without attributes: the bare value, unpromoted"
   {:base (sym "seqex-base") :base-shape :content-only
    :base-attr-keys (sorted-set)
    :attrs [] :drop-attrs #{} :mixed? false :mode :none
    :simple [:or :string [:enum "x"]]}))

(deftest an-empty-content-model-is-the-attribute-map-or-the-empty-map
  (synthetic-parity
   "with attributes"
   {:base (sym "merge-base") :base-shape :merge
    :base-attr-keys (sorted-set :createdBy :version)
    :attrs [] :drop-attrs #{} :mixed? false :mode :none :empty? true})
  (synthetic-parity
   "without any"
   {:base (sym "seqex-base") :base-shape :content-only
    :base-attr-keys (sorted-set)
    :attrs [] :drop-attrs #{} :mixed? false :mode :none :empty? true}))

(deftest an-extension-inherits-a-value-wrapped-bases-content
  (synthetic-parity
   "mode :base over a value-wrapped source"
   {:base (sym "value-wrapped-base") :base-shape :value-wrapped
    :base-attr-keys (sorted-set :currency)
    :attrs [[:extra {:xml/attr true} :string]] :drop-attrs #{}
    :mixed? false :mode :base
    :content-source (sym "value-wrapped-base") :content-shape :value-wrapped
    :content-head :cat :empty? false}))

(deftest a-seqex-splice-without-attributes-is-a-bare-cat
  (synthetic-parity
   "mode :splice-cat, no attributes"
   {:base (sym "seqex-base") :base-shape :content-only
    :base-attr-keys (sorted-set)
    :attrs [] :drop-attrs #{} :mixed? false :mode :splice-cat
    :content-source (sym "seqex-base") :content-shape :content-only
    :own-content [:tuple [:enum :b] :string]}))

(deftest a-derivation-that-assembles-nothing-is-refused-at-generation
  ;; The interpreter hands m/schema a nil form and throws at build time; a
  ;; chain has nothing to emit, so it says so where the source is written.
  (is (= :xsd-to-malli/empty-derivation
         (try (chain-code {:base (sym "seqex-base") :base-shape :content-only
                           :base-attr-keys (sorted-set)
                           :attrs [] :drop-attrs #{} :mixed? true
                           :mode :none :empty? false})
              nil
              (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))

;; ---------------------------------------------------------------------------
;; Hoisting
;; ---------------------------------------------------------------------------

(deftest payloads-that-grow-without-bound-live-in-their-own-def
  (let [plan {:base (sym "merge-base") :base-shape :merge
              :base-attr-keys (sorted-set :createdBy :version)
              :attrs [[:small {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/string]]
                      [:big {:xml/attr true} [:or [:enum "a"] [:enum "b"]]]]
              :drop-attrs #{} :mixed? false :mode :own
              :own-content content-map :empty? false}
        {:keys [hoists body]} (chain-code plan)]
    (is (= '[sch-attr-big sch-content] (mapv first hoists)))
    (testing "a keyword or a [:ref kw] attribute value stays in the chain"
      (is (some #(= % '(mu/assoc [:small {:xml/attr true}]
                                 [:ref :org.w3.www.2001.XMLSchema/string]))
                (tree-seq coll? seq body))))
    (testing "anything bigger is referenced by name"
      (is (some #(= % '(mu/assoc [:big {:xml/attr true}] sch-attr-big))
                (tree-seq coll? seq body))))
    (testing "an anonymous node has no namespace to hoist into, so it inlines"
      (is (empty? (:hoists (#'emit/derived-chain nil plan)))))))
