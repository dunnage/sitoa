(ns dunnage.sitoa.xsd-to-malli.emit
  "Write a compiled registry out as Clojure source.

  One .cljc namespace per registry type, plus an entry namespace that requires
  them all and assembles the registry, the top type and make-schema. A type
  that derives from another requires that type's namespace and rebuilds itself
  from its `sch` at schema-build time; every other cross-type edge stays a
  registry keyword, so the require graph is exactly the derivation graph and
  a cyclic XSD type graph still produces no cyclic requires.

  Every registry value is emitted as a self-contained m/IntoSchema, uniformly:
  a derived type's reify rebuilds it from its base, and an underived type's
  reify wraps its literal form in m/schema. One contract for consumers -
  registry values are schema constructors, never data to destructure.

  A derivation is written out as compiled code, not as data: a `->` chain of
  malli.util operations over the pieces dunnage.sitoa.xsd-to-malli.derive pulls
  out of the base's schema, so a reader sees the derivation - assoc this
  attribute, dissoc that prohibited one, merge in this content - rather than a
  plan only an interpreter could read. A derived type's file keeps its base
  require even when its chain never names the base, because the require graph
  is the derivation graph.

  The canonicalization, naming, reachability and classpath helpers below are
  copied from dunnage.sitoa.schema-namespaces rather than required from it:
  that namespace imports XSOM, and nothing under src/ may put XSOM on this
  project's runtime classpath. emit-test asserts the copies still agree with
  the originals."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [dunnage.sitoa.xsd-to-malli.compiler :as compiler]
            [dunnage.sitoa.xsd-to-malli.derive :as derive]
            [dunnage.sitoa.xsd-to-malli.loader :as loader]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [fipp.edn :refer [pprint] :rename {pprint fipp}])
  (:import (java.io File)
           (java.nio.file Files Paths)
           (java.nio.file.attribute FileAttribute)))

(def ^:private seq-suffix "-seq")

(def ^:private generated-doc
  "Generated from XSD by dunnage.sitoa.xsd-to-malli. Do not edit.")

;; ---------------------------------------------------------------------------
;; Canonicalization (schema_namespaces.clj lines 33-92)
;; ---------------------------------------------------------------------------

(defn- attr-row? [row]
  (and (vector? row) (map? (second row)) (:xml/attr (second row))))

(defn- sort-attribute-rows
  "Sort the :xml/attr rows of every :map form among themselves, reinserting
  them at the indexes attribute rows already occupied. Element row order is
  load-bearing for the parser, so only attribute rows move."
  [form]
  (walk/postwalk
   (fn [x]
     (if (and (vector? x) (= :map (first x)) (map? (second x)))
       (let [rows (vec (drop 2 x))
             idxs (into [] (keep-indexed (fn [i r] (when (attr-row? r) i))) rows)
             sorted-attrs (sort-by first (map rows idxs))]
         (into [(first x) (second x)]
               (persistent!
                (reduce (fn [acc [i r]] (assoc! acc i r))
                        (transient rows)
                        (map vector idxs sorted-attrs)))))
       x))
   form))

(defn- drop-empty-properties
  "Remove empty property maps, which malli treats as absent and m/form
  normalizes away, so generated source stays interchangeable with the EDN it
  replaces."
  [form]
  (walk/postwalk
   (fn [x]
     (if (and (vector? x)
              (< 1 (count x))
              (= {} (nth x 1))
              (or (keyword? (nth x 0)) (symbol? (nth x 0))))
       (into (subvec x 0 1) (subvec x 2))
       x))
   form))

(defn canonicalize-form
  "Attribute rows sorted, empty property maps dropped. Derivation nodes are
  records rather than vectors, so the walk passes over them and canonicalizes
  the literal data inside their plans."
  [form]
  (-> form sort-attribute-rows drop-empty-properties))

(defn canonicalize-registry [registry]
  (update-vals registry canonicalize-form))

;; ---------------------------------------------------------------------------
;; Reference collection (schema_namespaces.clj lines 98-141)
;; ---------------------------------------------------------------------------

(defn form-refs
  "Sorted set of registry keywords a compiled value references, in either
  emitted shape: [:ref :ns/Foo] and bare namespaced keywords alike, plus the
  keywords a derivation plan reaches through the base type it builds on."
  [registry-keys form]
  (into (sorted-set) (filter registry-keys) (compiler/form-deps form)))

(defn reachable-keys
  "Transitive closure of registry keys reachable from `seeds`."
  [registry seeds]
  (let [registry-keys (set (keys registry))
        missing (into (sorted-set) (remove registry-keys) seeds)]
    (when (seq missing)
      (throw (ex-info "top-types seeds absent from registry"
                      {:type :xsd-to-malli/unknown-top-type :missing missing})))
    (loop [queue (vec seeds)
           seen #{}]
      (if (empty? queue)
        seen
        (let [k (peek queue)
              queue (pop queue)]
          (if (contains? seen k)
            (recur queue seen)
            (recur (into queue
                         (comp (remove seen) (filter registry-keys))
                         (form-refs registry-keys (get registry k)))
                   (conj seen k))))))))

;; ---------------------------------------------------------------------------
;; Naming (schema_namespaces.clj lines 147-169)
;; ---------------------------------------------------------------------------

(defn kw->ns-sym
  "Namespace symbol for a registry keyword. A dot inside the local name would
  create a bogus extra namespace segment, so it is munged to an underscore."
  [k]
  (symbol (str (namespace k) "." (str/replace (name k) "." "_"))))

(defn ns-sym->path
  "Clojure's own source path rule: dashes to underscores, dots to separators."
  [ns-sym]
  (-> (str ns-sym)
      (str/replace "-" "_")
      (str/replace "." "/")
      (str ".cljc")))

(defn- base-kw
  "Fold :ns/Foo-seq into :ns/Foo when :ns/Foo is itself a registry key."
  [own-keys k]
  (let [n (name k)]
    (if (str/ends-with? n seq-suffix)
      (let [base (keyword (namespace k) (subs n 0 (- (count n) (count seq-suffix))))]
        (if (contains? own-keys base) base k))
      k)))

(defn- check-namespaced! [registry]
  (let [bare (into (sorted-set) (remove namespace) (keys registry))]
    (when (seq bare)
      (throw (ex-info "registry keys without a namespace cannot be mapped to files; supply :default-ns"
                      {:type :xsd-to-malli/missing-default-ns :keys bare})))))

(defn- check-path-collisions! [paths]
  (let [dupes (into (sorted-map)
                    (comp (map (fn [[k v]] [k (sort (map second v))]))
                          (filter (fn [[_ v]] (< 1 (count v)))))
                    (group-by (comp str/lower-case first) paths))]
    (when (seq dupes)
      (throw (ex-info "generated file paths collide after munging"
                      {:type :xsd-to-malli/path-collision :collisions dupes})))))

;; ---------------------------------------------------------------------------
;; Derivation nodes -> code
;; ---------------------------------------------------------------------------

(declare derived-chain)

(defn- ->code
  "Replace every derivation node in a value by the expression that rebuilds it.

  Each node becomes a self-contained IntoSchema, so it works wherever it lands:
  as a whole registry value, as a child inside a literal form, or as the type
  of a top-type arm - malli builds it with the options that carry the assembled
  registry, which is what its [:ref ...] children need.

  A node reached this way is nested inside another value and has no namespace
  of its own to hoist payloads into, so its chain carries its literals inline -
  which is where the plan literal it replaces sat as well, so the method-size
  profile is unchanged."
  [x]
  (cond
    (compiler/derived? x)
    (list 'reify 'm/IntoSchema
          (list '-into-schema '[_ _ _ options]
                (:body (derived-chain nil (:plan x)))))

    (map? x) (into (empty x) (map (fn [[k v]] [(->code k) (->code v)])) x)
    (vector? x) (mapv ->code x)
    (set? x) (into (empty x) (map ->code) x)
    :else x))

;; ---------------------------------------------------------------------------
;; Derivation plans -> malli.util chains
;; ---------------------------------------------------------------------------

(defn- attr-value-inline?
  "Attribute values a chain can carry in its method body: a registry keyword or
  a [:ref kw]. Both are bounded by construction; an anonymous simpleType
  enumerating thousands of codes is not, and goes to its own def."
  [v]
  (or (keyword? v)
      (and (vector? v) (= 2 (count v)) (= :ref (nth v 0)) (keyword? (nth v 1)))))

(defn- attr-def-name
  "Def name for a hoisted attribute value. Attribute keys are unique within one
  type, and the sch-/sch-seq- prefix keeps the two duals apart."
  [sch-name k]
  (symbol (str sch-name "-attr-" (str/replace (str (symbol k)) #"[^a-zA-Z0-9]" "_"))))

(defn- derived-chain
  "The defs and the -into-schema body for one derivation plan.

  Returns {:hoists [[name literal] ...] :body <code>}. `sch-name` names the
  registry value the chain belongs to and turns hoisting on; nil emits
  everything inline, which is what an anonymous node nested inside another
  value needs.

  The plan must already be CANONICAL (canonicalize-form). The interpreter this
  replaces dropped empty property maps over the whole assembled form at build
  time and so silently forgave an uncanonical literal; a chain hands its
  literals to m/schema as they stand, and m/form keeps empty entry properties,
  so an uncanonical plan would change the schema.

  The assembly mirrors runtime/assemble-complex, whose cond decides in this
  order: with an attribute map, simple content beats mixed content beats the
  content particle - so a mixed derivation value-wraps :xml/hiccup and drops
  its content in EVERY mode, and the content it would have built is not
  emitted at all. Without an attribute map mixed content is inert."
  [sch-name {:keys [base base-attr-keys attrs drop-attrs mixed? mode own-content
                    content-source content-shape content-head simple empty?]
             :as plan}]
  (let [top? (some? sch-name)
        attrs (vec attrs)
        base-attr-keys (set base-attr-keys)
        own-keys (mapv first attrs)
        redeclared (into (set drop-attrs) own-keys)
        ;; whether this type ends up with an attribute map at all is decided by
        ;; the rows that SURVIVE: a restriction can prohibit every inherited row
        final-attrs? (boolean (or (seq attrs) (seq (remove redeclared base-attr-keys))))
        content? (case mode
                   :own (some? own-content)
                   :base (not (contains? #{:attrs-only :empty} content-shape))
                   (:splice-map :splice-cat) true
                   false)
        kind (cond
               (some? simple) :simple
               (and final-attrs? mixed?) :mixed
               content? :content
               final-attrs? :attrs
               empty? :empty
               :else (throw (ex-info "derivation assembles neither attributes nor content"
                                     {:type :xsd-to-malli/empty-derivation :plan plan})))
        own-content? (and (= :content kind) (some? own-content))
        simple-literal? (and (= :simple kind) (not= :from-base simple))
        value-name (symbol (str sch-name "-value"))
        content-name (symbol (str sch-name "-content"))
        hoists (cond-> (into []
                             (keep (fn [row]
                                     (let [v (peek row)]
                                       (when (and top? (not (attr-value-inline? v)))
                                         [(attr-def-name sch-name (first row)) (->code v)]))))
                             attrs)
                 (and top? simple-literal?) (conj [value-name (->code simple)])
                 (and top? own-content?) (conj [content-name (->code own-content)]))
        ;; attr-def-name munges punctuation to '_', so distinct attribute keys
        ;; can collide on one def name; a later def would silently shadow the
        ;; earlier one and both mu/assoc sites would bind the last literal.
        _ (let [dupes (into (sorted-map)
                            (filter (fn [[_ v]] (< 1 v)))
                            (frequencies (map first hoists)))]
            (when (seq dupes)
              (throw (ex-info "hoisted def names collide after munging"
                              {:type :xsd-to-malli/hoist-name-collision
                               :sch-name sch-name :names (vec (keys dupes))}))))
        ;; the base is let-bound when the attribute thread and the tail both
        ;; read the same exported value; otherwise each names its own
        let? (and final-attrs?
                  (case kind
                    :simple (= :from-base simple)
                    :content (= content-source base)
                    false))
        sym-expr (fn [sym] (if (and let? (= sym base)) 'base (list 'm/schema sym 'options)))
        own-expr (list 'm/schema (if top? content-name (->code own-content)) 'options)
        value-expr (list 'm/schema (if top? value-name (->code simple)) 'options)
        simple-expr (if (= :from-base simple)
                      (list 'xd/content (sym-expr base))
                      value-expr)
        source-expr (list 'xd/content (sym-expr content-source))
        ;; :map and :merge content assembles as a :merge; anything else - a
        ;; seqex, a ref, a bare type - is value-wrapped
        merge-content? (case mode
                         :own (contains? #{:map :merge} (derive/form-tag own-content))
                         :base (contains? #{:map :merge} content-head)
                         :splice-map true
                         false)]
    {:hoists hoists
     :body
     (if final-attrs?
       (let [ops (into (into [] (map (fn [k] (list 'mu/dissoc k)))
                             (into (vec (sort drop-attrs)) (filter base-attr-keys) own-keys))
                       (map (fn [row]
                              (let [k (first row)
                                    props (when (= 3 (count row)) (nth row 1))
                                    v (peek row)]
                                (list 'mu/assoc
                                      (if (seq props) [k props] k)
                                      (if (and top? (not (attr-value-inline? v)))
                                        (attr-def-name sch-name k)
                                        (->code v))))))
                       attrs)
             tail (case kind
                    :simple [(list 'xd/value-wrapped simple-expr)]
                    :mixed [(list 'xd/value-wrapped :xml/hiccup)]
                    :attrs []
                    :content (case mode
                               :own [(list (if merge-content? 'xd/entries-merge 'xd/value-wrapped)
                                           own-expr)]
                               :base [(list (if merge-content? 'xd/entries-merge 'xd/value-wrapped)
                                            source-expr)]
                               :splice-map [(list 'xd/entries-merge source-expr own-expr)]
                               :splice-cat [(list 'xd/value-wrapped [:cat source-expr own-expr])]))
             thread (cons '-> (into (if let?
                                      [(list 'xd/attrs 'base)]
                                      [(list 'm/schema base 'options) 'xd/attrs])
                                    (concat ops tail)))]
         (if let?
           (list 'let ['base (list 'm/schema base 'options)] thread)
           thread))
       (case kind
         :simple simple-expr
         :content (case mode
                    :own own-expr
                    :base source-expr
                    :splice-map (list 'xd/entries-merge source-expr own-expr)
                    :splice-cat (list 'm/schema [:cat source-expr own-expr] 'options))
         :empty (list 'm/schema [:map {:empty true}] 'options)))}))

(defn- derivation-requires
  "Namespaces a compiled value has to require: the base namespaces its
  derivation nodes build on."
  [x]
  (cond
    (compiler/derived? x) (into (sorted-set) (:requires x))
    (map? x) (reduce-kv (fn [a k v] (into (into a (derivation-requires k))
                                          (derivation-requires v)))
                        (sorted-set) x)
    (coll? x) (reduce (fn [a v] (into a (derivation-requires v))) (sorted-set) x)
    :else (sorted-set)))

;; ---------------------------------------------------------------------------
;; Emission (schema_namespaces.clj lines 214-306)
;; ---------------------------------------------------------------------------

(defn- write-forms! [^File f forms]
  (io/make-parents f)
  (with-open [w (io/writer f :encoding "UTF-8")]
    (doseq [form forms]
      (fipp form {:writer w})))
  (.getPath f))

(defn- alias-namespaces
  "Namespace strings the qualified symbols in a piece of generated code use."
  [code]
  (into #{} (comp (filter symbol?) (keep namespace)) (tree-seq coll? seq code)))

(defn- code-requires
  "Require entries for a generated file: malli.core always, malli.util and the
  derivation vocabulary only when the code names them, then the base
  namespaces.

  A base namespace stays required even when the chain never mentions it - the
  no-attribute restrictions that dominate large schemas restate their content
  and read nothing off the base - because the require graph is the derivation
  graph and that is worth more than dropping an unused require."
  [code base-ns-syms]
  (let [used (alias-namespaces code)]
    (into (cond-> []
            (contains? used "xd") (conj '[dunnage.sitoa.xsd-to-malli.derive :as xd])
            :always (conj '[malli.core :as m])
            (contains? used "mu") (conj '[malli.util :as mu]))
          (map vector)
          base-ns-syms)))

(defn- registry-value-defs
  "The defs for one registry value, always ending in a self-contained
  m/IntoSchema named `sch-name`, so every generated registry value presents
  the same contract.

  The payload lives in its own def that the reify only references: a large
  literal built inside the -into-schema method body blows the JVM's 64KB
  method bytecode limit (FOP's block_List_FOP does), while the same literal
  compiles fine as a top-level def. An underived type's whole form is such a
  payload (`<name>-form`); a derived type's are the two literals that grow
  without bound - the content a restriction restates (`<name>-content`), the
  value type a simpleContent restriction narrows to (`<name>-value`) - and any
  attribute value bigger than a keyword or a [:ref kw]."
  [sch-name x]
  (if (compiler/derived? x)
    (let [{:keys [hoists body]} (derived-chain sch-name (:plan x))]
      (conj (into [] (map (fn [[n literal]] (list 'def n literal))) hoists)
            (list 'def sch-name
                  (list 'reify 'm/IntoSchema
                        (list '-into-schema '[_ _ _ options] body)))))
    (let [form-name (symbol (str sch-name "-form"))]
      [(list 'def form-name (->code x))
       (list 'def sch-name
             (list 'reify 'm/IntoSchema
                   (list '-into-schema '[_ _ _ options]
                         (list 'm/schema form-name 'options))))])))

(defn- type-file-forms [ns-sym registry-keys {:keys [sch sch-seq] :as group}]
  (let [values (if (contains? group :sch-seq) [sch sch-seq] [sch])
        defs (into (registry-value-defs 'sch sch)
                   (when (contains? group :sch-seq)
                     (registry-value-defs 'sch-seq sch-seq)))]
    (into [(list 'ns ns-sym generated-doc
                 (seq (into [:require] (code-requires defs (derivation-requires values)))))
           (list 'def 'deps (form-refs registry-keys values))]
          defs)))

(defn- entry-file-forms [entry-ns own-keys included top-type]
  (let [arms (into [] (map (fn [[tag arm]] [tag (canonicalize-form arm)]))
                   (sort-by first (drop 2 top-type)))
        ns-syms (into (sorted-set) (map (comp kw->ns-sym #(base-kw own-keys %))) included)
        arm-requires (derivation-requires arms)
        arm-code (->code arms)
        used (alias-namespaces arm-code)
        requires (into (cond-> [:require
                                '[dunnage.sitoa.xml-primitives :as xml-primitives]
                                '[dunnage.sitoa.xsd-to-malli.derive :as xd]]
                         (contains? used "m") (conj '[malli.core :as m])
                         (contains? used "mu") (conj '[malli.util :as mu]))
                       (map vector)
                       (into ns-syms arm-requires))
        reg-entries (into (sorted-map)
                          (map (fn [k]
                                 (let [bk (base-kw own-keys k)]
                                   [k (symbol (str (kw->ns-sym bk))
                                              (if (= bk k) "sch" "sch-seq"))])))
                          included)]
    [(list 'ns entry-ns generated-doc (seq requires))
     (list 'def 'registry
           (list 'merge 'xml-primitives/xmlschema-registry reg-entries))
     (list 'def 'top-type
           (list 'into [:multi {:dispatch 'first}] arm-code))
     (list 'defn 'make-schema
           (list [] (list 'make-schema 'top-type))
           (list '[start-type] (list 'xml-primitives/make-schema 'registry 'start-type)))
     (list 'defn 'closed-make-schema
           (list [] (list 'closed-make-schema 'top-type))
           (list '[start-type] (list 'xml-primitives/closed-make-schema
                                     (list 'xd/realize-registry 'registry)
                                     'start-type)))]))

(defn- group-entries
  "registry -> sorted map of base keyword -> {:sch value :sch-seq value}."
  [registry]
  (let [own-keys (set (keys registry))]
    (reduce-kv (fn [acc k v]
                 (let [bk (base-kw own-keys k)]
                   (assoc-in acc [bk (if (= bk k) :sch :sch-seq)] v)))
               (sorted-map)
               registry)))

(defn- check-require-acyclic!
  "Derivation is the only thing that turns a type edge into a namespace
  require, and derivation is acyclic - but an anonymous derived type nested
  inside another type adds an edge the symbol table's check never saw, so the
  graph the emitter is about to write is checked here as well."
  [groups]
  (let [edges (into (sorted-map)
                    (map (fn [[k group]]
                           [(kw->ns-sym k)
                            (derivation-requires (vals group))]))
                    groups)
        state (volatile! {})]
    (letfn [(visit [n path]
              (case (get @state n)
                :done nil
                :open (throw (ex-info "generated namespaces would require each other in a cycle"
                                      {:type :xsd-to-malli/require-cycle
                                       :cycle (vec (conj path n))}))
                (do (vswap! state assoc n :open)
                    (doseq [m (get edges n)]
                      (when (contains? edges m) (visit m (conj path n))))
                    (vswap! state assoc n :done))))]
      (doseq [n (keys edges)]
        (visit n [])))
    groups))

(defn emit-namespaces!
  "Write one .cljc file per registry type plus an entry namespace under
  :out-dir.

  Options:
    :out-dir   directory the source tree is written under (required)
    :entry-ns  symbol naming the aggregator namespace (required)
    :top-types optional registry keys used as reachability seeds; only their
               closure is registered by the entry namespace, though every type
               still gets a file.

  Files are overwritten but never deleted; regenerate into a clean directory.

  Returns {:files [...] :entry-ns sym :entry-file path :included #{kws}}."
  [{:keys [out-dir entry-ns top-types]} registry top-type]
  (assert (some? out-dir) ":out-dir is required")
  (assert (symbol? entry-ns) ":entry-ns must be a symbol")
  (let [registry (canonicalize-registry registry)
        _ (check-namespaced! registry)
        own-keys (set (keys registry))
        ;; deps name every registry keyword a value references, built-in
        ;; datatypes included, matching what the v1 emitter records
        registry-keys (into own-keys (keys xml-primitives/xmlschema-registry))
        groups (check-require-acyclic! (group-entries registry))
        included (if (seq top-types)
                   (into (sorted-set) (filter own-keys) (reachable-keys registry top-types))
                   (into (sorted-set) own-keys))
        entry-path (ns-sym->path entry-ns)
        paths (into [[entry-path entry-ns]]
                    (map (fn [k] (let [s (kw->ns-sym k)] [(ns-sym->path s) s])))
                    (keys groups))
        _ (check-path-collisions! paths)
        files (into []
                    (map (fn [[k group]]
                           (let [ns-sym (kw->ns-sym k)]
                             (write-forms! (io/file out-dir (ns-sym->path ns-sym))
                                           (type-file-forms ns-sym registry-keys group)))))
                    groups)
        entry-file (write-forms! (io/file out-dir entry-path)
                                 (entry-file-forms entry-ns own-keys included top-type))]
    {:files (conj files entry-file)
     :entry-ns entry-ns
     :entry-file entry-file
     :included (set included)}))

;; ---------------------------------------------------------------------------
;; Driver
;; ---------------------------------------------------------------------------

(defn compile-xsd->namespaces!
  "Load, compile and emit an XSD as a tree of malli namespaces.

  Options are emit-namespaces! options plus the loader's :resolver and the
  :default-ns applied to declarations without a targetNamespace."
  [{:keys [default-ns] :as opts} root]
  (let [loaded (loader/load-schemas opts root)
        compiled (compiler/compile-schemas {:default-ns default-ns} loaded)]
    (assoc (emit-namespaces! opts
                             (update-vals (:registry compiled) :emit)
                             (:top-type compiled))
           :compiled compiled
           :loaded loaded)))

;; ---------------------------------------------------------------------------
;; Loading generated output (schema_namespaces.clj lines 349-381)
;; ---------------------------------------------------------------------------

(defn- outermost-dynamic-loader
  "Clojure pushes a fresh DynamicClassLoader for every top-level form, so a URL
  added to the current base loader is lost as soon as that form finishes. Find
  the outermost DynamicClassLoader of the chain instead; it outlives the form."
  [cl]
  (loop [^ClassLoader cl cl
         found nil]
    (if (nil? cl)
      found
      (recur (.getParent cl)
             (if (instance? clojure.lang.DynamicClassLoader cl) cl found)))))

(defn ensure-on-classpath!
  "Add `dir` to the classpath so require can find files just written there."
  [dir]
  (let [path (.toAbsolutePath (Paths/get (str dir) (make-array String 0)))
        _ (Files/createDirectories path (make-array FileAttribute 0))
        url (.toURL (.toUri path))
        thread (Thread/currentThread)
        ctx (.getContextClassLoader thread)
        ctx-dcl (if (instance? clojure.lang.DynamicClassLoader ctx)
                  ctx
                  (doto (clojure.lang.DynamicClassLoader. ctx)
                    (->> (.setContextClassLoader thread))))]
    (.addURL ^clojure.lang.DynamicClassLoader ctx-dcl url)
    (when-some [dcl (outermost-dynamic-loader (clojure.lang.RT/baseLoader))]
      (when-not (identical? dcl ctx-dcl)
        (.addURL ^clojure.lang.DynamicClassLoader dcl url)))
    (str path)))
