(ns dunnage.sitoa.xsd-to-malli.emit
  "Write a compiled registry out as Clojure source.

  One .cljc namespace per registry type, plus an entry namespace that requires
  them all and assembles the registry, the top type and make-schema. A type
  that derives from another requires that type's namespace and rebuilds itself
  from its `sch` at schema-build time; every other cross-type edge stays a
  registry keyword, so the require graph is exactly the derivation graph and
  a cyclic XSD type graph still produces no cyclic requires.

  The canonicalization, naming, reachability and classpath helpers below are
  copied from dunnage.sitoa.schema-namespaces rather than required from it:
  that namespace imports XSOM, and nothing under src/ may put XSOM on this
  project's runtime classpath. emit-test asserts the copies still agree with
  the originals."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [dunnage.sitoa.xsd-to-malli.compiler :as compiler]
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

(defn- ->code
  "Replace every derivation node in a value by the expression that rebuilds it.

  Each node becomes a self-contained IntoSchema, so it works wherever it lands:
  as a whole registry value, as a child inside a literal form, or as the type
  of a top-type arm - malli builds it with the options that carry the assembled
  registry, which is what its [:ref ...] children need."
  [x]
  (cond
    (compiler/derived? x)
    (list 'reify 'm/IntoSchema
          (list '-into-schema '[_ _ _ options]
                (list 'rt/derive-complex (->code (:plan x)) 'options)))

    (map? x) (into (empty x) (map (fn [[k v]] [(->code k) (->code v)])) x)
    (vector? x) (mapv ->code x)
    (set? x) (into (empty x) (map ->code) x)
    :else x))

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

(defn- runtime-requires [ns-syms]
  (into ['[dunnage.sitoa.xsd-to-malli.runtime :as rt]
         '[malli.core :as m]]
        (map vector)
        ns-syms))

(defn- type-file-forms [ns-sym registry-keys {:keys [sch sch-seq] :as group}]
  (let [values (if (contains? group :sch-seq) [sch sch-seq] [sch])
        requires (derivation-requires values)]
    (cond-> [(if (seq requires)
               (list 'ns ns-sym generated-doc (seq (into [:require] (runtime-requires requires))))
               (list 'ns ns-sym generated-doc))
             (list 'def 'deps (form-refs registry-keys values))
             (list 'def 'sch (->code sch))]
      (contains? group :sch-seq)
      (conj (list 'def 'sch-seq (->code sch-seq))))))

(defn- entry-file-forms [entry-ns own-keys included top-type]
  (let [arms (into [] (map (fn [[tag arm]] [tag (canonicalize-form arm)]))
                   (sort-by first (drop 2 top-type)))
        ns-syms (into (sorted-set) (map (comp kw->ns-sym #(base-kw own-keys %))) included)
        code-requires (derivation-requires arms)
        requires (into (into [:require
                              '[dunnage.sitoa.xml-primitives :as xml-primitives]
                              '[dunnage.sitoa.xsd-to-malli.runtime :as rt]]
                             (when (seq code-requires) ['[malli.core :as m]]))
                       (map vector)
                       (into ns-syms code-requires))
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
           (list 'into [:multi {:dispatch 'first}] (->code arms)))
     (list 'defn 'make-schema
           (list [] (list 'make-schema 'top-type))
           (list '[start-type] (list 'xml-primitives/make-schema 'registry 'start-type)))
     (list 'defn 'closed-make-schema
           (list [] (list 'closed-make-schema 'top-type))
           (list '[start-type] (list 'xml-primitives/closed-make-schema
                                     (list 'rt/realize-registry 'registry)
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
