(ns dunnage.sitoa.schema-namespaces
  "Emit an XSD-derived malli registry as Clojure source instead of one EDN file.

  Every registry type becomes a .cljc namespace holding plain data defs, and a
  single entry namespace requires them all, assembles the registry and the top
  type, and exposes make-schema. Registry values are pure data, so per-type
  files need no :require at all and a cyclic XSD type graph can never produce
  cyclic namespace requires: every cross-type edge stays a keyword that malli's
  registry resolves at m/schema time.

  The EDN path (serialize-registry / serialize-schema) is untouched; this
  namespace is purely additive."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [fipp.edn :refer [pprint] :rename {pprint fipp}])
  (:import (com.sun.xml.xsom XSDeclaration XSSchemaSet)
           (java.io File)
           (java.nio.file Files Paths)
           (java.nio.file.attribute FileAttribute)))

(def ^:private seq-suffix "-seq")

(def ^:private generated-doc
  "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")

;; ---------------------------------------------------------------------------
;; Canonicalization
;; ---------------------------------------------------------------------------

(defn- attr-row? [row]
  (and (vector? row) (map? (second row)) (:xml/attr (second row))))

(defn- sort-attribute-rows
  "Sort the :xml/attr rows of every :map form among themselves, reinserting them
  at the indexes attribute rows already occupied.

  XSOM iterates attribute uses in an identity-hash sensitive order, so raw
  emission is not reproducible between runs. Element row order is load-bearing
  for the parser (it drives tag entry/exit), so only attribute rows move, and
  seqex children (:cat / :alt / :or / :tuple / :enum) are never reordered."
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
  "Remove empty property maps, which malli treats as absent.

  m/form normalizes them away, so every consumer written against serialized
  registries - the shape serialize-registry produces - sees schema and entry
  vectors without them. The pipeline emits the literal {} instead, and a
  consumer that destructures a form positionally reads that as a child rather
  than as properties. Emitting the normalized shape keeps generated source
  interchangeable with the EDN it replaces.

  Only vectors headed by a keyword or symbol are touched, so documentation and
  :enum values, which are data rather than schema forms, are left alone."
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
  "Put a form in the shape the generator emits: attribute rows sorted, empty
  property maps dropped. Attributes are sorted first, while :map forms still
  carry their properties at index 1."
  [form]
  (-> form sort-attribute-rows drop-empty-properties))

(defn canonicalize-registry
  "canonicalize-form over every registry value."
  [registry]
  (update-vals registry canonicalize-form))

;; ---------------------------------------------------------------------------
;; Reference collection
;; ---------------------------------------------------------------------------

(defn- collect-refs! [registry-keys acc form]
  (cond
    (keyword? form) (when (contains? registry-keys form) (vswap! acc conj form))
    (map? form) (reduce-kv (fn [_ k v]
                             (collect-refs! registry-keys acc k)
                             (collect-refs! registry-keys acc v)
                             nil)
                           nil form)
    (coll? form) (reduce (fn [_ x] (collect-refs! registry-keys acc x) nil) nil form)
    :else nil))

(defn form-refs
  "Sorted set of registry keywords referenced anywhere in `form`.

  The pipeline emits references in two shapes: [:ref :ns/Foo] from -seq-ref via
  wrap-ref-np, and bare namespaced keywords from base-type chains and from XSD
  builtin references that wrap-ref-np deliberately leaves unwrapped. Both count."
  [registry-keys form]
  (let [acc (volatile! (sorted-set))]
    (collect-refs! registry-keys acc form)
    @acc))

(defn reachable-keys
  "Transitive closure of registry keys reachable from `seeds`, computed on the
  raw registry data. Mirrors trim-registry-for-top-types, which walks compiled
  schemas for the same edges."
  [registry seeds]
  (let [registry-keys (set (keys registry))
        missing (into (sorted-set) (remove registry-keys) seeds)]
    (when (seq missing)
      (throw (ex-info "top-types seeds absent from registry"
                      {:missing missing})))
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
;; Naming
;; ---------------------------------------------------------------------------

(defn kw->ns-sym
  "Namespace symbol for a registry keyword. A dot inside the local name would
  create a bogus extra namespace segment, so it is munged to an underscore;
  dashes stay, following Clojure convention."
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

;; ---------------------------------------------------------------------------
;; Registry partitioning
;; ---------------------------------------------------------------------------

(defn- builtin-entry?
  "True only for an xmlschema-registry entry the pipeline seeded and nothing
  redeclared. A document-declared type that shadows a builtin key stays own."
  [k v]
  (and (contains? xml-primitives/xmlschema-registry k)
       (= v (get xml-primitives/xmlschema-registry k))))

(defn- own-entries [registry]
  (into (sorted-map) (remove (fn [[k v]] (builtin-entry? k v))) registry))

(defn- group-entries
  "own registry -> sorted map of base keyword -> {:sch form :sch-seq form}."
  [own]
  (let [own-keys (set (keys own))]
    (reduce-kv (fn [acc k v]
                 (let [bk (base-kw own-keys k)]
                   (assoc-in acc [bk (if (= bk k) :sch :sch-seq)] v)))
               (sorted-map)
               own)))

(defn- check-namespaced! [own]
  (let [bare (into (sorted-set) (remove namespace) (keys own))]
    (when (seq bare)
      (throw (ex-info "registry keys without a namespace cannot be mapped to files; supply :default-ns"
                      {:keys bare})))))

(defn- check-path-collisions! [paths]
  (let [dupes (into (sorted-map)
                    (comp (map (fn [[k v]] [k (sort (map second v))]))
                          (filter (fn [[_ v]] (< 1 (count v)))))
                    (group-by (comp str/lower-case first) paths))]
    (when (seq dupes)
      (throw (ex-info "generated file paths collide after munging"
                      {:collisions dupes})))))

;; ---------------------------------------------------------------------------
;; Emission
;; ---------------------------------------------------------------------------

(defn- write-forms! [^File f forms]
  (io/make-parents f)
  (with-open [w (io/writer f :encoding "UTF-8")]
    (doseq [form forms]
      (fipp form {:writer w})))
  (.getPath f))

(defn- type-file-forms [ns-sym registry-keys {:keys [sch sch-seq] :as group}]
  (cond-> [(list 'ns ns-sym generated-doc)
           (list 'def 'deps (form-refs registry-keys
                                       (if (contains? group :sch-seq) [sch sch-seq] [sch])))
           (list 'def 'sch sch)]
    (contains? group :sch-seq)
    (conj (list 'def 'sch-seq sch-seq))))

(defn- entry-file-forms [entry-ns own-keys included top-type]
  (let [ns-syms (into (sorted-set) (map (comp kw->ns-sym #(base-kw own-keys %))) included)
        requires (into [:require '[dunnage.sitoa.xml-primitives :as xml-primitives]]
                       (map vector)
                       ns-syms)
        reg-entries (into (sorted-map)
                          (map (fn [k]
                                 (let [bk (base-kw own-keys k)]
                                   [k (symbol (str (kw->ns-sym bk))
                                              (if (= bk k) "sch" "sch-seq"))])))
                          included)
        arms (into [] (map (fn [[tag arm]] [tag (canonicalize-form arm)]))
                   (sort-by first (drop 2 top-type)))]
    [(list 'ns entry-ns generated-doc (seq requires))
     (list 'def 'registry
           (list 'merge 'xml-primitives/xmlschema-registry reg-entries))
     (list 'def 'top-type
           (list 'into [:multi {:dispatch 'first}] arms))
     (list 'defn 'make-schema
           (list [] (list 'make-schema 'top-type))
           (list '[start-type] (list 'xml-primitives/make-schema 'registry 'start-type)))
     (list 'defn 'closed-make-schema
           (list [] (list 'closed-make-schema 'top-type))
           (list '[start-type] (list 'xml-primitives/closed-make-schema 'registry 'start-type)))]))

(defn emit-namespaces!
  "Write one .cljc file per registry type plus an entry namespace under out-dir.

  `registry` and `top-type` are the raw data structures produced by
  xsd->registry / xsd->top-type; the generator never re-walks XSOM.

  Options:
    :out-dir   directory the source tree is written under (required)
    :entry-ns  symbol naming the aggregator namespace (required)
    :top-types optional collection of registry keys used as reachability seeds,
               mirroring furl's trim-registry-for-top-types call. Only the
               reachable closure of own entries is required and registered by
               the entry namespace (the builtin xmlschema-registry is always
               merged whole; its unreachable entries are inert); unreachable
               types still get their own file. With seeds the entry's registry
               may not cover every top-type arm, so callers pick a start type
               with the 1-arity make-schema.

  Files are overwritten but never deleted, so a type dropped from the XSD
  leaves its previous file behind; regenerate into a clean directory.

  Returns {:files [...] :entry-ns sym :entry-file path :included #{kws}}."
  [{:keys [out-dir entry-ns top-types]} registry top-type]
  (assert (some? out-dir) ":out-dir is required")
  (assert (symbol? entry-ns) ":entry-ns must be a symbol")
  (let [registry (canonicalize-registry registry)
        registry-keys (set (keys registry))
        own (own-entries registry)
        _ (check-namespaced! own)
        own-keys (set (keys own))
        groups (group-entries own)
        included (if (seq top-types)
                   (into (sorted-set)
                         (filter own-keys)
                         (reachable-keys registry top-types))
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
;; XSD driver
;; ---------------------------------------------------------------------------

(defn- declared-names [^XSSchemaSet schema]
  (into #{}
        (comp (map (fn [^XSDeclaration x] (.getName x)))
              (remove nil?))
        (concat (iterator-seq (.iterateTypes schema))
                (iterator-seq (.iterateModelGroupDecls schema)))))

(defn- check-seq-name-collisions!
  "An XSD that declares both Foo and Foo-seq already collides inside
  xsd->registry (the -seq dual of Foo overwrites the declared Foo-seq); fail
  fast rather than emit a file that silently drops one of them."
  [names]
  (let [colliding (into (sorted-set)
                        (comp (filter #(str/ends-with? % seq-suffix))
                              (filter #(contains? names (subs % 0 (- (count %) (count seq-suffix))))))
                        names)]
    (when (seq colliding)
      (throw (ex-info "XSD declares both Foo and Foo-seq; the -seq registry dual collides"
                      {:names colliding})))))

(defn xsd->namespaces!
  "parse-xsd + xsd->registry + xsd->top-type + emit-namespaces!.

  Options are emit-namespaces! options plus :default-ns, the namespace applied
  to declarations without a targetNamespace."
  [{:keys [default-ns] :as opts} f]
  (let [schema (bs/parse-xsd f)
        context {:default-ns default-ns}]
    (check-seq-name-collisions! (declared-names schema))
    (emit-namespaces! opts
                      (bs/xsd->registry context schema)
                      (bs/xsd->top-type context schema))))

;; ---------------------------------------------------------------------------
;; Loading generated output
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
  "Add `dir` to the classpath so require can find files just written there.

  The URL is registered both on the outermost DynamicClassLoader of the current
  load (so a require in the same form sees it) and on the thread context class
  loader (so later forms do too)."
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

(comment
  (xsd->namespaces! {:default-ns "fop"
                     :out-dir "target/generated/fop/src"
                     :entry-ns 'dunnage.sitoa.gen.fop}
                    (io/file "dev-resources/fop.xsd"))
  (ensure-on-classpath! "target/generated/fop/src")
  (require 'dunnage.sitoa.gen.fop))
