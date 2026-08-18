(ns dunnage.sitoa.xsd-to-malli.support
  "Fixture wiring shared by the M2 tests.

  Each fixture is a delay so that loading, compiling, emitting and requiring
  happen once per JVM however many test namespaces ask for them; XMLSchema.xsd
  in particular is not cheap on either side."
  (:require [clojure.java.io :as io]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.unparser :as unparser]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [dunnage.sitoa.xsd-to-malli.compiler :as compiler]
            [dunnage.sitoa.xsd-to-malli.emit :as emit]
            [dunnage.sitoa.xsd-to-malli.loader :as loader]
            [dunnage.sitoa.xsd-to-malli.oracle :as oracle]
            [dunnage.sitoa.xsd-to-malli.resolver :as resolver]
            [malli.core :as m])
  (:import (java.io StringReader)))

(def out-root "target/xsd-to-malli-test")

(defn resolver [] (resolver/catalog-resolver oracle/catalog))

(defn load-schemas [f default-ns]
  (loader/load-schemas {:resolver (resolver) :default-ns default-ns} f))

(defn compile-schemas [f default-ns]
  (compiler/compile-schemas {:default-ns default-ns} (load-schemas f default-ns)))

(defn emit-into!
  "Emit a fixture into `out` without loading it."
  [f default-ns out entry-ns]
  (emit/compile-xsd->namespaces!
   {:resolver (resolver) :default-ns default-ns :out-dir out :entry-ns entry-ns}
   f))

(defn- generated
  "Emit a fixture, put it on the classpath and read back what the entry
  namespace exports."
  [f default-ns name entry-ns]
  (let [out (str out-root "/" name "/src")
        result (emit-into! f default-ns out entry-ns)]
    (emit/ensure-on-classpath! out)
    (require entry-ns)
    (let [var* (fn [sym] @(resolve (symbol (str entry-ns) sym)))
          registry (var* "registry")
          top-type (var* "top-type")
          schema ((resolve (symbol (str entry-ns) "make-schema")))]
      (assoc result
             :out out
             :registry registry
             :top-type top-type
             :schema schema
             :parse (parser/xml-parser schema)
             :closed-schema ((resolve (symbol (str entry-ns) "closed-make-schema")))))))

(defn- oracle-side [f default-ns]
  (let [schema-set (oracle/parse-xsd-offline f)
        context {:default-ns default-ns}
        registry (bs/xsd->registry context schema-set)
        top-type (bs/xsd->top-type context schema-set)]
    {:schema-set schema-set
     :registry registry
     :top-type top-type
     :parse (parser/xml-parser (xml-primitives/make-schema registry top-type))}))

(defn- fixture [name xsd-file default-ns entry-ns documents]
  (delay
    (merge (generated xsd-file default-ns name entry-ns)
           {:name name
            :xsd xsd-file
            :default-ns default-ns
            :documents documents
            :compiled (compile-schemas xsd-file default-ns)
            :oracle (oracle-side xsd-file default-ns)})))

(def multifile-xsd (io/file "dev-resources/multifile/main.xsd"))

(def fop
  (fixture "fop" (oracle/xsd "fop.xsd") "fop" 'dunnage.sitoa.gen.fop
           [(io/file ".." "serde" "dev-resources" "fopsample1.xml")
            (io/file ".." "serde" "dev-resources" "table-borders.fo")]))

(def junit
  (fixture "junit" (oracle/xsd "JUnit.xsd") "junit" 'dunnage.sitoa.gen.junit
           [(io/file "dev-resources/junit-report-min.xml")
            (io/file "dev-resources/junit-report.xml")]))

(def multifile
  (fixture "multifile" multifile-xsd "multi" 'dunnage.sitoa.gen.multifile
           (mapv #(io/file (str "dev-resources/multifile/" % ".xml"))
                 ["record" "strict" "price" "score" "records"])))

;; The per-type namespaces this fixture's entry requires
;; (org.w3.www.2001.XMLSchema.*) resolve to the checked-in meta-schema under
;; generated-src, which wins over the dynamically added emitted tree. In-process
;; tests over this fixture therefore pin the CHECKED-IN META-SCHEMA against the
;; oracle; the emitter's own XMLSchema output is covered by
;; equivalence-test/the-new-emitters-xmlschema-tree-holds-on-a-clean-classpath,
;; which runs the same bar in a child JVM without generated-src on the classpath.
(def xmlschema
  (fixture "xmlschema" (oracle/xsd "XMLSchema.xsd") "xsd" 'dunnage.sitoa.gen.xmlschema
           [(oracle/xsd "XMLSchema.xsd")]))

(def all-fixtures [fop junit multifile xmlschema])

;; ---------------------------------------------------------------------------
;; Parsing helpers, copied from the serde suite's working harness
;; (serde/test/dunnage/sitoa/schema_namespaces_serde_test.clj lines 25-46)
;; ---------------------------------------------------------------------------

(defn parse-doc [parse f]
  (with-open [s (io/reader f)]
    (parse (parser/make-stream-reader {} s))))

(defn start-schema
  "Consumer-style start type: the top-type arm for a document root, as a schema
  for its body. Whole-multi unparse is broken upstream, and real consumers pick
  a start type first."
  [registry top-type root-tag]
  (let [arm (some (fn [[tag arm]] (when (= tag root-tag) arm)) (drop 2 top-type))]
    (m/schema [:schema {:registry registry :topElement (name root-tag)} (peek arm)]
              xml-primitives/external-registry)))

(defn round-trip
  "Unparse a parsed document and parse the result back."
  [registry top-type parse f]
  (let [parsed (parse-doc parse f)
        root-tag (first parsed)
        emit-xml (unparser/xml-string-unparser (start-schema registry top-type root-tag))
        reparsed (with-open [s (StringReader. (emit-xml (nth parsed 1)))]
                   (parse (parser/make-stream-reader {} s)))]
    {:parsed parsed :reparsed reparsed}))

(defn validation
  "m/validate, or the failure it raised.

  Some schemas cannot produce a validator at all - fop's mutually recursive
  seqex types make malli refuse - so the comparable outcome is the result or
  the error type, not a boolean."
  [schema value]
  (try
    (m/validate schema value)
    (catch clojure.lang.ExceptionInfo e [::threw (:type (ex-data e))])
    (catch Throwable e [::threw (class e)])))
