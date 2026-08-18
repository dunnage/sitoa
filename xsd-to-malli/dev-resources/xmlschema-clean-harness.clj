;; MUST-HOLD bar for the NEW emitter's XMLSchema.xsd output, run in a child JVM
;; whose classpath REPLACES generated-src with the freshly emitted tree.
;;
;; Why a child JVM: the checked-in meta-schema under generated-src and the
;; emitter's XMLSchema output share their per-type namespace names
;; (org.w3.www.2001.XMLSchema.*), and the parent classpath wins over
;; dynamically added URLs, so an in-process require would silently bind the
;; meta-schema and test nothing. The resource assertion below turns that
;; failure mode from silent vacuity into a loud error.
;;
;; Invoked by equivalence-test/the-new-emitters-xmlschema-tree-holds-on-a-clean-classpath,
;; which emits into the tree path below and keeps it in sync with this file.
(require '[clojure.java.io :as io]
         'clojure.xml
         '[dunnage.sitoa.xsd-to-malli.oracle :as oracle]
         '[dunnage.sitoa.bootstrapped-schema :as bs]
         '[dunnage.sitoa.parser :as parser]
         '[dunnage.sitoa.unparser :as unparser]
         '[dunnage.sitoa.xml-primitives :as xml-primitives]
         '[malli.core :as m])
(import '(java.io StringReader))

(def tree-path "target/xsd-to-malli-test/xmlschema-clean/src")

;; 1. Prove the classpath serves the emitted tree, not generated-src.
(let [res (str (.getResource (clojure.lang.RT/baseLoader)
                             "org/w3/www/2001/XMLSchema/topLevelComplexType.cljc"))]
  (println "resource:" res)
  (assert (.contains res "xmlschema-clean")
          (str "wrong tree on classpath: " res)))

(require 'dunnage.sitoa.gen.xmlschema)

;; 2. Non-vacuity: derived types must be IntoSchema reifies, not plain vectors.
(let [reg @(resolve 'dunnage.sitoa.gen.xmlschema/registry)
      own (filter #(= "org.w3.www.2001.XMLSchema" (namespace %)) (keys reg))
      into-schemas (filter #(instance? malli.core.IntoSchema (get reg %)) own)]
  (println "own registry keys:" (count own)
           "IntoSchema values:" (count into-schemas))
  (assert (pos? (count into-schemas)) "no IntoSchema values - vacuous")
  (assert (instance? malli.core.IntoSchema
                     (get reg :org.w3.www.2001.XMLSchema/topLevelComplexType))))

;; 3. Build both sides.
(def gen-registry @(resolve 'dunnage.sitoa.gen.xmlschema/registry))
(def gen-top @(resolve 'dunnage.sitoa.gen.xmlschema/top-type))
(def gen-schema ((resolve 'dunnage.sitoa.gen.xmlschema/make-schema)))
(def gen-parse (parser/xml-parser gen-schema))

(def xsd-file (io/file ".." "bootstrapped-schema" "dev-resources" "XMLSchema.xsd"))
(def schema-set (oracle/parse-xsd-offline xsd-file))
(def octx {:default-ns "xsd"})
(def o-registry (bs/xsd->registry octx schema-set))
(def o-top (bs/xsd->top-type octx schema-set))
(def o-parse (parser/xml-parser (xml-primitives/make-schema o-registry o-top)))

(defn parse-doc [parse f]
  (with-open [s (io/reader f)]
    (parse (parser/make-stream-reader {} s))))

(defn start-schema [registry top-type root-tag]
  (let [arm (some (fn [[tag arm]] (when (= tag root-tag) arm)) (drop 2 top-type))]
    (m/schema [:schema {:registry registry :topElement (name root-tag)} (peek arm)]
              xml-primitives/external-registry)))

;; 4. Parse equality.
(def o-parsed (parse-doc o-parse xsd-file))
(def g-parsed (parse-doc gen-parse xsd-file))
(println "parse node count:" (count (tree-seq coll? seq g-parsed)))
(println "PARSE-EQUAL:" (= o-parsed g-parsed))
(assert (= o-parsed g-parsed))
(assert (= :schema (first g-parsed)))
(assert (< 5000 (count (tree-seq coll? seq g-parsed))))

;; 5. Fixpoint, twice.
(let [root (first g-parsed)
      emit-xml (unparser/xml-string-unparser (start-schema gen-registry gen-top root))
      xml1 (emit-xml (nth g-parsed 1))
      rep1 (with-open [s (StringReader. xml1)] (gen-parse (parser/make-stream-reader {} s)))
      xml2 (emit-xml (nth rep1 1))
      rep2 (with-open [s (StringReader. xml2)] (gen-parse (parser/make-stream-reader {} s)))]
  (println "FIXPOINT-1:" (= g-parsed rep1))
  (println "FIXPOINT-2:" (= rep1 rep2))
  (assert (= g-parsed rep1))
  (assert (= rep1 rep2)))

;; 6. Validation agreement.
(defn validation [schema value]
  (try (m/validate schema value)
       (catch clojure.lang.ExceptionInfo e [::threw (:type (ex-data e))])
       (catch Throwable e [::threw (class e)])))
(let [root (first g-parsed)
      v (nth g-parsed 1)
      gv (validation (start-schema gen-registry gen-top root) v)
      ov (validation (start-schema o-registry o-top root) v)]
  (println "VALIDATE gen:" (pr-str gv) "oracle:" (pr-str ov))
  (assert (= gv ov)))

(println "XMLSCHEMA-CLEAN-CLASSPATH: PASS")
(System/exit 0)
