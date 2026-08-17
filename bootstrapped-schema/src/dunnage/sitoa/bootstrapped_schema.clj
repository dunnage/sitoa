(ns dunnage.sitoa.bootstrapped-schema
  (:require [clojure.java.io :as io]
            [malli.core :as m]
            [malli.util :as mu]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            clojure.xml
            [clojure.tools.reader.edn :as edn]
            [clojure.pprint :as pp]
            [fipp.edn :refer [pprint] :rename {pprint fipp}])
  (:import (com.sun.xml.xsom.parser AnnotationContext AnnotationParser AnnotationParserFactory XSOMParser)
           (javax.xml.parsers SAXParserFactory)
           (com.sun.xml.xsom XSRestrictionSimpleType XSSimpleType XmlString XSComplexType XSTerm XSParticle XSModelGroup XSUnionSimpleType XSListSimpleType XSComponent XSDeclaration XSModelGroupDecl XSWildcard XSWildcard$Any XSType ForeignAttributes XSAttributeUse XSFacet XSSchemaSet XSElementDecl XSVariety)
           (org.xml.sax ContentHandler EntityResolver ErrorHandler SAXParseException)
           (java.net URI)
           (java.time LocalDate LocalDateTime)
           (clojure.lang IReduceInit)))

(defn collect-facets
  ([^XSRestrictionSimpleType x] (collect-facets () x))
  ([coll ^XSRestrictionSimpleType x]
   (if-some [base (.getSimpleBaseType x)]
     (if (.isRestriction base)
       (recur (into coll (map #(vector (.getName %) (.getValue %) (.isFixed %)))
                    (.getDeclaredFacets x)) base)
       (into coll (.getDeclaredFacets x)))
     coll)))

(defn get-primitive-type [^XSSimpleType x]
  (or (some-> x .getPrimitiveType .getName) (.getName x) #_"no-primative"))

(defn uri->ns [^String x]
  (let [uri
        (new URI x)]
    ;(prn (bean uri))
    (case (.getScheme uri)
      "urn" (-> []
                (into (reverse (clojure.string/split (str (.getSchemeSpecificPart uri)) #":")))
                (->> (clojure.string/join ".")))
      (-> []
          (into (reverse (clojure.string/split (.getHost uri) #"\.")))
          (into (remove empty?) (clojure.string/split (.getPath uri) #"\/"))
          (->> (clojure.string/join "."))))))

(defn not-empty-string [^String x]
  (when-not (.isEmpty x)
    x))

(defn unwrap-and [x if-empty]
  (case (count x)
    1 if-empty
    2 (nth x 1)
    x))

(def name-regex "^\\d{1,3}|\\d(([ ,]?\\d{3})*([.,]\\d{2}+)?$)")

(defn malli-string-primitive [prim {default-ns :default-ns :as context}]
  (let [facets (.getDeclaredFacets prim)
        baset (.getBaseType prim)
        base   (keyword (or (some-> (.getTargetNamespace baset)
                                    not-empty-string
                                    uri->ns)
                            default-ns) (.getName baset))
        base  (when (not= :org.w3.www.2001.XMLSchema/string base)
                base)
        {:keys [enum pattern] :as f}
        (reduce
         (fn
           [acc ^XSFacet facet]
           (let [name (.getName facet)
                 value (.getValue facet)
                 fixed (.isFixed facet)
                 annotations (some-> (.getAnnotation facet false)
                                     .getAnnotation)]
             (case name
               "enumeration" (update acc :enum (fn [old]
                                                 (let [old (if old
                                                             old
                                                             [:enum {}])
                                                       value (.toString ^XmlString value)
                                                       docs (not-empty (into []
                                                                             (comp
                                                                              (mapcat (fn [{:keys [tag content]}]
                                                                                        (when (= tag :xsd:documentation)
                                                                                          content))))
                                                                             annotations))]
                                                   (cond-> (conj old value)
                                                     docs
                                                     (assoc-in [1 :value-documentation value] docs)))))
               "length" (let [l (Long/parseLong (str value))]
                          (when annotations
                            (prn annotations))
                          (assoc acc :min l
                                 :max l))
               "maxLength" (do  (when annotations
                                  (prn annotations))
                                (assoc acc :max (Long/parseLong (str value))))
               "minLength" (do (when annotations
                                 (prn annotations))
                               (assoc acc :min (Long/parseLong (str value))))
               "pattern" (do     (when annotations
                                   (prn annotations))                            ;(prn (str value))
                                 (assoc acc :pattern [:re
                                                      (let [re (str value)]
                                                        (case re
                                                          "\\i\\c*" name-regex
                                                          "[\\i-[:]][\\c-[:]]*" name-regex
                                                          re))]))
               "whiteSpace" (do (when annotations
                                  (prn annotations))
                                acc))))                         ;"preserve replace collapse))
         {}
         facets)
        base-string (not-empty (dissoc f :enum :pattern))]
    (-> (cond-> [:and]
          base-string
          (conj [:string base-string])
          enum
          (conj enum)
          pattern
          (conj pattern)
          base
          (conj base))
        (unwrap-and :string))))

(defn ->nskw [^XSDeclaration x default-ns]
  (when-some [name (.getName x)]
    (if-some [n (some-> (.getTargetNamespace x)
                        not-empty-string
                        uri->ns)]
      (keyword n name)
      (if default-ns
        (keyword default-ns name)
        (keyword name)))))

(def xmlschema-ns "org.w3.www.2001.XMLSchema")

(def xsd-builtin-names
  "Local names XSOM synthesizes in the XML Schema namespace for the built-in
  datatype hierarchy; they come from XSOM's bundled datatypes.xsd, not from the
  document being parsed. The ones sitoa models are the keys of
  xml-primitives/xmlschema-registry; anyType and the three builtin list types
  are builtins too, sitoa simply has no mapping for them.

  Anything else in that namespace can only come from a document that declares
  it - the schema for schemas (XMLSchema.xsd) declares element, complexType,
  particle and friends there - so it is a real declaration and must reach the
  registry."
  (into #{"anyType" "IDREFS" "NMTOKENS" "ENTITIES"}
        (map name)
        (keys xml-primitives/xmlschema-registry)))

(defn xsd-builtin-kw?
  "True for a keyword naming a built-in XML Schema type."
  [x]
  (and (some? x)
       (= xmlschema-ns (namespace x))
       (contains? xsd-builtin-names (name x))))

(def xsd-unmodeled-builtin-forms
  "Inline malli forms for built-in XML Schema types xmlschema-registry does not
  model. They never become registry entries, so a reference to one is inlined
  instead of left dangling. anyType is unconstrained content."
  {"anyType" :xml/hiccup})

(defn- unmodeled-builtin-form [x]
  (when (= xmlschema-ns (namespace x))
    (let [n (name x)]
      (get xsd-unmodeled-builtin-forms
           (if (clojure.string/ends-with? n "-seq")
             (subs n 0 (- (count n) 4))
             n)))))

(defn wrap-ref-np [x]
  (or (unmodeled-builtin-form x)
      (if (xsd-builtin-kw? x)
        x
        [:ref x])))
(defn ->nskw-seq [^XSDeclaration x default-ns]
  (when-some [name (some-> (.getName x) (str "-seq"))]
    (if-some [n (some-> (.getTargetNamespace x)
                        not-empty-string
                        uri->ns)]
      (keyword n name)
      (if default-ns
        (keyword default-ns name)
        (keyword name)))))

(defn ->kw [^XSDeclaration x]
  (keyword (.getName x)))

(defprotocol MalliXML
  (-mtype [x context])
  (-seq-possible? [x context])
  (-seq-ref [x context]))

(declare handleparticle)

(defn anon-type? [ty]
  (or (.isLocal ty)
      (not (some-> (.getName ty) not-empty-string))))


(defn simplify-fields [props]
  (fn ([] [:map (assoc props :x :x)])
    ([acc]
     (if (-> acc first (= :map))
       (update acc 1 dissoc :x)
       acc))
    ([acc val]
     #_(when (#{:map :merge} (first acc))
         (prn acc))
     (case (first acc)
       :map
       (case (first val)
         :map (if (= (count acc) 2)
                [:merge {}
                 val]
                [:merge {}
                 (update acc 1 dissoc :x)
                 val])
         :merge (if (= (count acc) 2)
                  val
                  (into [:merge {} acc]
                        (drop 2)
                        val))
         (conj acc val))
       :merge
       (case (first val)
         :map  (conj acc val)
         :merge (into acc
                      (drop 2)
                      val)
         (let [last-index (dec (count acc))]
           (if (-> acc (get last-index) second :x)
             (update acc last-index conj val)
             (conj acc [:map (assoc props :x :x) val]))))))))

(defn handle-fields-wrapper2 [{optional-group :optional-group default-ns :default-ns :as context}]
  (fn handle-fields-wrapper2-  [^XSParticle in]
    (let [term (.getTerm in)
          min-occurs (long (.getMinOccurs in))
          max-occurs (long (.getMaxOccurs in))
          value-sequence? (or (> max-occurs 1) (= max-occurs -1))
          annotations (some-> (.getAnnotation term false)
                              .getAnnotation)
          docs (not-empty (into []
                                (comp
                                 (mapcat (fn [{:keys [tag content]}]
                                           (when (= tag :xsd:documentation)
                                             content))))
                                annotations))]
      ;(when docs (prn docs))
      (when (not= max-occurs 0) #_(not= (m/children ty-ref) [(keyword default-ns "Extension")])
            (or (when-some [x (.asElementDecl term)]
                  (let [ty (.getType x)
                        ty-ref (if (anon-type? ty)
                                 (-mtype ty context)
                                 (-seq-ref ty context))]

                    [(->kw x)
                     (cond-> {}
                       (= 0 min-occurs)
                       (assoc :optional true)
                       optional-group
                       (assoc :optional true
                              :required-in-group true)
                       docs
                       (assoc :documentation (first docs)))
                     (if value-sequence?
                       [:sequential (cond-> (if (= 0 min-occurs)
                                              {:min 1}
                                              {:min min-occurs})
                                      (not= max-occurs -1)
                                      (assoc :max max-occurs))
                        ty-ref]
                       ty-ref)]))
                (when-some [x (.asModelGroup term)]
                  (assert (= "sequence" (str (.getCompositor x))))
                  (assert (not value-sequence?))
                  (transduce
                   (keep (handle-fields-wrapper2 (assoc context :optional-group (= 0 min-occurs))))
                   (simplify-fields (cond-> {:closed true}
                                      (= 0 min-occurs)
                                      (assoc :optional-group true)))
                   (.getChildren x)))
                (when-some [mgd (.asModelGroupDecl term)]
                  (when-some [x (.getModelGroup mgd)]
                    (assert (= "sequence" (str (.getCompositor x))))
                    (assert (not value-sequence?))
                    (transduce
                     (keep (handle-fields-wrapper2 (assoc context :optional-group (= 0 min-occurs))))
                     (simplify-fields (cond-> {:closed true}
                                        (= 0 min-occurs)
                                        (assoc :optional-group true)))
                     (.getChildren x))))

                (prn :fail (bean in)))))))

(defn- mark-map-in-seq-ex
  "Maps that appear as sequence items (under :? / :* / :+ / :repeat) are entered
  at the first child start tag, not the parent. Flag :xml/in-seq-ex so the map
  parser does not advance past that tag (fixes StrucDoc.Thead/Tbody tr rows)."
  [msch]
  (if (and (vector? msch) (= :map (first msch)))
    (let [props (second msch)
          props (if (map? props) props {})]
      (assoc msch 1 (assoc props :xml/in-seq-ex true)))
    msch))

(defn- regex-occurrence
  "Malli seqex wrapper for a particle's min/maxOccurs."
  [min-occurs max-occurs msch]
  (let [can-be-empty? (= 0 min-occurs)
        unbounded? (= max-occurs -1)
        repeated? (or (> max-occurs 1) (= max-occurs -1))]
    (cond
      (and (not can-be-empty?) (not repeated?)) msch
      (and can-be-empty? (not repeated?)) [:? (mark-map-in-seq-ex msch)]
      (and (not can-be-empty?) repeated? unbounded?) [:+ (mark-map-in-seq-ex msch)]
      (and can-be-empty? repeated? unbounded?) [:* (mark-map-in-seq-ex msch)]
      :else
      [:repeat {:min min-occurs, :max max-occurs} (mark-map-in-seq-ex msch)])))

(defn wrap-regex [context ^XSParticle in msch]
  (let [min-occurs (.getMinOccurs in)
        max-occurs (.getMaxOccurs in)
        repeated? (or (> max-occurs 1) (= max-occurs -1))]
    (cond
      (= 0 max-occurs)
      nil
      (:sequence context)
      (regex-occurrence min-occurs max-occurs msch)
      ;; A complex type's own content is a child-element stream the parser reads
      ;; with seqex semantics, so a repeated top particle needs a malli regex.
      ;; :sequential there never matches a child start tag (StrucDoc.Thead/Tbody/
      ;; Tfoot/Tr/Colgroup rows). Non-repeated non-seq forms stay bare.
      (and repeated? (:content-particle context))
      (regex-occurrence min-occurs max-occurs msch)
      repeated?
      [:sequential msch]
      ;can-be-empty?
      ;[:maybe msch]
      :else
      msch)))

(defn handle-element-decl [{default-ns :default-ns :as context} ^XSElementDecl x]
  ;(prn (->nskw x (:default-ns context)) (.isGlobal x) (anon-type? x))
  (let [ty (.getType x)
        ;_  (prn  (anon-type? ty))
        ty-ref (if (anon-type? ty)
                 (-mtype ty (dissoc context :sequence))
                 (-seq-ref ty (dissoc context :sequence)))]
    [:tuple {} [:enum (->kw x)] ty-ref]))

(declare every-sequence?)
(defn particle-sequence? [map-keys ^XSParticle x]
  (let [term (.getTerm x)]
    (or (when-some [eldec (.asElementDecl term)]
          (let [kw (->kw eldec)]
            (if (contains? @map-keys kw)
              false
              (do (swap! map-keys conj kw)
                  true))))
        (when-some [mg (.asModelGroup term)]
          (let [max-occurs (long (.getMaxOccurs x))
                value-sequence? (or (> max-occurs 1) (= max-occurs -1))]
            (if value-sequence?
              false
              (when (= "sequence" (str (.getCompositor mg)))
                (every-sequence? map-keys mg)))))
        (when-some [mgd (.asModelGroupDecl term)]
          (let [max-occurs (long (.getMaxOccurs x))
                value-sequence? (or (> max-occurs 1) (= max-occurs -1))]
            (if value-sequence?
              false
              (when-some [mg (.getModelGroup mgd)]
                (when (= "sequence" (str (.getCompositor mg)))
                  (every-sequence? map-keys mg)))))))))
(defn every-sequence? [map-keys ^XSModelGroup x]
  (every? (partial particle-sequence? map-keys) (.getChildren x)))

(declare handle-model-group handle-model-group-decl
         handle-wildcard handle-model-group-seq handle-model-group-decl-seq)

(defn group-particle [context ^XSParticle in]
  (let [t (.getTerm in)
        out (or
             (when-some [el (.asElementDecl t)]
               (handle-element-decl context el))
             (some->> (.asModelGroup t)
                      (handle-model-group context))
             (some->> (.asModelGroupDecl t)
                      (handle-model-group-decl context))
             (some-> (.asWildcard t)
                     handle-wildcard))]
    (wrap-regex context in out)))

(defn handle-toplevel-particle [context ^XSParticle in]
  "Convert a complex type's content particle, honoring min/maxOccurs.

  Occurrence wrappers follow dual-mode registry design:
  - non-seq types (no :sequence in context) → bare, or :? / :* / :+ / :repeat
    when the top particle itself repeats (its content is a child stream)
  - *-seq types (:sequence true) → :? / :* / :+ / :repeat

  Previously maxOccurs was ignored at the top particle, so unbounded choice
  (e.g. StrucDoc.Text) collapsed to a single :or under *-seq forms."
  (let [t (.getTerm in)
        body (or
              (some->> (.asModelGroup t)
                       (handle-model-group context))
              (some->> (.asModelGroupDecl t)
                       (handle-model-group-decl context))
              (some-> (.asWildcard t)
                      handle-wildcard))]
    (when body
      (wrap-regex (assoc context :content-particle true) in body))))

(defn all-maps? [x]
  ;; Children can be bare keywords (:xml/hiccup, builtin type references), which
  ;; nth cannot index; treat those as a non-match instead of throwing.
  (transduce
   (drop 2)
   (fn ([acc] (if (nil? acc)
                false
                acc))
     ([acc nv]
      (if (and (vector? nv) (= #{:map :merge} (nth nv 0)))
        true
        (reduced false))))
   nil
   x))
(defn handle-model-group [context ^XSModelGroup x]
  (let [compositor (str (.getCompositor x))
        fields (.getChildren x)]
    (case compositor
      "sequence" (cond
                   (and (= 1 (count fields))
                        (when-some [wc (some-> fields first .getTerm .asWildcard)]
                          (instance? XSWildcard$Any wc)))
                   :xml/hiccup
                   (every-sequence? (atom #{}) x)
                   (if (:sequence context)
                     (transduce
                      (keep (handle-fields-wrapper2 (dissoc context :sequence)))
                      (simplify-fields {:xml/in-seq-ex true :closed true})
                      fields)
                     (transduce
                      (keep (handle-fields-wrapper2 context))
                      (simplify-fields {:closed true})
                      fields))
                   :default
                   (transduce
                    (map identity)
                    (fn
                      ([acc] (if (all-maps? acc)
                               (if (= (count acc)  3)
                                 (nth acc 2)
                                 (assoc acc 0 :merge))
                               acc))
                      ([acc nv]
                       (if-some [n (group-particle (assoc context
                                                          :sequence true
                                                          :compositor "sequence") nv)]
                         (conj acc n)
                         acc)))
                    [:cat {}]
                    fields))
      "choice" (cond
                 (and (= 1 (count fields))
                      (instance? XSWildcard$Any (first fields)))
                 :xml/hiccup
                 :default
                 ;; Dual-mode choice (serde must honor this split):
                 ;; - :sequence context → :alt  (seqex: parser wraps atomics as
                 ;;   one slot for parent :cat / regex — IVL_TS, Instruction, …)
                 ;; - non-seq (element/type body) → :or  (value-mode: parser
                 ;;   returns bare arm — StatusType, DateType, AllergyRestrictedChoice)
                 ;; Only *-seq may emit malli regex on arms; non-seq stays
                 ;; :or of bare tuples / :sequential. Multi-element sequence
                 ;; arms under :sequence still get :xml/in-seq-ex maps.
                 ;; Discriminated :multi is derived from these choices by
                 ;; or->multi below, as a load-time transform.
                 (into [(if (:sequence context)
                          :alt
                          :or)]
                       (keep #(group-particle (assoc context
                                                     :compositor "choice") %))
                       fields))
      "all" (cond
              (and (= 1 (count fields))
                   (instance? XSWildcard$Any (first fields)))
              :xml/hiccup
              (every-sequence? (atom #{}) x)
              (transduce
               (keep (handle-fields-wrapper2 context))
               (simplify-fields {:closed true})
               fields)
              :default [compositor (map #(group-particle context %) fields)]))))

(defn handle-model-group-decl [{default-ns :default-ns :as context} ^XSModelGroupDecl x]
  (if (anon-type? x)
    (let [mg (.getModelGroup x)]
      (handle-model-group context mg))
    (-seq-ref x context)))

(defn handle-wildcard [^XSWildcard x]
  [:xml/hiccup])

(defn complex-attrs-map [^XSComplexType x {default-ns :default-ns :as context}]
  (let [annotations (some-> (.getAnnotation x false)
                            .getAnnotation)
        docs (not-empty (into []
                              (comp
                               (mapcat (fn [{:keys [tag content]}]
                                         (when (= tag :xsd:documentation)
                                           content))))
                              annotations))]
    (some->> (eduction
              (map (fn [^XSAttributeUse attr-use]
                     (let [decl (.getDecl attr-use)
                           annotations (some-> (.getAnnotation decl false)
                                               .getAnnotation)
                           docs (not-empty (into []
                                                 (comp
                                                  (mapcat (fn [{:keys [tag content]}]
                                                            (when (= tag :xsd:documentation)
                                                              content))))
                                                 annotations))
                           name (.getName decl)
                           attrns (some-> (.getTargetNamespace decl) not-empty-string uri->ns)
                           ty (.getType decl)
                           tyref (if (anon-type? ty)
                                   (-mtype ty context)
                                   (-seq-ref ty context))]
                       [(if (and attrns (not (.isEmpty attrns)))
                          (keyword attrns name)
                          (keyword name))
                        (cond->  {:xml/attr true}
                          (not (.isRequired attr-use))
                          (assoc  :optional true)
                          docs
                          (assoc  :attr-documentation (first docs)))
                        tyref])))
              (.getAttributeUses x))
             not-empty
             (into [:map (cond-> {:closed        true}
                           docs
                           (assoc  :documentation (first docs)))]))))

(defn complex-tag [complex]
  (when (vector? complex)
    (nth complex 0)))

(defn- promote-value-choice-to-alt
  "Value-wrapped body is a stream of child elements. Promote bare :or
  (value-mode choice) to :alt (seqex) so the parser wraps atomic arms as one
  slot and parent :? / :cat can inline them (IVL_TS low/high, IVL_PQ, …).

  Map-field value types that are pure choice without attrs (SCRIPT StatusType,
  DateType) stay :or — they never pass through value-wrap."
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

(defn- value-wrap
  "Attrs map + :xml/value content under value-wrapped complex type."
  [attr-map content]
  (-> attr-map
      (update 1 assoc :xml/value-wrapped true)
      (conj [:xml/value {} (promote-value-choice-to-alt content)])))
(defn- strip-regex-wrapper
  "Unwrap :* / :? / :+ / :repeat to inspect the underlying content form."
  [form]
  (if (and (vector? form) (#{:* :? :+ :repeat} (first form)))
    (last form)
    form))

(defn- element-choice-form?
  "True when form is :or/:alt of element tuples (StrucDoc-style child choice)."
  [form]
  (let [form (strip-regex-wrapper form)]
    (and (vector? form)
         (#{:or :alt} (complex-tag form))
         (let [children (if (map? (second form))
                          (drop 2 form)
                          (rest form))]
           (and (seq children)
                (every? (fn [child]
                          (and (vector? child) (= :tuple (first child))))
                        children))))))

(defn- empty-struct-map?
  "Empty closed map with no entries (ST-style empty complex content)."
  [form]
  (and (vector? form)
       (= :map (first form))
       (<= (count form) 2)))

(defn- contains-form?
  "True if form tree contains `needle` (by =)."
  [form needle]
  (or (= form needle)
      (and (coll? form)
           (not (map-entry? form))
           (some #(contains-form? % needle) form))))

(defn- ed-style-mixed?
  "ED-like mixed: :cat with structured children plus :xml/hiccup tail.
  Keep typed structure for reference/thumbnail rather than collapsing to hiccup."
  [complex]
  (and (vector? complex)
       (= :cat (complex-tag complex))
       (contains-form? complex :xml/hiccup)))

(extend-protocol MalliXML
  XSComplexType
  (-mtype [x context]
    (let [ct (.getContentType x)
          mixed? (.isMixed x)
          attr-map (complex-attrs-map x context)
          simple (some-> ct
                         .asSimpleType
                         (-mtype context))
          empt (some-> ct
                       .asEmpty)
          complex (some->> ct
                           .asParticle
                           (handle-toplevel-particle context))]
      (cond
        (and attr-map simple)
        (value-wrap attr-map simple)

        ;; XSD mixed="true": character data + optional element children.
        ;; Always capture as :xml/hiccup for fidelity (StrucDoc, EN/PN, ED free
        ;; text, ADXP, …). Structured ED reference/thumbnail become hiccup nodes.
        (and attr-map mixed?)
        (value-wrap attr-map :xml/hiccup)

        (and attr-map complex)
        (case (complex-tag complex)
          :map
          [:merge {}
           attr-map
           complex]
          :merge
          (into [:merge {}
                 attr-map]
                (drop 2)
                complex)
          (value-wrap attr-map complex))

        (and attr-map (nil? complex)) attr-map
        simple simple
        complex complex
        empt (or attr-map [:map {:empty true}]) #_(do
                                                    (prn (bean ct))
                                                    (throw (ex-info "empty" {} #_{:x (bean ct)}))
                                                    :any))))
  (-seq-possible? [x context]
    true)
  (-seq-ref [x context]
    (if (and (some->> x
                      .getContentType
                      .asParticle)
             (:sequence context))
      (wrap-ref-np (->nskw-seq x (:default-ns context)))
      (wrap-ref-np (->nskw x (:default-ns context))))))

(defn union-reducible [^XSUnionSimpleType in]
  (let [cnt (.getMemberSize in)]
    (reify IReduceInit
      (reduce [this f init]
        (loop [i 0 acc init]
          (if (or (reduced? acc)
                  (not (< i cnt)))
            (unreduced acc)
            (let [member (.getMember in i)]
              (recur (inc i) (f acc member)))))))))

(extend-protocol MalliXML
  XSUnionSimpleType
  (-mtype [x context]
    (into [:or] (map #(-mtype % context)) (union-reducible x)))
  (-seq-possible? [x context]
    false)
  (-seq-ref [x context]
    [:ref (->nskw x (:default-ns context))]))

(extend-protocol MalliXML
  XSModelGroupDecl
  (-mtype [x context]
    (handle-model-group context (.getModelGroup x)))
  (-seq-possible? [x context]
    true)
  (-seq-ref [x context]
    (if (:sequence context)
      (wrap-ref-np (->nskw-seq x (:default-ns context)))
      (wrap-ref-np (->nskw x (:default-ns context))))))

(extend-protocol MalliXML
  XSRestrictionSimpleType
  (-mtype [x context]
    (let [prim-keyword (some->> x .getPrimitiveType .getName (keyword "org.w3.www.2001.XMLSchema"))
          base-type    (some-> x .getSimpleBaseType)]
      #_(prn (.getVariety x))
      #_(when (= (get-primitive-type x) "length_range_Type")
          (prn (type x))
          (pp/pprint (bean x)))
      (case (.toString (.getVariety x))
        "atomic"
        (if (.isPrimitive x)
          prim-keyword
          (case (get-primitive-type x)
            "decimal" prim-keyword                          ;java.math.BigDecimal
            "float" prim-keyword
            "boolean" prim-keyword
            "double" prim-keyword
            "base64Binary" prim-keyword
            "anyURI" prim-keyword
            "date", prim-keyword                            ;javax.xml.datatype.XMLGregorianCalendar
            "dateTime", prim-keyword                        ;javax.xml.datatype.XMLGregorianCalendar
            "string", (malli-string-primitive x context)
            ;; User simple types / unhandled primitives (FOP length_Type, …)
            (if base-type
              (-mtype base-type context)
              (or prim-keyword :string))))
        "list"
        (if-some [ltype (.asList x)]
          (let [it (.getItemType ltype)]
            [:sequential
             (if (anon-type? it)
               (-mtype it (dissoc context :sequence))
               (-seq-ref it (dissoc context :sequence)))])
          (case (get-primitive-type x)
            ("IDREFS" "ENTITIES" "NMTOKENS") :string
            ;; XSOM sometimes reports list variety without asList (FOP compounds)
            :string))
        "union"
        ;; Prefer member expansion when available; otherwise open string.
        (if (instance? XSUnionSimpleType x)
          (-mtype ^XSUnionSimpleType x context)
          (or (when base-type (-mtype base-type context))
              :string)))))
  (-seq-possible? [x context]
    false)
  (-seq-ref [x context]
    [:ref (->nskw x (:default-ns context))]))

(extend-protocol MalliXML
  XSListSimpleType
  (-mtype [x context]
    (let [ty (.getItemType x)]
      [:sequential (-mtype ty context)]))
  (-seq-possible? [x context]
    false)
  (-seq-ref [x context]
    [:ref (->nskw x (:default-ns context))]))

(defn ^XSSchemaSet parse-xsd
  [f]
  (let [parser
        (XSOMParser. (SAXParserFactory/newDefaultInstance))]
    (.setErrorHandler parser (reify org.xml.sax.ErrorHandler
                               (^void warning [_ ^SAXParseException x] (prn x))
                               (^void error [_ ^SAXParseException x] (prn x))
                               (^void fatalError [_ ^SAXParseException x] (prn x))))
    (.setAnnotationParser parser (reify AnnotationParserFactory
                                   (create [_]
                                     (push-thread-bindings {#'clojure.xml/*stack* nil
                                                            #'clojure.xml/*current* (struct clojure.xml/element)
                                                            #'clojure.xml/*state* :between
                                                            #'clojure.xml/*sb* nil})
                                     (proxy [AnnotationParser] []
                                       (getContentHandler [^AnnotationContext context,
                                                           ^String parentElementName,
                                                           ^ErrorHandler errorHandler,
                                                           ^EntityResolver entityResolver]

                                         clojure.xml/content-handler)
                                       (getResult [old]
                                         (let [result clojure.xml/*current*]

                                           (pop-thread-bindings)
                                           (into []
                                                 (mapcat :content)
                                                 (:content result))))))))

    (.parse parser f)
    (.getResult parser)))

(defn xsd->top-type [{default-ns :default-ns :as context} schema]
  (into [:multi {:dispatch first}]
        (comp (map (partial handle-element-decl context))
              (map (fn [[_ _ [_ tag] :as x]]
                     [tag x])))
        (iterator-seq (.iterateElementDecls schema))))

(defn xsd-builtin-decl?
  "True when a declaration is a built-in XML Schema type rather than something
  the parsed document declares. Decided from the declaration's own name so a
  -seq dual is dropped or kept together with its base entry."
  [^XSDeclaration x default-ns]
  (xsd-builtin-kw? (->nskw x default-ns)))

(defn xsd->registry [{default-ns :default-ns :as context} schema]
  (let [seq-context (assoc context :sequence true)]
    (-> xml-primitives/xmlschema-registry
        (into
         (comp
          (remove (fn [^XSType x]
                    (some-> x .asSimpleType .isPrimitive)))
          (filter #(-seq-possible? % nil))
          (remove #(xsd-builtin-decl? % default-ns))
          (map #(vector (->nskw-seq % default-ns) (-mtype % seq-context))))
         (iterator-seq (.iterateTypes schema)))
        (into
         (comp
          (remove (fn [^XSType x]
                    (some-> x .asSimpleType .isPrimitive)))
          (remove #(xsd-builtin-decl? % default-ns))
          (map #(vector (->nskw % default-ns) (-mtype % context))))
         (iterator-seq (.iterateTypes schema)))

        (into
         (comp
          (filter (fn [^XSModelGroupDecl x]
                    (.isGlobal x)))
          (remove #(xsd-builtin-decl? % default-ns))
          (map #(vector (->nskw-seq % default-ns) (-mtype % seq-context))))
         (iterator-seq (.iterateModelGroupDecls schema)))
        (into
         (comp
          (filter (fn [^XSModelGroupDecl x]
                    (.isGlobal x)))
          (remove #(xsd-builtin-decl? % default-ns))
          (map #(vector (->nskw % default-ns) (-mtype % context))))
         (iterator-seq (.iterateModelGroupDecls schema))))))

(defn xsd->schema [context f]
  (let [schema (parse-xsd f)
        registry (xsd->registry context schema)
        top-type (xsd->top-type context schema)]
    (xml-primitives/make-schema registry top-type)))

(defn raw-xsd->schema [context f]
  (let [schema (parse-xsd f)
        registry (xsd->registry context schema)
        top-type (xsd->top-type context schema)]
    [:schema {:registry registry}
     top-type]))

(defn trim-registry-for-top-types- [full-registry trimmed-registry next-keys]
  (if-some [new-keys (->> next-keys
                          (into []
                                (comp (remove trimmed-registry)))
                          not-empty)]
    (let [new-registry (select-keys full-registry new-keys)
          new-refs (atom #{})]
      (reduce-kv
       (fn [_ k v]
         (m/walk v
                 (m/schema-walker
                  (fn [sch]
                    (case (m/type sch)
                      :ref (swap! new-refs conj (-> sch m/children first))
                      :malli.core/schema (when (m/-reference? (m/form sch))
                                           (swap! new-refs conj (m/form sch)))
                      nil)
                    sch))))
       nil
       new-registry)
      (recur full-registry (into trimmed-registry new-registry) @new-refs))
    trimmed-registry))

(defn trim-registry-for-top-types [registry top-types]
  (trim-registry-for-top-types- registry {} top-types))

(defn into-sorted-map [x]
  (into (sorted-map) x))

;; Choice -> discriminated :multi

(defn- form-head
  "Head keyword of a schema form, or nil when the form is not a vector (registry
  values may be bare keyword aliases)."
  [form]
  (when (vector? form)
    (nth form 0 nil)))

(defn- form-props
  "Property map of a schema form, or nil when the form carries none."
  [form]
  (let [x (get form 1)]
    (when (map? x) x)))

(defn- form-children
  "Children of a schema form, skipping the optional property map at index 1."
  [form]
  (if (map? (get form 1))
    (subvec form 2)
    (subvec form 1)))

(defn leading-tag-dispatch
  "Dispatch fn for the schemas produced by `or->multi`: returns the leading XML
  tag keyword of a parsed value, or nil when there is none.

  Parsed values are either a flat tuple `[:Tag v]` or a flat chunk
  `[[:Tag v] ...]`: sitoa's `-cat-parser` treats `:cat`/`:alt` children as
  inline data and splices their chunks FLAT into the parent accumulator, so even
  a branch whose first child is a nested `:cat` parses to a flat chunk whose
  first element is a tagged tuple. The descent past the second level is harmless
  generality. Returning nil rather than throwing lets `:multi` report an invalid
  value for input it cannot dispatch."
  [v]
  (loop [x (when (sequential? v) (first v))]
    (cond
      (keyword? x) x
      (sequential? x) (recur (first x))
      :else nil)))

(defn keywordize-leading-tag
  "`:decode/string` `:enter` hook for the schemas produced by `or->multi`:
  keywordizes a string leading tag so `leading-tag-dispatch` can dispatch on
  decoded input. Descends the leading position (`[0]`, `[0 0]`, deeper for
  nested chunks) and passes everything else through untouched."
  [v]
  (if-not (vector? v)
    v
    (loop [path [0]
           x    (nth v 0 nil)]
      (cond
        (string? x) (update-in v path keyword)
        (vector? x) (recur (conj path 0) (nth x 0 nil))
        :else v))))

(defn- branch-key
  "The XML start tag discriminating a single choice arm. `form` is the (possibly
  nested) form under inspection, `arm` the enclosing arm kept for error context."
  [form arm]
  (case (form-head form)
    :tuple
    (let [enum-form (first (form-children form))]
      (when-not (= :enum (form-head enum-form))
        (throw (ex-info "or->multi: :tuple arm is not tagged with an [:enum tag]"
                        {:reason :not-enum-tagged :form form :arm arm})))
      (let [tags (form-children enum-form)]
        (when-not (= 1 (count tags))
          (throw (ex-info "or->multi: an [:enum ...] tag must hold exactly one tag"
                          {:reason :multi-tag-enum :tags tags :form form :arm arm})))
        (first tags)))

    :cat
    (let [children (form-children form)]
      (when (empty? children)
        (throw (ex-info "or->multi: empty :cat arm has no leading tag"
                        {:reason :empty-cat :form form :arm arm})))
      (recur (first children) arm))

    (throw (ex-info "or->multi: arm has no fixed leading XML tag"
                    {:reason :not-discriminable :head (form-head form)
                     :form  form :arm arm}))))

(defn- arm->branches
  "Branch entries `[tag form]` contributed by one choice arm.

  A `:cat` arm whose first child is an `:alt` is split into one branch per `:alt`
  member; each branch keeps the arm's properties and its shared tail, and the
  `[:cat ...]` wrapper is kept even when the tail is empty so the parse shape
  stays a chunk. A member that is itself a `:cat` stays nested. Every other arm
  yields a single branch holding the arm verbatim."
  [arm]
  (let [children   (when (= :cat (form-head arm)) (form-children arm))
        head-child (first children)]
    (if (= :alt (form-head head-child))
      (let [props     (form-props arm)
            alt-props (form-props head-child)
            tail      (subvec children 1)]
        ;; An empty property map is dropped losslessly; anything else would be
        ;; silently lost by the split, so refuse it.
        (when (seq alt-props)
          (throw (ex-info "or->multi: cannot split an :alt that carries properties"
                          {:reason :alt-properties :props alt-props :arm arm})))
        (mapv (fn [member]
                [(branch-key member arm)
                 (into (if props [:cat props member] [:cat member]) tail)])
              (form-children head-child)))
      [[(branch-key arm arm) arm]])))

(def ^:private or->multi-defaults
  {:decode-string? true
   :seqex-branches :throw})

(defn or->multi
  "Convert a discriminated choice form (an `:or`/`:alt` of element-tagged seqex
  arms) into `[:multi {:dispatch leading-tag-dispatch
                       :decode/string {:enter keywordize-leading-tag}} ...]`.

  Branch keys are the XML start tags of the arms, and they MUST stay that way:
  sitoa's serde parser (`-multi-parser`) ignores `:dispatch` entirely and looks
  the XML start-tag keyword up directly among the branch keys. The unparser and
  malli coercion are the ones that use `:dispatch`.

  Original properties and arms are preserved verbatim; a `:cat` arm led by an
  `:alt` is split into one branch per member, sharing the arm's tail.

  The result contains fn objects, so it is NOT EDN-serializable: this is a
  load-time transform, never something to serialize, and it must be applied
  AFTER any `*-seq` derivation (the derivation does not descend into `:multi`).

  Throws `ex-info` on arms with no fixed leading tag, on duplicate branch keys
  (`:multi` would silently keep the last), and — by default — on `:alt`-headed
  input that would produce a `:cat` branch, because an `:alt` chunk arm is
  spliced inline while a `:multi` consumes exactly one slot.

  opts:
    :decode-string? (default true)    attach the `:decode/string` enter hook
    :seqex-branches (default :throw)  `:allow` permits `:cat` branches from
                                      `:alt`-headed input"
  ([form] (or->multi form nil))
  ([form opts]
   (let [{:keys [decode-string? seqex-branches]} (merge or->multi-defaults opts)
         head (form-head form)]
     (when-not (or (= :or head) (= :alt head))
       (throw (ex-info "or->multi: expects an :or or :alt choice form"
                       {:reason :not-a-choice :head head :form form})))
     (let [branches   (into [] (mapcat arm->branches) (form-children form))
           duplicates (into []
                            (comp (filter (fn [[_ n]] (< 1 n)))
                                  (map first))
                            (frequencies (map first branches)))]
       (when (seq duplicates)
         (throw (ex-info "or->multi: duplicate branch keys would collide in :multi"
                         {:reason :duplicate-branch-keys :duplicates duplicates
                          :form   form})))
       (when (and (= :alt head)
                  (= :throw seqex-branches)
                  (some (fn [[_ branch]] (= :cat (form-head branch))) branches))
         (throw (ex-info "or->multi: :alt input would produce :cat branches, which changes how the value is consumed"
                         {:reason :seqex-branches :form form})))
       (into [:multi (cond-> (assoc (or (form-props form) {})
                                    :dispatch leading-tag-dispatch)
                       decode-string?
                       (assoc :decode/string {:enter keywordize-leading-tag}))]
             branches)))))

(defn or->multi-keys
  "Apply `or->multi` to the named entries of a keyword->form registry map,
  returning the updated registry. `opts` is passed through to `or->multi`.
  Throws on keys absent from the registry unless `{:missing :skip}`."
  ([registry ks] (or->multi-keys registry ks nil))
  ([registry ks opts]
   (let [missing (get opts :missing :throw)]
     (reduce
      (fn [acc k]
        (cond
          (contains? acc k) (assoc acc k (or->multi (get acc k) opts))
          (= :skip missing) acc
          :else             (throw (ex-info "or->multi-keys: key absent from registry"
                                            {:reason :missing-key :key k}))))
      registry
      ks))))

(defn serialize-registry [schema filename]
  (with-open [w (io/writer filename)]
    (binding [*out* w]
      (.write w "{")
      (reduce-kv
       (fn [acc k v]
         (.write w (pr-str k))
         (.write w "\n")
         (fipp (m/form v) {:writer w})
          ;(.write w "\n")
         )
       w
       (-> schema m/properties :registry into-sorted-map))
      (.write w "}"))))

(defn serialize-schema [schema filename]
  (with-open [w (io/writer filename)]
    (fipp (m/form (mu/update-properties schema :registry into-sorted-map)) {:writer w})))

