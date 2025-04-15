(ns dunnage.sitoa.bootstrapped-schema
  (:require [clojure.java.io :as io]
            [malli.core :as m]
            [malli.util :as mu]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [clojure.tools.reader.edn :as edn]
            [clojure.pprint :as pp]
            [fipp.edn :refer [pprint] :rename {pprint fipp}])
  (:import (com.sun.xml.xsom.parser XSOMParser)
           (javax.xml.parsers SAXParserFactory)
           (com.sun.xml.xsom XSRestrictionSimpleType XSSimpleType XmlString XSComplexType XSTerm XSParticle XSModelGroup XSUnionSimpleType XSListSimpleType XSComponent XSDeclaration XSModelGroupDecl XSWildcard XSWildcard$Any XSType ForeignAttributes XSAttributeUse XSFacet XSSchemaSet XSElementDecl XSVariety)
           (org.xml.sax ErrorHandler SAXParseException)
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
    (case (.getScheme uri )
      "urn" (-> []
                (into (reverse (clojure.string/split (str (.getSchemeSpecificPart uri)) #":")))
                (->> (clojure.string/join ".")))
      (-> []
          (into (reverse (clojure.string/split (.getHost uri) #"\.")))
          (into (remove empty?) (clojure.string/split (.getPath uri) #"\/"))
          (->> (clojure.string/join "."))))
    ))

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
                  fixed (.isFixed facet)]
              (case name
                "enumeration" (update acc :enum (fnil conj [:enum]) (.toString ^XmlString value))
                "length" (let [l (Long/parseLong (str value))]
                           (assoc acc :min l
                                      :max l))
                "maxLength" (assoc acc :max (Long/parseLong (str value)))
                "minLength" (assoc acc :min (Long/parseLong (str value)))
                "pattern" (do                               ;(prn (str value))
                            (assoc acc :pattern [:re
                                                 (let [re (str value)]
                                                   (case re
                                                     "\\i\\c*" name-regex
                                                     "[\\i-[:]][\\c-[:]]*" name-regex
                                                     re))
                                                 ]))
                "whiteSpace" acc)))                         ;"preserve replace collapse))
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

(defn handle-fields-wrapper [{default-ns :default-ns :as context}]
  (fn [^XSParticle in]
    (let [term (.getTerm in)
          min-occurs (long (.getMinOccurs in))
          max-occurs (long (.getMaxOccurs in))
          x (.asElementDecl term)
          ty (.getType x)
          ty-ref (if (anon-type? ty)
                   (-mtype ty context)
                   (-seq-ref ty context))]
      (assert x)
      (when (not= max-occurs 0) #_(not= (m/children ty-ref) [(keyword default-ns "Extension")])
        [(->kw x)
         (cond-> {}
                 (= 0 min-occurs)
                 (assoc :optional true))
         (if (or (> max-occurs 1) (= max-occurs -1))
           [:sequential (if (= 0 min-occurs)
                          {:min 1}
                          {:min min-occurs})
            ty-ref]
           ty-ref)])
      )))

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
          value-sequence? (or (> max-occurs 1) (= max-occurs -1))]
      (when (not= max-occurs 0) #_(not= (m/children ty-ref) [(keyword default-ns "Extension")])
        (doto (or (when-some [x (.asElementDecl term)]
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
                                      :required-in-group true))
                       (if value-sequence?
                         [:sequential (if (= 0 min-occurs)
                                        {:min 1}
                                        {:min min-occurs})
                          ty-ref]
                         ty-ref)])
                    )
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

                  (prn :fail (bean in))))))))

(defn wrap-regex [context ^XSParticle in msch]
  (let [min-occurs (.getMinOccurs in)
        max-occurs (.getMaxOccurs in)
        can-be-empty? (= 0 min-occurs)
        unbounded? (= max-occurs -1)
        repeated? (or (> max-occurs 1) (= max-occurs -1))]
    (cond
      (= 0 max-occurs)
      nil
      (:sequence context)
      (cond
        (and (not can-be-empty?) (not repeated?)) msch
        (and can-be-empty? (not repeated?)) [:? msch]
        (and (not can-be-empty?) repeated? unbounded?) [:+ msch]
        (and can-be-empty? repeated? unbounded?) [:* msch]
        :else
        [:repeat {:min min-occurs, :max max-occurs} msch])
      repeated?
      [:sequential msch]
      ;can-be-empty?
      ;[:maybe msch]
      :else
      msch
      )))

(defn handle-element-decl [{default-ns :default-ns :as context} ^XSElementDecl x]
  ;(prn (->nskw x (:default-ns context)) (.isGlobal x) (anon-type? x))
  (let [ty (.getType x)
        ;_  (prn  (anon-type? ty))
        ty-ref (if (anon-type? ty)
                 (-mtype ty (dissoc context :sequence))
                 (-seq-ref ty (dissoc context :sequence)))]
    [:tuple {} [:enum (->kw x)] ty-ref]))

(defn element-decl? [^XSParticle x]
  (some-> x .getTerm .asElementDecl))

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
  (let [t (.getTerm in)]
    (assert (not (some-> (.asElementDecl t))))
    ;(assert (not (.isRepeated x)) (pr-str (bean x)))
    (or
      (some->> (.asModelGroup t)
               (handle-model-group context))
      (some->> (.asModelGroupDecl t)
               (handle-model-group-decl context))
      (some-> (.asWildcard t)
              handle-wildcard))))

(defn all-maps? [x]
  (transduce
    (drop 2)
    (fn ([acc] (if (nil? acc)
                 false
                 acc))
      ([acc nv]
       (if (= #{:map :merge} (nth nv 0))
         true
         (reduced false))))
    nil
    x)
  )
(defn handle-model-group [context ^XSModelGroup x]
  (let [compositor (str (.getCompositor x))
        fields (.getChildren x)]
    (case compositor
      "sequence" (cond
                   (and (= 1 (count fields))
                        (when-some [wc (some-> fields first .getTerm .asWildcard)]
                          (instance? XSWildcard$Any wc)))
                   :any
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
                       ([acc] (if (all-maps? acc )
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
                 :any
                 :default
                 (into [(if (:sequence context)
                          :alt
                          :or)]
                       (keep #(group-particle (assoc context
                                                :compositor "choice") %))
                       fields)
                 #_(let [part (reduce
                                (fn [acc nv] (group-particle-seq
                                               (assoc context
                                                 :named-part false
                                                 :compositor "choice") acc nv))
                                []
                                fields)]
                     (if (every? (comp #{:tuple} first) part)
                       #_(into [:multi {:dispatch 'first}]
                               (keep (fn [[_tuple _args [_enum enum-value] t]]
                                       [enum-value [:tuple :keyword t]]))
                               part)
                       (into [:or] part)
                       (into [:or] part))))
      "all" (cond
              (and (= 1 (count fields))
                   (instance? XSWildcard$Any (first fields)))
              :any
              (every-sequence? (atom #{}) x)
              (transduce
                (keep(handle-fields-wrapper2 context))
                (simplify-fields {:closed true})
                fields)
              :default [compositor (map #(group-particle context %) fields)]))))

(defn handle-model-group-decl [{default-ns :default-ns :as context} ^XSModelGroupDecl x]
  (if (anon-type? x)
    (let [mg (.getModelGroup x)]
      (handle-model-group context mg))
    (-seq-ref x context)))

(defn handle-wildcard [^XSWildcard x]
  [:any])

(defn complex-attrs-map [^XSComplexType x {default-ns :default-ns :as context}]
  (some->> (eduction
             (map (fn [^XSAttributeUse attr-use]
                    (let [decl (.getDecl attr-use)
                          name (.getName decl)
                          attrns (some-> (.getTargetNamespace decl) not-empty-string uri->ns)
                          ty (.getType decl)
                          tyref (if (anon-type? ty)
                                  (-mtype ty context)
                                  (-seq-ref ty context))]
                      [(if (and attrns (not (.isEmpty attrns)))
                         (keyword attrns name)
                         (keyword name))
                       (if (.isRequired attr-use)
                         {:xml/attr true}
                         {:xml/attr true
                          :optional true})
                       tyref])))
             (.getAttributeUses x))
           not-empty
           (into [:map {:closed true}])))

(defn complex-tag [complex]
  (when (vector? complex)
    (nth complex 0)))

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
        (and attr-map simple) (-> attr-map
                                  (update 1 assoc :xml/value-wrapped true)
                                  (conj [:xml/value {} simple]))
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
          (-> attr-map
              (update 1 assoc :xml/value-wrapped true)
              (conj [:xml/value {} complex])))
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
      [:ref (->nskw-seq x (:default-ns context))]
      [:ref (->nskw x (:default-ns context))])
    ))

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
      [:ref (->nskw-seq x (:default-ns context))]
      [:ref (->nskw x (:default-ns context))])
    ))

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

            #_#_nil (do                                         ;(prn (bean x))
                  (-mtype base-type context))))
        "list"
        (if-some [ltype (.asList x)]
          (let [it (.getItemType ltype)]
            (do                                             ; (identical? ct x)
              #_(do (throw (ex-info "empty" {}))            ;(prn (.getName x) (.getDeclaredFacets x)) #_(pp/pprint (bean x))
                    :any)
              (prn it)
              [:sequence {:primitive true}
               (if (anon-type? it)
                   (-mtype it (dissoc context :sequence))
                   (-seq-ref it (dissoc context :sequence)))]))
          (case (get-primitive-type x)
            "IDREFS" :string
            "ENTITIES" :string
            "NMTOKENS" :string
            ))
        "union"
        (let []
          (throw (ex-info "union is not supported yet" {}))
          [:any {:name (.getName x)}]
          #_(if (identical? ct x)
            (do (prn (.getName x) (.getDeclaredFacets x)) #_(pp/pprint (bean x))
                )
            [:sequence {:primitive true}
             (if (anon-type? ct)
               (-mtype ct (dissoc context :sequence))
               (-seq-ref ct (dissoc context :sequence)))])))))
  (-seq-possible? [x context]
    false)
  (-seq-ref [x context]
    [:ref (->nskw x (:default-ns context))]
    ))

(extend-protocol MalliXML
  XSListSimpleType
  (-mtype [x context]
    (let [ty (.getItemType x)]
      [:sequential (-mtype ty context)]))
  (-seq-possible? [x context]
    false)
  (-seq-ref [x context]
    [:ref (->nskw x (:default-ns context))]
    ))

(defn ^XSSchemaSet parse-xsd
  [f]
  (let [parser
        (XSOMParser. (SAXParserFactory/newDefaultInstance))]
    (.setErrorHandler parser (reify org.xml.sax.ErrorHandler
                               (^void warning [_ ^SAXParseException x] (prn x))
                               (^void error [_ ^SAXParseException x] (prn x))
                               (^void fatalError [_ ^SAXParseException x] (prn x))))
    (.parse parser f)
    (.getResult parser)))

(defn xsd->top-type [{default-ns :default-ns :as context} schema]
  (into [:multi {:dispatch first}]
        (map (partial handle-element-decl context))
        (iterator-seq (.iterateElementDecls schema))))

(defn xsd->registry [{default-ns :default-ns :as context} schema]
  (let [seq-context (assoc context :sequence true)]
    (-> xml-primitives/xmlschema-registry
        (into
          (comp
            (remove (fn [^XSType x]
                      (some-> x .asSimpleType .isPrimitive)))
            (filter #(-seq-possible? % nil))
            (map #(vector (->nskw-seq % default-ns) (-mtype % seq-context))))
          (iterator-seq (.iterateTypes schema)))
        (into
          (comp
            (remove (fn [^XSType x]
                      (some-> x .asSimpleType .isPrimitive)))
            (map #(vector (->nskw % default-ns) (-mtype % context))))
          (iterator-seq (.iterateTypes schema)))

        (into
          (comp
            (filter (fn [^XSModelGroupDecl x]
                      (.isGlobal x)))
            (map #(vector (->nskw-seq % default-ns) (-mtype % seq-context))))
          (iterator-seq (.iterateModelGroupDecls schema)))
        (into
          (comp
            (filter (fn [^XSModelGroupDecl x]
                      (.isGlobal x)))
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
        (-> schema m/properties :registry))
      (.write w "}")
      )))

(defn serialize-schema [schema filename]
  (with-open [w (io/writer filename)]
    (fipp (m/form schema) {:writer w})))

