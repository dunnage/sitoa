(ns dunnage.sitoa.xsd-to-malli.compiler
  "Compile a loaded schema set into a malli registry.

  This is a data-side reimplementation of the decision tree
  dunnage.sitoa.bootstrapped-schema drives off XSOM. Every rule cites the
  oracle lines it mirrors, because the two have to agree: the registry keys,
  the map-mode/seqex-mode decisions and the occurrence wrappers all change what
  the streaming parser returns, so a plausible-looking difference is a bug.

  Derivation is the one deliberate departure. Where XSOM hands the oracle an
  already-flattened content model for an xs:extension or xs:restriction, this
  compiler keeps the edge: it produces, per registry key, both

    :form - the flattened form, used internally to decide shapes and -seq
            duals and to compare against the oracle, and
    :emit - the value to write out, which for a derived type is a Derived node
            carrying a plan that rebuilds the type from its base type's schema
            at schema-build time.

  For everything that is not derived the two are the same literal data."
  (:require [clojure.string :as str]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [dunnage.sitoa.xsd-to-malli.ast :as ast]
            [dunnage.sitoa.xsd-to-malli.runtime :as rt]
            [dunnage.sitoa.xsd-to-malli.symbols :as symbols]))

(def seq-suffix "-seq")

(defn seq-kw [k]
  (keyword (namespace k) (str (name k) seq-suffix)))

;; ---------------------------------------------------------------------------
;; Built-in datatypes
;; ---------------------------------------------------------------------------

(def name-regex
  "bootstrapped_schema.clj line 54: the substitute for the two XSD name
  patterns, whose \\i and \\c character-class escapes java.util.regex rejects."
  "^\\d{1,3}|\\d(([ ,]?\\d{3})*([.,]\\d{2}+)?$)")

(def builtin-datatypes
  "The XSD built-in datatype hierarchy, as XSOM presents it.

  :variety, :primitive, :primitive? and :base restate the XML Schema Part 2
  datatype definitions; :form is the exact result of running the oracle's
  -mtype over that built-in. Both are static properties of the specification,
  not of any document, so they are recorded here instead of being rederived -
  which also keeps XSOM off this project's runtime classpath. The table was
  dumped from XSOM (XSSchemaSet for the XML Schema namespace) and its :form
  column is asserted against the oracle in compiler-test.

  anyType, dayTimeDuration, yearMonthDuration and untypedAtomic have no XSOM
  definition: anyType is unconstrained content (wrap-ref-np inlines
  :xml/hiccup for it), the other three are XSD 1.1 / XPath types that an XSD
  1.0 document cannot name."
  {"ENTITIES"           {:variety :list :primitive nil :primitive? false :base nil :form :string}
   "ENTITY"             {:variety :atomic :primitive "string" :primitive? false :base "NCName"
                         :form :org.w3.www.2001.XMLSchema/NCName}
   "ID"                 {:variety :atomic :primitive "string" :primitive? false :base "NCName"
                         :form :org.w3.www.2001.XMLSchema/NCName}
   "IDREF"              {:variety :atomic :primitive "string" :primitive? false :base "NCName"
                         :form :org.w3.www.2001.XMLSchema/NCName}
   "IDREFS"             {:variety :list :primitive nil :primitive? false :base nil :form :string}
   "NCName"             {:variety :atomic :primitive "string" :primitive? false :base "Name"
                         :form [:and [:re name-regex] :org.w3.www.2001.XMLSchema/Name]}
   "NMTOKEN"            {:variety :atomic :primitive "string" :primitive? false :base "token"
                         :form [:and [:re "\\c+"] :org.w3.www.2001.XMLSchema/token]}
   "NMTOKENS"           {:variety :list :primitive nil :primitive? false :base nil :form :string}
   "NOTATION"           {:variety :atomic :primitive "NOTATION" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/NOTATION}
   "Name"               {:variety :atomic :primitive "string" :primitive? false :base "token"
                         :form [:and [:re name-regex] :org.w3.www.2001.XMLSchema/token]}
   "QName"              {:variety :atomic :primitive "QName" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/QName}
   "anySimpleType"      {:variety :atomic :primitive "anySimpleType" :primitive? true :base "anyType"
                         :form :org.w3.www.2001.XMLSchema/anySimpleType}
   "anyType"            {:variety :atomic :primitive nil :primitive? false :base nil
                         :form :xml/hiccup :complex? true}
   "anyURI"             {:variety :atomic :primitive "anyURI" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/anyURI}
   "base64Binary"       {:variety :atomic :primitive "base64Binary" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/base64Binary}
   "boolean"            {:variety :atomic :primitive "boolean" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/boolean}
   "byte"               {:variety :atomic :primitive "decimal" :primitive? false :base "short"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "date"               {:variety :atomic :primitive "date" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/date}
   "dateTime"           {:variety :atomic :primitive "dateTime" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/dateTime}
   "dayTimeDuration"    {:variety :atomic :primitive "duration" :primitive? false :base "duration"
                         :form :org.w3.www.2001.XMLSchema/dayTimeDuration}
   "decimal"            {:variety :atomic :primitive "decimal" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "double"             {:variety :atomic :primitive "double" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/double}
   "duration"           {:variety :atomic :primitive "duration" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/duration}
   "float"              {:variety :atomic :primitive "float" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/float}
   "gDay"               {:variety :atomic :primitive "gDay" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/gDay}
   "gMonth"             {:variety :atomic :primitive "gMonth" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/gMonth}
   "gMonthDay"          {:variety :atomic :primitive "gMonthDay" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/gMonthDay}
   "gYear"              {:variety :atomic :primitive "gYear" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/gYear}
   "gYearMonth"         {:variety :atomic :primitive "gYearMonth" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/gYearMonth}
   "hexBinary"          {:variety :atomic :primitive "hexBinary" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/hexBinary}
   "int"                {:variety :atomic :primitive "decimal" :primitive? false :base "long"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "integer"            {:variety :atomic :primitive "decimal" :primitive? false :base "decimal"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "language"           {:variety :atomic :primitive "string" :primitive? false :base "token"
                         :form [:and
                                [:re "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                                :org.w3.www.2001.XMLSchema/token]}
   "long"               {:variety :atomic :primitive "decimal" :primitive? false :base "integer"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "negativeInteger"    {:variety :atomic :primitive "decimal" :primitive? false :base "nonPositiveInteger"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "nonNegativeInteger" {:variety :atomic :primitive "decimal" :primitive? false :base "integer"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "nonPositiveInteger" {:variety :atomic :primitive "decimal" :primitive? false :base "integer"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "normalizedString"   {:variety :atomic :primitive "string" :primitive? false :base "string"
                         :form :string}
   "positiveInteger"    {:variety :atomic :primitive "decimal" :primitive? false :base "nonNegativeInteger"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "short"              {:variety :atomic :primitive "decimal" :primitive? false :base "int"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "string"             {:variety :atomic :primitive "string" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/string}
   "time"               {:variety :atomic :primitive "time" :primitive? true :base "anySimpleType"
                         :form :org.w3.www.2001.XMLSchema/time}
   "token"              {:variety :atomic :primitive "string" :primitive? false :base "normalizedString"
                         :form :org.w3.www.2001.XMLSchema/normalizedString}
   "unsignedByte"       {:variety :atomic :primitive "decimal" :primitive? false :base "unsignedShort"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "unsignedInt"        {:variety :atomic :primitive "decimal" :primitive? false :base "unsignedLong"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "unsignedLong"       {:variety :atomic :primitive "decimal" :primitive? false :base "nonNegativeInteger"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "unsignedShort"      {:variety :atomic :primitive "decimal" :primitive? false :base "unsignedInt"
                         :form :org.w3.www.2001.XMLSchema/decimal}
   "untypedAtomic"      {:variety :atomic :primitive "untypedAtomic" :primitive? true :base nil
                         :form :org.w3.www.2001.XMLSchema/untypedAtomic}
   "yearMonthDuration"  {:variety :atomic :primitive "duration" :primitive? false :base "duration"
                         :form :org.w3.www.2001.XMLSchema/yearMonthDuration}})

(def ^:private unmodeled-builtin-forms
  "bootstrapped_schema.clj lines 159-163."
  {"anyType" :xml/hiccup})

(defn- unmodeled-builtin-form [k]
  (when (= symbols/xmlschema-ns (namespace k))
    (let [n (name k)]
      (get unmodeled-builtin-forms
           (if (str/ends-with? n seq-suffix)
             (subs n 0 (- (count n) (count seq-suffix)))
             n)))))

(defn wrap-ref-np
  "bootstrapped_schema.clj lines 173-177."
  [k]
  (or (unmodeled-builtin-form k)
      (if (symbols/builtin-kw? k) k [:ref k])))

;; ---------------------------------------------------------------------------
;; Failure
;; ---------------------------------------------------------------------------

(defn- fail! [type message data]
  (throw (ex-info message (assoc data :type type))))

(defn- unsupported! [message data]
  (fail! :xsd-to-malli/unsupported-derivation-shape message data))

;; ---------------------------------------------------------------------------
;; Type references
;; ---------------------------------------------------------------------------

(defn builtin-type [local]
  {:type :builtin :local local :kw (keyword symbols/xmlschema-ns local)})

(def ^:private any-type (builtin-type "anyType"))
(def ^:private any-simple-type (builtin-type "anySimpleType"))

(defn- simple-node? [node] (= :simpleType (:kind node)))

(defn- entry->type [kw entry]
  (if (:node entry)
    {:type :named :kw kw :node (:node entry) :doc (:doc entry)
     :simple? (simple-node? (:node entry))}
    (builtin-type (:local entry))))

(defn resolve-type
  "Type named by a QName attribute value in `doc`."
  [ctx doc qname]
  (let [{:keys [kw builtin? local]} (symbols/resolve-qname ctx doc qname)]
    (if builtin?
      (builtin-type local)
      (if-some [entry (symbols/lookup (:table ctx) :types kw)]
        (entry->type kw entry)
        (fail! :xsd-to-malli/unresolved-type
               "type reference names no declared or built-in type"
               {:uri (str (:uri doc)) :qname qname :kw kw})))))

(defn- anon-type [node doc]
  {:type :anon :node node :doc doc :simple? (simple-node? node)})

(defn- anon? [ty] (contains? #{:anon :synthetic-simple} (:type ty)))

(defn- simple-type? [ty]
  (case (:type ty)
    :builtin (not (:complex? (builtin-datatypes (:local ty))))
    :synthetic-simple true
    (boolean (:simple? ty))))

(defn- type-name [ty]
  (case (:type ty)
    :builtin (:local ty)
    :named (name (:kw ty))
    nil))

;; ---------------------------------------------------------------------------
;; Simple types
;; ---------------------------------------------------------------------------

(def ^:private facet-kinds
  #{:length :minLength :maxLength :pattern :enumeration :whiteSpace
    :maxInclusive :minInclusive :maxExclusive :minExclusive
    :totalDigits :fractionDigits :explicitTimezone})

(defn- simple-step
  "The xs:restriction / xs:list / xs:union child that defines a simple type."
  [ty]
  (case (:type ty)
    :synthetic-simple {:kind :restriction :node (:restriction ty)}
    (when-some [step (ast/child (:node ty) #{:restriction :list :union})]
      {:kind (:kind step) :node step})))

(declare compile-type)

(defn- simple-base
  "Direct base type of a simple type restriction."
  [ctx ty]
  (case (:type ty)
    :synthetic-simple (:base ty)
    (let [step (:node (simple-step ty))]
      (if-some [base (ast/attr step :base)]
        (resolve-type ctx (:doc ty) base)
        (if-some [inline (ast/child step :simpleType)]
          (anon-type inline (:doc ty))
          (fail! :xsd-to-malli/unresolved-type
                 "xs:restriction declares neither a base nor an inline simple type"
                 {:uri (str (:uri (:doc ty)))}))))))

(defn simple-variety [ctx ty]
  (if (= :builtin (:type ty))
    (:variety (builtin-datatypes (:local ty)))
    (case (:kind (simple-step ty))
      :restriction (simple-variety ctx (simple-base ctx ty))
      :list :list
      :union :union
      nil)))

(defn- simple-primitive
  "Local name of the XSD primitive a simple type descends from, or nil."
  [ctx ty]
  (if (= :builtin (:type ty))
    (:primitive (builtin-datatypes (:local ty)))
    (case (:kind (simple-step ty))
      :restriction (simple-primitive ctx (simple-base ctx ty))
      nil)))

(defn- facet-values [step kind]
  (into [] (comp (filter (comp #{kind} :kind)) (map #(ast/attr % :value)))
        (:children step)))

(defn- string-primitive-form
  "Port of malli-string-primitive, bootstrapped_schema.clj lines 56-123.

  Only the facets a simple type DECLARES take part, and the base reference is
  the direct base rather than the primitive - which is what makes a restriction
  chain compile to a reference to its base instead of a copy of it.

  Enumeration annotations feed a :value-documentation property in the oracle.
  They cannot reach it here: that capture keys off the literal `xsd:` prefix in
  the annotation markup (see the deviation note in the compiler tests), and no
  documentation property survives on either side for any fixture."
  [ctx ty]
  (let [step (:node (simple-step ty))
        base-kw (:kw (simple-base ctx ty))
        base-kw (when (not= :org.w3.www.2001.XMLSchema/string base-kw) base-kw)
        {:keys [enum pattern] :as facets}
        (reduce
         (fn [acc facet]
           (let [value (ast/attr facet :value)]
             (case (:kind facet)
               :enumeration (update acc :enum (fnil conj [:enum {}]) value)
               :length (let [l (Long/parseLong value)] (assoc acc :min l :max l))
               :maxLength (assoc acc :max (Long/parseLong value))
               :minLength (assoc acc :min (Long/parseLong value))
               :pattern (assoc acc :pattern [:re (case value
                                                   "\\i\\c*" name-regex
                                                   "[\\i-[:]][\\c-[:]]*" name-regex
                                                   value)])
               acc)))
         {}
         (filterv (comp facet-kinds :kind) (:children step)))
        base-string (not-empty (dissoc facets :enum :pattern))
        form (cond-> [:and]
               base-string (conj [:string base-string])
               enum (conj enum)
               pattern (conj pattern)
               base-kw (conj base-kw))]
    ;; unwrap-and, bootstrapped_schema.clj lines 48-52
    (case (count form)
      1 :string
      2 (nth form 1)
      form)))

(def ^:private primitive-passthrough
  "Primitives whose facets the oracle drops, bootstrapped_schema.clj 734-742."
  #{"decimal" "float" "boolean" "double" "base64Binary" "anyURI" "date" "dateTime"})

(declare type-ref)

(defn- union-members [ctx ty]
  (let [step (:node (simple-step ty))
        doc (:doc ty)]
    (into (into [] (comp (remove str/blank?) (map #(resolve-type ctx doc %)))
                (str/split (or (ast/attr step :memberTypes) "") #"\s+"))
          (map #(anon-type % doc))
          (ast/children-of step :simpleType))))

(defn compile-simple
  "-mtype for a simple type. Ports the XSRestrictionSimpleType, XSUnionSimpleType
  and XSListSimpleType implementations, bootstrapped_schema.clj lines 690-778."
  [ctx ty]
  (if (= :builtin (:type ty))
    (:form (builtin-datatypes (:local ty)))
    (let [variety (simple-variety ctx ty)
          step (simple-step ty)
          prim (simple-primitive ctx ty)
          prim-kw (when prim (keyword symbols/xmlschema-ns prim))]
      (case variety
        :atomic
        (let [key (or prim (type-name ty))]
          (cond
            (contains? primitive-passthrough key) prim-kw
            (= "string" key) (string-primitive-form ctx ty)
            :else (let [base (simple-base ctx ty)]
                    (if base (compile-simple ctx base) (or prim-kw :string)))))

        :list
        (if (= :list (:kind step))
          (let [item (if-some [q (ast/attr (:node step) :itemType)]
                       (resolve-type ctx (:doc ty) q)
                       (anon-type (ast/child (:node step) :simpleType) (:doc ty)))]
            ;; XSListSimpleType -mtype inlines the item type, lines 770-774
            [:sequential (compile-simple ctx item)])
          ;; a restriction of a list: XSOM reports the variety without an
          ;; XSListSimpleType to read, lines 755-758
          :string)

        :union
        (if (= :union (:kind step))
          (into [:or] (map #(compile-simple ctx %)) (union-members ctx ty))
          (let [base (simple-base ctx ty)]
            (if base (compile-simple ctx base) :string)))))))

;; ---------------------------------------------------------------------------
;; Complex types: structure
;; ---------------------------------------------------------------------------

(def ^:private particle-kinds #{:element :group :sequence :choice :all :any})

(defn- occurs [node]
  [(if-some [v (ast/attr node :minOccurs)] (Long/parseLong v) 1)
   (if-some [v (ast/attr node :maxOccurs)]
     (if (= "unbounded" v) -1 (Long/parseLong v))
     1)])

(declare particle)

(defn- model-group-term [ctx doc node]
  {:kind :model-group
   :compositor (:kind node)
   :node node
   :doc doc
   :children (into [] (comp (filter (comp particle-kinds :kind))
                            (map #(particle ctx doc %)))
                   (:children node))})

(defn- global-lookup [ctx doc section qname what]
  (let [{:keys [kw]} (symbols/resolve-qname ctx doc qname)]
    (or (symbols/lookup (:table ctx) section kw)
        (fail! :xsd-to-malli/unresolved-reference
               (str what " reference does not resolve")
               {:uri (str (:uri doc)) :qname qname :kw kw :section section}))))

(defn- particle [ctx doc node]
  (let [[min-occurs max-occurs] (occurs node)]
    {:node node
     :doc doc
     :min min-occurs
     :max max-occurs
     :term
     (case (:kind node)
       :element (if-some [q (ast/attr node :ref)]
                  (let [entry (global-lookup ctx doc :elements q "xs:element")]
                    {:kind :element :name (:local entry) :node (:node entry) :doc (:doc entry)})
                  {:kind :element :name (ast/attr node :name) :node node :doc doc})
       :group (let [entry (global-lookup ctx doc :groups (ast/attr node :ref) "xs:group")]
                {:kind :group-decl :kw (:kw entry) :entry entry})
       (:sequence :choice :all) (model-group-term ctx doc node)
       :any {:kind :wildcard :node node
             :any? (contains? #{nil "##any"} (ast/attr node :namespace))})}))

(defn- group-decl-term
  "Model group a global xs:group declaration wraps."
  [ctx entry]
  (let [node (:node entry)
        mg (ast/child node #{:sequence :choice :all})]
    (when mg (model-group-term ctx (:doc entry) mg))))

(defn- synthetic-sequence
  "The content model xs:extension produces: the base's particle followed by the
  extension's own, in one sequence, exactly as XSOM presents it."
  [base-particle own-particle]
  {:node nil :doc nil :min 1 :max 1
   :term {:kind :model-group :compositor :sequence :node nil :doc nil
          :children [base-particle own-particle]}})

(defn- derivation-step
  "The xs:extension / xs:restriction under xs:simpleContent or xs:complexContent."
  [ty]
  (when (= :complexType (:kind (:node ty)))
    (when-some [content (ast/child (:node ty) #{:simpleContent :complexContent})]
      (when-some [step (ast/child content #{:extension :restriction})]
        {:method (:kind step) :step step :wrapper content
         :content (if (= :simpleContent (:kind content)) :simple :complex)}))))

(defn complex-parts
  "Where a complex type's own attributes and particle live, and what it derives
  from. A base of xs:anyType is not derivation - anyType constrains nothing -
  so the type is treated as declaring its own content outright."
  [ctx ty]
  (let [node (:node ty)
        step (derivation-step ty)
        holder (if step (:step step) node)
        base (when step
               (when-some [q (ast/attr (:step step) :base)]
                 (resolve-type ctx (:doc ty) q)))
        derived? (and base (not= "anyType" (when (= :builtin (:type base)) (:local base))))]
    {:derivation (when derived? (assoc step :base base))
     :holder holder
     :own-particle (when-some [p (first (filter (comp #{:sequence :choice :all :group} :kind)
                                                (:children holder)))]
                     (particle ctx (:doc ty) p))
     :own-mixed? (or (= "true" (ast/attr node :mixed))
                     (and step (= "true" (ast/attr (:wrapper step) :mixed))))}))

(defn complex-mixed? [ctx ty]
  (let [{:keys [derivation own-mixed?]} (complex-parts ctx ty)]
    (boolean (or own-mixed?
                 (when (and derivation (= :complex (:content derivation)))
                   (complex-mixed? ctx (:base derivation)))))))

(defn- simple-content-type
  "Simple type an xs:simpleContent derivation ends up with."
  [ctx ty]
  (let [{:keys [derivation]} (complex-parts ctx ty)
        base (:base derivation)]
    (case (:method derivation)
      :extension (if (simple-type? base)
                   base
                   (simple-content-type ctx base))
      :restriction (if-some [inline (ast/child (:step derivation) :simpleType)]
                     (anon-type inline (:doc ty))
                     {:type :synthetic-simple
                      :restriction (:step derivation)
                      :doc (:doc ty)
                      :base (if (simple-type? base) base (simple-content-type ctx base))}))))

(defn content-model
  "Effective content model of a complex type: :simple, :empty or :particle."
  [ctx ty]
  (if (= :builtin (:type ty))
    {:kind :particle :particle nil}                         ; anyType, never compiled
    (let [{:keys [derivation own-particle]} (complex-parts ctx ty)]
      (cond
        (nil? derivation)
        (if own-particle {:kind :particle :particle own-particle} {:kind :empty})

        (= :simple (:content derivation))
        {:kind :simple :type (simple-content-type ctx ty)}

        (= :extension (:method derivation))
        (let [base-model (content-model ctx (:base derivation))]
          (cond
            (nil? own-particle) base-model
            (= :particle (:kind base-model))
            {:kind :particle :particle (synthetic-sequence (:particle base-model) own-particle)}
            :else {:kind :particle :particle own-particle}))

        :else
        (if own-particle {:kind :particle :particle own-particle} {:kind :empty})))))

(defn complex-has-particle?
  "Whether a complex type's effective content is a particle - the test -seq-ref
  makes before choosing the -seq dual, bootstrapped_schema.clj lines 682-688."
  [ctx ty]
  (if (= :builtin (:type ty))
    true
    (= :particle (:kind (content-model ctx ty)))))

;; ---------------------------------------------------------------------------
;; Attribute uses
;; ---------------------------------------------------------------------------

(defn- qualified-attribute? [doc node]
  (= "qualified" (or (ast/attr node :form) (:attribute-form-default doc))))

(defn- attribute-type [ctx doc node]
  (cond
    (ast/attr node :type) (resolve-type ctx doc (ast/attr node :type))
    (ast/child node :simpleType) (anon-type (ast/child node :simpleType) doc)
    :else any-simple-type))

(defn- attribute-use [ctx doc node]
  (if-some [q (ast/attr node :ref)]
    (let [entry (global-lookup ctx doc :attributes q "xs:attribute")
          decl (:node entry)
          decl-doc (:doc entry)
          ns-part (symbols/document-namespace decl-doc)]
      {:key (if (seq ns-part)
              (keyword (symbols/uri->ns ns-part) (:local entry))
              (keyword (:local entry)))
       :required? (= "required" (ast/attr node :use))
       :prohibited? (= "prohibited" (ast/attr node :use))
       :type (attribute-type ctx decl-doc decl)
       :type-doc decl-doc})
    (let [ns-part (when (qualified-attribute? doc node) (symbols/document-namespace doc))]
      {:key (if (seq ns-part)
              (keyword (symbols/uri->ns ns-part) (ast/attr node :name))
              (keyword (ast/attr node :name)))
       :required? (= "required" (ast/attr node :use))
       :prohibited? (= "prohibited" (ast/attr node :use))
       :type (attribute-type ctx doc node)
       :type-doc doc})))

(defn- own-attribute-uses
  "Attribute uses `node` declares, with attributeGroup references expanded in
  document order. XSOM resolves the groups itself; the oracle only ever sees
  the flattened list."
  ([ctx doc node] (own-attribute-uses ctx doc node #{}))
  ([ctx doc node seen]
   (into []
         (mapcat
          (fn [child]
            (case (:kind child)
              :attribute [(attribute-use ctx doc child)]
              :attributeGroup
              (if-some [q (ast/attr child :ref)]
                (let [entry (global-lookup ctx doc :attribute-groups q "xs:attributeGroup")]
                  (when (contains? seen (:kw entry))
                    (fail! :xsd-to-malli/attribute-group-cycle
                           "xs:attributeGroup references form a cycle"
                           {:kw (:kw entry) :uri (str (:uri doc))}))
                  (own-attribute-uses ctx (:doc entry) (:node entry) (conj seen (:kw entry))))
                [])
              nil)))
         (:children node))))

(defn- merge-uses
  "Inherited uses overridden in place by redeclared ones, then prohibited uses
  dropped - the effective attribute uses XSOM reports."
  [inherited own]
  (let [own-by-key (into {} (map (juxt :key identity)) own)
        replaced (mapv (fn [u] (get own-by-key (:key u) u)) inherited)
        inherited-keys (into #{} (map :key) inherited)
        added (into [] (remove (comp inherited-keys :key)) own)]
    (into [] (remove :prohibited?) (into replaced added))))

(defn effective-attribute-uses [ctx ty]
  (let [{:keys [derivation holder]} (complex-parts ctx ty)
        inherited (if (and derivation (not (simple-type? (:base derivation))))
                    (effective-attribute-uses ctx (:base derivation))
                    [])]
    (merge-uses inherited (own-attribute-uses ctx (:doc ty) holder))))

(defn- attr-row [ctx use]
  [(:key use)
   (cond-> {:xml/attr true}
     (not (:required? use)) (assoc :optional true))
   (let [ty (:type use)]
     (if (anon? ty) (compile-simple ctx ty) (type-ref ctx ty)))])

(defn- attr-rows [ctx uses]
  (mapv #(attr-row ctx %) uses))

;; ---------------------------------------------------------------------------
;; Particles and model groups
;; ---------------------------------------------------------------------------

(declare handle-model-group every-sequence? compile-complex)

(defn type-ref
  "-seq-ref: the reference form for a named type, bootstrapped_schema.clj lines
  682-688 (complex), 707-708 / 767-768 / 777-778 (simple)."
  [ctx ty]
  (if (simple-type? ty)
    [:ref (:kw ty)]
    (wrap-ref-np (if (and (:sequence ctx) (complex-has-particle? ctx ty))
                   (seq-kw (:kw ty))
                   (:kw ty)))))

(defn- element-type [ctx term]
  (let [node (:node term)
        doc (:doc term)]
    (cond
      (ast/attr node :type) (resolve-type ctx doc (ast/attr node :type))
      (ast/child node :complexType) (anon-type (ast/child node :complexType) doc)
      (ast/child node :simpleType) (anon-type (ast/child node :simpleType) doc)
      :else any-type)))

(defn- type-or-inline [ctx ty]
  (if (anon? ty) (compile-type ctx ty) (type-ref ctx ty)))

(defn handle-element-decl
  "bootstrapped_schema.clj lines 346-353."
  [ctx term]
  (let [ty (element-type ctx term)]
    [:tuple {} [:enum (keyword (:name term))]
     (type-or-inline (dissoc ctx :sequence) ty)]))

(defn- value-sequence? [p]
  (or (> (:max p) 1) (= (:max p) -1)))

(defn- particle-sequence?
  "bootstrapped_schema.clj lines 356-378, mutable first-wins tag set included:
  a tag seen twice makes the whole group ineligible for map mode."
  [ctx map-keys p]
  (let [term (:term p)]
    (or (when (= :element (:kind term))
          (let [kw (keyword (:name term))]
            (if (contains? @map-keys kw)
              false
              (do (vswap! map-keys conj kw) true))))
        (when (= :model-group (:kind term))
          (if (value-sequence? p)
            false
            (when (= :sequence (:compositor term))
              (every-sequence? ctx map-keys term))))
        (when (= :group-decl (:kind term))
          (if (value-sequence? p)
            false
            (when-some [mg (group-decl-term ctx (:entry term))]
              (when (= :sequence (:compositor mg))
                (every-sequence? ctx map-keys mg))))))))

(defn every-sequence? [ctx map-keys mg]
  (every? #(particle-sequence? ctx map-keys %) (:children mg)))

(defn handle-fields
  "bootstrapped_schema.clj lines 238-297."
  [ctx p]
  (let [term (:term p)
        min-occurs (:min p)
        max-occurs (:max p)]
    (when (not= max-occurs 0)
      (case (:kind term)
        :element
        (let [ty (element-type ctx term)
              ty-ref (type-or-inline ctx ty)]
          [(keyword (:name term))
           (cond-> {}
             (= 0 min-occurs) (assoc :optional true)
             (:optional-group ctx) (assoc :optional true :required-in-group true))
           (if (value-sequence? p)
             [:sequential (cond-> (if (= 0 min-occurs) {:min 1} {:min min-occurs})
                            (not= max-occurs -1) (assoc :max max-occurs))
              ty-ref]
             ty-ref)])

        :model-group
        (do (assert (= :sequence (:compositor term)))
            (assert (not (value-sequence? p)))
            (rt/combine-fields (cond-> {:closed true}
                                 (= 0 min-occurs) (assoc :optional-group true))
                               (keep #(handle-fields (assoc ctx :optional-group (= 0 min-occurs)) %)
                                     (:children term))))

        :group-decl
        (when-some [mg (group-decl-term ctx (:entry term))]
          (assert (= :sequence (:compositor mg)))
          (assert (not (value-sequence? p)))
          (rt/combine-fields (cond-> {:closed true}
                               (= 0 min-occurs) (assoc :optional-group true))
                             (keep #(handle-fields (assoc ctx :optional-group (= 0 min-occurs)) %)
                                   (:children mg))))

        nil))))

(defn- wrap-regex
  "bootstrapped_schema.clj lines 324-344."
  [ctx p msch]
  (let [repeated? (value-sequence? p)]
    (cond
      (= 0 (:max p)) nil
      (:sequence ctx) (rt/regex-occurrence (:min p) (:max p) msch)
      (and repeated? (:content-particle ctx)) (rt/regex-occurrence (:min p) (:max p) msch)
      repeated? [:sequential msch]
      :else msch)))

(defn- handle-model-group-decl
  "bootstrapped_schema.clj lines 513-517. A global xs:group declaration is never
  anonymous, so a reference to one always emits a registry reference."
  [ctx term]
  (wrap-ref-np (if (:sequence ctx) (seq-kw (:kw term)) (:kw term))))

(defn- group-particle
  "bootstrapped_schema.clj lines 385-396."
  [ctx p]
  (let [term (:term p)
        out (case (:kind term)
              :element (handle-element-decl ctx term)
              :model-group (handle-model-group ctx term)
              :group-decl (handle-model-group-decl ctx term)
              :wildcard [:xml/hiccup])]
    (wrap-regex ctx p out)))

(defn- only-any-wildcard? [fields]
  (and (= 1 (count fields))
       (let [term (:term (first fields))]
         (and (= :wildcard (:kind term)) (:any? term)))))

(defn handle-model-group
  "bootstrapped_schema.clj lines 433-511.

  Two of the oracle's wildcard shortcuts are unreachable: the choice and all
  branches test the PARTICLE against XSWildcard$Any instead of its term, so
  they never fire. Reproducing them would change nothing, so only the sequence
  shortcut is implemented, and the all compositor keeps its degenerate
  fall-through."
  [ctx mg]
  (let [fields (:children mg)]
    (case (:compositor mg)
      :sequence
      (cond
        (only-any-wildcard? fields) :xml/hiccup

        (every-sequence? ctx (volatile! #{}) mg)
        (if (:sequence ctx)
          (rt/combine-fields {:xml/in-seq-ex true :closed true}
                             (keep #(handle-fields (dissoc ctx :sequence) %) fields))
          (rt/combine-fields {:closed true} (keep #(handle-fields ctx %) fields)))

        :else
        (let [acc (reduce (fn [acc p]
                            (if-some [n (group-particle (assoc ctx :sequence true :compositor :sequence) p)]
                              (conj acc n)
                              acc))
                          [:cat {}]
                          fields)]
          ;; all-maps? never holds, see runtime/all-maps?
          (if (rt/all-maps? acc)
            (if (= (count acc) 3) (nth acc 2) (assoc acc 0 :merge))
            acc)))

      :choice
      (into [(if (:sequence ctx) :alt :or)]
            (keep #(group-particle (assoc ctx :compositor :choice) %))
            fields)

      :all
      (if (every-sequence? ctx (volatile! #{}) mg)
        (rt/combine-fields {:closed true} (keep #(handle-fields ctx %) fields))
        ["all" (map #(group-particle ctx %) fields)]))))

(defn handle-toplevel-particle
  "bootstrapped_schema.clj lines 398-417."
  [ctx p]
  (when p
    (let [term (:term p)
          body (case (:kind term)
                 :model-group (handle-model-group ctx term)
                 :group-decl (handle-model-group-decl ctx term)
                 :wildcard [:xml/hiccup]
                 nil)]
      (when body
        (wrap-regex (assoc ctx :content-particle true) p body)))))

;; ---------------------------------------------------------------------------
;; Complex types: flattened form
;; ---------------------------------------------------------------------------

(declare emit-derived derived-complex?)

(defn- simple-base-extension?
  "xs:simpleContent xs:extension of a SIMPLE type: the derivation adds
  attributes to a value type and inherits nothing else, so it needs no code -
  naming the base's registry entry is the whole derivation, the same way a
  simple type restriction chain names its base."
  [ctx ty]
  (let [{:keys [derivation]} (complex-parts ctx ty)]
    (boolean (and derivation
                  (= :simple (:content derivation))
                  (= :extension (:method derivation))
                  (simple-type? (:base derivation))))))

(defn flat-ctx
  "Context that always produces flattened forms. Shape decisions and the
  oracle comparison run on those; only emission substitutes derivation code."
  [ctx]
  (dissoc ctx :emit?))

(defn compile-complex
  "-mtype for a complex type.

  With :emit? set, a derived type becomes a Derived node instead of its
  flattened form - that substitution is the whole point of the project. Without
  it the result is flattened exactly like the oracle's, which is what shape
  decisions and the differential comparison run on."
  [ctx ty]
  (if (and (:emit? ctx) (derived-complex? ctx ty))
    (if (simple-base-extension? ctx ty)
      (rt/assemble-complex
       {:attrs (attr-rows ctx (effective-attribute-uses ctx ty))
        :mixed? (complex-mixed? ctx ty)
        :simple (:kw (:base (:derivation (complex-parts ctx ty))))})
      (emit-derived ctx ty (boolean (:sequence ctx))))
    (let [model (content-model ctx ty)
          attrs (attr-rows ctx (effective-attribute-uses ctx ty))]
      (rt/assemble-complex
       {:attrs attrs
        :mixed? (complex-mixed? ctx ty)
        :simple (when (= :simple (:kind model)) (compile-simple ctx (:type model)))
        :content (when (= :particle (:kind model))
                   (handle-toplevel-particle ctx (:particle model)))
        :empty? (= :empty (:kind model))}))))

(defn compile-type [ctx ty]
  (if (simple-type? ty)
    (compile-simple ctx ty)
    (compile-complex ctx ty)))

;; ---------------------------------------------------------------------------
;; Derived types: emission plans
;; ---------------------------------------------------------------------------

(defrecord Derived [plan deps requires])

(defn derived? [x] (instance? Derived x))

(defn- type-ns-sym
  "Namespace symbol holding a registry type, matching the emitter's rule."
  [k]
  (symbol (str (namespace k) "." (str/replace (name k) "." "_"))))

(defn- sch-sym [k seq?]
  (symbol (str (type-ns-sym k)) (if seq? "sch-seq" "sch")))

(defn- collect-kws [x acc]
  (cond
    (keyword? x) (if (namespace x) (conj acc x) acc)
    (derived? x) (into acc (:deps x))
    (map? x) (reduce-kv (fn [a k v] (collect-kws v (collect-kws k a))) acc x)
    (coll? x) (reduce (fn [a v] (collect-kws v a)) acc x)
    :else acc))

(defn form-deps
  "Namespaced keywords a compiled value mentions, including those a nested
  Derived node reaches through its plan."
  [x]
  (collect-kws x (sorted-set)))

(defn- form-requires [x]
  (cond
    (derived? x) (into (sorted-set) (:requires x))
    (map? x) (reduce-kv (fn [a k v] (into (into a (form-requires k)) (form-requires v)))
                        (sorted-set) x)
    (coll? x) (reduce (fn [a v] (into a (form-requires v))) (sorted-set) x)
    :else (sorted-set)))

(defn- base-content-source
  "Where a splice takes the base type's content from.

  For map-mode the splice re-expands the base particle in place, which is what
  the base's own non-seq content already is - unless the base's particle is an
  xs:group reference, where the expansion is the group's own registry entry
  rather than the reference to it. For seqex mode the base's -seq content is
  the slot verbatim, occurrence wrapper included."
  [ctx base base-particle mode]
  (let [ctx (flat-ctx ctx)]
    (case mode
      :splice-map
      (let [term (:term base-particle)]
        (when-not (and (= 1 (:min base-particle)) (= 1 (:max base-particle)))
          (unsupported! "map-mode extension of a base whose content particle repeats or is optional"
                        {:type-kw (:kw base) :occurs [(:min base-particle) (:max base-particle)]}))
        (case (:kind term)
          :group-decl {:sym (sch-sym (:kw term) false)
                       :form (handle-model-group (dissoc ctx :sequence)
                                                 (group-decl-term ctx (:entry term)))
                       :dep (:kw term)}
          :model-group {:sym (sch-sym (:kw base) false)
                        :form (compile-complex (dissoc ctx :sequence) base)
                        :dep (:kw base)}
          (unsupported! "map-mode extension of a base whose content particle is not a model group"
                        {:type-kw (:kw base) :term (:kind term)})))

      :splice-cat
      {:sym (sch-sym (:kw base) true)
       :form (compile-complex (assoc ctx :sequence true) base)
       :dep (seq-kw (:kw base))})))

(defn- restricts-value?
  "Whether an xs:simpleContent xs:restriction narrows the value type rather
  than only redeclaring attributes."
  [derivation]
  (boolean (or (ast/child (:step derivation) :simpleType)
               (seq (filterv (comp facet-kinds :kind) (:children (:step derivation)))))))

(defn- row-keys
  "Element tags a map-mode content form declares."
  [form]
  (case (rt/form-tag form)
    :map (into #{} (map first) (rt/form-children form))
    :merge (into #{} (mapcat row-keys) (rt/form-children form))
    #{}))

(defn- derivation-plan
  "Emission plan for a derived complex type.

  `seq?` selects the -seq dual. The plan names the base namespace's exported
  schema values; everything else is literal data compiled here."
  [ctx ty seq?]
  (let [ctx (if seq? (assoc ctx :sequence true) (dissoc ctx :sequence))
        {:keys [derivation own-particle]} (complex-parts ctx ty)
        base (:base derivation)
        base-form (compile-complex (flat-ctx (dissoc ctx :sequence)) base)
        base-shape (rt/shape-of base-form)
        own-uses (own-attribute-uses ctx (:doc ty) (:step derivation))
        attrs (attr-rows ctx (remove :prohibited? own-uses))
        drop-attrs (into (sorted-set) (comp (filter :prohibited?) (map :key)) own-uses)
        mixed? (complex-mixed? ctx ty)
        common {:base (sch-sym (:kw base) false)
                :base-shape base-shape
                :attrs attrs
                :drop-attrs drop-attrs
                :mixed? mixed?}
        base-model (content-model ctx base)]
    (cond
      (= :simple (:content derivation))
      (assoc common
             :mode :none
             ;; The value type is inherited unless the restriction narrows it;
             ;; a restriction that only redeclares attributes keeps the base's.
             :simple (if (or (= :extension (:method derivation))
                             (not (restricts-value? derivation)))
                       :from-base
                       (compile-simple ctx (simple-content-type ctx ty))))

      (= :restriction (:method derivation))
      (assoc common
             :mode (if own-particle :own :none)
             :own-content (handle-toplevel-particle ctx own-particle)
             :empty? (nil? own-particle))

      ;; extension
      (nil? own-particle)
      (assoc common
             :mode (if (= :empty (:kind base-model)) :none :base)
             :content-source (sch-sym (:kw base) seq?)
             :content-shape (rt/shape-of (compile-complex (flat-ctx ctx) base))
             :content-dep (if seq? (seq-kw (:kw base)) (:kw base))
             :empty? (= :empty (:kind base-model)))

      (= :empty (:kind base-model))
      (assoc common
             :mode :own
             :own-content (handle-toplevel-particle ctx own-particle))

      :else
      (let [base-particle (:particle base-model)
            splice (synthetic-sequence base-particle own-particle)
            map-mode? (every-sequence? ctx (volatile! #{}) (:term splice))
            mode (if map-mode? :splice-map :splice-cat)
            source (base-content-source ctx base base-particle mode)
            own-content (if map-mode?
                          (handle-fields (assoc ctx :optional-group (= 0 (:min own-particle)))
                                         own-particle)
                          (group-particle (assoc ctx :sequence true :compositor :sequence)
                                          own-particle))]
        (when (and map-mode? (or (= 0 (:max own-particle)) (value-sequence? own-particle)))
          (unsupported! "map-mode extension whose own content particle repeats"
                        {:type-kw (:kw ty)}))
        (when map-mode?
          (let [clash (filter (row-keys (rt/content-of (:form source)
                                                       (rt/shape-of (:form source))))
                              (row-keys own-content))]
            (when (seq clash)
              ;; The oracle compiles a duplicated tag as a seqex of two maps,
              ;; not as one map with the base row replaced, so a collision means
              ;; this plan is the wrong one - not merely a lossy one.
              (unsupported! "extension repeats an element tag the base already declares"
                            {:type-kw (:kw ty) :tags (vec clash)}))))
        (assoc common
               :mode mode
               :content-source (:sym source)
               :content-shape (rt/shape-of (:form source))
               :content-dep (:dep source)
               :own-content own-content
               :splice-props (cond-> {:closed true}
                               (:sequence ctx) (assoc :xml/in-seq-ex true)))))))

(defn- emit-derived [ctx ty seq?]
  (let [plan (derivation-plan ctx ty seq?)
        base-kw (:kw (:base (:derivation (complex-parts ctx ty))))
        deps (into (form-deps [(:attrs plan) (:own-content plan) (:simple plan)])
                   (remove nil?)
                   [base-kw (:content-dep plan)])
        ;; the namespaces to require are the ones the plan's symbols name; a
        ;; -seq dual lives in its base type's file, so the keyword is not the
        ;; namespace
        requires (into (sorted-set)
                       (comp (filter symbol?) (map (comp symbol namespace)))
                       (vals plan))
        nested (form-requires [(:attrs plan) (:own-content plan)])]
    (->Derived (dissoc plan :content-dep)
               deps
               (into requires nested))))

;; ---------------------------------------------------------------------------
;; Registry
;; ---------------------------------------------------------------------------

(defn- derived-complex? [ctx ty]
  (and (not (simple-type? ty))
       (some? (:derivation (complex-parts ctx ty)))))

(defn- compile-registry-entry
  "Flattened form plus the value to emit, for one registry key."
  [ctx ty seq?]
  (let [ctx (if seq? (assoc ctx :sequence true) (dissoc ctx :sequence))]
    {:form (compile-type (flat-ctx ctx) ty)
     :emit (compile-type (assoc ctx :emit? true) ty)}))

(defn- check-seq-collisions!
  "An XSD declaring both Foo and Foo-seq collides in the registry: the dual of
  Foo overwrites the declared Foo-seq. Fail before emitting either."
  [names]
  (let [colliding (into (sorted-set)
                        (comp (filter #(str/ends-with? % seq-suffix))
                              (filter #(contains? names (subs % 0 (- (count %) (count seq-suffix))))))
                        names)]
    (when (seq colliding)
      (fail! :xsd-to-malli/seq-name-collision
             "XSD declares both Foo and Foo-seq; the -seq registry dual collides"
             {:names colliding}))))

(defn compile-schemas
  "Compile a loaded schema set into {:registry {kw {:form :emit}}, :top-type}.

  `loaded` is what loader/load-schemas returns."
  [{:keys [default-ns]} loaded]
  (let [table (:symbols loaded)
        ctx {:default-ns default-ns :table table}
        type-keys (symbols/own-keys table :types)
        group-keys (symbols/own-keys table :groups)
        element-keys (symbols/own-keys table :elements)]
    (check-seq-collisions!
     (into #{} (map name) (concat type-keys group-keys)))
    (let [registry
          (as-> {} registry
            (reduce (fn [acc kw]
                      (let [ty (entry->type kw (symbols/lookup table :types kw))]
                        (if (simple-type? ty)
                          (assoc acc kw (compile-registry-entry ctx ty false))
                          (-> acc
                              (assoc kw (compile-registry-entry ctx ty false))
                              (assoc (seq-kw kw) (compile-registry-entry ctx ty true))))))
                    registry
                    type-keys)
            (reduce (fn [acc kw]
                      (let [entry (symbols/lookup table :groups kw)
                            mg (group-decl-term ctx entry)
                            group-entry (fn [c] (handle-model-group c (group-decl-term c entry)))]
                        (-> acc
                            (assoc kw {:form (group-entry ctx)
                                       :emit (group-entry (assoc ctx :emit? true))})
                            (assoc (seq-kw kw)
                                   (let [c (assoc ctx :sequence true)]
                                     {:form (group-entry c)
                                      :emit (group-entry (assoc c :emit? true))})))))
                    registry
                    group-keys))
          arms (fn [c]
                 (into []
                       (map (fn [kw]
                              (let [entry (symbols/lookup table :elements kw)
                                    term {:kind :element :name (:local entry)
                                          :node (:node entry) :doc (:doc entry)}]
                                [(keyword (:local entry)) (handle-element-decl c term)])))
                       element-keys))]
      {:registry registry
       :top-type (into [:multi {:dispatch 'first}] (arms (assoc ctx :emit? true)))
       :flat-top-type (into [:multi {:dispatch 'first}] (arms ctx))})))

(defn flat-registry
  "The compiled registry as plain forms, in the shape xsd->registry returns."
  [compiled]
  (into (into {} xml-primitives/xmlschema-registry)
        (map (fn [[k v]] [k (:form v)]))
        (:registry compiled)))
