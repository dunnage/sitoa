(ns dunnage.sitoa.parser
  (:require [clojure.java.io :as io]

            [malli.core :as m]
           )
  (:import
    (java.io InputStream Reader StringReader)
    (javax.xml.stream
      XMLInputFactory XMLStreamReader XMLStreamConstants)
    (clojure.lang IReduceInit MapEntry ITransientCollection)
    (java.time LocalDate LocalDateTime ZonedDateTime LocalTime)
    (java.time.format DateTimeParseException)
    (java.nio.file Files Path)))

(def ^:dynamic *ref-parsers* false)
(def ^:dynamic *ref-parsers-in-seq* false)

(defn take-while-plus-1
  "Returns a lazy sequence of successive items from coll while
  (pred item) returns logical true. pred must be free of side-effects.
  Returns a transducer when no collection is provided."
  {:added "1.0"
   :static true}
  ([pred]
   (fn [rf]
     (fn
       ([] (rf))
       ([result] (rf result))
       ([result input]
        (if (pred input)
          (rf result input)
          (ensure-reduced (rf result input))))))))

(def ^{:private true} input-factory-props
  {:allocator XMLInputFactory/ALLOCATOR
   :coalescing XMLInputFactory/IS_COALESCING
   :namespace-aware XMLInputFactory/IS_NAMESPACE_AWARE
   :replacing-entity-references XMLInputFactory/IS_REPLACING_ENTITY_REFERENCES
   :supporting-external-entities XMLInputFactory/IS_SUPPORTING_EXTERNAL_ENTITIES
   :validating XMLInputFactory/IS_VALIDATING
   :reporter XMLInputFactory/REPORTER
   :resolver XMLInputFactory/RESOLVER
   :support-dtd XMLInputFactory/SUPPORT_DTD})

(defn- make-input-factory ^XMLInputFactory [props]
  (let [fac (XMLInputFactory/newInstance)]
    (doseq [[k v] props
            :when (contains? input-factory-props k)
            :let [prop (input-factory-props k)]]
      (.setProperty fac prop v))
    fac))

(defn safe-next-tag [^XMLStreamReader r]
  (when (.hasNext r)
    (loop [tok (.next r)]
      ;(prn tok)
      (case tok
        (1 2 8)                                                 ;START_ELEMENT
        tok
        (3 4 5 6 7 11)                                       ;COMMENT
        (when (.hasNext r)
          (recur (.next r)))
        ; (8) (assert false)                                  ;START_DOCUMENT
        )))
  #_(when-not (= (.getEventType r) 8)
    (.nextTag r)))
(defn ensure-safe-next-tag [^XMLStreamReader r]
  (case (.getEventType r)
    (1 8)                                                 ;START_ELEMENT
    (.getEventType r)
    (2 3 4 5 6 7 11)                                       ;COMMENT
    (safe-next-tag r)
    ; (8) (assert false)                                  ;START_DOCUMENT
    ))

(defn make-stream-reader [props source]
  (let [fac (make-input-factory props)]
    (cond
      (instance? Reader source) (.createXMLStreamReader fac ^Reader source)
      (instance? InputStream source) (.createXMLStreamReader fac ^InputStream source)
      :else (throw (IllegalArgumentException.
                     "source should be java.io.Reader or java.io.InputStream")))))

(defn source [s]
  (io/reader s))

#_(case tok
  1                                             :START_ELEMENT
  2                                             :END_ELEMENT
  3                                             :PROCESSING_INSTRUCTION
  4                                             :CHARACTERS
  5                                             :COMMENT
  6                                             :SPACE
  7                                             :START_DOCUMENT
  8                                             :END_DOCUMENT
  9                                             :ENTITY_REFERENCE
  10                                            :ATTRIBUTE
  11                                            :DTD
  12                                            :CDATA
  13                                            :NAMESPACE
  14                                            :NOTATION_DECLARATION
  15                                            :ENTITY_DECLARATION
  )

(declare -xml-parser make-tag-discriminator -sequential-parser)

(defn debug-element [^XMLStreamReader r]
  (case (.getEventType r)
    1 {:type :START_ELEMENT :name (.getLocalName r)}
    2 {:type :END_ELEMENT :name (.getLocalName r)}
    3 {:type :PROCESSING_INSTRUCTION}
    4 {:type :CHARACTERS :text (.getText r)}
    5 {:type :COMMENT}
    6 {:type :SPACE}
    7 {:type :START_DOCUMENT}
    8 {:type :END_DOCUMENT}
    9 {:type :ENTITY_REFERENCE}
    10 {:type :ATTRIBUTE}
    11 {:type :DTD}
    12 {:type :CDATA}
    13 {:type :NAMESPACE}
    14 {:type :NOTATION_DECLARATION}
    15 {:type :ENTITY_DECLARATION}
    ))
(defn skip-closing-and-charactors [^XMLStreamReader r]
  (loop [tok (.getEventType r)]
    ;(prn tok)
    (case tok
      1                                        ;START_ELEMENT
      nil
      (2 3 4 5 6)                                ;COMMENT
      (recur (.next r))
      (7 8) (assert false)                     ;START_DOCUMENT
      ))
  )

(defn skip-characters [^XMLStreamReader r]
  (loop [tok (.getEventType r)]
    ;(prn tok)
    (case tok
      1                                        ;START_ELEMENT
      nil
      (2 3 4 5 6)                                ;COMMENT
      (recur (.next r))
      (7 8) (assert false)                     ;START_DOCUMENT
      ))
  )
(defn string-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      (safe-next-tag r)
      ;(prn :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      txt)))

(defn local-date-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      (safe-next-tag r)
      ;(prn :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (LocalDate/parse txt))))

(defn local-time-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      (safe-next-tag r)
      ;(prn :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (LocalTime/parse txt))))

(defn local-date-time-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      (safe-next-tag r)
      ;(prn :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (try
        (LocalDateTime/parse txt)
        (catch DateTimeParseException e
          (ZonedDateTime/parse txt))))))

(defn zoned-date-time-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      (safe-next-tag r)
      ;(prn :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (ZonedDateTime/parse txt))))

(defn decimal-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      (safe-next-tag r)
      ;(prn :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (BigDecimal. txt))))

(defn boolean-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
       (Boolean/parseBoolean txt))))

(defn attribute-reducible
  ""
  [^XMLStreamReader r]
  (let [cnt (.getAttributeCount r)]
    (reify IReduceInit
      (reduce [this f init]
        (loop [i 0 acc init]
          (if (or (reduced? acc)
                  (not (< i cnt)))
            (unreduced acc)
            (let [attr (.getAttributeLocalName r i)
                  attr-ns (.getAttributeNamespace r i)]
              (recur (inc i) (f acc (MapEntry. (if (and attr-ns (not (.isEmpty attr-ns)))
                                         (keyword attr-ns attr)
                                         (keyword attr))
                                       (.getAttributeValue r i)))))))))))

(defn ap [x]
  x)

(defn get-tag-kw [^XMLStreamReader r]
  (let [tag (.getLocalName r)]
    (keyword tag)))

(defn get-ns-tag-kw [^XMLStreamReader r]
  (let [tag (.getLocalName r)
        tagns (.getNamespaceURI r)]
    (if (and tagns (not (.isEmpty tagns)))
      (keyword tagns tag)
      (keyword tag))))

(defn sequence-map-parser [^XMLStreamReader r tag-parsers tags m]
  (transduce
    (map identity)
    (fn
      ([acc] (if (instance? ITransientCollection acc)
               (persistent! acc)
               acc))
      ([acc [nexttagk tag-parser tag-descrim]]
       (loop [tok (.getEventType r) val acc]
         (prn :map (debug-element r) tags)
         (case tok
           1                                                ;START_ELEMENT
           (let [tagk (get-tag-kw r)]
             (if (= nexttagk tagk)
               (let [val (assoc! val tagk (tag-parser r))]
                 (recur (.getEventType r) val))
               (do (prn :leave tagk)
                   val)))

           2                                                ;END_ELEMENT
           (let [tagk (get-tag-kw r)]
             (if (tags tagk)
               (recur (.next r) val)
               (do (prn :leave tagk)
                   val)))
           (3 4 5 6)                                        ;COMMENT
           (recur (.next r) val)
           (7 8) (assert false)                             ;START_DOCUMENT
           ))))
          m
          tag-parsers)
  )

(defn wrap-next-before-tag [parser]
  (fn [^XMLStreamReader r]
    (safe-next-tag r)
    (parser r)))
(defn wrap-next-after-tag [parser]
  (fn [^XMLStreamReader r]
    (let [v (parser r)]
      (safe-next-tag r)
      v)))

(defn -map-parser [x]
  (let [children (-> x m/children)
        {:keys [xml/value-wrapped xml/in-seq-ex]} (m/properties x)
        attribute-parsers (transduce
                            (filter (fn [[_ opts]] (-> opts :xml/attr)))
                            (fn
                              ([acc]acc)
                              ([acc [attribute-name opts subschema]]
                               (assoc acc attribute-name ap)))
                            {}
                            children)
        tag-parsers (transduce
                      (remove (fn [[_ opts]] (-> opts :xml/attr)))
                      (fn ([acc]acc)
                        ([acc [tag opts subschema]]
                         ;(prn (m/form (m/deref subschema)))
                         (conj acc [tag
                                    (case (m/form (m/deref subschema))
                                      :org.w3.www.2001.XMLSchema/dateTime
                                      (-xml-parser subschema)
                                      (case (-> subschema m/deref-all m/type)
                                        :sequential (-sequential-parser tag subschema)
                                        (:alt :cat :or) (wrap-next-before-tag (-xml-parser subschema))
                                        (-xml-parser subschema)))
                                    (case (-> subschema m/deref-all m/type)
                                      (:alt :cat :or :sequential) (make-tag-discriminator subschema)
                                      nil)])
                         ))
                      [] children)
        tags        (into #{} (map first tag-parsers))
        ]
    (fn [^XMLStreamReader r]
      (skip-characters r)
      ;(when (= (.getEventType r) 4)
      ;  (safe-next-tag r))
      ;(assert (= (.getEventType r) 1) (pr-str (.getEventType r)
      ;                                   (.getText r)
      ;                                   x))
      ;(prn :map (.getLocalName r))

      (let [val2 (reduce
                   (fn [acc entry]
                     (if-some [attr-parser (get attribute-parsers (key entry))]
                       (conj! acc (attr-parser entry))
                       (do (prn :skip entry)
                           acc)))
                   (transient {})
                   (attribute-reducible r))
            ]
        (if value-wrapped
          (let [[k valueparser] (transduce
                                  (comp (filter #(= :value (nth % 0)))
                                        (halt-when some?))
                                  (fn  ([acc] acc)
                                    ([acc nv] nv))
                                  nil
                                  tag-parsers)

                parsed-value (valueparser r)]
            ;(ensure-safe-next-tag r)
            #_(when-not in-seq-ex
                  (safe-next-tag r))
            (persistent! (assoc! val2 :value parsed-value)))
          (do
            (when-not in-seq-ex
              (safe-next-tag r))
            (sequence-map-parser r tag-parsers tags val2)))))))

(defn -map-discriminator [x]
  (into #{}
        (comp (remove (fn [[_ opt]]
                        ; (prn :mapdes opt)
                        (:xml/attr opt)))
              (take-while-plus-1 (fn [[_ opt]]
                                   (= (:optional opt) true)))
              (map (fn [[tag]]
                     tag)))
        (-> x m/children)))

(defn seqex-optional [x]
  (case (m/type x)
    :repeat (let [{:keys [min]} (m/properties x)]
             (= min 0))
    :? true
    :* true
    :+ true
    :alt false
    false))

(defn -cat-discriminator [x]
  (into #{}
        (comp (take-while-plus-1 (fn [item]
                                   (seqex-optional item)))
              (mapcat (fn [item]
                     (make-tag-discriminator item))))
        (-> x m/children)))

(defn -alt-discriminator [x]
  (into #{}
        (comp (mapcat (fn [item]
                     (make-tag-discriminator item))))
        (-> x m/children)))

(defn tag-enum-tag [x]
  (let [children (-> x m/children)
        _        (assert (= (count children) 1))]
    (first children)))

(defn special-tuple-tag [x]
  (let [children (-> x m/children)
        first-item (first children)
        tag-name (tag-enum-tag first-item)]
    tag-name))

(defn -single-sub-item [x]
  (let [children (-> x m/children)
        _ (assert (= (count children) 1))]
    (first children)))

(defn -tuple-discriminator [x]
  #{(special-tuple-tag x)})

(defn allways-true-discriminator [^XMLStreamReader r] true)
(defn make-tag-discriminator [x]
  (case (m/type x)
    :schema (make-tag-discriminator (m/deref x))
    :malli.core/schema (make-tag-discriminator (m/deref x))
    :ref (make-tag-discriminator (m/deref x))
    (:? :*  :+  :repeat :sequential) (make-tag-discriminator (-single-sub-item x))
    :map (-map-discriminator x)
    :merge  (-alt-discriminator x)
    (:string :zoned-dateTime :local-date :local-dateTime :enum :re :decimal) nil #_(do #{allways-true-discriminator})
    ;:any (string-parser x)
    :tuple (-tuple-discriminator x)
    :alt  (-alt-discriminator x)
    :or (do                                                 ;(prn x)
          (-alt-discriminator x))
    :cat (-cat-discriminator x)
    :any nil))

(defn skip-seqex [x]
  (case (m/type x)
    :? (-single-sub-item x)
    :* (-single-sub-item x)
    :+ (-single-sub-item x)
    :range (-single-sub-item x)))

(defn -alt-parser [x]
  (let [children (-> x m/children)
        ;_ (prn children)
        discriminator-parsers (into [] (map (juxt make-tag-discriminator
                                                  #(case (-> % m/deref-all m/type)
                                                     ;(:alt :cat) (wrap-next-after-tag (-xml-parser %))
                                                     ;(:tuple) (wrap-next-before-tag (-xml-parser %))
                                                     (-xml-parser %))
                                                  #(case (-> % m/deref-all m/type)
                                                     (:alt :cat :? :+ :repeat :*) false
                                                     true))) children)]
    (fn [^XMLStreamReader r]
      #_(assert (= (safe-next-tag r) 1) (pr-str (.getEventType r)
                                         x))
      (prn :-alt-parser (.getLocalName r))
      (reduce
        (fn [acc [discriminator parser wrap?]]
          (let [tagk (get-tag-kw r)]
            (prn :alt tagk discriminator (discriminator tagk))
            (if (discriminator tagk)
              (let [v (parser r)]
                (prn :alt tagk v :before-return (debug-element r))
                ;(skip-closing-and-charactors r)
                (if wrap?
                  (reduced [v])
                  (reduced v)))
              acc)))
        nil
        discriminator-parsers))))

(defn -or-parser [x]
  (let [children (-> x m/children)
        ;_ (prn children)
        discriminator-parsers (into [] (map (juxt make-tag-discriminator
                                                  #(case (-> % m/deref-all m/type)
                                                     ;(:alt :cat) (wrap-next-after-tag (-xml-parser %))
                                                     ;(:tuple) (wrap-next-before-tag (-xml-parser %))
                                                     (-xml-parser %)))) children)]
    (fn [^XMLStreamReader r]
      #_(assert (= (safe-next-tag r) 1) (pr-str (.getEventType r)
                                                x))
      (prn :-or-parser (.getLocalName r) (debug-element r))
      (reduce
        (fn [acc [discriminator parser]]
          (let [tagk (get-tag-kw r)]
            (prn :or tagk discriminator (discriminator tagk))
            (if (discriminator tagk)
              (let [v (parser r)]
                (prn :or tagk v :before-return(debug-element r))
                ;(skip-closing-and-charactors r)
                (reduced v))
              acc)))
        nil
        discriminator-parsers))))

(defn -maybe-parser [x]
  (let [children (-> x m/children)
        ;_ (prn children)
        discriminator-parsers (into [] (map (juxt make-tag-discriminator
                                                  #(case (-> % m/deref-all m/type)
                                                     ;(:alt :cat) (wrap-next-after-tag (-xml-parser %))
                                                     ;(:tuple) (wrap-next-before-tag (-xml-parser %))
                                                     (-xml-parser %)))) children)]
    (fn [^XMLStreamReader r]
      #_(assert (= (safe-next-tag r) 1) (pr-str (.getEventType r)
                                                x))
      (prn :-or-parser (.getLocalName r) (debug-element r))
      (reduce
        (fn [acc [discriminator parser]]
          (let [tagk (get-tag-kw r)]
            (prn :or tagk discriminator (discriminator tagk))
            (if (discriminator tagk)
              (let [v (parser r)]
                (prn :or tagk v :before-return(debug-element r))
                ;(skip-closing-and-charactors r)
                (reduced v))
              acc)))
        nil
        discriminator-parsers))))

(defn -and-parser [x]
  (let [children (-> x m/children)]
    (-xml-parser (first children))))


(defn -cat-parser [x]
  (let [children (-> x m/children)
        discriminator-parsers (into []
                                    (map (juxt
                                           make-tag-discriminator
                                           #(case (-> % m/deref-all m/type)
                                              (:alt :cat :? :+ :repeat :*) [true (-xml-parser %)]
                                              ;(:ref) (do (prn :ref)
                                              ;           [true (-xml-parser %)])
                                              ;(:or) [false (wrap-next-after-tag (-xml-parser %))]
                                              ; (:tuple) (wrap-next-before-tag (-xml-parser %))
                                              [false (-xml-parser %)])
                                           #_(prn (-> % m/deref-all m/children first m/type))))
                                    children)]
    (fn [^XMLStreamReader r]
      ;(assert (= (safe-next-tag r) 1))

      (reduce
        (fn [acc [discriminator [inline-data? parser]]]
          (let [tagk (get-tag-kw r)]
            (prn :cat tagk discriminator inline-data? (discriminator tagk) (debug-element r))
            (if (discriminator tagk)
              (let [v (parser r)]
                (prn :catv v :before-return (debug-element r))
                ;(skip-closing-and-charactors r)
                ;(safe-next-tag r)
                ;(prn :catv :return(debug-element r))
                (if inline-data?
                  (into acc v)
                  (conj acc v)))
              acc)))
        []
        discriminator-parsers))))

(defn -tuple-parser [x]
  (let [[enum sub :as tuple-children] (m/children x)
        _ (assert (= 2 (count tuple-children)))
        schema-tag (tag-enum-tag enum)
        subparser (case (-> sub m/deref-all m/type)
                    (:alt :cat :or) (wrap-next-before-tag (-xml-parser sub))
                    (-xml-parser sub))]
    (fn [^XMLStreamReader r]
      (let [tagk (get-tag-kw r)
            _    (assert (= schema-tag tagk) (debug-element r))
            _    (prn :tuple schema-tag tagk :parse (debug-element r))
            toreturn [tagk (subparser r)]]
        (prn :tuple toreturn :before-return (debug-element r))
        ;(skip-closing-and-charactors r)
        (when (= schema-tag (get-tag-kw r))
          (safe-next-tag r))
        toreturn
        ))))

(defn -sequential-parser [sequence-tag x]
  (let [children (m/children x)
        _    (assert (= 1 (count children)))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-parser (case (-> child m/deref-all m/type)
                     (:alt :cat :or) (wrap-next-before-tag (-xml-parser child))
                     (-xml-parser child))]
    (fn [^XMLStreamReader r]
      (loop [tag (.getEventType r) acc (transient [])]
        ;(assert (= tag 1) (debug-element r))
        (if (= tag 1)
          (let [tagk (get-tag-kw r)]
            (prn :-sequential-parser tagk :?= sequence-tag)
            (if (= sequence-tag tagk)
              (let [v (sub-parser r)]
                (prn :-sequential-parser tagk (debug-element r) v)
                (recur (safe-next-tag r) (conj! acc v)))
              (not-empty (persistent! acc))))
          (not-empty (persistent! acc)))))))


(defn -regex-parser [x]
  (let [children (m/children x)
        _    (assert (= 1 (count children)))
        child (first children)
        sub-discriminator (make-tag-discriminator child)
        ;_ (prn child (-> child m/deref-all m/type))
        ;_    (prn (-> child m/deref-all m/type))
        inline?     (case (-> child m/deref-all m/type)
                      (:alt :cat :? :+ :repeat :*) true
                      ;(:ref) (do (prn :ref)
                      ;           true)
                      false)
        sub-parser (-xml-parser child)]
    (fn [^XMLStreamReader r]
      (loop [start-tag? (= (.getEventType r) 1) acc (transient [])]
        (if start-tag?
          (let [tagk (get-tag-kw r)]
            (prn :-regex-parser-outer tagk sub-discriminator child (debug-element r))
            (if (sub-discriminator tagk)
              (let [v (sub-parser r)]
                ;(ensure-safe-next-tag r)
                (prn :-regex-parser tagk (m/deref-all child) (debug-element r) v)
                (recur (= (.getEventType r) 1) (if inline?
                                                 (reduce conj! acc v)
                                                 (conj! acc v))))
              (do (.getEventType r)
                  (not-empty (persistent! acc)))))
          (not-empty (persistent! acc)))))))

(defn get-first-tag [parser]
  (fn [^XMLStreamReader r]
    (let [x (safe-next-tag r)
          _ (assert (= x 1 ) (pr-str x))
          result (parser r)]
      (.close r)
      result)))

(defn ref-parser [x]
  (let [child (nth (m/children x) 0)
        refparsers *ref-parsers*]
    (fn [^XMLStreamReader r]
      ((get @refparsers child) r))))

(defn -xml-parser [x]
  (case (m/type x)
    :schema (-xml-parser (m/deref x))
    :malli.core/schema
    (let []
      ;(prn x)
      (case (m/form x)
        :org.w3.www.2001.XMLSchema/dateTime
        (local-date-time-parser x)
        (-xml-parser (m/deref x))))
    :ref (ref-parser x)                                     ; (-xml-parser (m/deref x))
    :merge (-xml-parser (m/deref x))
    :map (-map-parser x)
    :string (string-parser x)
    :re (string-parser x)
    :local-dateTime (local-date-time-parser x)
    :zoned-dateTime (zoned-date-time-parser x)
    :local-date (local-date-parser x)
    :local-time (local-time-parser x)
    ;:re (string-parser x)
    :enum (string-parser x)
    :decimal (decimal-parser x)
    :double (decimal-parser x)
    :any (string-parser x)
    :tuple (-tuple-parser x)
    :alt  (-alt-parser x)
    :or  (-or-parser x)
    :and (-and-parser x)
    :cat (-cat-parser x)
    :sequential (-sequential-parser "cat" x)
    :boolean (boolean-parser x)
    :? (-regex-parser x)
    :* (-regex-parser x)
    :+ (-regex-parser x)
    :repeat (-regex-parser x)
    :nil (fn [r]nil )))

(defn xml-parser
  "Returns an pure xml-parser function of type `x -> boolean` for a given Schema.
   Caches the result for [[Cached]] Schemas with key `:xml-parser`."
  ([?schema]
   (xml-parser ?schema nil))
  ([?schema options]
   (binding [*ref-parsers* (atom {})]
     (let [items (into {} (map (fn [[k v]]
                                 [k (-xml-parser (m/-set-children ?schema [v]))]))
                       (:registry (m/properties ?schema)))
           _ (swap! *ref-parsers*
                  into
                  items)]
       (get-first-tag (-xml-parser (m/schema ?schema options)))))

   #_(m/-cached (m/schema ?schema options) :xml-parser -xml-parser)))
