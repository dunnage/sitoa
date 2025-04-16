(ns dunnage.sitoa.parser
  (:require [clojure.java.io :as io]
            [malli.core :as m]
            [clojure.xml :as xml]
            [io.pedestal.log :as log]
            [malli.util :as mu])
  (:import
    (java.io InputStream Reader StringReader)
    (java.time.temporal ChronoField)
    (javax.xml.stream
      XMLInputFactory XMLStreamReader XMLStreamConstants)
    (clojure.lang IReduceInit MapEntry ITransientCollection)
    (java.time LocalDate LocalDateTime OffsetDateTime ZonedDateTime LocalTime)
    (java.time.format DateTimeFormatter DateTimeFormatterBuilder DateTimeParseException)
    (java.nio.file Files Path)))

(set! *warn-on-reflection* true)
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

(defn- make-input-factory ^XMLInputFactory [props]
  (let [fac (XMLInputFactory/newInstance)]
    (doseq [[k v] props
            :when (contains? input-factory-props k)
            :let [prop (input-factory-props k)]]
      (.setProperty fac prop v))
    fac))

(defn tag-enum-tag [x]
  (let [children (-> x m/children)
        _        (assert (= (count children) 1))]
    (first children)))

(defn get-tag-kw [^XMLStreamReader r]
  (let [tag (.getLocalName r)]
    (keyword tag)))

(defn safe-next-tag ^long [^XMLStreamReader r]
  (when (.hasNext r)
    (loop [tok (.next r)]
      ;(prn :safe-next-tag (debug-element r))
      (case tok
        (1 2)                                                 ;START_ELEMENT
        tok
        (3 4 5 6 7 11)                                      ;COMMENT
        (if (.hasNext r)
          (recur (.next r))
          (throw (ex-info "reached end without safe next tag" {})) );START_DOCUMENT
        (8) (throw (ex-info "safe next tag will not exit tag or document" {})) ;START_DOCUMENT
        )))
  #_(when-not (= (.getEventType r) 8)
    (.nextTag r)))

(defn ensure-open-tag ^long [^XMLStreamReader r]
  (loop [tok (.getEventType r)]
    ;(prn :ensure-open-tag (debug-element r))
    (case tok
      (1 2)                                                   ;START_ELEMENT
      tok
      (3 4 5 6 7 11)                                        ;COMMENT
      (when (.hasNext r)
        (recur (.next r)))
      ; (8) (assert false)                                  ;START_DOCUMENT
      ))
  #_(when-not (= (.getEventType r) 8)
      (.nextTag r)))
(defn assert-not-close! [^XMLStreamReader  r]
  (case (.getEventType r)
    2 (throw (ex-info "cannot start with close" {}))
    nil))

(defn exit-tag [tag]
  (fn [^XMLStreamReader r]
    ;(prn :exit tag (debug-element r))
    (let [tok (.getEventType r)]
      ;(log/info tok)
      (case tok
        (1 8)                                             ;START_ELEMENT
        (throw (ex-info (str "expected to exit " tag)  (debug-element r)))
        (2)
        (if (= tag (get-tag-kw r))
          (if (.hasNext r)
            (do (.next r)
                (.getEventType r))
            #_tok
            (throw (ex-info (str "expected to exit " tag)  (debug-element r)))
            #_(recur r))
          (throw (ex-info (str "expected to exit " tag)  (debug-element r))))
        (3 4 5 6 7 11)                                      ;COMMENT
        (when (.hasNext r)
          (.next r)
          (recur r))))))

(defn single-tag-parser [tag parser]
  (fn [^XMLStreamReader r]
    ;(prn :single-tag tag (debug-element r))
    (let [tagk (get-tag-kw r)]
      ;(prn :single-tag tag tagk)
      (when (= tag tagk)
        (let [is-empty? (.isEndElement r)
              ret (parser r)
              ;_   (prn :single-tag-after tag (debug-element r))
              exiter (exit-tag tag)]
          (exiter r)
          ret)))))

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


(defn skip-closing-and-charactors [^XMLStreamReader r]
  (loop [tok (.getEventType r)]
    ;(log/info tok)
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
    ;(log/info tok)
    (case tok
      (1 2)                                                   ;START_ELEMENT
      tok
      (3 4 5 6)                                ;COMMENT
      (recur (.next r))
      (7 8) (assert false)                     ;START_DOCUMENT
      )))

(defn any-skip-parser
  [^XMLStreamReader r]
  ;(prn :any (debug-element r))
  (let [txt (.getElementText r)]
    ;(safe-exit-tag r)
    ;(log/info :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
    txt))

(defn string-parser [x]
  (fn [^XMLStreamReader r]
    ;(prn :string-parser (debug-element r))
    (let [txt (.getElementText r)]
      ;(safe-exit-tag r)
      ;(log/info :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      txt)))

(defn local-date-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      ;(safe-exit-tag r)
      ;(log/info :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (LocalDate/parse txt))))

(defn local-time-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      ;(safe-exit-tag r)
      ;(log/info :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (LocalTime/parse txt))))

(defn local-date-time-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      ;(safe-exit-tag r)
      ;(log/info :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (LocalDateTime/parse txt)
      #_(try
        (LocalDateTime/parse txt)
        (catch DateTimeParseException e
          (ZonedDateTime/parse txt))))))

(defn make-formatter []
  (-> (new DateTimeFormatterBuilder)
      ;(.appendPattern "yyyy-MM-dd'T'HH:mm:ss[[.SSSSSS][XXXXX][.SSSSSSXXXXX]]")
      ;(.appendPattern "yyyy-MM-dd'T'HH:mm:ss[[.SSSSSSSSS][XXXXX][.SSSSSSSSSXXXXX]]")
      ;(.appendPattern "yyyy-MM-dd'T'HH:mm:ss[.][XXXXX]")
      (.appendPattern "yyyy-MM-dd'T'HH:mm:ss")
      (.optionalStart)
      ;(.appendLiteral ".")
      (.appendFraction ChronoField/NANO_OF_SECOND, 1, 9, true)
      (.optionalEnd)
      ; (.appendPattern "[XXXXX]")
      ; (.appendPattern "[[.SSSSSS][.SSSSSSSSS]")
      (.optionalStart)
      (.appendOffset "+HH:MM:ss" "Z")
      (.optionalEnd)

      (.optionalStart)
      ;(.appendLiteral ".")
      (.appendFraction ChronoField/NANO_OF_SECOND, 0, 9, true)
      (.appendOffset "+HH:MM:ss" "Z")
      (.optionalEnd)
      (.parseDefaulting ChronoField/NANO_OF_SECOND 0)
      (.parseDefaulting ChronoField/OFFSET_SECONDS 0)

      (.toFormatter)))
(defn offset-date-time-parser [x]
  (fn [^XMLStreamReader r]
    (log/info :type :time/offset-date-time-parser
              :debug (debug-element r))
    (let [txt (.getElementText r)]
      ;(safe-exit-tag r)
      ;(log/info :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (OffsetDateTime/parse txt  (make-formatter)))))

(defn decimal-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      ;(safe-exit-tag r)
      ;(log/info :string-parser (debug-element r) (safe-next-tag r) (debug-element r))
      (BigDecimal. txt))))

(defn boolean-parser [x]
  (fn [^XMLStreamReader r]
    (let [txt (.getElementText r)]
      ;(safe-exit-tag r)
       (Boolean/parseBoolean txt))))

(defn attribute-reducible
  ""
  [^XMLStreamReader r]
  (let [cnt (.getAttributeCount r)]
    ;(prn cnt)
    (assert (>= cnt 0))
    (reify IReduceInit
      (reduce [this f init]
        (loop [i 0 acc init]
          (if (reduced? acc)
            (unreduced acc)
            (if (< i cnt)
              (let [attr (.getAttributeLocalName r i)
                    attr-ns (.getAttributeNamespace r i)]
                (recur (inc i) (f acc (MapEntry. (if (and attr-ns (not (.isEmpty attr-ns)))
                                                   (keyword attr-ns attr)
                                                   (keyword attr))
                                                 (.getAttributeValue r i)))))
              acc)))))))

(defn ap [x]
  x)



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
         ; (log/info :map (debug-element r) tags)
         (case tok
           1                                                ;START_ELEMENT
           (let [tagk (get-tag-kw r)]
             ;(prn :sequence-map-parser tagk nexttagk)
             (if (= nexttagk tagk)
               (let [_ (assert (not (get val tagk)))
                     val (assoc! val tagk (tag-parser r))]
                 ;(prn :parsed tagk)
                 val
                 ;(recur (.getEventType r) val)
                 )
               (do (log/info :leave-start-next tagk :nexttagk nexttagk)
                   val)))
           2 (do
               ;(assert (= (get-tag-kw r) nexttagk) (debug-element r))
               (reduced val))

           ;2                                                ;END_ELEMENT
           ;(let [tagk (get-tag-kw r)]
           ;  (if (tags tagk)
           ;    (recur (.next r) val)
           ;    (do (log/info :leave tagk)
           ;        val)))
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

(defn required-parser [tag parser]
  (fn [^XMLStreamReader r]
    (if-some [val (parser r)]
      val
      (throw (ex-info (str "required parser failed " tag " failed got ") (debug-element r))))))
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
                      (remove (fn [[tag opts]]
                                (or (-> opts :xml/attr)
                                    (= tag :xml/value))))
                      (fn ([acc]acc)
                        ([acc [tag opts subschema]]
                         ;(log/info (m/form (m/deref subschema)))
                         (let [dsubschema (-> subschema m/deref-all)
                               parser
                               (case (m/form (m/deref dsubschema))
                                 :org.w3.www.2001.XMLSchema/dateTime
                                 (-xml-parser dsubschema)
                                 (case (-> dsubschema m/type)
                                   (:sequential) (-sequential-parser tag subschema)
                                   (:alt :cat :or) (->> (wrap-next-before-tag (-xml-parser subschema))
                                                       (single-tag-parser tag ))
                                   (single-tag-parser tag (-xml-parser subschema))))]
                           (conj acc [tag
                                      (if (:optional opts)
                                        parser
                                        (required-parser tag parser))
                                      (case (-> dsubschema m/type)
                                        (:alt :cat :or :sequential) (make-tag-discriminator dsubschema)
                                        nil)]))
                         ))
                      [] children)
        valueparser (transduce
                          (filter (fn [[tag]]
                                    (= tag :xml/value)))
                          (fn ([acc] acc)
                            ([acc [tag opts subschema]]
                             ;(log/info (m/form (m/deref subschema)))
                             (let [dsubschema (-> subschema m/deref-all)
                                   parser
                                   (case (m/form (m/deref dsubschema))
                                     :org.w3.www.2001.XMLSchema/dateTime
                                     (-xml-parser dsubschema)
                                     (case (-> dsubschema m/type)
                                       (:sequential) (-sequential-parser tag subschema)
                                       (:alt :cat :or) (wrap-next-before-tag (-xml-parser subschema))
                                       (-xml-parser subschema)))]
                               (required-parser tag parser))
                             ))
                          [] children)
        tags        (into #{} (map first tag-parsers))
        ]
    (fn [^XMLStreamReader r]
      (assert-not-close! r)
      (skip-characters r)
      (assert-not-close! r)
      ;(when (= (.getEventType r) 4)
      ;  (safe-next-tag r))
      ;(assert (= (.getEventType r) 1) (pr-str (.getEventType r)
      ;                                   (.getText r)
      ;                                   x))
      ;(log/info :map (.getLocalName r))

      (let [tagk (get-tag-kw r)
            ;_ (prn :map :parsing tagk)
            ;_ (prn (debug-element r))
            ;_ (prn (.getAttributeCount r))
            val2 (reduce
                   (fn [acc entry]
                     (if-some [attr-parser (get attribute-parsers (key entry))]
                       (conj! acc (attr-parser entry))
                       (do (log/info :skip entry :attribute-parsers (keys attribute-parsers))
                           acc)))
                   (transient {})
                   (attribute-reducible r))
            ]

        (if value-wrapped
          (let [                                            ;_ (prn :value-wrapped (debug-element r))
                parsed-value (valueparser r)]
            ;(ensure-safe-next-tag r)
            #_(when-not in-seq-ex
                (safe-next-tag r))
            (persistent! (assoc! val2 :xml/value parsed-value)))
          (do
            (if in-seq-ex
              (sequence-map-parser r tag-parsers tags val2)
              (do
                (safe-next-tag r)
                (sequence-map-parser r tag-parsers tags val2)))))))))

(defn -map-discriminator [x]
  (into #{}
        (comp (remove (fn [[_ opt]]
                        ; (log/info :mapdes opt)
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
    (:string :time/offset-date-time :time/local-date :time/local-date-time :enum :re :decimal :double) nil #_(do #{allways-true-discriminator})
    ;:any (string-parser x)
    :tuple (-tuple-discriminator x)
    :alt  (-alt-discriminator x)
    :or (do                                                 ;(log/info x)
          (-alt-discriminator x))
    :cat (-cat-discriminator x)
    :and (let [f (first (m/children x))]
           (make-tag-discriminator f))
    :any nil))

(defn skip-seqex [x]
  (case (m/type x)
    :? (-single-sub-item x)
    :* (-single-sub-item x)
    :+ (-single-sub-item x)
    :range (-single-sub-item x)))

(defn -alt-parser [x]
  (let [children (-> x m/children)
        ;_ (log/info children)
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
      (log/info :-alt-parser (.getLocalName r))
      (reduce
        (fn [acc [discriminator parser wrap?]]
          (let [tagk (get-tag-kw r)]
            (log/info :alt tagk :discriminator discriminator :tag-discriminator (discriminator tagk))
            (if (discriminator tagk)
              (let [v (parser r)]
                (log/info :type :alt :tagk tagk :v v :before-return (debug-element r))
                ;(skip-closing-and-charactors r)
                (if wrap?
                  (reduced [v])
                  (reduced v)))
              acc)))
        nil
        discriminator-parsers))))

(defn -or-parser [x]
  (let [children (-> x m/children)
        ;_ (log/info children)
        discriminator-parsers (into [] (map (juxt make-tag-discriminator
                                                  #(case (-> % m/deref-all m/type)
                                                     ;(:alt :cat) (wrap-next-after-tag (-xml-parser %))
                                                     ;(:tuple) (wrap-next-before-tag (-xml-parser %))
                                                     (-xml-parser %)))) children)]
    (fn [^XMLStreamReader r]
      #_(assert (= (safe-next-tag r) 1) (pr-str (.getEventType r)
                                                x))
      (log/info :type :-or-parser :local (.getLocalName r) :debug (debug-element r))
      (reduce
        (fn [acc [discriminator parser]]
          (let [tagk (get-tag-kw r)]
            (log/info :or tagk discriminator (discriminator tagk))
            (if (discriminator tagk)
              (let [v (parser r)]
                (log/info :type :or :tagk tagk :v v :before-return (debug-element r))
                ;(skip-closing-and-charactors r)
                ;((exit-tag tagk) r)
                (reduced v))
              acc)))
        nil
        discriminator-parsers))))

(defn -maybe-parser [x]
  (let [children (-> x m/children)
        ;_ (log/info children)
        discriminator-parsers (into [] (map (juxt make-tag-discriminator
                                                  #(case (-> % m/deref-all m/type)
                                                     ;(:alt :cat) (wrap-next-after-tag (-xml-parser %))
                                                     ;(:tuple) (wrap-next-before-tag (-xml-parser %))
                                                     (-xml-parser %)))) children)]
    (fn [^XMLStreamReader r]
      #_(assert (= (safe-next-tag r) 1) (pr-str (.getEventType r)
                                                x))
      (log/info :type :-or-parser :local (.getLocalName r) :debug (debug-element r))
      (reduce
        (fn [acc [discriminator parser]]
          (let [tagk (get-tag-kw r)]
            (log/info :type :or :tagk tagk :discriminator discriminator :discriminatork (discriminator tagk))
            (if (discriminator tagk)
              (let [v (parser r)]
                (log/info :type :or :tagk tagk :v v :before-return (debug-element r))
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
                                              ;(:ref) (do (log/info :ref)
                                              ;           [true (-xml-parser %)])
                                              ;(:or) [false (wrap-next-after-tag (-xml-parser %))]
                                              ; (:tuple) (wrap-next-before-tag (-xml-parser %))
                                              [false (-xml-parser %)])
                                           #_(log/info (-> % m/deref-all m/children first m/type))))
                                    children)]
    (fn [^XMLStreamReader r]
      ;(assert (= (safe-next-tag r) 1))

      (reduce
        (fn [acc [discriminator [inline-data? parser]]]
          ;(log/info :cat :pre (debug-element r))
          (skip-characters r)
          (let [tagk (get-tag-kw r)]
            (log/info :cat tagk discriminator inline-data? (discriminator tagk) (debug-element r))
            (if (discriminator tagk)
              (let [v (parser r)]
                (log/info :catv v :before-return (debug-element r))
                ;(skip-closing-and-charactors r)
                ;(safe-next-tag r)
                ;(log/info :catv :return(debug-element r))
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
        ;wrap?       (case (-> sub m/deref-all m/type)
        ;              (:alt :cat :or) true
        ;              false)
        subparser (case (-> sub m/deref-all m/type)
                    (:alt :cat :or) (wrap-next-before-tag (-xml-parser sub))
                    (-xml-parser sub))]
    (fn [^XMLStreamReader r]
      (let [tagk (get-tag-kw r)
            _    (assert (= schema-tag tagk) (conj (debug-element r) [:schema-tag schema-tag]))
            _ (log/info :tuple schema-tag :tagk tagk        ;:wrap? wrap?
                        :parse (debug-element r)
                        ; :sub sub :derefed (-> sub m/deref-all)
                        ;:subparser subparser
                        )
            is-empty? (.isEndElement r)
            ;_ (prn (debug-element r))
            ;_ (prn :is-empty? is-empty?)
            toreturn [tagk (subparser r)]]
        ;(prn (debug-element r))
        ;  (log/info :type :tuple :toreturn toreturn :debug   (debug-element r))
        ((exit-tag tagk) r)
        (log/info :tuple toreturn :before-return (debug-element r))
        ;(skip-closing-and-charactors r)
        ;(assert (= schema-tag (get-tag-kw r)) (pr-str (debug-element r)))

        ;(.next r)
        #_(when (= schema-tag (get-tag-kw r))
          (safe-next-tag r))
        toreturn
        ))))

(defn -sequential-parser [sequence-tag x]
  (let [children (m/children x)
        _    (when-not (= 1 (count children))
               (throw (ex-info "sequential should have one child" {:got children})))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-parser (case (-> child m/deref-all m/type)
                     (:alt :cat :or) (wrap-next-before-tag (-xml-parser child))
                     (:tuple) (-xml-parser child)
                     #_(:map ) (single-tag-parser sequence-tag (-xml-parser child))
                     #_(-xml-parser child))]
    (fn [^XMLStreamReader r]
      (assert-not-close! r)
      (loop [tag (.getEventType r) acc (transient [])]
        ;(assert (= tag 1) (debug-element r))
        (if (= tag 1)
          (let [tagk (get-tag-kw r)]
            (log/info :-sequential-parser tagk :?= sequence-tag)
            (if (= sequence-tag tagk)
              (let [v (sub-parser r)]
                ;(prn v)
                (skip-characters r)
                (log/info :-sequential-parser tagk (debug-element r) v)
                (recur (.getEventType r) (conj! acc v)))
              (not-empty (persistent! acc))))
          (not-empty (persistent! acc)))))))


(defn -regex-parser [x]
  (let [children (m/children x)
        _    (assert (= 1 (count children)))
        child (first children)
        sub-discriminator (make-tag-discriminator child)
        ;_ (log/info child (-> child m/deref-all m/type))
        ;_    (log/info (-> child m/deref-all m/type))
        inline?     (case (-> child m/deref-all m/type)
                      (:alt :cat :? :+ :repeat :*) true
                      ;(:ref) (do (log/info :ref)
                      ;           true)
                      false)
        sub-parser (-xml-parser child)
        dereffed-child (m/deref-all child)]
    (fn [^XMLStreamReader r]
      (loop [event-type (.getEventType r) acc (transient [])]
        (case event-type
          1 (let [tagk (get-tag-kw r)]
              (log/info :type :-regex-parser-outer :tagk tagk :sub-discriminator sub-discriminator :child child :debug (debug-element r))
              (if (sub-discriminator tagk)
                (let [v (sub-parser r)]
                  ;(ensure-safe-next-tag r)
                  (log/info :type :-regex-parser :tagk tagk :schema dereffed-child :debug (debug-element r) :v v)
                  (recur (.getEventType r)
                         (if inline?
                           (reduce conj! acc v)
                           (conj! acc v))))
                (do                                         ;(prn :exit-regex (debug-element r))
                    (not-empty (persistent! acc)))))
          (do
            (skip-characters r)
            (if (= (.getEventType r) 1)
              (recur (.getEventType r)
                     acc)
              (do
                ;(prn :exit-regex-non-element (debug-element r))
                (not-empty (persistent! acc))))))))))

(defn get-first-tag [parser]
  (fn [^XMLStreamReader r]
    (assert (= (.getEventType r) 7))
    (.next r)
    (let [x (ensure-open-tag r)
          _ (assert (= x 1 ) (pr-str x))
          result (parser r)]
      ;(prn :end (debug-element r))
      (assert (= (.getEventType r) 8))

      ;(.next r)
      (.close r)
      result)))

(defn ref-parser [x]
  (let [child (nth (m/children x) 0)
        refparsers *ref-parsers*]
    (fn [^XMLStreamReader r]
      ;(assert-not-close! r)
      (log/info :type :refparser :child child :debug  (debug-element r))
      ((get @refparsers child) r))))

(defn simplify-reduce
  ([] nil)
  ([acc] acc)
  ([acc item]
   (if (nil? acc)
     item
     (case [(m/type acc) (m/type item)]
       [:re :string] acc
       [:enum :re] acc                                      ;should filter enum items by regex
       [:enum :enum] (m/schema
                       (into [:enum]
                             (clojure.set/intersection
                               (into #{} (m/children acc))
                               (into #{} (m/children item)))))

       ))))
(defn simplify [schema]
  (case (m/type schema)
    :and
    (->> schema
         m/children
         (transduce
           (map
             (fn [x]
               (cond
                 (keyword? x)
                 (m/deref-all x)
                 (m/schema? x)
                 (-> x m/deref simplify)
                 :default x)
               ))
           simplify-reduce))

    :malli.core/schema (recur (m/deref schema))
    (:enum :re :string) schema
    ))

(defn toplevel-wrapper [x p]
  (let [{:keys [topElement]} (m/properties x)]
    (if topElement
      (single-tag-parser (keyword topElement) p)
      p)))

(defn -xml-parser [x]
  (case (m/type x)
    :schema (-xml-parser (m/deref x))
    :malli.core/schema
    (let []
      ;(log/info x)
      (case (m/form x)
        :org.w3.www.2001.XMLSchema/dateTime
        (offset-date-time-parser x)
        (-xml-parser (m/deref x))))
    :ref (ref-parser x)                                     ; (-xml-parser (m/deref x))
    :merge (-xml-parser (m/deref x))
    :map (-map-parser x)
    :string (string-parser x)
    :re (string-parser x)
    :time/local-date-time (local-date-time-parser x)
    :time/offset-date-time (offset-date-time-parser x)
    :time/local-date (local-date-parser x)
    :time/local-time (local-time-parser x)
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
    :sequential (let [tuplechild (-> x m/children first)
                      t (m/type tuplechild)
                      key (-> tuplechild m/children first)
                      keyvalue (-> key m/children first)]
                  (case t
                    :tuple (-sequential-parser keyvalue x)
                    :or (do
                          ;(prn x)
                          (-sequential-parser keyvalue x))
                    :and (let [s (simplify tuplechild)]
                           (-sequential-parser keyvalue (mu/assoc x 0 s)))
                    :malli.core/schema
                    (-sequential-parser keyvalue (m/deref x))))
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
       (get-first-tag (toplevel-wrapper ?schema (-xml-parser (m/schema ?schema options))))))

   #_(m/-cached (m/schema ?schema options) :xml-parser -xml-parser)))


(comment
  (def offset-patterns ["2007-12-03T10:15:30+01:00",
                        "2007-12-03T10:15:30Z",
                        "2016-03-02T17:09:55",
                        "2016-03-02T17:09:55Z"
                        "2022-10-26T21:08:15.258598"
                        "2022-10-26T21:08:15.258598Z"
                        "2022-10-26T21:08:15.2585981"
                        "2022-10-26T21:08:15.258598+01:00"
                        ])
  (into [] (map (fn [txt]  (OffsetDateTime/parse txt (make-formatter)))) offset-patterns)

  )