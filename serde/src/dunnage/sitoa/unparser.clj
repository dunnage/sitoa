(ns dunnage.sitoa.unparser
  (:require [clojure.java.io :as io]
            [net.cgrand.xforms :as xforms]
            [malli.core :as m]
            [io.pedestal.log :as log]
            [malli.transform :as transform])
  (:import
    (java.io OutputStream Writer StringWriter)
    (javax.xml.stream
      XMLOutputFactory XMLStreamWriter XMLStreamConstants)
    (com.sun.xml.txw2.output IndentingXMLStreamWriter)
    (java.time OffsetDateTime LocalDateTime LocalDate)))


(defn make-stream-writer [props source]
  (let [fac (XMLOutputFactory/newInstance)]
    (do                                                     ;IndentingXMLStreamWriter.
      (cond->
        (cond
          (instance? Writer source) (.createXMLStreamWriter fac ^Writer source)
          (instance? OutputStream source) (.createXMLStreamWriter fac ^OutputStream source)
          :else (throw (IllegalArgumentException.
                         "source should be java.io.Reader or java.io.OutputStream")))
        (:indent props)
        (-> (IndentingXMLStreamWriter.)
            (doto (.setIndentStep "    ")))))))

(defn sink [s]
  (io/writer s))

(defn nothing-handler [^XMLStreamWriter r stop state]
  state)
(declare -xml-unparser -xml-discriminator)


(defn seqex? [x]
  (case (m/type x)
    :repeat true
    :? true
    :* true
    :+ true
    :alt true
    :cat true
    :map (-> x m/properties :xml/in-seq-ex boolean)
    false))

(defn seqex-optional? [x]
  #_(log/info :seqex-optional? x)
  (case (m/type x)
    :repeat (-> x m/properties :min (< 1))
    :? true
    :* true
    :+ false
    :alt (every? seqex-optional? (m/children x))
    :cat (every? seqex-optional? (m/children x))
    :map (and                                               ;(-> x m/properties :xml/in-seq-ex boolean)
           (every? (comp :optional second) (m/children x)))
    false))


#_(defn single-top-unparser [elements-unparsers]
    (make-loop {:start-element
                (fn [^XMLStreamWriter r stop state]
                  (let [tag (.getLocalName r)]
                    (if-some [tag-unparser (get elements-unparsers tag)]
                      (tag-unparser r)
                      (assert false))))
                :end-element
                (fn [^XMLStreamWriter r stop state]
                  (let [tag (.getLocalName r)]
                    (if-some [tag-unparser (get elements-unparsers tag)]
                      (persistent! state)
                      (assert false))))
                :end-document
                valid-end-single-document}))

;if in-regex returns a function that returns pos after consumption
(defn local-date-time-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? LocalDateTime (nth data pos))
        (inc pos)
        pos))
    (fn [data]
      (instance? LocalDateTime data))))

(defn zoned-dateTime-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? OffsetDateTime (nth data pos))
        (inc pos)
        pos))
    (fn [data]
      (instance? OffsetDateTime data))))

(defn -alt-discriminator [x in-regex?]
  (let [children (m/children x)
        sub-discriminators (into []
                                 (map (juxt #(-xml-discriminator % true) seqex? seqex-optional? identity))
                                 children)
        f
        (fn [data pos]
          (or (xforms/some
                (keep (fn [[discriminator seqex? optional? sch]]
                        (let [disc (discriminator data pos)]
                          (log/info :type :-alt-discriminator
                                    :tag [disc pos]
                                    :sch sch
                                    ; :data data
                                    )
                          (if (> disc pos)
                            disc))))
                sub-discriminators)
              pos))]
    (if in-regex?
      f
      (fn [data] (pos? (f data 0))))))

(defn -tuple-discriminator [x in-regex?]
  (let [[enum] (m/children x)
        _ (assert (= (m/type enum) :enum))
        options (m/children enum)
        _ (assert (= (count options) 1))
        tag (nth options 0)
        f
        (fn [data] (and (vector? data) (= tag (nth data 0))))]
    (if in-regex?
      (fn [data pos]
        (log/info :type :-tuple-discriminator :vector (vector? data)
                  :data data :pos pos
                  :result (f (nth data (or pos 0)))
                    :tag tag)

        (if (f (nth data (or pos 0)))
          (inc pos)
          pos))
      f)))

(defn -regex-discriminator [x in-regex?]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        discriminator (-xml-discriminator child true)
        seqex? (seqex? child)
        optional? (seqex-optional? child)
        sch child
        f
        (fn [data ogpos]
          (loop [pos (or ogpos 0)]
            (if (< pos (count data))
              (let [next-pos (discriminator data pos)]
                (log/info :type :-regex-discriminator
                          next-pos pos
                          :sch sch)
                (if (> next-pos pos)
                  (recur next-pos)
                  pos))
              pos)))]
    (if in-regex?
      f
      (fn [data] (pos? (f data 0))))))

(defn -cat-discriminator [x in-regex?]
  (let [children (m/children x)
        sub-discriminators
        (into []
              (map (juxt #(-xml-discriminator % true) seqex? seqex-optional? identity))
              children)
        f
        (fn [data ogpos]
          (log/info :type :cat-seq )
          (loop [pos (or ogpos 0) sub-discriminators sub-discriminators]
            (if (< pos (count data))
              (if-some [[discriminator seqex? optional? sch] (first sub-discriminators)]
                (let [next-pos (discriminator data pos)]
                  (log/info :type :cat-seq :optional? optional? :descrim next-pos :item-data (nth data pos) :check sch)
                  (if (and next-pos (> next-pos pos))
                    (recur next-pos (rest sub-discriminators))
                    (if optional?
                      (recur pos (rest sub-discriminators))
                      (do
                        #_(throw (ex-info "missing-required" {:pos pos
                                                              :fn  (pr-str discriminator)}))
                        ogpos))))
                (do (log/info :type :cat-seq
                              :exhausted true
                              :descrim pos
                              :item-data (nth data pos))
                    (assert false (nth data pos))
                    pos))
              pos)
            ))]
    (if in-regex?
      f
      (fn [data] (pos? (f data 0))))))

(defn -sequential-discriminator [x in-regex?]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-discriminator (-xml-discriminator child false)]
    (if in-regex?
      (throw (ex-info "cannot have sequencial in sequence expression"
                      {:schema x
                       :sub-schem child}))
      #_(fn [data pos]
        (sub-discriminator (first data) nil))
      (fn [data]
        (sub-discriminator (first data))))))

(defn -map-discriminator [x in-regex?]
  (let [children (m/children x)
        {:keys [xml/value-wrapped xml/in-seq-ex]} (m/properties x)
        required-attrs (transduce
                         (remove (fn [[_ opts]] (-> opts :optional)))
                         (fn
                           ([acc] acc)
                           ([acc [attribute-name opts subschema]]
                            (conj acc attribute-name)))
                         []
                         children)]
    (if in-regex?
      (fn [data pos]
        #_(log/info :data data :pos pos)
        (if (< pos (count data))
          (let [item (nth data pos)
                has-required (reduce
                               (fn [acc attr]
                                 (if (contains? item attr)
                                   true
                                   (reduced false)))
                               false
                               required-attrs)]
            (log/info :type :map-seq :map? (map? item)
                      :r
                      has-required
                      :item-data item :check x)
            (if (map? item)
              (if has-required
                (inc pos)
                pos)
              pos))
          pos))
      (do (assert (not in-seq-ex))
          (fn [data]
            (reduce
              (fn [acc attr]
                (if (contains? data attr)
                  true
                  (reduced false)))
              false
              required-attrs))))))

(defn -xml-discriminator [x in-regex?]
  (case (m/type x)
    :schema (-xml-discriminator (m/deref x) in-regex?)
    :malli.core/schema
    (-xml-discriminator (m/deref x) in-regex?)
    :ref (-xml-discriminator (m/deref x) in-regex?)
    ;:merge (-xml-discriminator (m/deref x))
    :map (-map-discriminator x in-regex?)
    ;:string (string-discriminator x)
    ;:re (string-discriminator x)
    :local-date-time (local-date-time-discriminator x in-regex?)
    :offset-date-time (zoned-dateTime-discriminator x in-regex?)
    ;:local-date (local-date-discriminator x)
    ;;:re (string-discriminator x)
    ;:enum (string-discriminator x)
    ;:decimal (decimal-discriminator x)
    ;:any (string-discriminator x)
    :tuple (-tuple-discriminator x in-regex?)
    :alt  (-alt-discriminator x in-regex?)
    ;:or  (-or-discriminator x)
    ;:and (-and-discriminator x)
    :cat (-cat-discriminator x in-regex?)
    :sequential (-sequential-discriminator x in-regex?)
    ;:boolean (boolean-discriminator x)
    :? (-regex-discriminator x in-regex?)
    :* (-regex-discriminator x in-regex?)
    :+ (-regex-discriminator x in-regex?)
    :repeat (-regex-discriminator x in-regex?)
    ;:nil (fn [r]nil )
    ))

(defn string-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (.writeCharacters w data)
      (inc pos))
    (fn [data ^XMLStreamWriter w]
      (.writeCharacters w data)
      true )))

(defn boolean-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (.writeCharacters w data)
      (inc pos))
    (fn [data ^XMLStreamWriter w]
      (.writeCharacters w data)
      true)))


(defn ex [data pos ^XMLStreamWriter w])

(defn -or-unparser [x in-regex?]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt #(-xml-discriminator % false) #(-xml-unparser % false) identity))
                         children)]
    (if in-regex?
      (fn [data pos ^XMLStreamWriter w]
        (reduce (fn [acc [discriminator unparser]]
                  (let [disc (discriminator data pos)]
                    (log/info :type :or :discriminator disc :pos pos :data data)
                    (when disc
                      (unparser data nil w)
                      (reduced (inc pos)))))
                pos
                subparsers))
      (fn [data ^XMLStreamWriter w]
        (reduce (fn [acc [discriminator unparser _sche]]
                  (let [disc (discriminator data)]
                    (log/info :type :or :discriminator disc  :data data)
                    (when disc
                      (reduced  (unparser data w)))))
                nil
                subparsers)))))

(defn -tuple-unparser [x in-regex?]
  (let [[enum child] (m/children x)
        tags (m/children enum)
        _ (assert (= 1 (count tags)))
        tag (name (first tags))
        child-writer (-xml-unparser (m/deref child) false)
        ]
    #_(log/info tag child)
    (assert child-writer)
    (if in-regex?
      (fn [data pos ^XMLStreamWriter w]
        (.writeStartElement w tag)
        (child-writer
          (some-> (nth data pos)
                  (nth 1))
          w)
        (.writeEndElement w)
        (inc pos)
        )
      (fn [data ^XMLStreamWriter w]
        (.writeStartElement w tag)
        (child-writer
          (nth data 1)
          w)
        (.writeEndElement w)
        true
        ))))

(defn -map-unparser [x in-regex?]
  (let [children (m/children x)
        {:keys [xml/value-wrapped xml/in-seq-ex]} (m/properties x)
        attribute-writers (transduce
                            (filter (fn [[_ opts]] (-> opts :xml/attr)))
                            (fn
                              ([acc] acc)
                              ([acc [attribute-name opts subschema]]
                               (conj acc [attribute-name (-xml-unparser (m/deref subschema) false)])))
                            []
                            children)
        tag-writers (transduce
                      (remove (fn [[_ opts]] (-> opts :xml/attr)))
                      (fn ([acc] acc)
                        ([acc [tag opts subschema]]
                         #_(log/info :form (m/form (m/deref subschema)))
                         (conj acc (case (-> subschema m/type)
                                     :sequential
                                     (let [subsubschema (m/children subschema)]
                                       (assert (= 1 (count subsubschema)))
                                       [tag
                                        (-xml-unparser (first subsubschema) false)
                                        true])
                                     [tag
                                      (-xml-unparser subschema false)
                                      false]))))
                      []
                      children)]
    (if in-regex?
      (do (assert in-seq-ex)
          (fn [data pos ^XMLStreamWriter w]
            (let [item-data (nth data pos)]
              (log/info :type :map-unparse-in-regex :data data :pos pos)
              (run! (fn [[key subwriter seq?]]
                      (if seq?
                        (doseq [subdata (get item-data key)]
                          (.writeStartElement w (name key))
                          (subwriter subdata w)
                          (.writeEndElement w))
                        (when-some [subdata (get item-data key)]
                          (.writeStartElement w (name key))
                          (subwriter subdata w)
                          (.writeEndElement w))))
                    tag-writers))
            (inc pos)))
      (fn [data ^XMLStreamWriter w]
        (run! (fn [[key subwriter]]
                (when-some [subdata (get data key)]
                  (.writeAttribute w (name key) subdata)
                  ;(subwriter subdata w)
                  )
                )
              attribute-writers)
        (if value-wrapped
          (let [[k valuewriter] (transduce
                                  (comp (filter #(= :value (nth % 0)))
                                        (halt-when some?))
                                  (fn ([acc] acc)
                                    ([acc nv] nv))
                                  nil
                                  tag-writers)
                value (:value data)]
            (valuewriter value w))
          (run! (fn [[key subwriter seq?]]
                  (if seq?
                    (doseq [subdata (get data key)]
                      (.writeStartElement w (name key))
                      (subwriter subdata w)
                      (.writeEndElement w))
                    (when-some [subdata (get data key)]
                      (.writeStartElement w (name key))
                      (subwriter subdata w)
                      (.writeEndElement w))))
                tag-writers))
        true))))

(defn -cat-unparser [x in-regex?]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt #(-xml-discriminator % true) #(-xml-unparser % true) seqex? seqex-optional? identity))
                         children)
        f (fn [data ogpos ^XMLStreamWriter w]
            (assert ogpos)
            (loop [pos ogpos subparsers subparsers]
              #_(when (nil? pos)
                  (log/info data))
              (if (< pos (count data))
                (if-some [[discriminator unparser seqex? optional? sch] (first subparsers)]
                  (let [progress (discriminator data pos)]
                    ; (log/info pos (nth data pos) sch)
                    (if (> progress pos)
                      (let [progress2 (unparser data pos w)]
                        (assert progress2 (pr-str  seqex? optional? sch))
                        (recur progress2 (rest subparsers)))
                      (if optional?
                        (recur pos (rest subparsers))
                        (do (assert (> pos ogpos) (pr-str data sch))
                            pos))))
                  (do (assert false (pr-str [pos (drop pos data)]))
                      pos))
                pos)
              ))]
    (if in-regex?
      f
      (fn [data ^XMLStreamWriter w]
        (let [consumed (f data 0 w)]
          (assert (or (= consumed (count data))
                      (= consumed 0)))
          ;no partial consumption on outside of regex
          consumed)))))

(defn string-encode-unparser [x in-regex?]
  (let [encoder (m/encoder x transform/string-transformer)]
    (if in-regex?
      (fn [data pos ^XMLStreamWriter w]
        (.writeCharacters w (encoder data))
        (inc pos))
      (fn [data ^XMLStreamWriter w]
        (.writeCharacters w (encoder data))
        true)))
  )

(defn offset-datetime-unparser [x in-regex?]
  (if in-regex?
    (fn [^OffsetDateTime data pos ^XMLStreamWriter w]
      (.writeCharacters w (str (.toInstant data)))
      (inc pos))
    (fn [^OffsetDateTime data ^XMLStreamWriter w]
      (.writeCharacters w (str (.toInstant data)))
      true))
  )
(defn -alt-unparser [x in-regex?]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt #(-xml-discriminator % true) #(-xml-unparser % true) seqex? seqex-optional? identity))
                         children)
        f
        (fn [data pos ^XMLStreamWriter w]
          (reduce (fn [acc [discriminator unparser seqex? ?optional sch]]
                    (let [progress (discriminator data pos)]
                      (log/info :type :seq :progress progress :pos pos
                                :data data  :sch sch)
                      (if (> progress pos)
                        (let [x (unparser data pos w)]
                          (assert (> x pos) (pr-str pos sch data))
                          (log/info :alt x)
                          (reduced x))
                        pos)))
                  pos
                  subparsers))]
    (if in-regex?
      f
      (fn [data ^XMLStreamWriter w]
        (let [consumed (f data 0 w)]
          (assert (or (= consumed (count data))
                      (= consumed 0)))
          ;no partial consumption on outside of regex
          consumed)))))

(defn -sequential-unparser [x in-regex?]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-unparser (-xml-unparser child false)]
    (if in-regex?
      (fn [data pos ^XMLStreamWriter w]
        (reduce (fn [acc item]
                  (sub-unparser item nil w))
                []
                (nth data pos))
        (inc pos))
      (fn [data ^XMLStreamWriter w]
        (reduce (fn [acc item]
                  (sub-unparser item w))
                []
                data)))))

(defn cannoical-unparser [x in-regex?]
  (let [children (m/children x)
        sub-unparser
        (or (reduce
              (fn [acc subschema]
                (case (m/type subschema)
                  (:string :enum :re) (reduced (-xml-unparser subschema in-regex?))
                  acc))
              nil
              children)
            ;dereference types
            )]
    (assert sub-unparser (pr-str x))
    sub-unparser))
(defn -and-unparser [x in-regex?]
  (let [unparser (cannoical-unparser x in-regex?)]
    unparser))

(defn -regex-unparser [x in-regex?]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        discriminator (-xml-discriminator child true)
        unparser (-xml-unparser child true)
        seqex? (seqex? child)
        optional? (seqex-optional? child)
        sch child
        f
        (fn [data ogpos ^XMLStreamWriter w]
          (let [ogpos (or ogpos 0)]
            (loop [pos ogpos]
              (if (< pos (count data))
                (let [progress (discriminator data pos)]
                  (if (> progress pos)
                    (let [next-pos (unparser data pos w)]
                      (assert next-pos (pr-str pos sch data))
                      (assert (> next-pos pos) (pr-str pos sch data))
                      (recur next-pos))
                    (if optional?
                      pos
                      (do (assert (> pos ogpos) (pr-str data sch))
                          pos))))
                pos))))]
    (if in-regex?
      f
      (fn [data ^XMLStreamWriter w]
        (let [consumed (f data 0 w)]
          (assert (or (= consumed (count data))
                      (= consumed 0)))
          ;no partial consumption on outside of regex
          consumed)))))

(defn -xml-unparser [x in-regex?]
  (case (m/type x)
    :schema (let [{:keys [topElement]} (m/properties x)
                  p (-xml-unparser (m/deref x) in-regex?)]
              (if topElement
                (fn [data pos ^XMLStreamWriter w]
                  (.writeStartElement w topElement)
                  ;(.writeAttribute w "xmlns:xsd" "http://www.w3.org/2001/XMLSchema")
                  ;(.writeAttribute w "xmlns:xsi" "http://www.w3.org/2001/XMLSchema-instance")
                  (let [result (p data w)]
                    (.writeEndElement w)
                    (.close w)
                    result))
                (fn [data pos ^XMLStreamWriter w]
                  ;(.writeAttribute w "xmlns:xsd" "http://www.w3.org/2001/XMLSchema")
                  ;(.writeAttribute w "xmlns:xsi" "http://www.w3.org/2001/XMLSchema-instance")
                  (let [result (p data w)]
                    (.close w)
                    result))))
    :malli.core/schema
    (-xml-unparser (m/deref x) in-regex?)
    :ref (-xml-unparser (m/deref x) in-regex?)
    :merge (-xml-unparser (m/deref x) in-regex?)
    :map (-map-unparser x in-regex?)
    :string (string-unparser x in-regex?)
    :re (string-unparser x in-regex?)
    :local-date-time (string-encode-unparser x in-regex?)
    :offset-date-time (offset-datetime-unparser x in-regex?)
    :local-date (string-encode-unparser x in-regex?)
    :zoned-date (string-encode-unparser x in-regex?)
    :enum (string-unparser x in-regex?)
    :decimal (string-encode-unparser x in-regex?)
    :any (string-unparser  x in-regex?)
    :tuple (-tuple-unparser x in-regex?)
    :alt (-alt-unparser x in-regex?)
    :or (-or-unparser  x in-regex?)
    :and (-and-unparser x in-regex?)
    :cat (-cat-unparser x in-regex?)
    :sequential (-sequential-unparser x in-regex?)
    :boolean (boolean-unparser x in-regex?)
    :? (-regex-unparser x in-regex?)
    :* (-regex-unparser x in-regex?)
    :+ (-regex-unparser x in-regex?)
    :repeat (-regex-unparser x in-regex?)
    ;:nil (fn [r]nil )
    ))
(defn document-writer [f]
  (fn [data ^XMLStreamWriter w]
    (.writeStartDocument w "UTF-8" "1.0")
    (f data nil w)
    (.writeEndDocument w)
    ))
(defn string-writer
  ([f]
   (string-writer f {}))
  ([f options]
   (fn [data]
     (with-open [s (StringWriter.)]
       (with-open [w ^XMLStreamWriter (make-stream-writer options s)]
         (.writeStartDocument w "UTF-8" "1.0")
         (f data nil w)
         (.writeEndDocument w))
       (str s)))))

(defn xml-unparser
  "takes malli schema and options
  Returns a document-writer a function that takes edn-data and a XMLStreamWriter
   when the document-writer is called it outputs the xml to the writer according to the
   directions in the schema."
  ([?schema]
   (xml-unparser ?schema nil))
  ([?schema options]
   (document-writer (-xml-unparser (m/schema ?schema options) false))

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))

(defn xml-string-unparser
  "takes malli schema and options
  Returns a function that takes edn-data and returns a string."
  ([?schema]
   (xml-string-unparser ?schema nil))
  ([?schema options]
   (string-writer (-xml-unparser (m/schema ?schema options) false) options)

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))


