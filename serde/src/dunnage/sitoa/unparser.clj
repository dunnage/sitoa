(ns dunnage.sitoa.unparser
  (:require [clojure.java.io :as io]
            [net.cgrand.xforms :as xforms]
            [malli.core :as m]
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
        (IndentingXMLStreamWriter. )))))

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
  ; (prn x)
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
(defn local-date-time-discriminator [x]
  (fn [data pos]
    (instance? LocalDateTime data)))

(defn zoned-dateTime-discriminator [x]
  (fn [data pos]
    (instance? OffsetDateTime data)))

(defn -alt-discriminator [x]
  (let [children (m/children x)
        sub-discriminators (into []
                                 (map (juxt -xml-discriminator seqex? seqex-optional? identity))
                                 children)]
    (fn [data pos]
      (xforms/some
        (keep (fn [[discriminator seqex? optional? sch]]
               (if seqex?
                 (discriminator data pos)
                 (let [data-element (nth data pos)]
                   (if (discriminator data-element nil)
                     (inc pos))))))
        sub-discriminators))))

(defn -tuple-discriminator [x]
  (let [[enum] (m/children x)
        _  (assert (= (m/type enum) :enum))
         options (m/children enum)
        _ (assert (= (count options) 1))
        tag (nth options 0)]
    (fn [data pos]
      (and (vector? data) (= tag (nth data 0) )))))

(defn -regex-discriminator [x]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        discriminator (-xml-discriminator child)
        seqex?       (seqex? child)
        optional?     (seqex-optional? child)
        sch child]
    (fn [data ogpos]
      (loop [pos (or ogpos 0)]
        (if (< pos (count data))
          (if seqex?
            (if-some [next-pos (discriminator data pos)]
              (do   (prn  sch)
                (if (> next-pos pos)
                  (recur next-pos)
                  ogpos))
              (if optional?
                pos
                ogpos))
            (let [item-data (nth data pos)]
              ; (prn item-data :check sch)
              (if (discriminator item-data nil)
                (recur (inc pos))
                (if optional?
                  pos
                  ogpos))))
          pos)
        ))))

(defn -cat-discriminator [x]
  (let [children (m/children x)
        sub-discriminators
        (into []
              (map (juxt -xml-discriminator seqex? seqex-optional? identity))
              children)]
    (fn [data ogpos]
      (loop [pos (or ogpos 0) sub-discriminators sub-discriminators]
        (if (< pos (count data))
          (if-some [[discriminator seqex? optional? sch] (first sub-discriminators)]
            (if seqex?
              (if-some [next-pos (discriminator data pos)]
                (if (> next-pos pos)
                  (recur next-pos (rest sub-discriminators))
                  ogpos)
                (if optional?
                  (recur pos (rest sub-discriminators))
                  ogpos))
              (let [item-data (nth data pos)]
                ; (prn item-data :check sch)
                (if (discriminator item-data nil)
                  (recur (inc pos) (rest sub-discriminators))
                  (if optional?
                    (recur pos (rest sub-discriminators))
                    ogpos))))
            (do (assert false data)
                pos))
          pos)
        ))))

(defn -sequential-discriminator [x]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-discriminator (-xml-discriminator child)]
    (fn [data pos]
      (sub-discriminator (first data) nil))))

(defn -map-discriminator [x]
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
    (if in-seq-ex
      (fn [data pos]
        (prn data pos)
        (if (map? data)
          (if (reduce
                (fn [acc attr]
                  (if (contains? data attr)
                    true
                    (reduced false)))
                false
                required-attrs)
            (inc pos)
            pos)
          pos))
      (fn [data pos]
        (reduce
          (fn [acc attr]
            (if (contains? data attr)
              true
              (reduced false)))
          false
          required-attrs)))))

(defn ^:boolean -xml-discriminator [x]
  (case (m/type x)
    :schema (-xml-discriminator (m/deref x))
    :malli.core/schema
    (-xml-discriminator (m/deref x))
    :ref (-xml-discriminator (m/deref x))
    ;:merge (-xml-discriminator (m/deref x))
    :map (-map-discriminator x)
    ;:string (string-discriminator x)
    ;:re (string-discriminator x)
    :local-date-time (local-date-time-discriminator x)
    :offset-date-time (zoned-dateTime-discriminator x)
    ;:local-date (local-date-discriminator x)
    ;;:re (string-discriminator x)
    ;:enum (string-discriminator x)
    ;:decimal (decimal-discriminator x)
    ;:any (string-discriminator x)
    :tuple (-tuple-discriminator x)
    :alt  (-alt-discriminator x)
    ;:or  (-or-discriminator x)
    ;:and (-and-discriminator x)
    :cat (-cat-discriminator x)
    :sequential (-sequential-discriminator x)
    ;:boolean (boolean-discriminator x)
    :? (-regex-discriminator x)
    :* (-regex-discriminator x)
    :+ (-regex-discriminator x)
    :repeat (-regex-discriminator x)
    ;:nil (fn [r]nil )
    ))

(defn string-unparser [x]
  (fn [data pos ^XMLStreamWriter w]
    (.writeCharacters w data)))

(defn boolean-unparser [x]
  (fn [data pos ^XMLStreamWriter w]
    (.writeCharacters w data)))


(defn ex [data pos ^XMLStreamWriter w])

(defn -or-unparser [x]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt -xml-discriminator -xml-unparser))
                         children)]
    (fn [data pos ^XMLStreamWriter w]
      (reduce (fn [acc [discriminator unparser]]
                (when (discriminator data pos)
                  (reduced (unparser data nil w))))
              nil
              subparsers))))

(defn -tuple-unparser [x]
  (let [[enum child] (m/children x)
        tags (m/children enum)
        _ (assert (= 1 (count tags)))
        tag (name (first tags))
        child-writer (-xml-unparser (m/deref child))
        ]
    ;(prn tag child)
    (fn [data pos ^XMLStreamWriter w]
      (.writeStartElement w tag)
      (when child-writer
        (child-writer (nth data 1) nil w))
      (.writeEndElement w)
      )))

(defn -map-unparser [x]
  (let [children (m/children x)
        {:keys [xml/value-wrapped xml/in-seq-ex]} (m/properties x)
        attribute-writers (transduce
                            (filter (fn [[_ opts]] (-> opts :xml/attr)))
                            (fn
                              ([acc]acc)
                              ([acc [attribute-name opts subschema]]
                               (conj acc [attribute-name  (-xml-unparser (m/deref subschema))])))
                            []
                            children)
        tag-writers (transduce
                      (remove (fn [[_ opts]] (-> opts :xml/attr)))
                      (fn ([acc]acc)
                        ([acc [tag opts subschema]]
                         ;(prn (m/form (m/deref subschema)))
                         (conj acc (case (-> subschema m/type)
                                     :sequential
                                     (let [subsubschema (m/children subschema)]
                                       (assert (= 1 (count subsubschema)))
                                       [tag
                                        (-xml-unparser (first subsubschema))
                                        true])
                                     [tag
                                      (-xml-unparser subschema)
                                      false]))))
                      []
                      children)]
    (if in-seq-ex
      (fn [data pos ^XMLStreamWriter w]
        ;(prn data pos x)
        (run! (fn [[key subwriter seq?]]
                (if seq?
                  (doseq [subdata (get data key)]
                    (.writeStartElement w (name key))
                    (subwriter subdata nil w)
                    (.writeEndElement w))
                  (when-some [subdata (get data key)]
                    (.writeStartElement w (name key))
                    (subwriter subdata nil w)
                    (.writeEndElement w))))
              tag-writers)
        (inc pos))
      (fn [data pos ^XMLStreamWriter w]
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
            (valuewriter value nil w))
          (run! (fn [[key subwriter seq?]]
                  (if seq?
                    (doseq [subdata (get data key)]
                      (.writeStartElement w (name key))
                      (subwriter subdata nil w)
                      (.writeEndElement w))
                    (when-some [subdata (get data key)]
                      (.writeStartElement w (name key))
                      (subwriter subdata nil w)
                      (.writeEndElement w))))
                tag-writers))))))

(defn -cat-unparser [x]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt -xml-discriminator -xml-unparser seqex? seqex-optional? identity))
                         children)]
    (fn [data pos ^XMLStreamWriter w]
      (loop [pos (or pos 0) subparsers subparsers]
        #_(when (nil? pos)
          (prn data ))
        (if (< pos (count data))
          (if-some [[discriminator unparser seqex? optional? sch] (first subparsers)]
            (do                                             ;(prn pos (nth data pos) sch)
                (if seqex?
                  (let [progress (discriminator data pos)]
                    (if (> progress pos)
                      (recur (unparser data pos w) (rest subparsers))
                      (if optional?
                        (recur pos (rest subparsers))
                        (assert false (pr-str data sch)))))
                  (let [item-data (nth data pos)]
                     (prn :check item-data  sch)
                    (if (discriminator item-data nil)
                      (do
                        (prn :disc item-data sch)
                        (unparser item-data nil w)
                        (recur (inc pos) (rest subparsers)))
                      (if optional?
                        (recur pos (rest subparsers))
                        (assert false (pr-str item-data sch)))))))
            (do (assert false (pr-str [pos (drop pos data)]))
                pos))
          pos)
        ))))

(defn string-encode-unparser [x]
  (let [encoder (m/encoder x transform/string-transformer)]
    (fn [data pos ^XMLStreamWriter w]
      (.writeCharacters w (encoder data))))
  )

(defn offset-datetime-unparser [x]
  (let []
    (fn [^OffsetDateTime data pos ^XMLStreamWriter w]
      (.writeCharacters w (str (.toInstant data)))))
  )
(defn -alt-unparser [x]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt -xml-discriminator -xml-unparser seqex? seqex-optional? identity))
                         children)]
    (fn [data pos ^XMLStreamWriter w]
      (let [pos (or pos 0)]
        (doto (reduce (fn [acc [discriminator unparser seqex? ?optional sch]]
                        (if seqex?
                          (let [progress (discriminator data pos)]
                            (prn :seq progress data pos sch )
                            (if (> progress pos)
                              (let [x (unparser data pos w)]
                                (assert (> x pos) (pr-str pos sch data))
                                (prn :alt x)
                                (reduced x))
                              pos))
                          (let [subdata (nth data pos)]
                            (prn :not-seqex subdata pos sch)
                            (if (discriminator subdata nil)
                              (let [x (unparser subdata nil w)]
                                (prn :alt x sch )
                                (reduced (inc pos)))
                              false))))
                      pos
                      subparsers)
          prn)))))

(defn -sequential-unparser [x]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-unparser (-xml-unparser child)]
    (fn [data pos ^XMLStreamWriter w]
      (reduce (fn [acc item]
                (sub-unparser item nil w))
              []
              data))))

(defn cannoical-unparser [x]
  (let [children (m/children x)
        sub-unparser
        (or (reduce
              (fn [acc subschema]
                (case (m/type subschema)
                  (:string :enum :re) (reduced (-xml-unparser subschema))
                  acc))
              nil
              children)
            ;dereference types
            )]
    (assert sub-unparser (pr-str x))
    sub-unparser))
(defn -and-unparser [x]
  (let [unparser (cannoical-unparser x)]
    unparser))

(defn -regex-unparser [x]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        discriminator (-xml-discriminator child)
        unparser      (-xml-unparser child)
        seqex?       (seqex? child)
        optional?     (seqex-optional? child)
        sch child]
    (fn [data pos ^XMLStreamWriter w]
      (loop [pos (or pos 0)]
        (if (< pos (count data))
          (if seqex?
            (let [progress (discriminator data pos)]
              (if (> progress pos)
                (let [next-pos (unparser data pos w)]
                  (assert next-pos (pr-str pos sch data))
                  (assert (> next-pos pos) (pr-str pos sch data))
                  (recur next-pos))
                (if optional?
                  pos
                  (assert false (pr-str data sch)))))
            (let [item-data (nth data pos)]
              ; (prn item-data :check sch)
              (if (discriminator item-data nil)
                (do (unparser item-data pos w)
                    (recur (inc pos)))
                (if optional?
                  pos
                  (assert false (pr-str item-data sch))))))
          pos)))))

(defn -xml-unparser [x]
  (case (m/type x)
    :schema (let [{:keys [topElement]} (m/properties x)
                  p (-xml-unparser (m/deref x))]
              (if topElement
                (fn [data pos ^XMLStreamWriter w]
                  (.writeStartElement w topElement)
                  ;(.writeAttribute w "xmlns:xsd" "http://www.w3.org/2001/XMLSchema")
                  ;(.writeAttribute w "xmlns:xsi" "http://www.w3.org/2001/XMLSchema-instance")
                  (let [result (p data nil w)]
                    (.writeEndElement w)
                    (.close w)
                    result))
                (fn [data pos ^XMLStreamWriter w]
                  ;(.writeAttribute w "xmlns:xsd" "http://www.w3.org/2001/XMLSchema")
                  ;(.writeAttribute w "xmlns:xsi" "http://www.w3.org/2001/XMLSchema-instance")
                  (let [result (p data nil w)]
                    (.close w)
                    result))))
    :malli.core/schema
    (-xml-unparser (m/deref x))
    :ref (-xml-unparser (m/deref x))
    :merge (-xml-unparser (m/deref x))
    :map (-map-unparser x)
    :string (string-unparser x)
    :re (string-unparser x)
    :local-date-time (string-encode-unparser x)
    :offset-date-time (offset-datetime-unparser x)
    :local-date (string-encode-unparser x)
    :zoned-date (string-encode-unparser x)
    :enum (string-unparser x)
    :decimal (string-encode-unparser x)
    :any (string-unparser x)
    :tuple (-tuple-unparser x)
    :alt  (-alt-unparser x)
    :or  (-or-unparser x)
    :and (-and-unparser x)
    :cat (-cat-unparser x)
    :sequential (-sequential-unparser x)
    :boolean (boolean-unparser x)
    :? (-regex-unparser x)
    :* (-regex-unparser x)
    :+ (-regex-unparser x)
    :repeat (-regex-unparser x)
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
         (.writeStartDocument w "utf-8" "1.0")
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
   (document-writer (-xml-unparser (m/schema ?schema options)))

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))

(defn xml-string-unparser
  "takes malli schema and options
  Returns a function that takes edn-data and returns a string."
  ([?schema]
   (xml-string-unparser ?schema nil))
  ([?schema options]
   (string-writer (-xml-unparser (m/schema ?schema options)) options)

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))


