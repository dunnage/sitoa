(ns dunnage.sitoa.unparser
  (:require [clojure.java.io :as io]
            [malli.core :as m]
            [malli.transform :as transform])
  (:import
    (java.io OutputStream Writer StringWriter)
    (javax.xml.stream
      XMLOutputFactory XMLStreamWriter XMLStreamConstants)
    (com.sun.xml.txw2.output IndentingXMLStreamWriter)
    (java.time ZonedDateTime LocalDateTime LocalDate)))


(defn make-stream-writer [props source]
  (let [fac (XMLOutputFactory/newInstance)]
    (IndentingXMLStreamWriter.
      (cond
        (instance? Writer source) (.createXMLStreamWriter fac ^Writer source)
        (instance? OutputStream source) (.createXMLStreamWriter fac ^OutputStream source)
        :else (throw (IllegalArgumentException.
                       "source should be java.io.Reader or java.io.OutputStream"))))))

(defn sink [s]
  (io/writer s))

(defn nothing-handler [^XMLStreamWriter r stop state]
  state)
(declare -xml-unparser -xml-discriminator)

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
  (fn [data]
    (instance? LocalDateTime data)))

(defn zoned-dateTime-discriminator [x]
  (fn [data]
    (instance? ZonedDateTime data)))

(defn -alt-discriminator [x]
  true)

(defn -tuple-discriminator [x]
  (let [[enum] (m/children x)
        _  (assert (= (m/type enum) :enum))
         options (m/children enum)
        _ (assert (= (count options) 1))
        tag (nth options 0)]
    (fn [data]
      (and (vector? data) (= tag (nth data 0) )))))

(defn -regex-discriminator [x]
  (fn [data]
    true)
  )

(defn -cat-discriminator [x]
  (fn [data]
    true))

(defn -sequential-discriminator [x]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-discriminator (-xml-discriminator child)]
    (fn [data]
      (sub-discriminator (first data)))))

(defn -map-discriminator [x]
  (let [children (m/children x)
        required-attrs (transduce
                            (remove (fn [[_ opts]] (-> opts :optional)))
                            (fn
                              ([acc] acc)
                              ([acc [attribute-name opts subschema]]
                               (conj acc attribute-name)))
                            []
                            children)]
    (fn [data]
      (reduce
        (fn [acc attr]
          (if (contains? data attr)
            acc
            (reduced false)))
        true
        required-attrs))))

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
    :local-dateTime (local-date-time-discriminator x)
    :zoned-dateTime (zoned-dateTime-discriminator x)
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
  (fn [data ^XMLStreamWriter w]
    (.writeCharacters w data)))

(defn boolean-unparser [x]
  (fn [data ^XMLStreamWriter w]
    (.writeCharacters w data)))


(defn ex [data ^XMLStreamWriter w])

(defn -or-unparser [x]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt -xml-discriminator -xml-unparser))
                         children)]
    (fn [data ^XMLStreamWriter w]
      (reduce (fn [acc [discriminator unparser]]
                (when (discriminator data)
                  (reduced (unparser data w))))
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
    (fn [data ^XMLStreamWriter w]
      (.writeStartElement w tag)
      (when child-writer
        (child-writer (nth data 1) w))
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
                         (conj acc [tag
                                    (-xml-unparser subschema)
                                    ])
                         ))
                      []
                      children)]

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
                                (fn  ([acc] acc)
                                  ([acc nv] nv))
                                nil
                                tag-writers)
              value (:value data)]
          (valuewriter value w))
        (run! (fn [[key subwriter]]
                (when-some [subdata (get data key)]
                  (.writeStartElement w (name key))
                  (subwriter subdata w)
                  (.writeEndElement w)
                  )
                )
              tag-writers)))))

(defn -cat-unparser [x]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt -xml-discriminator -xml-unparser))
                         children)]
    (fn [data ^XMLStreamWriter w]
      (loop [pos 0 subparsers subparsers acc []]
        (if (< pos (count data))
          (if-some [[discriminator unparser] (first subparsers)]
            (let [item-data (nth data pos)]
              (if (discriminator item-data)
                (recur (inc pos) (rest subparsers) (into acc (unparser item-data w)))
                acc)
              )
            (do (assert false data)
                acc))
          acc)
        ))))

(defn string-encode-unparser [x]
  (let [encoder (m/encoder x transform/string-transformer)]
    (fn [data ^XMLStreamWriter w]
      (.writeCharacters w (encoder data))))
  )
(defn -alt-unparser [x]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt -xml-discriminator -xml-unparser))
                         children)]
    (fn [data ^XMLStreamWriter w]
      (reduce (fn [acc [discriminator unparser]]
                (when (discriminator data)
                  (reduced [(unparser data w)])))
              nil
              subparsers))))

(defn -sequential-unparser [x]
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        child (first children)
        ;sub-discriminator (make-tag-discriminator child)
        sub-unparser (-xml-unparser child)]
    (fn [data ^XMLStreamWriter w]
      (reduce (fn [acc [discriminator unparser]]
                (conj acc (sub-unparser data w)))
              []
              data))))

(defn cannoical-unparser [x]
  (let [children (m/children x)
        sub-unparser
        (reduce
          (fn [acc subschema]
            (case (m/type subschema)
              (:string) (reduced (-xml-unparser subschema))
              acc))
          nil
          children)]
    (assert sub-unparser)
    sub-unparser))
(defn -and-unparser [x]
  (let [unparser (cannoical-unparser x)]
    unparser))

(defn -regex-unparser [x]
  (let [children (m/children x)
        subparsers (into []
                         (map (juxt -xml-discriminator -xml-unparser))
                         children)]
    (fn [data ^XMLStreamWriter w]
      (reduce (fn [acc [discriminator unparser]]
                (if (discriminator data)
                  (into acc (unparser data w))
                  acc))
              []
              subparsers))))

(defn -xml-unparser [x]
  (case (m/type x)
    :schema (let [p (-xml-unparser (m/deref x))]
              (fn [data ^XMLStreamWriter w]
                (let [result (p data w)]
                  (.close w)
                  result)))
    :malli.core/schema
    (-xml-unparser (m/deref x))
    :ref (-xml-unparser (m/deref x))
    :merge (-xml-unparser (m/deref x))
    :map (-map-unparser x)
    :string (string-unparser x)
    :re (string-unparser x)
    :local-dateTime (string-encode-unparser x)
    :zoned-dateTime (string-encode-unparser x)
    :local-date (string-encode-unparser x)
    :zoned-date (string-encode-unparser x)
    ;;:re (string-unparser x)
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
    (.writeStartDocument w)
    (f data w)
    (.writeEndDocument w)
    ))
(defn string-writer [f]
  (fn [data]
    (with-open [s (StringWriter.)]
      (with-open [w ^XMLStreamWriter (make-stream-writer {} s)]
        (.writeStartDocument w)
        (f data w)
        (.writeEndDocument w))
      (str s))))

(defn xml-unparser
  "Returns an pure xml-unparser function of type `x -> boolean` for a given Schema.
   Caches the result for [[Cached]] Schemas with key `:xml-unparser`."
  ([?schema]
   (xml-unparser ?schema nil))
  ([?schema options]
   (document-writer (-xml-unparser (m/schema ?schema options)))

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))

(defn xml-string-unparser
  ([?schema]
   (xml-string-unparser ?schema nil))
  ([?schema options]
   (string-writer (-xml-unparser (m/schema ?schema options)))

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))


