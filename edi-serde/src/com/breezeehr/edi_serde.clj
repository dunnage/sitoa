(ns com.breezeehr.edi-serde
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [malli.core :as m]
            [malli.experimental.time])
  (:import (io.xlate.edi.schema Schema SchemaFactory)
           (io.xlate.edi.stream EDIInputFactory EDIStreamConstants EDIStreamConstants$Standards EDIStreamReader EDIOutputFactory EDIStreamWriter)
           (java.io PushbackReader)
           (java.time LocalDate LocalTime)
           (java.time.format DateTimeFormatter DateTimeFormatterBuilder)))

(defn foo
  "I don't do a whole lot."
  [x]
  (prn x "Hello, World!"))


(defn next-map-sch [sch]
  (case (m/type sch)
    :map sch
    (:sequential :vector :set) (recur (-> sch m/children first))
    nil))

(defn next-map-type [sch]
  (some-> sch next-map-sch m/properties :type))

(defn on-transaction [ ot]
  (fn [^EDIStreamReader r]
    (loop [results []]
      (if (.hasNext r)
        (let [next (.next r)]
          (when (.isError next)
            (prn (.getErrorType r)))
          (prn (.name (.getEventType r)))
          (if (= (.name (.getEventType r)) "START_TRANSACTION")
            (do
              (.next r)
              (recur (conj results (ot r))))
            (recur results)))
        (not-empty results)))))

(defn parse-starter [ot]
  (fn [^EDIStreamReader r]
    (assert (.hasNext r))
    (let [next (.next r)]
      (when (.isError next)
        (prn (.getErrorType r)))
      (ot r))))

(defn skip-elements [element-pos from-pos]
  (let [skip (- element-pos from-pos)]
    (fn [r]
      (loop [skip skip]
        (when (= (.name (.getEventType r)) "ELEMENT_DATA")
          (if (zero? skip)
            r
            (do (.next r)
              (recur (dec skip)))))))))

(def edi-date (-> (DateTimeFormatterBuilder.)
                  (.appendOptional (DateTimeFormatter/ofPattern "yyyyMMdd"))
                  (.appendOptional (DateTimeFormatter/ofPattern "yyMMdd"))
                  .toFormatter))

(def edi-time (-> (DateTimeFormatterBuilder.)
                  (.appendOptional (DateTimeFormatter/ofPattern "HHmmssSS"))
                  (.appendOptional (DateTimeFormatter/ofPattern "HHmmss"))
                  (.appendOptional (DateTimeFormatter/ofPattern "HHmm"))
                  .toFormatter))

(defn make-primitive-parser [sch]
  (case (->  sch m/deref m/type)
    :enum (fn [^String s]
            (when-not (.isEmpty s)
              s))
    :string (fn [^String s]
              (when-not (.isEmpty s)
                s))
    :time/local-date (fn [s]
                       (when-not (.isEmpty s)
                         (LocalDate/parse s edi-date)))
    :time/local-time (fn [s]
                       (when-not (.isEmpty s)
                         (LocalTime/parse s edi-time)))
    :int (fn [^String s]
           (when-not (.isEmpty s)
             (Long/parseLong s)))
    decimal? (fn [^String s]
           (when-not (.isEmpty s)
             (bigdec s)))))

(defn make-element-parser [k meta sch from-pos]
  (let [element-pos (:sequence meta)
        skipper (skip-elements element-pos from-pos)
        prim-parser (make-primitive-parser sch)]
    (fn [r]
      ;(prn (-> r .getLocation))
      (when (skipper r)
        (if-some [txt (prim-parser (.getText r))]
          (do (.next r)
              [k txt])
          (do  (.next r)
               nil))))))

(defn consume-composite [r]
  (if (= (.name (.getEventType r)) "END_COMPOSITE")
    (when (.hasNext r)
      (.next r))))

(defn make-composite-parser [k meta sch from-pos]
  (let [nm (next-map-sch sch)
        element-pos (:sequence meta)
        collection? (case (m/type sch)
                      (:sequential :vector :set) true
                      false)
        sub-element-pos (volatile! 0)
        sub-parsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (let [epos (inc @sub-element-pos)]
                                   (vreset! sub-element-pos (-> meta :sequence))
                                   (make-element-parser k meta sub-schema epos))))
                          (m/children nm))
        skipper (skip-elements element-pos from-pos)]
    (if collection?
      (fn [r] (assert false))
      #_(fn [r]
        (assert (= (.name (.getEventType r)) "START_COMPOSITE"))
        (loop [data []]
          (if (= (-> r .getLocation .getSegmentTag) tag)
            (do
              (.next r)
              (let [m (into {}
                            (map (fn [sub-parser]
                                   (sub-parser r)))
                            sub-parsers)]
                (consume-composite r)
                (recur (conj data m))))
            (when-some [coll (not-empty data)]
              [k coll]))))
      (fn [r]

        #_(when-not  (= (.name (.getEventType r)) "START_COMPOSITE")
          (throw (ex-info "should be composite"
                          {:sch sch
                           :data (str r)})))

        (when (skipper r)
          (let [m (into {}
                        (map (fn [sub-parser]
                               (sub-parser r)))
                        sub-parsers)]
            (consume-composite r)
            [k m]))))))

(defn consume-segment [r]
  (assert (.name (.getEventType r)) "END_SEGMENT")
  (if (= (.name (.getEventType r)) "END_SEGMENT")
    (when (.hasNext r)
      (.next r))
    #_(when (.hasNext r)
      (.next r)
      (recur r))))

(defn make-segment-parser [k meta sch]
  (let [nm (next-map-sch sch)
        collection? (case (m/type sch)
                     (:sequential :vector :set) true
                     false)
        element-pos (volatile! 0)
        sub-parsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (let [epos (inc @element-pos)]
                                   (vreset! element-pos (-> meta :sequence))
                                   (case (next-map-type sub-schema)
                                     :composite (make-composite-parser k meta sub-schema epos)
                                     nil (make-element-parser k meta sub-schema epos)))))
                          (m/children nm))
        tag (-> nm m/properties :segment-id)
        validator (m/coercer sch)]
    (if collection?
      (fn [r]
        (assert (= (.name (.getEventType r)) "START_SEGMENT"))
        (loop [data []]
          (if (= (-> r .getLocation .getSegmentTag) tag)
            (do
              (.next r)
              (let [m (into {}
                            (map (fn [sub-parser]
                                   (sub-parser r)))
                            sub-parsers)]
                (consume-segment r)
                (recur (conj data m))))
            (when-some [coll (not-empty data)]
              [k coll]))))
      (fn [r]
        (assert (= (.name (.getEventType r)) "START_SEGMENT"))
        (when (= (-> r .getLocation .getSegmentTag) tag)
          (.next r)
          (let [m (into {}
                        (map (fn [sub-parser]
                               (sub-parser r)))
                        sub-parsers)]
            (validator m)
            ;(prn (.name (.getEventType r)))
            (consume-segment r)
            [k m]))))))

(defn make-composite-unparser [k meta sub-schema epos]
  )

(defn make-element-unparser [k meta sub-schema epos]
  )

(defn make-segment-unparser [k meta sch]
  (let [nm (next-map-sch sch)
        collection? (case (m/type sch)
                      (:sequential :vector :set) true
                      false)
        element-pos (volatile! 0)
        sub-unparsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (let [epos (inc @element-pos)]
                                   (vreset! element-pos (-> meta :sequence))
                                   (case (next-map-type sub-schema)
                                     :composite (make-composite-unparser k meta sub-schema epos)
                                     nil (make-element-unparser k meta sub-schema epos)))))
                          (m/children nm))
        tag (-> nm m/properties :segment-id)
        validator (m/coercer sch)]
    (if collection?
      (fn [w data]
        ;(assert (= (.name (.getEventType r)) "START_SEGMENT"))
        #_(loop [data data]
          (if (= (-> r .getLocation .getSegmentTag) tag)
            (do
              (.next r)
              (let [m (into {}
                            (map (fn [sub-parser]
                                   (sub-parser r)))
                            sub-parsers)]
                (consume-segment r)
                (recur (conj data m))))
            (when-some [coll (not-empty data)]
              [k coll]))))
      (fn [w data]
        #_(assert (= (.name (.getEventType r)) "START_SEGMENT"))
        #_(when (= (-> r .getLocation .getSegmentTag) tag)
          (.next r)
          (let [m (into {}
                        (map (fn [sub-parser]
                               (sub-parser r)))
                        sub-parsers)]
            (validator m)
            ;(prn (.name (.getEventType r)))
            (consume-segment r)
            [k m]))))))

(defn first-segment [sch]
  (when-some [nm (next-map-sch sch)]
    (case (-> nm m/properties :type)
      :loop (when-some [[_ _ fchild] (-> nm m/children first)]
              ;(prn fchild)
              (recur fchild))
      :segment nm)))

(defn make-loop-parser [k meta sch]
  (if-some [nm (next-map-sch sch)]
    (let [collection? (case (m/type sch)
                        (:sequential :vector :set) true
                        false)
          sub-parsers (into []
                            (map (fn [x]
                                   (let [[k meta sub-schema] x]
                                     (case (next-map-type sub-schema)
                                       :loop (make-loop-parser k meta sub-schema)
                                       :segment (make-segment-parser k meta sub-schema)))))
                            (m/children nm))
          tag (some-> nm first-segment m/properties :segment-id)]
      (if collection?
        (fn [r]
          ;(prn tag)
          (assert (= (.name (.getEventType r)) "START_SEGMENT"))
          (loop [data []]
            (if (= (-> r .getLocation .getSegmentTag) tag)
              (recur (conj data (into {}
                                      (map (fn [sub-parser]
                                             (sub-parser r)))
                                      sub-parsers)))
              (when-some [v (not-empty data)]
                [k v]))))
        (fn [r]
          ;(prn tag)
          (assert (= (.name (.getEventType r)) "START_SEGMENT"))
          (when (= (-> r .getLocation .getSegmentTag) tag)
            (let [m (into {}
                          (map (fn [sub-parser]
                                 (sub-parser r)))
                          sub-parsers)]
              [k m])))))
    (assert false)
    #_(fn [r]nil)))


(defn make-transaction-parser [sch]
  (let [sub-parsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (case (next-map-type sub-schema)
                                   :segment (make-segment-parser k meta sub-schema)
                                   :loop (make-loop-parser k meta sub-schema))))
                          (m/children sch))]
    (fn [r]
      (into {}
            (map (fn [sub-parser]
                   (sub-parser r)))
            sub-parsers))))

(defn make-transactions-parser [k meta sch ]
  (let [sub-parsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (case (next-map-type sub-schema)
                                   :segment (make-segment-parser k meta sub-schema)
                                   :loop (make-loop-parser k meta sub-schema))))
                          (-> sch m/children first m/children))]
    (fn [r]
      ;(prn tag)
      (assert (= (.name (.getEventType r)) "START_TRANSACTION") )
      (loop [data []]
        (if (= (.name (.getEventType r)) "START_TRANSACTION")
          (let [_ (.next r)
                next-data (conj data (into {}
                                           (map (fn [sub-parser]
                                                  (sub-parser r)))
                                           sub-parsers))]
            (assert (= (.name (.getEventType r)) "END_TRANSACTION") (pr-str {:et (.name (.getEventType r))
                                                                             :loc (-> r .getLocation #_.getSegmentTag)}))
            (.next r)
            (recur next-data))
          (when-some [v (not-empty data)]
            [k v]))))))



(defn make-group-parser [k meta sch ]
  (let [sub-parsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (case (next-map-type sub-schema)
                                   :segment (make-segment-parser k meta sub-schema)
                                   :transaction-set  (make-transactions-parser k meta sub-schema))))
                          (-> sch m/children first m/children))]
    (fn [r]
      (assert (= (.name (.getEventType r)) "START_GROUP") )
      (loop [data []]
        (if (= (.name (.getEventType r)) "START_GROUP")
          (let [_ (.next r)
                next-data (conj data (into {}
                                           (map (fn [sub-parser]
                                                  (sub-parser r)))
                                           sub-parsers))]
            (assert (= (.name (.getEventType r)) "END_GROUP") (pr-str {:et (.name (.getEventType r))
                                                                       :loc (-> r .getLocation #_.getSegmentTag)}))
            (.next r)
            (recur next-data))
          (when-some [v (not-empty data)]
            [k v]))))))

(defn make-interchange-parser [sch]
  (let [sub-parsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (case (next-map-type sub-schema)
                                   :segment (make-segment-parser k meta sub-schema)
                                   :group (make-group-parser k meta sub-schema))))
                          (m/children sch))]
    (parse-starter
      (fn [r]
        (assert (= (.name (.getEventType r)) "START_INTERCHANGE"))
        (let [_ (.next r)
              next-data (into {}
                              (map (fn [sub-parser]
                                     (sub-parser r)))
                              sub-parsers)]
          (assert (= (.name (.getEventType r)) "END_INTERCHANGE"))
          (when (.hasNext r)
            (.next r))
          next-data)))))

(defn make-parser [sch]
  (assert (-> sch m/properties :type (= :interchange)))
  (make-interchange-parser sch))

(defn make-interchange-unparser [sch]
  (let [sub-unparsers (into []
                            (keep (fn [[k meta sub-schema]]
                                   (case (next-map-type sub-schema)
                                     :segment (make-segment-unparser k meta sub-schema)
                                     :group nil #_(make-group-parser k meta sub-schema))))
                            (m/children sch))]
    (fn [^EDIStreamWriter w data]
      (let [sf (SchemaFactory/newFactory)
            schema (.getControlSchema sf EDIStreamConstants$Standards/X12 (into-array String ["00501"]))]
        (.setControlSchema w schema)
        (.startInterchange w)


        (.endInterchange w)))))
(defn make-unparser [sch]
  (assert (-> sch m/properties :type (= :interchange)))
  (make-interchange-unparser sch))

(comment

  (require 'malli.dev)
  (malli.dev/start!)
  (def sch (-> (io/resource "x12_271.edn")
               io/reader
               PushbackReader.
               edn/read
               (m/schema {:registry (merge (m/default-schemas) (malli.experimental.time/schemas))})
               ))


  (let [fact  (EDIInputFactory/newFactory)]
    #_(.setProperty fact EDIInputFactory/EDI_IGNORE_EXTRANEOUS_CHARACTERS true)
    #_(.setProperty fact EDIInputFactory/EDI_VALIDATE_CONTROL_STRUCTURE false)
    (with-open [r (.createEDIStreamReader fact (io/input-stream (io/resource
                                                                  #_"270-3.edi"
                                                                  "271/section6-3.edi"
                                                                  #_"simple_with_binary_segment.edi"
                                                                  #_"sample837-original.edi")))]
      (let [consumer (make-interchange-parser sch)
            #_(make-parser sch)]
        (def edi-out (consumer r)))))


  edi-out
  (let [fact  (EDIOutputFactory/newFactory)
        sch (-> (io/resource "x12_271.edn")
                io/reader
                PushbackReader.
                edn/read
                (m/schema {:registry (merge (m/default-schemas) (malli.experimental.time/schemas))})
                )]
    (with-open [r (.createEDIStreamWriter fact (io/output-stream "out.edi"))]
      (let [producer (make-unparser sch)
            #_(make-parser sch)]
        (producer r edi-out)))))


