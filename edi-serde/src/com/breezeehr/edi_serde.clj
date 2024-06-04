(ns com.breezeehr.edi-serde
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [malli.core :as m]
            [malli.experimental.time])
  (:import (io.xlate.edi.stream EDIInputFactory EDIStreamReader)
           (java.io PushbackReader)
           (java.time LocalDate LocalTime)
           (java.time.format DateTimeFormatter)))

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
          ;(prn (.name (.getEventType r)))
          (if (= (.name (.getEventType r)) "START_TRANSACTION")
            (do
              (.next r)
              (recur (conj results (ot r))))
            (recur results)))
        (not-empty results)))))
(defn skip-elements [element-pos from-pos]
  (let [skip (- element-pos from-pos)]
    (fn [r]
      (loop [skip skip]
        (when (= (.name (.getEventType r)) "ELEMENT_DATA")
          (if (zero? skip)
            r
            (do (.next r)
              (recur (dec skip)))))))))

(defn make-primitive-parser [sch]
  (case (->  sch m/deref m/type)
    :enum identity
    :string identity
    :time/local-date (fn [s]
                       (LocalDate/parse s (DateTimeFormatter/ofPattern "yyyyMMdd" )))
    :time/local-time (fn [s]
                       (LocalTime/parse s (DateTimeFormatter/ofPattern "HHmmss" )))
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
      (prn (-> r .getLocation))
      (when (skipper r)
        (let [txt (prim-parser (.getText r))]
          (.next r)
          [k txt])))))

(defn make-composite-parser [k meta sch from-pos]
  (fn [r]
    (loop []
      (if (.hasNext r)
        (let [next (.next r)]
          (when (.isError next)
            (prn (.getErrorType r)))
          (prn (str (.getLocation r) " " (when (.hasText r)
                                           (.getText r)))
               (str next))
          (if (= (.name (.getEventType r)) "END_TRANSACTION")
            [:segment :done]
            (recur)))
        ))))

(defn consume-segment [r]
  (if (= (.name (.getEventType r)) "END_SEGMENT")
    (when (.hasNext r)
      (.next r))
    (when (.hasNext r)
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
        tag (-> nm m/properties :segment-id)]
    (fn [r]
      (assert  (= (.name (.getEventType r)) "START_SEGMENT"))
      (when (= (-> r .getLocation .getSegmentTag) tag)
        (.next r)
        (let [m (into {}
                      (map (fn [sub-parser]
                             (sub-parser r)))
                      sub-parsers)]
          (prn (.name (.getEventType r)) )
          (consume-segment r)
          [k m])))))

(defn first-segment [sch]
  (when-some [nm (next-map-sch sch)]
    (case (-> nm m/properties :type)
      :loop (when-some [[_ _ fchild] (-> nm m/children first)]
              (prn fchild)
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
      (fn [r]
        (prn tag)
        (assert (= (.name (.getEventType r)) "START_SEGMENT"))
        (when (= (-> r .getLocation .getSegmentTag) tag)
          (let [m (into {}
                        (map (fn [sub-parser]
                               (sub-parser r)))
                        sub-parsers)]
            [k m]))))
    (fn [r]nil)))


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

(defn make-parser [sch]
  (assert (-> sch m/properties :type (= :transaction)))
  (on-transaction
    (make-transaction-parser sch)))

(defn make-unparser [sch])

(comment
  (def sch (-> (io/resource "x12_271.edn")
               io/reader
               PushbackReader.
               edn/read
               (m/schema {:registry (merge (m/default-schemas) (malli.experimental.time/schemas))})
               ))

  (def fact  (EDIInputFactory/newFactory))
  (.setProperty fact EDIInputFactory/EDI_IGNORE_EXTRANEOUS_CHARACTERS true)
  (.setProperty fact EDIInputFactory/EDI_VALIDATE_CONTROL_STRUCTURE false)
  (with-open [r (.createEDIStreamReader fact (io/input-stream (io/resource  "271-3.edi"
                                                          #_"simple_with_binary_segment.edi"
                                                          #_"sample837-original.edi")))]
    (let [consumer  (make-parser sch)]
      (consumer r))))
