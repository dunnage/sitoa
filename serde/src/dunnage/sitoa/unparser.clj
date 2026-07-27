(ns dunnage.sitoa.unparser
  (:require [clojure.java.io :as io]
            [net.cgrand.xforms :as xforms]
            [malli.core :as m]
            [io.pedestal.log :as log]
            [malli.experimental.time.transform :as mett]
            [malli.transform :as transform])
  (:import
   (java.io OutputStream Writer StringWriter)
   (javax.xml.stream
    XMLOutputFactory XMLStreamWriter XMLStreamConstants)
   (com.sun.xml.txw2.output IndentingXMLStreamWriter)
   (java.time OffsetDateTime LocalDateTime LocalDate Duration Period Year YearMonth MonthDay Month)
   (java.nio ByteBuffer)
   (java.time.format DateTimeFormatter)))

(set! *warn-on-reflection* true)
(def full-string-transformer (transform/transformer transform/string-transformer mett/time-transformer))

(def ^:dynamic *discriminator-refs* nil)
(def ^:dynamic *unparser-refs* nil)

(def ^:private xsi-ns "http://www.w3.org/2001/XMLSchema-instance")
(def ^:private xsi-type-meta-key :dunnage.sitoa/xsi-type)

(defn- xsi-type-of
  "Type keyword stamped by the parser when xsi:type selected a concrete type."
  [data]
  (when (instance? clojure.lang.IMeta data)
    (get (meta data) xsi-type-meta-key)))

(defn- lookup-type-unparser
  "Resolve a registry type keyword to a non-regex unparser fn [data w]."
  [type-kw]
  (when (and type-kw *unparser-refs*)
    ;; Cache is dual-mode: [kw in-regex?]. xsi:type always wants value-mode.
    (when-let [d (get @*unparser-refs* [type-kw false])]
      (let [u (if (delay? d) @d d)]
        ;; Unparsers are either (fn [data w]) or multi-arity; call with data+w.
        u))))

(defn- write-element-with-body
  "Write start tag, optional xsi:type + typed body, else default body, end tag."
  [^XMLStreamWriter w tag-name data default-writer]
  (let [tag (if (keyword? tag-name) (name tag-name) (str tag-name))
        type-kw (xsi-type-of data)]
    (.writeStartElement w tag)
    (when type-kw
      ;; Prefer a real xsi:type so reparse hits the same concrete type.
      (.writeNamespace w "xsi" xsi-ns)
      (.writeAttribute w "xsi" xsi-ns "type" (name type-kw)))
    (if-let [typed (when type-kw (lookup-type-unparser type-kw))]
      (typed data w)
      (default-writer data w))
    (.writeEndElement w)))

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

(defn string-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (and (< pos (count data)) (string? (nth data pos)))
        (inc pos)
        pos))
    (fn [data]
      (string? data))))

(defn hiccup-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (and (< pos (count data)) (or (vector? (nth data pos)) (string? (nth data pos))))
        (inc pos)
        pos))
    ;; Content-only sequences (value-wrapped mixed) are sequential; full
    ;; element hiccup is a vector; plain text is a string.
    (fn [data]
      (or (sequential? data) (string? data)))))

(defn byte-buffer-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? ByteBuffer (nth data pos)) (inc pos) pos))
    (fn [data]
      (instance? ByteBuffer data))))

(defn duration-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? Duration (nth data pos)) (inc pos) pos))
    (fn [data]
      (instance? Duration data))))

(defn period-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? Period (nth data pos)) (inc pos) pos))
    (fn [data]
      (instance? Period data))))

(defn year-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? Year (nth data pos)) (inc pos) pos))
    (fn [data]
      (instance? Year data))))

(defn year-month-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? YearMonth (nth data pos)) (inc pos) pos))
    (fn [data]
      (instance? YearMonth data))))

(defn month-day-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? MonthDay (nth data pos)) (inc pos) pos))
    (fn [data]
      (instance? MonthDay data))))

(defn month-discriminator [x in-regex?]
  (if in-regex?
    (fn [data pos]
      (if (instance? Month (nth data pos)) (inc pos) pos))
    (fn [data]
      (instance? Month data))))

(defn -alt-discriminator [x in-regex?]
  "Dual-mode: in-regex children are pos-based; value-mode children stay value-mode
  so :sequential arms of a map-field :alt/:or do not force in-regex construction."
  (let [children (m/children x)]
    (if in-regex?
      (let [sub-discriminators (into []
                                     (map (juxt #(-xml-discriminator % true) seqex? seqex-optional? identity))
                                     children)]
        (fn [data pos]
          (or (xforms/some
               (keep (fn [[discriminator seqex? optional? sch]]
                       (let [disc (discriminator data pos)]
                         (log/debug :type :-alt-discriminator
                                    :tag [disc pos]
                                    :sch sch)
                         (when (and disc (> disc pos))
                           disc))))
               sub-discriminators)
              pos)))
      (let [sub-discriminators (into []
                                     (map #(-xml-discriminator % false))
                                     children)]
        (fn [data]
          (boolean (some (fn [d] (d data)) sub-discriminators)))))))

(defn -or-discriminator [x in-regex?]
  "Dual-mode: same as -alt-discriminator. Value-mode must not force children with
  in-regex? true (that makes :sequential arms throw at construction)."
  (let [children (m/children x)]
    (if in-regex?
      (let [sub-discriminators (into []
                                     (map (juxt #(-xml-discriminator % true) seqex? seqex-optional? identity))
                                     children)]
        (fn [data pos]
          (or (xforms/some
               (keep (fn [[discriminator seqex? optional? sch]]
                       (let [disc (discriminator data pos)]
                         (log/debug :type :-or-discriminator
                                    :tag [disc pos]
                                    :sch sch)
                         (when (and disc (> disc pos))
                           disc))))
               sub-discriminators)
              pos)))
      (let [sub-discriminators (into []
                                     (map #(-xml-discriminator % false))
                                     children)]
        (fn [data]
          (boolean (some (fn [d] (d data)) sub-discriminators)))))))
(defn -multi-discriminator [x in-regex?]
  (let [children (m/children x)
        dispatch (-> x m/properties :dispatch)
        tags (into #{}
                   (map first)
                   children)]
    (if in-regex?
      ;; Inspect the slot at pos; advance by 1 on match else stay.
      (fn [data pos]
        (if (and (< pos (count data))
                 (let [item (nth data pos)]
                   (and (vector? item) (tags (dispatch item)))))
          (inc pos)
          pos))
      ;; Value-mode: whole data is the multi payload — do not call (f data 0).
      (fn [data]
        (and (vector? data) (tags (dispatch data)))))))

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
        (log/debug :type :-tuple-discriminator :vector (vector? data)
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
                (log/debug :type :-regex-discriminator
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
          (log/debug :type :cat-seq)
          (loop [pos (or ogpos 0) sub-discriminators sub-discriminators]
            (if (< pos (count data))
              (if-some [[discriminator seqex? optional? sch] (first sub-discriminators)]
                (let [next-pos (discriminator data pos)]
                  (log/debug :type :cat-seq :optional? optional? :descrim next-pos :item-data (nth data pos) :check sch)
                  (if (and next-pos (> next-pos pos))
                    (recur next-pos (rest sub-discriminators))
                    (if optional?
                      (recur pos (rest sub-discriminators))
                      (do
                        #_(throw (ex-info "missing-required" {:pos pos
                                                              :fn  (pr-str discriminator)}))
                        ogpos))))
                (do (log/debug :type :cat-seq
                               :exhausted true
                               :descrim pos
                               :item-data (nth data pos))
                    (assert in-regex? (nth data pos))
                    pos))
              pos)))]
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
      (fn [data]
        (sub-discriminator (first data))))))
(defn -map-discriminator [x in-regex?]
  (let [children (m/children x)
        {:keys [xml/value-wrapped xml/in-seq-ex]} (m/properties x)
        ;; Parser map disc uses non-attr element tags as the identity signal.
        ;; Mirror that: required non-attr, non-:xml/value keys identify a map arm.
        required-element-keys (transduce
                               (comp (remove (fn [[_ opts]] (-> opts :xml/attr)))
                                     (remove (fn [[k]] (= k :xml/value)))
                                     (remove (fn [[_ opts]] (-> opts :optional))))
                               (fn
                                 ([acc] acc)
                                 ([acc [attribute-name _opts _subschema]]
                                  (conj acc attribute-name)))
                               []
                               children)
        required-attrs (transduce
                        (comp (filter (fn [[_ opts]] (-> opts :xml/attr)))
                              (remove (fn [[_ opts]] (-> opts :optional))))
                        (fn
                          ([acc] acc)
                          ([acc [attribute-name _opts _subschema]]
                           (conj acc attribute-name)))
                        []
                        children)
        ;; Value-mode keeps prior semantics: all required keys (attrs + elements).
        all-required (transduce
                      (remove (fn [[_ opts]] (-> opts :optional)))
                      (fn
                        ([acc] acc)
                        ([acc [attribute-name _opts _subschema]]
                         (conj acc attribute-name)))
                      []
                      children)
        has-all? (fn [item keys]
                   (every? #(contains? item %) keys))]
    (if in-regex?
      (fn [data pos]
        (if (< pos (count data))
          (let [item (nth data pos)]
            (log/debug :type :map-seq :map? (map? item)
                       :required-elements required-element-keys
                       :required-attrs required-attrs
                       :item-data item :check x)
            (if (map? item)
              (if (seq required-element-keys)
                ;; (a) has all required non-attr element keys
                (if (has-all? item required-element-keys)
                  (inc pos)
                  pos)
                ;; (b) no required element keys: map? + any required attrs
                (if (or (empty? required-attrs)
                        (has-all? item required-attrs))
                  (inc pos)
                  pos))
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
             all-required))))))

(defn ensure-discriminator-ref [x in-regex?]
  (assert *discriminator-refs*)
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        key (first children)
        ;; Dual-mode cache: first in-regex? mode must not win forever.
        cache-key [key in-regex?]]
    (if-some [existing (get @*discriminator-refs* cache-key)]
      existing
      (let [d (delay (-xml-discriminator (m/deref x) in-regex?))]
        (swap! *discriminator-refs* assoc cache-key d)
        d))))

(defn -ref-discriminator [x in-regex?]
  (let [sub-discriminator (ensure-discriminator-ref x in-regex?)]
    (if in-regex?
      (fn [data pos] (@sub-discriminator data pos))
      (fn [data] (@sub-discriminator data)))))

(defn -xml-discriminator [x in-regex?]
  ;(prn (m/form x))
  (case (m/type x)
    :schema (-xml-discriminator (m/deref x) in-regex?)
    :malli.core/schema
    (-xml-discriminator (m/deref x) in-regex?)
    :ref (-ref-discriminator x in-regex?)
    :merge (-xml-discriminator (m/deref x) in-regex?)
    :map (-map-discriminator x in-regex?)
    (:string :re :enum :any) (string-discriminator x in-regex?)
    :time/local-date-time (local-date-time-discriminator x in-regex?)
    :time/offset-date-time (zoned-dateTime-discriminator x in-regex?)
    :time/local-date (string-discriminator x in-regex?)
    :time/local-time (string-discriminator x in-regex?)
    :decimal (string-discriminator x in-regex?)
    :int (string-discriminator x in-regex?)
    :double (string-discriminator x in-regex?)
    :boolean (string-discriminator x in-regex?)
    :xml/hiccup (hiccup-discriminator x in-regex?)
    :xml/base64Binary (byte-buffer-discriminator x in-regex?)
    :xml/hexBinary (byte-buffer-discriminator x in-regex?)
    :time/duration (duration-discriminator x in-regex?)
    :time/period (period-discriminator x in-regex?)
    :time/year (year-discriminator x in-regex?)
    :time/year-month (year-month-discriminator x in-regex?)
    :time/month-day (month-day-discriminator x in-regex?)
    :time/month (month-discriminator x in-regex?)
    :tuple (-tuple-discriminator x in-regex?)
    :alt  (-alt-discriminator x in-regex?)
    :or  (-or-discriminator x in-regex?)
    :multi  (-multi-discriminator x in-regex?)
    :and (let [f (first (m/children x))]
           (-xml-discriminator f in-regex?))
    :cat (-cat-discriminator x in-regex?)
    :sequential (-sequential-discriminator x in-regex?)
    :? (-regex-discriminator x in-regex?)
    :* (-regex-discriminator x in-regex?)
    :+ (-regex-discriminator x in-regex?)
    :repeat (-regex-discriminator x in-regex?)
    :nil (if in-regex?
           (fn [data pos] pos)
           (fn [data] false))
    ))

(defn string-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (.writeCharacters w (nth data pos)))
      (inc pos))
    (fn [data ^XMLStreamWriter w]
      (.writeCharacters w data)
      true)))

(defn boolean-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [v (nth data pos)]
          (cond
            (true? v) (.writeCharacters w "true")
            (false? v) (.writeCharacters w "false")
            :else (throw (ex-info "not a valid bool" {:data v})))))
      (inc pos))
    (fn [data ^XMLStreamWriter w]
      (cond
        (true? data) (.writeCharacters w "true")
        (false? data) (.writeCharacters w "false")
        :else (throw (ex-info "not a valid bool" {:data data})))
      true)))

(defn encode-base64 [^bytes b]
  (.encodeToString (java.util.Base64/getEncoder) b))

(defn encode-hex [^bytes bytes]
  (let [sb (StringBuilder.)]
    (doseq [b bytes]
      (.append sb (format "%02X" b)))
    (.toString sb)))

(defn write-hiccup [data ^XMLStreamWriter w]
  (cond
    (vector? data)
    (let [tag (name (first data))
          has-attrs? (map? (second data))
          attrs (if has-attrs? (second data) nil)
          children (if has-attrs? (nnext data) (next data))]
      (.writeStartElement w tag)
      (when attrs
        (doseq [[k v] attrs]
          (.writeAttribute w (name k) (str v))))
      (doseq [child children]
        (write-hiccup child w))
      (.writeEndElement w))
    (string? data) (.writeCharacters w data)
    (nil? data) nil
    :else (.writeCharacters w (str data))))

(defn hiccup-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (write-hiccup (nth data pos) w)
      (inc pos))
    ;; Parent already opened the element (map child or value-wrapped).
    ;; - Full element form [:tag attrs? & children]: apply attrs onto the open
    ;;   start tag, then children (standalone :xml/hiccup element body).
    ;; - Content sequence (value-wrapped mixed after content-only parse): emit
    ;;   child nodes/text only — parent map owns tag + attributes.
    (fn [val ^XMLStreamWriter w]
      (cond
        (and (vector? val) (keyword? (first val)))
        (let [has-attrs? (map? (second val))
              attrs (when has-attrs? (second val))
              children (if has-attrs? (nnext val) (next val))]
          (when attrs
            (doseq [[k v] attrs]
              (.writeAttribute w (name k) (str v))))
          (doseq [child children]
            (write-hiccup child w)))
        (sequential? val)
        (doseq [child val]
          (write-hiccup child w))
        (some? val)
        (.writeCharacters w (str val)))
      true)))

(defn base64-binary-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^ByteBuffer item (nth data pos)]
          (.writeCharacters w (encode-base64 (.array item)))))
      (inc pos))
    (fn [^ByteBuffer data ^XMLStreamWriter w]
      (.writeCharacters w (encode-base64 (.array data)))
      true)))

(defn hex-binary-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^ByteBuffer item (nth data pos)]
          (.writeCharacters w (encode-hex (.array item)))))
      (inc pos))
    (fn [^ByteBuffer data ^XMLStreamWriter w]
      (.writeCharacters w (encode-hex (.array data)))
      true)))

(defn duration-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^java.time.Duration item (nth data pos)]
          (.writeCharacters w (.toString item))))
      (inc pos))
    (fn [^java.time.Duration data ^XMLStreamWriter w]
      (.writeCharacters w (.toString data))
      true)))

(defn period-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^java.time.Period item (nth data pos)]
          (.writeCharacters w (.toString item))))
      (inc pos))
    (fn [^java.time.Period data ^XMLStreamWriter w]
      (.writeCharacters w (.toString data))
      true)))

(defn year-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^java.time.Year item (nth data pos)]
          (.writeCharacters w (.toString item))))
      (inc pos))
    (fn [^java.time.Year data ^XMLStreamWriter w]
      (.writeCharacters w (.toString data))
      true)))

(defn year-month-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^java.time.YearMonth item (nth data pos)]
          (.writeCharacters w (.toString item))))
      (inc pos))
    (fn [^java.time.YearMonth data ^XMLStreamWriter w]
      (.writeCharacters w (.toString data))
      true)))

(defn month-day-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^java.time.MonthDay item (nth data pos)]
          (.writeCharacters w (.toString item))))
      (inc pos))
    (fn [^java.time.MonthDay data ^XMLStreamWriter w]
      (.writeCharacters w (.toString data))
      true)))

(defn month-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^java.time.Month item (nth data pos)]
          (.writeCharacters w (format "--%02d" (.getValue item)))))
      (inc pos))
    (fn [^java.time.Month data ^XMLStreamWriter w]
      (.writeCharacters w (format "--%02d" (.getValue data)))
      true)))

(defn ex [data pos ^XMLStreamWriter w])

(defn -or-unparser [x in-regex?]
  "Try branches in order.
  In-regex: pos-based child discriminators/unparsers so :cat/:tuple children
  (e.g. IVL_TS) advance by the correct number of slots.
  Value-mode: value-mode children so a map-field :or may include :sequential arms
  (e.g. AllergyRestrictedChoice) without constructing sequential in-regex."
  (let [children (m/children x)]
    (if in-regex?
      (let [subparsers (into []
                             (map (juxt #(-xml-discriminator % true)
                                        #(-xml-unparser % true)
                                        seqex?
                                        seqex-optional?
                                        identity))
                             children)]
        (fn [data pos ^XMLStreamWriter w]
          (reduce (fn [acc [discriminator unparser _seqex? _optional? sch]]
                    (let [progress (discriminator data pos)]
                      (log/debug :type :or :progress progress :pos pos :sch sch)
                      (if (and progress (> progress pos))
                        (let [next-pos (unparser data pos w)]
                          (assert (and next-pos (> next-pos pos))
                                  (pr-str pos sch data))
                          (reduced next-pos))
                        pos)))
                  pos
                  subparsers)))
      (let [subparsers (into []
                             (map (juxt #(-xml-discriminator % false)
                                        #(-xml-unparser % false)
                                        identity))
                             children)]
        (fn [data ^XMLStreamWriter w]
          (reduce (fn [acc [discriminator unparser sch]]
                    (if (discriminator data)
                      (do (log/debug :type :or-value :sch sch)
                          (reduced (unparser data w)))
                      acc))
                  nil
                  subparsers))))))
(defn -multi-unparser [x in-regex?]
  (let [children (m/children x)
        dispatch (-> x m/properties :dispatch)
        ;; Children are value-mode unparsers; regex multi passes the slot item.
        subparsers (into {}
                         (map (juxt first (comp #(-xml-unparser % false) #(nth % 2))))
                         children)]
    (if in-regex?
      (fn [data pos ^XMLStreamWriter w]
        (let [item (nth data pos)
              k (dispatch item)]
          (when-some [sub-unparser (get subparsers k)]
            (sub-unparser item w))
          (inc pos)))
      (fn [data ^XMLStreamWriter w]
        (let [k (dispatch data)]
          (when-some [sub-unparser (get subparsers k)]
            (sub-unparser data w)))))))

(defn -tuple-unparser [x in-regex?]
  (let [[enum child] (m/children x)
        tags (m/children enum)
        _ (assert (= 1 (count tags)))
        tag (name (first tags))
        child-writer (-xml-unparser child false)]
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
        (inc pos))
      (fn [data ^XMLStreamWriter w]
        (.writeStartElement w tag)
        (child-writer
         (nth data 1)
         w)
        (.writeEndElement w)
        true))))

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
      ;; Sequence-item maps (incl. IVL_TS choice arms and in-seq-ex maps) are
      ;; addressed by index; write element children of the map at that slot.
      (fn [data pos ^XMLStreamWriter w]
        (let [item-data (nth data pos)]
          (log/debug :type :map-unparse-in-regex :data data :pos pos)
          (run! (fn [[key subwriter seq?]]
                  (if seq?
                    (doseq [subdata (get item-data key)]
                      (write-element-with-body w key subdata subwriter))
                    (when-some [subdata (get item-data key)]
                      (write-element-with-body w key subdata subwriter))))
                tag-writers))
        (inc pos))
      (fn [data ^XMLStreamWriter w]
        (run! (fn [[key subwriter]]
                (when-some [subdata (get data key)]
                  (.writeAttribute w (name key) (str subdata))
                  ;(subwriter subdata w)
                  ))
              attribute-writers)
        (if value-wrapped
          (let [[k valuewriter] (transduce
                                 (comp (filter #(= :xml/value (nth % 0)))
                                       (halt-when some?))
                                 (fn ([acc] acc)
                                   ([acc nv] nv))
                                 nil
                                 tag-writers)
                value (:xml/value data)]
            (when (some? value)
              (valuewriter value w)))
          (run! (fn [[key subwriter seq?]]
                  (if seq?
                    (doseq [subdata (get data key)]
                      (write-element-with-body w key subdata subwriter))
                    (when-some [subdata (get data key)]
                      (write-element-with-body w key subdata subwriter))))
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
                  (do (assert in-regex? (pr-str [pos (drop pos data)]))
                      pos))
                pos)))]
    (if in-regex?
      f
      (fn [data ^XMLStreamWriter w]
        (let [consumed (f data 0 w)]
          (assert (or (= consumed (count data))
                      (= consumed 0)))
          ;no partial consumption on outside of regex
          consumed)))))

(defn string-encode-unparser [x in-regex?]
  (let [encoder (m/encoder x full-string-transformer)]
    (if in-regex?
      (fn [data pos ^XMLStreamWriter w]
        (when (< pos (count data))
          (.writeCharacters w (encoder (nth data pos))))
        (inc pos))
      (fn [data ^XMLStreamWriter w]
        (.writeCharacters w (encoder data))
        true))))

(defn offset-datetime-unparser [x in-regex?]
  (if in-regex?
    (fn [data pos ^XMLStreamWriter w]
      (when (< pos (count data))
        (let [^OffsetDateTime item (nth data pos)]
          (.writeCharacters w (.format item DateTimeFormatter/ISO_OFFSET_DATE_TIME))))
      (inc pos))
    (fn [^OffsetDateTime data ^XMLStreamWriter w]
      (.writeCharacters w (.format data DateTimeFormatter/ISO_OFFSET_DATE_TIME))
      true)))

(defn -alt-unparser [x in-regex?]
  "Dual-mode sibling of -or-unparser: in-regex keeps pos-based children;
  value-mode uses value-mode children."
  (let [children (m/children x)]
    (if in-regex?
      (let [subparsers (into []
                             (map (juxt #(-xml-discriminator % true)
                                        #(-xml-unparser % true)
                                        seqex?
                                        seqex-optional?
                                        identity))
                             children)]
        (fn [data pos ^XMLStreamWriter w]
          (reduce (fn [acc [discriminator unparser seqex? ?optional sch]]
                    (let [progress (discriminator data pos)]
                      (log/debug :type :seq :progress progress :pos pos
                                 :data data :sch sch)
                      (if (and progress (> progress pos))
                        (let [x (unparser data pos w)]
                          (assert (> x pos) (pr-str pos sch data))
                          (log/debug :alt x)
                          (reduced x))
                        pos)))
                  pos
                  subparsers)))
      (let [subparsers (into []
                             (map (juxt #(-xml-discriminator % false)
                                        #(-xml-unparser % false)
                                        identity))
                             children)]
        (fn [data ^XMLStreamWriter w]
          (reduce (fn [acc [discriminator unparser sch]]
                    (if (discriminator data)
                      (reduced (unparser data w))
                      acc))
                  nil
                  subparsers))))))
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

(defn ensure-unparser-ref [x in-regex?]
  (assert *unparser-refs*)
  (let [children (m/children x)
        _ (assert (= 1 (count children)))
        key (first children)
        ;; Dual-mode cache: first in-regex? mode must not win forever.
        cache-key [key in-regex?]]
    (if-some [existing (get @*unparser-refs* cache-key)]
      existing
      (let [d (delay (-xml-unparser (m/deref x) in-regex?))]
        (swap! *unparser-refs* assoc cache-key d)
        d))))

(defn -ref-unparser [x in-regex?]
  (let [sub-unparser (ensure-unparser-ref x in-regex?)]
    (if in-regex?
      (fn [data pos w]
        (@sub-unparser data pos w))
      (fn
        ([data]
         (@sub-unparser data))
        ([data w]
         (@sub-unparser data w))))))

(defn -xml-unparser [x in-regex?]
  (case (m/type x)
    :schema (let [{:keys [topElement]} (m/properties x)
                  p (-xml-unparser (m/deref x) in-regex?)]
              (if topElement
                ;; Document root only: open top element, write body, close element.
                ;; Writer lifecycle (start/end document, close) is owned by string-writer.
                (fn [data pos ^XMLStreamWriter w]
                  (.writeStartElement w topElement)
                  (let [result (p data w)]
                    (.writeEndElement w)
                    result))
                ;; Nested :schema (registry entries) — same arity as body unparser.
                p))
    :malli.core/schema
    (-xml-unparser (m/deref x) in-regex?)
    :ref (-ref-unparser x in-regex?)
    :merge (-xml-unparser (m/deref x) in-regex?)
    :map (-map-unparser x in-regex?)
    :string (string-unparser x in-regex?)
    :re (string-unparser x in-regex?)
    :time/local-date-time (string-encode-unparser x in-regex?)
    :time/offset-date-time (offset-datetime-unparser x in-regex?)
    :time/local-date (string-encode-unparser x in-regex?)
    :time/local-time (string-encode-unparser x in-regex?)
    :zoned-date (string-encode-unparser x in-regex?)
    :enum (string-unparser x in-regex?)
    :decimal (string-encode-unparser x in-regex?)
    :int (string-encode-unparser x in-regex?)
    :any (string-unparser  x in-regex?)
    :xml/hiccup (hiccup-unparser x in-regex?)
    :xml/base64Binary (base64-binary-unparser x in-regex?)
    :xml/hexBinary (hex-binary-unparser x in-regex?)
    :time/duration (duration-unparser x in-regex?)
    :time/period (period-unparser x in-regex?)
    :time/year (year-unparser x in-regex?)
    :time/year-month (year-month-unparser x in-regex?)
    :time/month-day (month-day-unparser x in-regex?)
    :time/month (month-unparser x in-regex?)
    :tuple (-tuple-unparser x in-regex?)
    :alt (-alt-unparser x in-regex?)
    :or (-or-unparser  x in-regex?)
    :multi (-multi-unparser  x in-regex?)
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

(defn resolve-val-delays [x]
  (reduce-kv
   (fn [acc k v]
     (when-not (realized? v)
       (deref v))
     nil)
   nil
   x))
(defn fixed-point [x f]
  (loop [last-x @x]
    (do (f last-x)
        (let [new-x @x]
          (if (= last-x new-x)
            x
            (recur new-x))))))

(defn -xml-unparser- [x in-regex?]
  (let [discriminator-refs (atom {})
        unparser-refs (atom {})
        schema (m/schema x)
        props (m/properties schema)
        ;; Pre-register every registry type (same idea as parser/*ref-parsers*)
        ;; so xsi:type emission can look up PQ/IVL_TS/… even when the declared
        ;; schema path never :ref'd that type (e.g. Observation.value → CD).
        ;; Strip :topElement — that wrapper is only for the document root.
        reg (or (:registry props) {})
        reg-schema-props (dissoc props :topElement)]
    (binding [*discriminator-refs* discriminator-refs
              *unparser-refs* unparser-refs]
      (doseq [[k form] reg]
        ;; Dual-mode cache key [kw in-regex?]; pre-register value-mode for xsi:type.
        (swap! unparser-refs assoc [k false]
               (delay
                 (-xml-unparser
                  (m/schema [:schema reg-schema-props form])
                  false))))
      (let [up (-xml-unparser schema in-regex?)]
        (fixed-point unparser-refs resolve-val-delays)
        (fixed-point discriminator-refs resolve-val-delays)
        ;; Re-bind ref tables on every write so xsi:type lookup sees them
        ;; (dynamic bindings from this builder do not survive return).
        (fn
          ([data]
           (binding [*discriminator-refs* discriminator-refs
                     *unparser-refs* unparser-refs]
             (up data)))
          ([data w]
           (binding [*discriminator-refs* discriminator-refs
                     *unparser-refs* unparser-refs]
             (up data w)))
          ([data pos w]
           (binding [*discriminator-refs* discriminator-refs
                     *unparser-refs* unparser-refs]
             (up data pos w))))))))

(defn document-writer [f]
  (fn [data ^XMLStreamWriter w]
    (.writeStartDocument w "UTF-8" "1.0")
    (f data nil w)
    (.writeEndDocument w)))
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
   (document-writer (-xml-unparser- (m/schema ?schema options) false))

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))

(defn xml-string-unparser
  "takes malli schema and options
  Returns a function that takes edn-data and returns a string."
  ([?schema]
   (xml-string-unparser ?schema nil))
  ([?schema options]
   (string-writer (-xml-unparser- (m/schema ?schema options) false) options)

   #_(m/-cached (m/schema ?schema options) :xml-unparser -xml-unparser)))


