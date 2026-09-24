(ns com.breezeehr.edi-serde
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [malli.core :as m]
            [malli.experimental.time])
  (:import (io.xlate.edi.schema Schema SchemaFactory)
           (io.xlate.edi.stream EDIInputFactory EDIStreamConstants EDIStreamConstants$Standards EDIStreamReader EDIOutputFactory EDIStreamWriter Location)
           (java.io ByteArrayInputStream PushbackReader)
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

(defn on-transaction [ot]
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

(declare lookahead-reader)

(defn parse-starter [ot]
  (fn [r]
    (let [^EDIStreamReader r (lookahead-reader r)]
      (assert (.hasNext r))
      (let [next (.next r)]
        (when (.isError next)
          (prn (.getErrorType r)))
        (ot r)))))

(defn- event-name [r] (.name (.getEventType r)))

(defn- advance!
  "Steps to the next event, or does nothing at the end of the stream. Guarded
   because a malformed interchange can end mid-segment, and an unguarded
   `.next` there throws from inside the reader rather than from the parser."
  [r]
  (when (.hasNext r) (.next r)))

(defn skip-composite
  "Advances from a composite's START_COMPOSITE to just past its END_COMPOSITE."
  [r]
  (loop []
    (if (= (event-name r) "END_COMPOSITE")
      (advance! r)
      (when (.hasNext r)
        (.next r)
        (recur)))))

(defn skip-elements
  "Positions the reader at element `element-pos`, counting from `from-pos`, and
   returns the reader -- or nil when the segment ran out before that position.

   An element is a POSITION, not an event. A simple element is one
   ELEMENT_DATA, but a composite occupies one position and spans
   START_COMPOSITE .. END_COMPOSITE, so passing over it means passing the whole
   span.

   Counting events instead was the single defect behind every composite symptom
   this parser had. At a START_COMPOSITE the old gate matched nothing and
   returned nil, so the composite was neither entered nor skipped and the
   reader stayed parked on it -- every later parser for that segment then
   no-opped in turn, and `consume-segment` drained into a `case` with no
   START_COMPOSITE branch. Hence `No matching clause: START_COMPOSITE` for a
   composite the schema declared perfectly well, and silently empty segments
   (HI, say) where the composites were merely dropped."
  [element-pos from-pos]
  (let [skip (- element-pos from-pos)]
    (fn [r]
      (loop [skip skip]
        (case (event-name r)
          ("ELEMENT_DATA" "ELEMENT_DATA_BINARY" "START_COMPOSITE")
          (if (zero? skip)
            r
            (do (if (= (event-name r) "START_COMPOSITE")
                  (skip-composite r)
                  (do
                    (when (= (event-name r) "ELEMENT_DATA_BINARY")
                      (.transferTo (.getBinaryData r) (java.io.OutputStream/nullOutputStream)))
                    (advance! r)))
                (recur (dec skip))))
          nil)))))

(def edi-date (-> (DateTimeFormatterBuilder.)
                  (.appendOptional (DateTimeFormatter/ofPattern "yyyyMMdd"))
                  (.appendOptional (DateTimeFormatter/ofPattern "yyMMdd"))
                  .toFormatter))

(def edi-time (-> (DateTimeFormatterBuilder.)
                  (.appendOptional (DateTimeFormatter/ofPattern "HHmmssSS"))
                  (.appendOptional (DateTimeFormatter/ofPattern "HHmmss"))
                  (.appendOptional (DateTimeFormatter/ofPattern "HHmm"))
                  .toFormatter))

(defn datetime-multiformat [formats]
  (assert (not-empty formats))
  (if (= (count formats) 1)
    (DateTimeFormatter/ofPattern (first formats))
    (.toFormatter
     (reduce
      (fn [^DateTimeFormatterBuilder acc fmt]
        (.appendOptional acc (DateTimeFormatter/ofPattern fmt)))
      (DateTimeFormatterBuilder.)
      formats))))

(defn make-primitive-parser [sch]
  (case (->  sch m/deref m/type)
    bytes? (fn [^java.io.InputStream stream] (.readAllBytes stream))
    :enum (fn [^String s]
            (when-not (.isEmpty s)
              s))
    :string (fn [^String s]
              (when-not (.isEmpty s)
                s))
    :time/local-date (let [formats (-> sch m/properties :formats)
                           format (datetime-multiformat formats)]
                       (fn [s]
                         (when-not (.isEmpty s)
                           (LocalDate/parse s format))))

    :time/local-time (let [formats (-> sch m/properties :formats)
                           format (datetime-multiformat formats)]
                       (fn [s]
                         (when-not (.isEmpty s)
                           (LocalTime/parse s format))))
    :int (fn [^String s]
           (when-not (.isEmpty s)
             (Long/parseLong s)))
    decimal? (fn [^String s]
               (when-not (.isEmpty s)
                 (bigdec s)))))

(defn make-primitive-unparser [sch]
  (case (->  sch m/deref m/type)
    bytes? (fn [^EDIStreamWriter w payload]
             (when-not (bytes? payload)
               (throw (ex-info "Binary element requires a byte array" {})))
             (.writeStartElementBinary w)
             (.writeBinaryData w (java.io.ByteArrayInputStream. ^bytes payload))
             (.endElement w))
    :enum (fn [^EDIStreamWriter w ^String s]
            (if s
              (.writeElement w s)
              (.writeEmptyElement w)))
    :string (fn [w ^String s]
              (if s
                (.writeElement w s)
                (.writeEmptyElement w)))
    :time/local-date (if-some [formats (-> sch m/properties :formats)]
                       (let [fmt (DateTimeFormatter/ofPattern (first formats))]
                         (fn [w ^LocalDate s]
                           (if s
                             (.writeElement w (.format fmt s))
                             (.writeEmptyElement w))))
                       (assert false))
    :time/local-time (if-some [formats (-> sch m/properties :formats)]
                       (let [fmt (DateTimeFormatter/ofPattern (first formats))]
                         (fn [w ^LocalTime s]
                           (if s
                             (.writeElement w (.format fmt s))
                             (.writeEmptyElement w))))
                       (assert false) #_(fn [w ^LocalTime s]
                                          (if s
                                            (.writeElement w (.format (DateTimeFormatter/ofPattern "HHmmss") s))
                                            (.writeEmptyElement w))))
    :int (let [fmt (if-some [min (-> sch m/properties :min-chars)]
                     (str "%0" min "d")
                     (str "%" "d"))]
           (fn [w x]
             (if x
               (.writeElement w (format fmt x))
               (.writeEmptyElement w))))
    decimal? (fn [w ^String s]
               (if s
                 (.writeElement w (str s))
                 (.writeEmptyElement w)))))

(defn make-primitive-component-unparser [sch]
  (case (->  sch m/deref m/type)
    :enum (fn [^EDIStreamWriter w ^String s]
            (if s
              (.writeComponent w s)
              (.writeEmptyComponent w)))
    :string (fn [w ^String s]
              (if s
                (.writeComponent w s)
                (.writeEmptyComponent w)))
    :time/local-date (if-some [format (-> sch m/properties :format)]
                       (fn [w ^LocalDate s]
                         (if s
                           (.writeComponent w (.format (DateTimeFormatter/ofPattern format) s))
                           (.writeEmptyComponent w)))
                       (fn [w ^LocalDate s]
                         (if s
                           (.writeComponent w (.format (DateTimeFormatter/ofPattern "yyMMdd") s))
                           (.writeEmptyComponent w))))
    :time/local-time (if-some [format (-> sch m/properties :format)]
                       (fn [w ^LocalTime s]
                         (if s
                           (.writeComponent w (.format (DateTimeFormatter/ofPattern format) s))
                           (.writeEmptyComponent w)))
                       (fn [w ^LocalTime s]
                         (if s
                           (.writeComponent w (.format (DateTimeFormatter/ofPattern "HHmmss") s))
                           (.writeEmptyComponent w))))
    :int (let [fmt (if-some [min (-> sch m/properties :min-chars)]
                     (str "%0" min "d")
                     "%d")]
           (fn [w x]
             (if x
               (.writeComponent w (format fmt x))
               (.writeEmptyComponent w))))
    decimal? (fn [w ^String s]
               (if s
                 (.writeComponent w (str s))
                 (.writeEmptyComponent w)))))

(defn make-element-parser [k meta sch from-pos]
  (let [element-pos (:sequence meta)
        skipper (skip-elements element-pos from-pos)
        prim-parser (make-primitive-parser sch)
        binary? (= 'bytes? (-> sch m/deref m/type))]
    (fn [r]
      ;(prn (-> r .getLocation))
      (when (skipper r)
        (cond
          binary?
          (let [payload (case (event-name r)
                            "ELEMENT_DATA_BINARY" (prim-parser (.getBinaryData r))
                            "ELEMENT_DATA" (if (= "" (.getText r))
                                             (byte-array 0)
                                             (throw (ex-info "Expected a binary stream event" {:element k})))
                            (throw (ex-info "Expected a binary stream event"
                                            {:event (event-name r) :element k})))]
            (.next r)
            [k payload])

          (= (event-name r) "ELEMENT_DATA_BINARY")
          (throw (ex-info "Binary data requires a bytes? schema" {:element k}))

          (= (event-name r) "START_COMPOSITE")
          ;; The schema calls this position simple and the interchange sent a
          ;; composite. Step over it: `.getText` on a START_COMPOSITE has no
          ;; text to give, and leaving the reader here would strand the rest of
          ;; the segment.
          (do (skip-composite r) nil)
          :else
          (if-some [txt (prim-parser (.getText r))]
            (do (.next r)
                [k txt])
            (do  (.next r)
                 nil)))))))

(defn consume-composite
  "Drains what is left of the current composite and steps past its
   END_COMPOSITE.

   It used to advance only when the reader was already sitting exactly on
   END_COMPOSITE, so a composite carrying more components than the schema
   declares left the reader inside it and desynchronised everything after."
  [r]
  (loop []
    (case (event-name r)
      "END_COMPOSITE" (advance! r)
      "ELEMENT_DATA" (when (.hasNext r)
                       (.next r)
                       (recur))
      nil)))

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
        (when (skipper r)
          (case (event-name r)
            "START_COMPOSITE"
            (do
              ;; Step INTO the composite before running the component parsers.
              ;; They count positions from the FIRST COMPONENT, so a reader
              ;; still sitting on the START event is one position out and every
              ;; component reads as absent.
              (.next r)
              (let [m (into {}
                            (map (fn [sub-parser]
                                   (sub-parser r)))
                            sub-parsers)]
                (consume-composite r)
                [k m]))

            ;; X12 lets a composite whose only populated component is the first
            ;; travel without a component separator, and a reader has nothing
            ;; to distinguish that from a simple element. Read it as component
            ;; one rather than losing it.
            "ELEMENT_DATA"
            (when-some [kv ((first sub-parsers) r)]
              [k (conj {} kv)])

            nil))))))

(defn- collect-composite-components
  "The components of the composite the reader is sitting on, leaving the reader
   just past its END_COMPOSITE."
  [r]
  (loop [acc []]
    (advance! r)
    (case (event-name r)
      "ELEMENT_DATA" (recur (conj acc (.getText r)))
      "END_COMPOSITE" (do (advance! r) acc)
      acc)))

(defn collect-extra-elements
  "Drains the rest of a segment, returning the elements no parser claimed. A
   composite comes back as a vector of its components.

   The `case` used to have two branches and no default, so an undeclared
   composite ended the parse with `No matching clause: START_COMPOSITE` -- an
   IllegalArgumentException naming the event and nothing else, from a function
   whose whole job is to be tolerant of what it does not recognise. Anything
   still unaccounted for now says where it was."
  [r]
  (loop [data []]
    (case (event-name r)
      "END_SEGMENT"
      (do
        (when (.hasNext r)
          (.next r))
        data)
      "ELEMENT_DATA"
      (let [el (.getText r)]
        (when (.hasNext r)
          (.next r))
        (recur (conj data el)))
      "ELEMENT_DATA_BINARY"
      (do
        ;; Do not print or retain unclaimed attachment contents.
        (.transferTo (.getBinaryData r) (java.io.OutputStream/nullOutputStream))
        (advance! r)
        (recur data))
      "START_COMPOSITE"
      (recur (conj data (collect-composite-components r)))
      (throw (ex-info "unexpected event while draining a segment"
                      {:event (event-name r)
                       :location (str (.getLocation r))})))))

(defn consume-segment [r]
  (if (= (.name (.getEventType r)) "END_SEGMENT")
    (when (.hasNext r)
      (.next r))
    (let [loc (.getLocation r)
          extra (collect-extra-elements r)]
      (prn  :extra-data-on-segment (str loc) extra))))

(defn- bin-keys [sch]
  (when (= "BIN" (:segment-id (m/properties sch)))
    (let [entries (into {} (map (fn [[k props child]]
                                 [(:sequence props) [k child]]))
                        (m/children sch))
          [length-key] (get entries 1)
          [payload-key payload-schema] (get entries 2)]
      (when (and payload-schema (= 'bytes? (-> payload-schema m/deref m/type)))
        (when-not length-key
          (throw (ex-info "BIN requires its length element" {})))
        [length-key payload-key]))))

(defn- reader-manages-binary-length? [^EDIStreamReader r]
  ;; At START_SEGMENT, a selected StAEDI transaction schema may already
  ;; declare BIN02 binary. In that case StAEDI sets the length itself at
  ;; BIN01; setting it a second time queues a duplicate binary event.
  (let [payload-type (some-> r .getSchemaTypeReference .getReferencedType
                             .getReferences second .getReferencedType)]
    (and (instance? io.xlate.edi.schema.EDISimpleType payload-type)
         (= io.xlate.edi.schema.EDISimpleType$Base/BINARY (.getBase payload-type)))))

(defn- start-bin! [^EDIStreamReader r reader-managed?]
  (when-not (and (= "ELEMENT_DATA" (event-name r))
                 (= 1 (.getElementPosition (.getLocation r))))
    (throw (ex-info "BIN01 byte length is missing" {})))
  (let [text (.getText r)]
    (when-not (re-matches #"[0-9]{1,15}" text)
      (throw (ex-info "Invalid BIN01 byte length" {:length text})))
    (let [length (Long/parseLong text)]
      ;; The public representation is a JVM byte array.
      (when (> length Integer/MAX_VALUE)
        (throw (ex-info "BIN payload exceeds byte-array capacity" {:length length})))
      ;; Zero bytes need no binary mode. Setting it after BIN*0~ would
      ;; enqueue a binary event after the segment has already ended.
      (when (and (pos? length) (not reader-managed?)) (.setBinaryDataLength r length))
      length)))

(defn- check-bin [data [length-key payload-key] expected]
  (let [payload (get data payload-key)]
    (when-not (bytes? payload)
      (throw (ex-info "BIN02 requires a byte array" {})))
    (let [actual (alength ^bytes payload)
          declared (get data length-key)]
      (when (or (and (some? expected) (not= expected actual))
                (and (some? declared) (not= declared actual)))
        (throw (ex-info "BIN01 does not match BIN02 byte length"
                        {:declared (or expected declared) :actual actual})))
      (assoc data length-key actual))))

;; ---------------------------------------------------------------------------
;; Telling same-id sibling slots apart
;; ---------------------------------------------------------------------------
;;
;; Implementation guides give sibling slots one segment id and tell them apart
;; by a qualifier: an 837's 2000A, 2000B and 2000C all begin with HL and differ
;; in HL03, and a 2300 carries a separate DTP slot per DTP01 code. Chosen by tag
;; alone, the first such slot took every segment of that id -- a repeating one
;; swallowed its siblings' segments unvalidated, and a single one failed
;; validation on the first segment that was not its own.
;;
;; The schema already states each qualifier: the slots pin different code lists
;; at that position. So a slot with a same-id rival reads the segment one step
;; ahead, and passes on it when the rival admits a value this slot's code lists
;; reject and the segment carries no value only this slot admits. Nothing names
;; a qualifier; the code lists decide, wherever they differ.
;;
;; Lookahead rather than backtracking: a guide tells its same-id slots apart
;; within the segment itself, so one segment decides. And a slot only passes on
;; a segment its own code lists reject, so a segment its slot accepts never
;; moves.

(defprotocol SegmentLookahead
  (segment-values [r]
    "The segment the reader is on, read AHEAD without consuming it, as
     `{[element component] text}` -- first occurrence of each position. A simple
     element is component 1 of its position, which is also how the reader
     reports a composite that travelled with its first component and no
     separator. The reader goes on to replay the segment to whichever parser
     takes it."))

(defn- capture-event [^EDIStreamReader r]
  (let [event (.getEventType r)]
    {:event event
     :text (when (.hasText r) (.getText r))
     ;; `.getLocation` is ONE mutable object the reader updates in place.
     :location (.copy (.getLocation r))
     :error-type (when (.isError event) (.getErrorType r))
     :reference-code (.getReferenceCode r)
     :type-reference (.getSchemaTypeReference r)
     :binary (when (= "ELEMENT_DATA_BINARY" (.name event))
               (.readAllBytes (.getBinaryData r)))}))

(defn- values-of [events]
  (into {}
        (keep (fn [{:keys [event text ^Location location]}]
                (when (and (= "ELEMENT_DATA" (.name ^Enum event))
                           (= 1 (.getElementOccurrence location)))
                  [[(.getElementPosition location) (max 1 (.getComponentPosition location))]
                   text])))
        events))

(defn- not-in-this-state [what]
  (IllegalStateException. (str what " is not available for the current event")))

(deftype LookaheadReader [^EDIStreamReader r
                          ^:unsynchronized-mutable buffered
                          ^:unsynchronized-mutable ^long at
                          ^:unsynchronized-mutable values]
  SegmentLookahead
  (segment-values [_]
    (cond
      (and buffered (zero? at)) values
      buffered (throw (IllegalStateException. "Lookahead starts at a START_SEGMENT, not mid-segment"))
      (not= "START_SEGMENT" (.name (.getEventType r)))
      (throw (IllegalStateException. (str "Lookahead starts at a START_SEGMENT, not "
                                          (.name (.getEventType r)))))
      :else
      (let [events (loop [events [(capture-event r)]]
                     (if (or (= "END_SEGMENT" (.name (.getEventType r))) (not (.hasNext r)))
                       events
                       (do (.next r) (recur (conj events (capture-event r))))))]
        (set! buffered events)
        (set! at 0)
        (set! values (values-of events))
        values)))

  EDIStreamReader
  (next [_]
    (if buffered
      (if (< at (dec (count buffered)))
        (do (set! at (inc at))
            (:event (nth buffered at)))
        (do (set! buffered nil)
            (set! values nil)
            (.next r)))
      (.next r)))
  (hasNext [_] (or (and (some? buffered) (< at (dec (count buffered)))) (.hasNext r)))
  (getEventType [_] (if buffered (:event (nth buffered at)) (.getEventType r)))
  (hasText [_] (if buffered (some? (:text (nth buffered at))) (.hasText r)))
  (getText [_]
    (if buffered
      (or (:text (nth buffered at)) (throw (not-in-this-state "Text")))
      (.getText r)))
  (getTextCharacters [this]
    (if buffered (.toCharArray (.getText this)) (.getTextCharacters r)))
  (getTextCharacters [this source-start target target-start length]
    (if buffered
      (let [text (.getText this)
            n (min length (- (count text) source-start))]
        (.getChars text (int source-start) (int (+ source-start n)) ^chars target (int target-start))
        (int n))
      (.getTextCharacters r source-start target target-start length)))
  (getTextStart [_] (if buffered 0 (.getTextStart r)))
  (getTextLength [this] (if buffered (count (.getText this)) (.getTextLength r)))
  (getLocation [_] (if buffered (:location (nth buffered at)) (.getLocation r)))
  (getErrorType [_]
    (if buffered
      (or (:error-type (nth buffered at)) (throw (not-in-this-state "An error type")))
      (.getErrorType r)))
  (getReferenceCode [_] (if buffered (:reference-code (nth buffered at)) (.getReferenceCode r)))
  (getSchemaTypeReference [_]
    (if buffered (:type-reference (nth buffered at)) (.getSchemaTypeReference r)))
  (getBinaryData [_]
    (if buffered
      (if-some [b (:binary (nth buffered at))]
        (ByteArrayInputStream. ^bytes b)
        (throw (not-in-this-state "Binary data")))
      (.getBinaryData r)))
  (setBinaryDataLength [_ length]
    ;; Too late: a segment read ahead was read as text. No slot that needs
    ;; lookahead holds binary data, and this says so if one ever does.
    (when buffered
      (throw (IllegalStateException. "A segment read ahead cannot switch to binary data")))
    (.setBinaryDataLength r length))
  (nextTag [_]
    (when buffered
      (throw (UnsupportedOperationException. "nextTag inside a segment read ahead")))
    (.nextTag r))
  (close [_] (.close r))
  (getProperty [_ name] (.getProperty r name))
  (getDelimiters [_] (.getDelimiters r))
  (getStandard [_] (.getStandard r))
  (getVersion [_] (.getVersion r))
  (getTransactionVersion [_] (.getTransactionVersion r))
  (getTransactionVersionString [_] (.getTransactionVersionString r))
  (getTransactionType [_] (.getTransactionType r))
  (getControlSchema [_] (.getControlSchema r))
  (setControlSchema [_ schema] (.setControlSchema r schema))
  (getTransactionSchema [_] (.getTransactionSchema r))
  (setTransactionSchema [_ schema] (.setTransactionSchema r schema)))

(extend-protocol SegmentLookahead
  EDIStreamReader
  (segment-values [_]
    (throw (ex-info (str "This slot shares its segment id with a sibling, and telling "
                         "them apart means reading a segment ahead. `make-parser` "
                         "arranges that; to run a loop or segment parser directly, "
                         "wrap the reader with `lookahead-reader` first.")
                    {}))))

(defn lookahead-reader
  "A reader a parser can read one segment ahead on. `make-parser` wraps its
   reader itself; call this only to run a loop or segment parser directly."
  ^EDIStreamReader [r]
  (if (instance? LookaheadReader r) r (LookaheadReader. r nil 0 nil)))

(defn first-segment [sch]
  (when-some [nm (next-map-sch sch)]
    (case (-> nm m/properties :type)
      :loop (when-some [[_ _ fchild] (-> nm m/children first)]
              ;(prn fchild)
              (recur fchild))
      :segment nm)))

(defn- enum-codes [sch]
  (let [sch (m/deref sch)]
    (when (= :enum (m/type sch))
      (set (m/children sch)))))

(defn- code-lists
  "Every code list a segment schema pins, as `{[element component] #{code}}`."
  [seg]
  (into {}
        (mapcat (fn [[_ {e :sequence} child]]
                  (if (= :composite (next-map-type child))
                    (for [[_ {c :sequence} component] (m/children (next-map-sch child))
                          :let [codes (enum-codes component)]
                          :when codes]
                      [[e c] codes])
                    (when-some [codes (enum-codes child)]
                      [[[e 1] codes]]))))
        (m/children seg)))

(defn- where-code-lists-differ
  "`[[position ours theirs] ...]` for every position both segment schemas pin
   with a code list, and pin differently."
  [seg rival]
  (let [theirs (code-lists rival)]
    (into []
          (keep (fn [[position ours]]
                  (when-some [t (get theirs position)]
                    (when (not= ours t) [position ours t]))))
          (code-lists seg))))

(defn- the-rivals?
  "True when a segment's values fit a rival's code lists and not ours: some
   value only the rival admits, and none only we admit.

   A segment carrying evidence BOTH ways is not the rival's. It stays where its
   tag puts it and fails that slot's validation, naming the element -- which is
   the better error than a segment no slot claimed."
  [differences values]
  (= #{:theirs}
     (into #{}
           (keep (fn [[position ours theirs]]
                   (when-some [v (get values position)]
                     (cond (and (theirs v) (not (ours v))) :theirs
                           (and (ours v) (not (theirs v))) :ours))))
           differences)))

(defn- takes-segment?
  "Whether the segment the reader is on belongs to the slot whose segment -- or
   whose loop's first segment -- is `seg`. `follow` holds the segment schemas of
   every slot that could take the segment instead: later siblings, and whatever
   may follow the enclosing loops.

   Only a rival with the same segment id and a different code list somewhere
   makes the slot read ahead. Every other slot decides on the tag alone and
   never reads ahead, which keeps lookahead away from binary segments: their
   length has to be set before their payload is read."
  [seg follow]
  (let [tag (-> seg m/properties :segment-id)
        contests (into []
                       (comp (filter #(= tag (-> % m/properties :segment-id)))
                             (map #(where-code-lists-differ seg %))
                             (remove empty?)
                             (distinct))
                       follow)]
    (if (empty? contests)
      (fn [^EDIStreamReader r] (= tag (-> r .getLocation .getSegmentTag)))
      (fn [^EDIStreamReader r]
        (and (= tag (-> r .getLocation .getSegmentTag))
             (= "START_SEGMENT" (event-name r))
             (let [values (segment-values r)]
               (not-any? #(the-rivals? % values) contests)))))))

(defn make-segment-parser
  ([k meta sch] (make-segment-parser k meta sch []))
  ([k meta sch follow]
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
         takes? (takes-segment? nm follow)
         validator (m/coercer nm)
         binary-keys (bin-keys nm)
         parse-one (fn [^EDIStreamReader r]
                     (let [reader-managed? (and binary-keys (reader-manages-binary-length? r))
                           _ (.next r)
                           expected (when binary-keys (start-bin! r reader-managed?))
                           data (into {} (map #(% r)) sub-parsers)
                           ;; StAEDI truncates an empty final element when its
                           ;; TRUNCATE_EMPTY_ELEMENTS option is enabled.
                           data (if (and (= 0 expected)
                                         (= "END_SEGMENT" (event-name r))
                                         (not (contains? data (second binary-keys))))
                                  (assoc data (second binary-keys) (byte-array 0))
                                  data)]
                       (when binary-keys
                         (check-bin data binary-keys expected)
                         (when-not (= "END_SEGMENT" (event-name r))
                           (throw (ex-info "Expected segment end after BIN02"
                                           {:event (event-name r)}))))
                       (validator data)
                       (consume-segment r)
                       data))]
     (if collection?
       (fn [r]
         (assert (= (event-name r) "START_SEGMENT"))
         (loop [data []]
           (if (and (= (event-name r) "START_SEGMENT")
                    (takes? r))
             (recur (conj data (parse-one r)))
             (when-some [coll (not-empty data)] [k coll]))))
       (fn [r]
         (assert (= (event-name r) "START_SEGMENT"))
         (when (takes? r)
           [k (parse-one r)]))))))

(defn make-component-unparser [k meta sub-schema epos]
  (let [unparser (make-primitive-component-unparser sub-schema)]
    unparser))

(defn empty-component-unparser [cnt]
  (fn [w data]
    (loop [cnt cnt]
      (when (pos? cnt)
        (do (.writeEmptyComponent w)
            (recur (dec cnt)))))))

(defn make-composite-unparser [k meta sch epos]
  (let [nm (next-map-sch sch)
        collection? (case (m/type sch)
                      (:sequential :vector :set) true
                      false)
        element-pos (volatile! 0)
        sub-unparsers (into []
                            (mapcat (fn [[k meta sub-schema]]
                                      (let [epos (inc @element-pos)]
                                        (vreset! element-pos (-> meta :sequence))
                                        ;; Pads a hole the schema leaves before this
                                        ;; component, as the segment unparser does for
                                        ;; elements.
                                        (-> (if (> (-> meta :sequence) epos)
                                              [[nil (empty-component-unparser (- (-> meta :sequence) epos))]]
                                              [])
                                            (conj
                                             (case (next-map-type sub-schema)
                                                ;:composite [k (make-composite-unparser k meta sub-schema epos)]
                                               nil [k (make-component-unparser k meta sub-schema epos)]))))))
                            (m/children nm))
        tag (-> nm m/properties :segment-id)]
    (if collection?
      (fn [w data]
        (run!
         (fn [data]
           (.writeStartElement w)
           (run! (fn [[k unparse]]
                   (unparse w (get data k)))
                 sub-unparsers)
           (.endElement w))
         data))
      (fn [w data]
        (if data
          (do
            (.writeStartElement w)
            (run! (fn [[k unparse]]
                    (unparse w (get data k)))
                  sub-unparsers)
            (.endElement w))
          (.writeEmptyElement w))))))

(defn make-element-unparser [k meta sub-schema epos]
  (let [unparser (make-primitive-unparser sub-schema)]
    unparser))

(defn empty-element-unparser [cnt]
  (fn [w data]
    (loop [cnt cnt]
      (when (pos? cnt)
        (do (.writeEmptyElement w)
            (recur (dec cnt)))))))

(defn make-segment-unparser [k meta sch]
  (let [nm (next-map-sch sch)
        collection? (case (m/type sch)
                      (:sequential :vector :set) true
                      false)
        element-pos (volatile! 0)
        sub-unparsers (into []
                            (mapcat (fn [[k meta sub-schema]]
                                      (let [epos (inc @element-pos)]
                                        (vreset! element-pos (-> meta :sequence))
                                        (-> (if (> (-> meta :sequence) epos)
                                              [[nil (empty-element-unparser (- (-> meta :sequence) epos))]]
                                              [])
                                            (conj
                                             (case (next-map-type sub-schema)
                                               :composite [k (make-composite-unparser k meta sub-schema epos)]
                                               nil [k (make-element-unparser k meta sub-schema epos)]))))))
                            (m/children nm))
        tag (-> nm m/properties :segment-id)
        binary-keys (bin-keys nm)
        write-one (fn [^EDIStreamWriter w data]
                    (let [data (if binary-keys (check-bin data binary-keys nil) data)]
                      (.writeStartSegment w tag)
                      (run! (fn [[k unparse]] (unparse w (get data k))) sub-unparsers)
                      (.writeEndSegment w)))]
    (if collection?
      (fn [w data] (run! #(write-one w %) data))
      write-one)))

(declare make-loop-parser)

(defn- slot-parsers
  "Parsers for the slots of one loop or transaction set, in schema order.

   Each is told what could take a segment if it does not: every later slot in
   this body, then `after`, which is what may follow the body itself. A slot
   needs that list to tell its own segments from a same-id sibling's."
  [children after]
  (let [children (vec children)
        firsts (mapv (fn [[_ _ sch]] (first-segment sch)) children)]
    (into []
          (map-indexed
           (fn [i [k meta sch]]
             (let [follow (-> [] (into (remove nil?) (subvec firsts (inc i))) (into after))]
               (case (next-map-type sch)
                 :loop (make-loop-parser k meta sch follow)
                 :segment (make-segment-parser k meta sch follow)))))
          children)))

(defn make-loop-parser
  ([k meta sch] (make-loop-parser k meta sch []))
  ([k meta sch follow]
   (if-some [nm (next-map-sch sch)]
     (let [collection? (case (m/type sch)
                         (:sequential :vector :set) true
                         false)
           seg (first-segment nm)
           ;; A repeating loop's body can be followed by the loop again.
           sub-parsers (slot-parsers (m/children nm) (if (and collection? seg) (into [seg] follow) follow))
           starts? (if seg (takes-segment? seg follow) (constantly false))
           ;; An iteration that consumed nothing is not an iteration. Its first
           ;; slot can pass on a segment the loop itself would take, when the
           ;; rival that owns it is a later slot of this same loop.
           iteration (fn [r] (not-empty (into {} (map (fn [sub-parser] (sub-parser r))) sub-parsers)))]
       (if collection?
         (fn [^EDIStreamReader r]
           (assert (= (.name (.getEventType r)) "START_SEGMENT"))
           (loop [data []]
             (if-some [m (when (starts? r) (iteration r))]
               (recur (conj data m))
               (when-some [v (not-empty data)]
                 [k v]))))
         (fn [^EDIStreamReader r]
           (assert (= (.name (.getEventType r)) "START_SEGMENT"))
           (when-some [m (when (starts? r) (iteration r))]
             [k m]))))
     (assert false)
     #_(fn [r] nil))))

(defn make-transaction-parser [sch]
  (let [sub-parsers (slot-parsers (m/children sch) [])]
    (fn [r]
      (into {}
            (map (fn [sub-parser]
                   (sub-parser r)))
            sub-parsers))))

(defn make-transactions-parser [k meta sch]
  (let [sub-parsers (slot-parsers (-> sch m/children first m/children) [])]
    (fn [r]
      ;(prn tag)
      (assert (= (.name (.getEventType r)) "START_TRANSACTION"))
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

(defn make-group-parser [k meta sch]
  (let [sub-parsers (into []
                          (map (fn [[k meta sub-schema]]
                                 (case (next-map-type sub-schema)
                                   :segment (make-segment-parser k meta sub-schema)
                                   :transaction-set  (make-transactions-parser k meta sub-schema))))
                          (-> sch m/children first m/children))]
    (fn [r]
      (assert (= (.name (.getEventType r)) "START_GROUP") (.name (.getEventType r)))
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

(defn make-loop-unparser [k meta sch]
  (if-some [nm (next-map-sch sch)]
    (let [collection? (case (m/type sch)
                        (:sequential :vector :set) true
                        false)
          sub-unparsers (into []
                              (keep (fn [[k meta sub-schema]]
                                      (case (next-map-type sub-schema)
                                        :segment [k (make-segment-unparser k meta sub-schema)]
                                        :loop  [k (make-loop-unparser k meta sub-schema)])))
                              (m/children nm))
          tag (some-> nm first-segment m/properties :segment-id)]
      (if collection?
        (fn [w data]
          (run!
           (fn [transaction-set-data]
             (run!
              (fn [[k unparser]]
                (when-some [subdata (get transaction-set-data k)]
                  (unparser w subdata)))
              sub-unparsers))
           data))
        (fn [w transaction-set-data]
          (run!
           (fn [[k unparser]]
             (when-some [subdata (get transaction-set-data k)]
               (unparser w subdata)))
           sub-unparsers))))
    (assert false)
    #_(fn [r] nil)))

(defn make-transactions-unparser [k meta sch]
  (let [sub-unparsers (into []
                            (keep (fn [[k meta sub-schema]]
                                    (case (next-map-type sub-schema)
                                      :segment [k (make-segment-unparser k meta sub-schema)]
                                      :loop  [k (make-loop-unparser k meta sub-schema)])))
                            (-> sch m/children first m/children))]
    (fn [^EDIStreamWriter w data]
      (run!
       (fn [transaction-set-data]
         (run!
          (fn [[k unparser]]
            (when-some [subdata (get transaction-set-data k)]
              (unparser w subdata)))
          sub-unparsers))
       data))))

(defn make-group-unparser [k meta sch]
  (let [sub-unparsers (into []
                            (keep (fn [[k meta sub-schema]]
                                    (case (next-map-type sub-schema)
                                      :segment [k (make-segment-unparser k meta sub-schema)]
                                      :transaction-set  [k (make-transactions-unparser k meta sub-schema)])))
                            (-> sch m/children first m/children))]
    (fn [^EDIStreamWriter w data]
      (run!
       (fn [group-data]
         (run!
          (fn [[k unparser]]
            (when-some [subdata (get group-data k)]
              (unparser w subdata)))
          sub-unparsers))
       data))))

(defn make-interchange-unparser [sch]
  (let [sub-unparsers (into []
                            (keep (fn [[k meta sub-schema]]
                                    (case (next-map-type sub-schema)
                                      :segment [k (make-segment-unparser k meta sub-schema)]
                                      :group [k (make-group-unparser k meta sub-schema)])))
                            (m/children sch))]
    (fn [^EDIStreamWriter w data]
      (let [sf (SchemaFactory/newFactory)
            schema (.getControlSchema sf EDIStreamConstants$Standards/X12 (into-array String ["00501"]))]
        (.setControlSchema w schema)
        (.startInterchange w)
        (run!
         (fn [[k unparser]]
           (when-some [subdata (get data k)]
             (unparser w subdata)))
         sub-unparsers)
        (.endInterchange w)))))
(defn make-unparser [sch]
  (assert (-> sch m/properties :type (= :interchange)))
  (make-interchange-unparser sch))

(defonce default-output-factory  (EDIOutputFactory/newFactory))
(.setProperty default-output-factory EDIOutputFactory/PRETTY_PRINT true)
(.setProperty default-output-factory EDIOutputFactory/TRUNCATE_EMPTY_ELEMENTS true)

(defonce default-input-factory  (EDIInputFactory/newFactory))
#_(.setProperty default-input-factory EDIInputFactory/EDI_IGNORE_EXTRANEOUS_CHARACTERS true)
(.setProperty default-input-factory EDIInputFactory/EDI_VALIDATE_CONTROL_STRUCTURE true)

(comment

  (require 'malli.dev)
  (malli.dev/start!)
  (def sch (-> (io/resource "x12_271.edn")
               io/reader
               PushbackReader.
               edn/read
               (m/schema {:registry (merge (m/default-schemas) (malli.experimental.time/schemas))})))

  (with-open [r (.createEDIStreamReader default-input-factory (io/input-stream (io/resource
                                                                                #_"270-3.edi"
                                                                                #_"271/section6-3.edi"
                                                                                "271/sample-Acacianna.edi"
                                                                                #_"271/sample-BaxterDallesandro.edi"
                                                                                #_"simple_with_binary_segment.edi"
                                                                                #_"sample837-original.edi")))]
    (let [consumer (make-parser sch)
          #_(make-parser sch)]
      (def edi-out (consumer r))))

  edi-out
  (with-open [r (.createEDIStreamWriter default-output-factory (io/output-stream "out.edi"))]
    (let [producer (make-unparser sch)
          #_(make-parser sch)]
      (producer r edi-out))))


