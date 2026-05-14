(ns dunnage.sitoa.unparser-test
  "Regression / reproduction tests for sitoa unparser issues."
  (:require
   ;; clojure.set is referenced unqualified inside parser.clj and must be
   ;; loaded before that ns compiles.
   [clojure.set]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.generators :as gen]
   [dunnage.sitoa.parser :as parser]
   [dunnage.sitoa.unparser :as unparser]
   [dunnage.sitoa.xml-primitives :as xml-primitives]
   [malli.core :as m]
   [malli.generator :as mg])
  (:import
   (java.io StringReader)
   (java.time OffsetDateTime)
   (javax.xml.stream XMLStreamReader)))

(defn- tiny-schema
  "Build an xml-primitives-based schema rooted at [:map [:val body-type]]."
  [body-type]
  (m/schema
   [:schema
    {:registry   {:test/Root [:map {:closed true} [:val {} body-type]]}
     :topElement "Root"}
    :test/Root]
   xml-primitives/external-registry))

(defn- mini-unparser [body-type]
  (unparser/xml-string-unparser (tiny-schema body-type)))

(defn- mini-parser [body-type]
  (let [p (parser/xml-parser (tiny-schema body-type))]
    (fn [xml-str]
      (with-open [r ^XMLStreamReader (parser/make-stream-reader {} (StringReader. xml-str))]
        (p r)))))

;; ---------- :Base64Binary (and friends) typed as :multi of :string ----------
;;
;; HISTORICAL CONTEXT
;; xmlschema-registry previously mapped several XSD types to :any:
;;   base64Binary, hexBinary, duration, dayTimeDuration, yearMonthDuration,
;;   gDay, gMonth, gMonthDay, gYear, gYearMonth.
;; The unparser dispatched :any to string-unparser, which calls
;; (.writeCharacters w data) — a direct cast to java.lang.String. malli's
;; default :any generator emits arbitrary values (Ratio, Vector, Map, Set,
;; nil, ...), and every non-String crashed with
;;
;;   class clojure.lang.<Foo> cannot be cast to class java.lang.String
;;
;; SURFACED BY
;; A generative XML round-trip test in the consuming furl project hit this
;; while exercising :script/Extension :xml/value, whose :Base64Data arm is
;; typed [:ref :org.w3.www.2001.XMLSchema/base64Binary].
;;
;; FIX (xml-primitives.clj)
;; Replaced :any with a shared schema:
;;
;;   (def xsd-textual-value
;;     [:multi {:dispatch class}
;;      [java.lang.String :string]])
;;
;; The :multi dispatches malli generation to the listed arm (String only,
;; for these textually-represented XSD types) and routes unparsing through
;; -multi-unparser → -string-unparser without the broken cast. Add more arms
;; if a real-world payload legitimately carries another Clojure type.

(deftest base64binary-typed-as-multi-of-string
  (testing "xmlschema-registry now uses xsd-textual-value (a :multi)"
    (is (= xml-primitives/xsd-textual-value
           (:org.w3.www.2001.XMLSchema/base64Binary xml-primitives/xmlschema-registry)))
    (is (= :multi (first xml-primitives/xsd-textual-value))))
  (testing "Unparsing a String body through xsd-textual-value works"
    (let [up (mini-unparser xml-primitives/xsd-textual-value)]
      (is (= "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root><val>hi</val></Root>"
             (up {:val "hi"})))))
  (testing "Generation from xsd-textual-value produces only Strings (no Ratio/Vector/Map)"
    (let [schema  (m/schema xml-primitives/xsd-textual-value xml-primitives/external-registry)
          samples (repeatedly 50 #(gen/generate (mg/generator schema) 10))]
      (is (every? string? samples)
          (str "Expected every sample to be String, got types: "
               (->> samples (map type) distinct))))))

;; ---------- Direct :any element body still crashes (independent of XSD fix) ----------
;;
;; The xml-primitives fix above only rerouted the *XSD* :any mappings
;; (base64Binary etc.) — the malli :any schema itself is still dispatched to
;; string-unparser, which casts data directly to java.lang.String. Any
;; consumer schema that uses :any directly will hit this fragility.
;;
;; PROPOSED FIX (unparser.clj line for :any): route :any through a coercing
;; handler that does (.writeCharacters w (str data)) instead of casting.

(deftest any-element-body-rejects-non-string-data
  (let [up (mini-unparser :any)]
    (testing "String body passes (baseline)"
      (is (= "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root><val>hi</val></Root>"
             (up {:val "hi"}))))
    (testing "Ratio body throws ClassCastException"
      (is (thrown-with-msg? ClassCastException #"Ratio cannot be cast"
                            (up {:val 1/2}))))
    (testing "Long body throws ClassCastException"
      (is (thrown-with-msg? ClassCastException #"Long cannot be cast"
                            (up {:val 42}))))
    (testing "Map / Vector / Set / Keyword all crash the same way"
      (is (thrown? ClassCastException (up {:val {}})))
      (is (thrown? ClassCastException (up {:val []})))
      (is (thrown? ClassCastException (up {:val #{}})))
      (is (thrown? ClassCastException (up {:val :kw}))))))

;; ---------- :int element body round-trips to BigDecimal ----------
;;
;; The unparser writes :int via string-encode-unparser (correct: emits the
;; integer as text). The parser, however, reads the text back through the
;; :decimal decode path (or an equivalent bigdec coercion), so the value
;; comes out as java.math.BigDecimal instead of Long. Any consumer comparing
;; via = will see the round-trip fail because (= 0 0M) is false.
;;
;; PROPOSED FIX (parser primitive path): when the target schema type is
;; :int, decode element text via Long/parseLong (or similar) rather than
;; bigdec. The unparser side is already correct.

(deftest int-element-body-roundtrips-as-bigdecimal
  (let [up    (mini-unparser :int)
        down  (mini-parser   :int)
        input 42
        xml   (up {:val input})
        parsed (:val (down xml))]
    (testing "Unparser emits the integer as plain text"
      (is (= "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root><val>42</val></Root>"
             xml)))
    (testing "Parser reads it back as BigDecimal, losing the Long type"
      (is (instance? java.math.BigDecimal parsed))
      (is (not= input parsed))
      (is (= 42M parsed)))))

;; ---------- :double has no entry in the unparser dispatch table ----------
;;
;; xmlschema-registry maps both :org.w3.www.2001.XMLSchema/float and
;; :org.w3.www.2001.XMLSchema/double to malli's :double. The unparser
;; dispatch (-xml-unparser, around line 716) handles :int, :decimal,
;; :string, :boolean, etc., but has no clause for :double. The case form
;; therefore raises IllegalArgumentException ("No matching clause: :double")
;; the moment a :double-typed element is unparsed.
;;
;; PROPOSED FIX (unparser.clj dispatch table)
;; Add :double (and :float, if applicable) to the case form, routed through
;; string-encode-unparser like :int / :decimal:
;;
;;   :double (string-encode-unparser x in-regex?)

(deftest double-element-body-unparser-missing
  (testing "Building an unparser over a :double element crashes — :double is not in the dispatch table"
    (is (thrown-with-msg? IllegalArgumentException #"No matching clause: :double"
                          (mini-unparser :double)))))

;; ---------- OffsetDateTime round-trip loses the timezone offset ----------
;;
;; CONTEXT
;; The unparser handler for :time/offset-date-time (line ~568) writes the
;; value via:
;;
;;   (.writeCharacters w (str (.toInstant data)))
;;
;; (.toInstant data) normalizes any OffsetDateTime to UTC, dropping the
;; offset. The parser then reads back a UTC OffsetDateTime. Result: an
;; OffsetDateTime with offset +05:00 (or any non-Zero offset) round-trips
;; to the same instant but with offset Z — they compare !=  for any data
;; consumer that distinguishes wall-clock from UTC.
;;
;; SURFACED BY
;; A generative XML round-trip test against the SCRIPT schema produced
;; OffsetDateTime values with arbitrary offsets (e.g. "1969-12-31T19:59:59.999-04:00")
;; in :SentTime / :OtherMedicationDate / Extension :DateTime arms. Round-trip
;; comparisons showed every such value collapse to Z.
;;
;; PROPOSED FIX (unparser.clj line ~568)
;; Write the value using its own toString (which preserves the offset):
;;
;;   (defn offset-datetime-unparser [x in-regex?]
;;     (if in-regex?
;;       (fn [^OffsetDateTime data pos ^XMLStreamWriter w]
;;         (.writeCharacters w (.toString data))
;;         (inc pos))
;;       (fn [^OffsetDateTime data ^XMLStreamWriter w]
;;         (.writeCharacters w (.toString data))
;;         true)))
;;
;; OffsetDateTime.toString emits ISO_OFFSET_DATE_TIME with the original
;; offset (e.g. "1969-12-31T19:59:59.999-04:00"). The existing parser path
;; already accepts ISO_OFFSET_DATE_TIME with any offset.

(deftest offset-datetime-roundtrip-drops-timezone-offset
  (let [up     (mini-unparser :time/offset-date-time)
        down   (mini-parser   :time/offset-date-time)
        input  (OffsetDateTime/parse "1969-12-31T19:59:59.999-04:00")
        xml    (up {:val input})
        parsed (:val (down xml))]
    (testing "XML body collapses the offset to Z (loses original -04:00)"
      (is (= "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root><val>1969-12-31T23:59:59.999Z</val></Root>"
             xml)))
    (testing "Same instant, different OffsetDateTime → not= round-trip"
      (is (= (.toInstant input) (.toInstant parsed))
          "Both name the same instant in time")
      (is (not= input parsed)
          "But OffsetDateTime equality includes the offset, so original != parsed"))))
