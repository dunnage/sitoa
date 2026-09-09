(ns com.breezeehr.edi-binary-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [malli.core :as m]
            [com.breezeehr.edi-serde :as serde])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Arrays]))

(def bin-schema
  [:map {:type :segment :segment-id "BIN"}
   [:length {:sequence 1} :int]
   [:payload {:sequence 2} [bytes? {:edi/data-type "B"}]]])

(defn text-segment [tag n]
  (into [:map {:type :segment :segment-id tag}]
        (for [i (range 1 (inc n))]
          [(keyword (str "e" i)) {:sequence i} :string])))

(def schema
  (m/schema
   [:map {:type :interchange}
    [:isa (text-segment "ISA" 16)]
    [:groups [:sequential
              [:map {:type :group}
               [:gs (text-segment "GS" 8)]
               [:transactions [:sequential
                               [:map {:type :transaction-set}
                                [:st (text-segment "ST" 3)]
                                [:bins [:sequential bin-schema]]
                                [:ref (text-segment "REF" 2)]
                                [:se (text-segment "SE" 2)]]]]
               [:ge (text-segment "GE" 2)]]]]
    [:iea (text-segment "IEA" 2)]]))

(def header
  (str "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
       "*260908*1407*^*00501*000000001*0*T*:~"
       "GS*PI*SENDER*RECEIVER*20260908*1407*1*X*005010X210~"
       "ST*275*0001*005010X210~"))

(defn wire [& parts]
  (let [out (ByteArrayOutputStream.)]
    (doseq [part parts]
      (.write out ^bytes (if (bytes? part) part (.getBytes (str part) "US-ASCII"))))
    (.toByteArray out)))

(defn fixture [& payloads]
  (apply wire header
         (concat (mapcat (fn [p] [(str "BIN*" (alength ^bytes p) "*") p "~"]) payloads)
                 [(str "REF*ZZ*AFTER~SE*" (+ 3 (count payloads)) "*0001~GE*1*1~IEA*1*000000001~")])))

(defn parse [input]
  (with-open [r (.createEDIStreamReader serde/default-input-factory (ByteArrayInputStream. input))]
    ((serde/make-parser schema) r)))

(defn write-message [data]
  (let [out (ByteArrayOutputStream.)]
    (with-open [w (.createEDIStreamWriter serde/default-output-factory out)]
      ((serde/make-unparser schema) w data))
    (.toByteArray out)))

(def bin-path [:groups 0 :transactions 0 :bins])
(defn bins [data] (get-in data bin-path))
(def all-bytes (byte-array (map unchecked-byte (range 256))))

(defn normalized [data]
  (update-in data bin-path #(mapv (fn [bin] (update bin :payload vec)) %)))

(deftest arbitrary-bytes-round-trip
  (doseq [payload [(byte-array 0) (wire "*~:^\r\n") all-bytes
                   (byte-array (take 65537 (cycle (seq all-bytes))))]]
    (testing (str "payload length " (alength ^bytes payload))
      (let [parsed (parse (fixture payload))
            output (write-message parsed)
            reparsed (parse output)]
        (is (Arrays/equals ^bytes payload ^bytes (:payload (first (bins parsed)))))
        (is (= (normalized parsed) (normalized reparsed)))
        (is (= "AFTER" (get-in reparsed [:groups 0 :transactions 0 :ref :e2])))
        (is (= (alength ^bytes payload) (:length (first (bins reparsed)))))))))

(deftest consecutive-binary-segments
  (let [parsed (parse (fixture all-bytes (wire "second~*:^")))
        reparsed (parse (write-message parsed))]
    (is (= 2 (count (bins reparsed))))
    (is (= (normalized parsed) (normalized reparsed)))))

(deftest output-length-is-derived-or-checked
  (let [parsed (parse (fixture all-bytes))
        missing (update-in parsed (conj bin-path 0) dissoc :length)]
    (is (= 256 (:length (first (bins (parse (write-message missing)))))))
    (doseq [length [-1 0 255 257 "256"]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"BIN01 does not match"
                           (write-message (assoc-in parsed (conj bin-path 0 :length) length)))))
    (doseq [payload [nil "text" [1 2 3]]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"BIN02 requires"
                           (write-message (assoc-in parsed (conj bin-path 0 :payload) payload)))))))

(deftest malformed-and-truncated-binary-is-rejected
  (doseq [length ["" "-1" "+1" "x" "1.5" "1000000000000000" "2147483648"]]
    (testing (str "invalid length " (pr-str length))
      (is (thrown? Exception (parse (wire header "BIN*" length "*a~"))))))
  (doseq [body ["BIN*3*ab" "BIN*3*abc" "BIN*2*abc~" "BIN*4*abc~"
                "BIN*0*a~" "BIN*3*abc*extra~" "BIN*3~"]]
    (testing body
      (is (thrown? Exception (parse (wire header body)))))))

(defn reader-at-bin []
  (let [r (.createEDIStreamReader serde/default-input-factory
                                  (ByteArrayInputStream. (fixture all-bytes)))
        schema (.createSchema (io.xlate.edi.schema.SchemaFactory/newFactory)
                              (io/resource "com/breezeehr/binary-schema.xml"))]
    (loop []
      (.next r)
      (when-not (= "START_TRANSACTION" (.name (.getEventType r))) (recur)))
    (.setTransactionSchema r schema)
    (loop []
      (.next r)
      (when-not (and (= "START_SEGMENT" (.name (.getEventType r)))
                     (= "BIN" (.getSegmentTag (.getLocation r))))
        (recur)))
    r))

(deftest reader-with-transaction-schema
  (with-open [r (reader-at-bin)]
    (let [[_ bin] ((serde/make-segment-parser :bin {} (m/schema bin-schema)) r)]
      (is (= 256 (:length bin)))
      (is (Arrays/equals all-bytes ^bytes (:payload bin)))
      (is (= "REF" (.getSegmentTag (.getLocation r)))))))

(deftest unclaimed-binary-events-are-drained
  (doseq [consume [#((serde/skip-elements 3 2) %)
                   serde/collect-extra-elements]]
    (with-open [r (reader-at-bin)]
      (.next r)
      (.next r)
      (is (= "ELEMENT_DATA_BINARY" (.name (.getEventType r))))
      (consume r)
      (when (= "END_SEGMENT" (.name (.getEventType r))) (.next r))
      (is (= "START_SEGMENT" (.name (.getEventType r))))
      (is (= "REF" (.getSegmentTag (.getLocation r)))))))

(deftest incorrect-lengths-do-not-succeed-with-a-valid-envelope
  (doseq [body ["BIN*2*abc~" "BIN*4*abc~" "BIN*0*a~" "BIN*3*abc*extra~"]]
    (is (thrown? Exception
                 (parse (wire header body "REF*ZZ*AFTER~SE*4*0001~GE*1*1~IEA*1*000000001~")))))
  (let [valid (fixture all-bytes)]
    ;; Cut inside BIN02, after its final byte, and inside the trailer.
    (doseq [end [(+ (count header) 8) (+ (count header) 264) (dec (alength valid))]]
      (is (thrown? Exception (parse (Arrays/copyOf valid end)))))))
