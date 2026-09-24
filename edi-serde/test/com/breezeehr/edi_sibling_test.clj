(ns com.breezeehr.edi-sibling-test
  "Sibling slots that share a segment id.

   An implementation guide often gives two slots in one place the same segment
   id and tells them apart by a qualifier: an 837's 2000A, 2000B and 2000C all
   begin with HL and differ in HL03; a 2300 carries a separate DTP slot per
   DTP01. The parser used to choose a slot by the segment's TAG alone, so the
   first slot in schema order took every segment of that id:

     * a repeating LOOP went on consuming its siblings' segments -- the 837
       subscriber's `HL*2*1*22*1` was fed back to the 2000A parser and died
       coercing `22` against 2000A's HL03, so no 837 could be read at all;
     * a single SEGMENT slot took the first segment of its id and failed
       validation against its own qualifier, so only a PREFIX of the schema's
       order parsed;
     * a repeating SEGMENT slot swallowed every later segment of its id without
       complaint, so they came back under the wrong key.

   Every schema below is cut down from a real guide shape, and each test names
   the one it stands for."
  (:require [clojure.test :refer [deftest is testing]]
            [malli.core :as m]
            [com.breezeehr.edi-serde :as serde])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

;; ---------------------------------------------------------------------------
;; Schemas
;; ---------------------------------------------------------------------------

(defn- text-segment [tag n]
  (into [:map {:type :segment :segment-id tag}]
        (for [i (range 1 (inc n))]
          [(keyword (str "e" i)) {:sequence i :optional true} :string])))

(defn- segment [tag & elements]
  (into [:map {:type :segment :segment-id tag}] elements))

(defn- interchange-schema
  "An interchange around one transaction set whose body is `slots`."
  [& slots]
  (m/schema
   [:map {:type :interchange}
    [:isa (text-segment "ISA" 16)]
    [:groups
     [:sequential
      [:map {:type :group}
       [:gs (text-segment "GS" 8)]
       [:transactions
        [:sequential
         (-> [:map {:type :transaction-set} [:st (text-segment "ST" 3)]]
             (into slots)
             (conj [:se (text-segment "SE" 2)]))]]
       [:ge (text-segment "GE" 2)]]]]
    [:iea (text-segment "IEA" 2)]]))

(defn- hl [level child-codes]
  (segment "HL"
           [:id-01 {:sequence 1} :string]
           [:parent-02 {:sequence 2 :optional true} :string]
           [:level-03 {:sequence 3} [:enum level]]
           [:child-04 {:sequence 4} (into [:enum] child-codes)]))

(defn- nm1 [entity]
  (segment "NM1"
           [:entity-01 {:sequence 1} [:enum entity]]
           [:name-03 {:sequence 3} :string]))

(defn- dtp [& qualifiers]
  (segment "DTP"
           [:qualifier-01 {:sequence 1} (into [:enum] qualifiers)]
           [:format-02 {:sequence 2} [:enum "D8"]]
           [:date-03 {:sequence 3} :string]))

(defn- ref-segment [& qualifiers]
  (segment "REF"
           [:qualifier-01 {:sequence 1} (into [:enum] qualifiers)]
           [:value-02 {:sequence 2} :string]))

(defn- hi [& qualifiers]
  (segment "HI"
           [:code-01 {:sequence 1}
            [:map {:type :composite}
             [:qualifier-01 {:sequence 1} (into [:enum] qualifiers)]
             [:code-02 {:sequence 2} :string]]]))

(defn- pwk [report-types transmissions]
  (segment "PWK"
           [:report-type-01 {:sequence 1} (into [:enum] report-types)]
           [:transmission-02 {:sequence 2} (into [:enum] transmissions)]))

(def ^:private claim-2300
  "A 2300 cut down to the same-id slot runs a real one carries."
  ["2300" {:optional true}
   [:sequential
    [:map {:type :loop}
     [:clm (segment "CLM" [:id-01 {:sequence 1} :string])]
     ;; DTP: one slot per DTP01, the last of them repeating.
     [:onset {:optional true} (dtp "431")]
     [:initial-treatment {:optional true} (dtp "454")]
     [:assumed-care {:optional true} [:sequential (dtp "090" "091")]]
     ;; REF: two REPEATING slots whose code lists overlap in `2U`, as the
     ;; generated 837P 2400 prior-authorization and referral-number slots do.
     [:prior-authorization {:optional true} [:sequential (ref-segment "G1" "2U")]]
     [:referral-number {:optional true} [:sequential (ref-segment "9F" "2U")]]
     ;; PWK: the first element OVERLAPS (both admit CT) and only the second
     ;; tells the slots apart, as in the 837P 2400.
     [:supplemental {:optional true} [:sequential (pwk ["03" "CT" "OB"] ["BM" "EL" "FX"])]]
     [:dme-certificate {:optional true} (pwk ["CT"] ["AB" "AD"])]
     ;; HI: the qualifier is component 1 of a composite.
     [:diagnosis {:optional true} (hi "ABK" "BK")]
     [:condition {:optional true} [:sequential (hi "BG")]]]]])

(def flat-837
  "The 837 shape: 2000A, 2000B and 2000C are SIBLING repeating loops under the
   transaction set, told apart only by HL03."
  (interchange-schema
   ["2000A" [:sequential [:map {:type :loop}
                          [:hl (hl "20" ["1"])]
                          [:billing-provider {:optional true} (nm1 "85")]]]]
   ["2000B" [:sequential [:map {:type :loop}
                          [:hl (hl "22" ["0" "1"])]
                          [:sbr {:optional true} (segment "SBR" [:payer-01 {:sequence 1} :string])]]]]
   ["2000C" {:optional true}
    [:sequential [:map {:type :loop}
                  [:hl (hl "23" ["0"])]
                  claim-2300]]]))

(def nested-270
  "The 270 shape: the HL levels NEST, so a dependent 2000D ends when the next
   SUBSCRIBER begins -- a sibling of its enclosing 2000C, not of itself."
  (interchange-schema
   ["2000A" [:sequential
             [:map {:type :loop}
              [:hl (hl "20" ["1"])]
              ["2000B" [:sequential
                        [:map {:type :loop}
                         [:hl (hl "21" ["1"])]
                         ["2000C" [:sequential
                                   [:map {:type :loop}
                                    [:hl (hl "22" ["0" "1"])]
                                    [:subscriber (nm1 "IL")]
                                    ["2000D" {:optional true}
                                     [:sequential
                                      [:map {:type :loop}
                                       [:hl (hl "23" ["0"])]
                                       [:dependent (nm1 "03")]]]]]]]]]]]]]))

;; ---------------------------------------------------------------------------
;; Reading and writing
;; ---------------------------------------------------------------------------

(defn- interchange [& segments]
  (str "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
       "*260923*1200*^*00501*000000001*0*T*:~"
       "GS*HC*SENDER*RECEIVER*20260923*1200*1*X*005010X222A1~"
       "ST*837*0001*005010X222A1~"
       (apply str (map #(str % "~") segments))
       "SE*" (+ 2 (count segments)) "*0001~GE*1*1~IEA*1*000000001~"))

(defn- reader-on [^bytes b]
  (.createEDIStreamReader serde/default-input-factory (ByteArrayInputStream. b)))

(defn- parse-bytes [schema ^bytes b]
  (with-open [r (reader-on b)]
    ((serde/make-parser schema) r)))

(defn- unparse [schema data]
  (let [out (ByteArrayOutputStream.)]
    (with-open [w (.createEDIStreamWriter serde/default-output-factory out)]
      ((serde/make-unparser schema) w data))
    (.toByteArray out)))

(defn- parse-interchange [schema segments]
  (parse-bytes schema (.getBytes ^String (apply interchange segments) "US-ASCII")))

(defn- body
  "The transaction set of a parsed interchange, without ST and SE."
  [parsed]
  (-> parsed (get-in [:groups 0 :transactions 0]) (dissoc :st :se)))

(defn- parse [schema & segments]
  (body (parse-interchange schema segments)))

(defn- claim [& segments]
  (-> (apply parse flat-837 "HL*1**20*1" "HL*2*1*22*1" "HL*3*2*23*0" "CLM*A1" segments)
      (get-in ["2000C" 0 "2300" 0])
      (dissoc :clm)))

;; ---------------------------------------------------------------------------

(deftest sibling-loops-that-share-a-first-segment-are-told-apart
  (testing "the 837 defect itself. Every 837 has at least two HLs, and the
            second is the subscriber's, so before this nothing could be read
            back at all: `HL*2*1*22*1` went to the 2000A parser and failed
            coercing 22 against its HL03 enum."
    (is (= {"2000A" [{:hl {:id-01 "1" :level-03 "20" :child-04 "1"}
                      :billing-provider {:entity-01 "85" :name-03 "ACME"}}]
            "2000B" [{:hl {:id-01 "2" :parent-02 "1" :level-03 "22" :child-04 "1"}
                      :sbr {:payer-01 "P"}}]
            "2000C" [{:hl {:id-01 "3" :parent-02 "2" :level-03 "23" :child-04 "0"}
                      "2300" [{:clm {:id-01 "A1"}}]}]}
           (parse flat-837
                  "HL*1**20*1" "NM1*85**ACME"
                  "HL*2*1*22*1" "SBR*P"
                  "HL*3*2*23*0" "CLM*A1"))))
  (testing "a repeating loop still repeats, and stops where its sibling
            begins"
    (let [parsed (parse flat-837
                        "HL*1**20*1" "HL*2**20*1"
                        "HL*3*2*22*0" "HL*4*2*22*0")]
      (is (= ["1" "2"] (mapv #(get-in % [:hl :id-01]) (get parsed "2000A"))))
      (is (= ["3" "4"] (mapv #(get-in % [:hl :id-01]) (get parsed "2000B"))))
      (is (not (contains? parsed "2000C"))
          "an optional sibling with nothing to read stays absent"))))

(deftest a-nested-loop-yields-to-its-enclosing-loops-next-iteration
  (testing "the 270's repeated subscriber. The dependent 2000D's rival for
            the next HL is not a sibling of its own but the 2000C around it,
            which may repeat -- so what can follow a slot has to include what
            can follow every loop that encloses it."
    (let [parsed (parse nested-270
                        "HL*1**20*1" "HL*2*1*21*1"
                        "HL*3*2*22*1" "NM1*IL**FIRST"
                        "HL*4*3*23*0" "NM1*03**CHILD"
                        "HL*5*2*22*0" "NM1*IL**SECOND")
          subscribers (get-in parsed ["2000A" 0 "2000B" 0 "2000C"])]
      (is (= ["FIRST" "SECOND"] (mapv #(get-in % [:subscriber :name-03]) subscribers)))
      (is (= ["CHILD"] (mapv #(get-in % [:dependent :name-03]) (get (first subscribers) "2000D"))))
      (is (not (contains? (second subscribers) "2000D")))))
  (testing "and a subscriber with no dependents does not hand the next
            subscriber to an empty 2000D"
    (let [parsed (parse nested-270
                        "HL*1**20*1" "HL*2*1*21*1"
                        "HL*3*2*22*0" "NM1*IL**FIRST"
                        "HL*4*2*22*0" "NM1*IL**SECOND")]
      (is (= 2 (count (get-in parsed ["2000A" 0 "2000B" 0 "2000C"])))))))

(deftest single-segment-slots-with-one-id-are-told-apart-by-qualifier
  (testing "the PREFIX defect: a claim with an initial-treatment date and no
            onset date. The onset slot took `DTP*454` and failed validation
            against its own DTP01."
    (is (= {:initial-treatment {:qualifier-01 "454" :format-02 "D8" :date-03 "20260101"}}
           (claim "DTP*454*D8*20260101"))))
  (testing "slots are still filled in schema order, and a repeating one still
            repeats"
    (is (= {:onset {:qualifier-01 "431" :format-02 "D8" :date-03 "20251201"}
            :assumed-care [{:qualifier-01 "090" :format-02 "D8" :date-03 "20260102"}
                           {:qualifier-01 "091" :format-02 "D8" :date-03 "20260103"}]}
           (claim "DTP*431*D8*20251201" "DTP*090*D8*20260102" "DTP*091*D8*20260103")))))

(deftest a-repeating-slot-does-not-swallow-its-siblings-segments
  (testing "the SILENT defect: no error, just every REF under the first
            repeating REF key."
    (is (= {:prior-authorization [{:qualifier-01 "G1" :value-02 "AUTH1"}
                                  {:qualifier-01 "G1" :value-02 "AUTH2"}]
            :referral-number [{:qualifier-01 "9F" :value-02 "REF1"}]}
           (claim "REF*G1*AUTH1" "REF*G1*AUTH2" "REF*9F*REF1"))))
  (testing "a code BOTH slots admit stays with the first. The schema cannot
            say whose it is, so neither can the parser, and nor could
            backtracking: both readings validate."
    (is (= {:prior-authorization [{:qualifier-01 "2U" :value-02 "PAYER"}]}
           (claim "REF*2U*PAYER")))))

(deftest the-qualifier-is-wherever-the-code-lists-differ
  (testing "inside a composite: HI's qualifier is HI01-1"
    (is (= {:condition [{:code-01 {:qualifier-01 "BG" :code-02 "01"}}
                        {:code-01 {:qualifier-01 "BG" :code-02 "02"}}]}
           (claim "HI*BG:01" "HI*BG:02")))
    (is (= {:diagnosis {:code-01 {:qualifier-01 "ABK" :code-02 "J449"}}
            :condition [{:code-01 {:qualifier-01 "BG" :code-02 "01"}}]}
           (claim "HI*ABK:J449" "HI*BG:01"))))
  (testing "not in the first element: both PWK slots admit CT in PWK01, and
            only PWK02 separates them. Choosing on the first differing element
            would send `PWK*CT*AB` to the wrong slot."
    (is (= {:dme-certificate {:report-type-01 "CT" :transmission-02 "AB"}}
           (claim "PWK*CT*AB")))
    (is (= {:supplemental [{:report-type-01 "03" :transmission-02 "EL"}
                           {:report-type-01 "CT" :transmission-02 "FX"}]
            :dme-certificate {:report-type-01 "CT" :transmission-02 "AD"}}
           (claim "PWK*03*EL" "PWK*CT*FX" "PWK*CT*AD")))))

(defn- coercion-failure [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e
         (when (= :malli.core/coercion (:type (ex-data e)))
           (-> e ex-data :data :explain :errors first (select-keys [:in :value]))))))

(deftest a-segment-no-sibling-can-take-fails-where-its-tag-sends-it
  (testing "a qualifier NO slot admits is still reported by the slot its tag
            selects, naming the element -- not as a segment nobody claimed"
    (is (= {:in [:level-03] :value "99"}
           (coercion-failure #(parse flat-837 "HL*1**20*1" "HL*2*1*99*1")))))
  (testing "a segment with evidence BOTH ways stays where its tag sends it.
            HL04 `0` is admitted only by 2000B and HL03 `20` only by 2000A, so
            neither slot's claim is clean; the segment stays in 2000A and fails
            there on the element that is actually wrong."
    (is (= {:in [:child-04] :value "0"}
           (coercion-failure #(parse flat-837 "HL*1**20*0"))))))

(deftest what-the-writer-writes-the-reader-reads-back
  (doseq [[label schema segments]
          [["flat 837" flat-837
            ["HL*1**20*1" "NM1*85**ACME" "HL*2*1*22*1" "SBR*P" "HL*3*2*23*0"
             "CLM*A1" "DTP*454*D8*20260101" "DTP*090*D8*20260102"
             "REF*G1*AUTH1" "REF*9F*REF1" "PWK*CT*FX" "PWK*CT*AD"
             "HI*ABK:J449" "HI*BG:01" "CLM*A2" "DTP*431*D8*20251201"]]
           ["nested 270" nested-270
            ["HL*1**20*1" "HL*2*1*21*1" "HL*3*2*22*1" "NM1*IL**FIRST"
             "HL*4*3*23*0" "NM1*03**CHILD" "HL*5*2*22*0" "NM1*IL**SECOND"]]]]
    (testing label
      (let [parsed (parse-interchange schema segments)
            reparsed (parse-bytes schema (unparse schema parsed))]
        (is (= parsed reparsed) "parse -> unparse -> parse is stable")
        (is (= (count segments)
               (- (count (re-seq #"~" (String. (unparse schema parsed) "US-ASCII"))) 6))
            "and nothing was dropped on the way: every body segment is written
             back (six are envelope: ISA GS ST SE GE IEA)")))))

;; ---------------------------------------------------------------------------
;; The reader underneath
;; ---------------------------------------------------------------------------

(defn- event-trace
  "Every event in `b` as [event text element component], calling
   `segment-values` at each START_SEGMENT when `peek?`."
  [^bytes b peek?]
  (with-open [raw (reader-on b)]
    (let [r (if peek? (serde/lookahead-reader raw) raw)]
      (loop [acc []]
        (if (.hasNext r)
          (let [event (.name (.next r))
                _ (when (and peek? (= "START_SEGMENT" event)) (serde/segment-values r))
                loc (.getLocation r)]
            (recur (conj acc [event (when (.hasText r) (.getText r))
                              (.getElementPosition loc) (.getComponentPosition loc)])))
          acc)))))

(deftest reading-ahead-changes-nothing-a-parser-sees
  (testing "a segment read ahead is replayed event for event: same events,
            same text, same positions -- composites included"
    (let [b (.getBytes ^String (interchange "HL*1**20*1" "HI*ABK:J449*BG:01" "REF*G1*A^B")
                       "US-ASCII")]
      (is (= (event-trace b false) (event-trace b true)))))
  (testing "and what it reads ahead is keyed the way the schema is: a simple
            element is component 1, a composite component is its own position"
    (with-open [r (serde/lookahead-reader
                   (reader-on (.getBytes ^String (interchange "HI*ABK:J449*BG") "US-ASCII")))]
      (loop []
        (.next r)
        (when-not (and (= "START_SEGMENT" (.name (.getEventType r)))
                       (= "HI" (.getSegmentTag (.getLocation r))))
          (recur)))
      (is (= {[1 1] "ABK" [1 2] "J449" [2 1] "BG"} (serde/segment-values r))))))

(deftest a-parser-run-directly-says-when-it-needs-to-read-ahead
  (testing "`make-parser` wraps its reader. A loop or segment parser run on a
            bare reader says what to do, rather than failing on a protocol"
    (let [onset (serde/make-segment-parser :onset {} (m/schema (dtp "431"))
                                           [(m/schema (dtp "454"))])
          b (.getBytes ^String (interchange "DTP*431*D8*20251201") "US-ASCII")
          at-dtp (fn [r] (while (not= "DTP" (.getSegmentTag (.getLocation r))) (.next r)) r)]
      (with-open [r (reader-on b)]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"lookahead-reader" (onset (at-dtp r)))))
      (with-open [r (serde/lookahead-reader (reader-on b))]
        (is (= "431" (:qualifier-01 (second (onset (at-dtp r))))))))))
