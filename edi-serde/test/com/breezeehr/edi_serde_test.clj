(ns com.breezeehr.edi-serde-test
  "Composite elements.

   An X12 element is either simple -- one value -- or composite: several
   components separated by the character ISA16 declares. The reader surfaces a
   composite as START_COMPOSITE, one ELEMENT_DATA per component, END_COMPOSITE,
   and that is what `composite-events-are-what-the-reader-emits` pins, because
   every parser here is written against it.

   This parser counted EVENTS where it meant to count POSITIONS, so a composite
   -- three events for one position -- broke `skip-elements`, which gated on
   ELEMENT_DATA and therefore matched nothing at a START_COMPOSITE. The reader
   was left parked on the composite, every remaining parser for that segment
   no-opped in turn, and the segment drained into a `case` with no branch for
   it.

   The three symptoms below all came from that one defect, and they look
   nothing like each other, which is why they are tested separately:

     * a declared composite threw `No matching clause: START_COMPOSITE`;
     * the SIMPLE element after a composite went missing, because the reader
       never got past the composite to reach it;
     * a segment of composites (X12's HI, say) came back empty and then failed
       schema validation for a missing required key -- a parse bug wearing a
       validation error's clothes."
  (:require [clojure.test :refer [deftest is testing]]
            [malli.core :as m]
            [com.breezeehr.edi-serde :as serde])
  (:import [java.io ByteArrayInputStream]))

;; ---------------------------------------------------------------------------
;; Fixtures
;; ---------------------------------------------------------------------------

(defn- interchange
  "A minimal well-formed 005010 interchange carrying `segments`.

   ISA is fixed-width and its last element is the COMPONENT separator, so the
   `:` in ISA16 is what makes `HC:99213:25` a composite rather than one value
   containing colons. Getting that wrong does not fail loudly -- the reader
   simply never emits a composite event -- so it is stated once, here."
  [& segments]
  (str "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
       "*260304*1407*^*00501*000000001*0*T*:~"
       "GS*HS*SENDER*RECEIVER*20260304*1407*1*X*005010X279A1~"
       "ST*270*0001*005010X279A1~"
       (apply str (map #(str % "~") segments))
       "SE*" (+ 2 (count segments)) "*0001~GE*1*1~IEA*1*000000001~"))

(defn- reader-on [^String edi]
  (.createEDIStreamReader serde/default-input-factory
                          (ByteArrayInputStream. (.getBytes edi "US-ASCII"))))

(defn- events
  "The segment-level events of the first `tag` segment in `edi`, as
   `[event-name text-or-nil]`."
  [tag edi]
  (with-open [r (reader-on edi)]
    (loop [acc []]
      (if (.hasNext r)
        (do (.next r)
            (let [ev (.name (.getEventType r))
                  in? (= tag (-> r .getLocation .getSegmentTag))]
              (if (and in? (#{"START_SEGMENT" "ELEMENT_DATA" "START_COMPOSITE"
                              "END_COMPOSITE" "END_SEGMENT"} ev))
                (let [acc (conj acc [ev (when (= ev "ELEMENT_DATA") (.getText r))])]
                  (if (= ev "END_SEGMENT") acc (recur acc)))
                (recur acc))))
        acc))))

(defn- seek-segment!
  "Advances `r` to the START_SEGMENT of the first `tag`, the position every
   segment parser expects to be handed."
  [r tag]
  (loop []
    (when (.hasNext r)
      (.next r)
      (if (and (= (.name (.getEventType r)) "START_SEGMENT")
               (= (-> r .getLocation .getSegmentTag) tag))
        r
        (recur)))))

(defn- parse-segment
  "Runs a segment parser over the first `tag` segment in `edi`."
  [schema tag edi]
  (with-open [r (reader-on edi)]
    (when (seek-segment! r tag)
      (second ((serde/make-segment-parser :seg {} (m/schema schema)) r)))))

;; EQ, cut down to the three elements that matter: a simple one, a composite,
;; and a simple one AFTER the composite.
(def ^:private eq-schema
  [:map {:type :segment :segment-id "EQ"}
   [:service-type-code-01 {:sequence 1 :optional true} [:string {:min 1 :max 2}]]
   [:composite-medical-procedure-identifier-02
    {:sequence 2 :optional true}
    [:map {:type :composite}
     [:product-or-service-id-qualifier-01 {:sequence 1 :optional true} [:string {:min 1 :max 2}]]
     [:procedure-code-02 {:sequence 2 :optional true} [:string {:min 1 :max 48}]]
     [:procedure-modifier-03 {:sequence 3 :optional true} [:string {:min 1 :max 2}]]]]
   [:coverage-level-code-03 {:sequence 3 :optional true} [:string {:min 3 :max 3}]]])

;; ---------------------------------------------------------------------------

(deftest composite-events-are-what-the-reader-emits
  (testing "the contract every parser in this namespace is written against. If
            this changes, the position arithmetic below is wrong rather than
            merely failing."
    (is (= [["START_SEGMENT" nil]
            ["ELEMENT_DATA" "98"]
            ["START_COMPOSITE" nil]
            ["ELEMENT_DATA" "HC"]
            ["ELEMENT_DATA" "99213"]
            ["ELEMENT_DATA" "25"]
            ["END_COMPOSITE" nil]
            ["ELEMENT_DATA" "FAM"]
            ["END_SEGMENT" nil]]
           (events "EQ" (interchange "EQ*98*HC:99213:25*FAM"))))))

(deftest a-declared-composite-is-read
  (testing "this threw `No matching clause: START_COMPOSITE` -- from
            `collect-extra-elements`, whose job is to tolerate what it does not
            recognise, about a composite the schema declared in full."
    (is (= {:service-type-code-01 "98"
            :composite-medical-procedure-identifier-02
            {:product-or-service-id-qualifier-01 "HC"
             :procedure-code-02 "99213"
             :procedure-modifier-03 "25"}
            :coverage-level-code-03 "FAM"}
           (parse-segment eq-schema "EQ" (interchange "EQ*98*HC:99213:25*FAM"))))))

(deftest the-element-after-a-composite-survives
  (testing "the quiet half of the same defect, and the more dangerous one. A
            reader parked on a composite never reaches the elements behind it,
            so they read as absent -- no error, just a smaller answer."
    (let [parsed (parse-segment eq-schema "EQ" (interchange "EQ*98*HC:99213:25*FAM"))]
      (is (= "FAM" (:coverage-level-code-03 parsed))
          "EQ03 sits after the composite and is what a payer is told the
           coverage level is"))))

(deftest a-composite-with-one-component-is-not-lost
  (testing "X12 permits a composite whose only populated component is the first
            to travel without a separator, and a reader has nothing to tell
            that from a simple element -- so the composite parser accepts a
            bare ELEMENT_DATA as component one."
    (is (= {:product-or-service-id-qualifier-01 "HC"}
           (:composite-medical-procedure-identifier-02
            (parse-segment eq-schema "EQ" (interchange "EQ*98*HC*FAM")))))))

(deftest a-composite-shorter-than-the-schema-is-fine
  (testing "trailing components are optional and simply absent."
    (is (= {:product-or-service-id-qualifier-01 "HC"
            :procedure-code-02 "99213"}
           (:composite-medical-procedure-identifier-02
            (parse-segment eq-schema "EQ" (interchange "EQ*98*HC:99213*FAM")))))))

(deftest an-undeclared-trailing-composite-is-drained-not-thrown
  (testing "`collect-extra-elements` is the tolerant path -- it exists to
            absorb what no parser claimed. It threw on a composite, which is
            the one shape it most needed to absorb. A composite comes back as
            its components."
    (let [schema [:map {:type :segment :segment-id "EQ"}
                  [:service-type-code-01 {:sequence 1 :optional true} [:string {:min 1 :max 2}]]]]
      (is (= {:service-type-code-01 "98"}
             (parse-segment schema "EQ" (interchange "EQ*98*HC:99213:25*FAM")))
          "the declared element still parses, and the rest is drained rather
           than ending the parse"))))

(deftest a-simple-element-position-holding-a-composite-does-not-strand-the-reader
  (testing "schema says simple, interchange sent composite. `.getText` on a
            START_COMPOSITE has no text to give, so the position is skipped --
            but the reader must still end up past it, or the segment is lost."
    (let [schema [:map {:type :segment :segment-id "EQ"}
                  [:service-type-code-01 {:sequence 1 :optional true} [:string {:min 1 :max 2}]]
                  [:not-really-composite-02 {:sequence 2 :optional true} [:string {:min 1 :max 40}]]
                  [:coverage-level-code-03 {:sequence 3 :optional true} [:string {:min 3 :max 3}]]]]
      (is (= {:service-type-code-01 "98" :coverage-level-code-03 "FAM"}
             (parse-segment schema "EQ" (interchange "EQ*98*HC:99213:25*FAM")))
          "EQ03 is still reached"))))
