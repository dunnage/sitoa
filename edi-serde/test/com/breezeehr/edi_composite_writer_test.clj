(ns com.breezeehr.edi-composite-writer-test
  "Writing a composite whose schema leaves component positions out.

   An implementation guide marks some components of a composite `not used`,
   and a generated schema omits them, so the declared sequences have holes: a
   275's STC01 declares components 01, 02 and 04, and an 837I's HI declares 01,
   02 and 05 for a value code. The component after a hole has to go out at its
   own position, with an empty component standing in for each one left out --
   `STC*R4:18842-5::LOI`.

   The segment writer has always padded the elements it skips. The composite
   writer compared the gap the wrong way round, so it never padded, and the
   component after a hole was written early, into the hole:
   `STC*R4:18842-5:LOI` puts LOI in component 03, where a reader takes it for
   an entity identifier code. Its padding branch also returned a bare
   `[nil pad]` where a `[k unparse]` pair belonged, which could not surface
   while the branch never ran.

   Padding fills interior holes only. Nothing is written after the last
   component a composite carries, as X12 requires. That trimming is StAEDI's,
   not the writer's: the writer emits an empty component for every declared
   position it has no value for, and `default-output-factory` sets
   TRUNCATE_EMPTY_ELEMENTS, which drops empty trailing components and
   elements. `trailing-empty-components-are-trimmed-by-the-factory` pins that."
  (:require [clojure.test :refer [deftest is testing]]
            [malli.core :as m]
            [com.breezeehr.edi-serde :as serde])
  (:import [io.xlate.edi.stream EDIOutputFactory]
           [java.io ByteArrayInputStream ByteArrayOutputStream]))

;; ---------------------------------------------------------------------------
;; Schemas
;; ---------------------------------------------------------------------------

(defn- text-segment [tag n]
  (into [:map {:type :segment :segment-id tag}]
        (for [i (range 1 (inc n))]
          [(keyword (str "e" i)) {:sequence i :optional true} :string])))

(defn- composite
  "A composite declaring only the component positions in `positions`."
  [& positions]
  (into [:map {:type :composite}]
        (for [p positions]
          [(keyword (format "c%02d" p)) {:sequence p :optional true} :string])))

(defn- segment-schema
  "A segment whose element 01 is `comp`, followed by a simple element 02, so a
   test can see that what follows the composite is unmoved."
  [tag comp]
  [:map {:type :segment :segment-id tag}
   [:e01 {:sequence 1 :optional true} comp]
   [:e02 {:sequence 2 :optional true} :string]])

(defn- interchange-schema [body]
  (m/schema
   [:map {:type :interchange}
    [:isa (text-segment "ISA" 16)]
    [:groups
     [:sequential
      [:map {:type :group}
       [:gs (text-segment "GS" 8)]
       [:transactions
        [:sequential
         [:map {:type :transaction-set}
          [:st (text-segment "ST" 3)]
          [:body {:optional true} body]
          [:se (text-segment "SE" 2)]]]]
       [:ge (text-segment "GE" 2)]]]]
    [:iea (text-segment "IEA" 2)]]))

;; STC01 as the 275 declares it: component 03 is not used.
(def ^:private stc (segment-schema "STC" (composite 1 2 4)))

;; HI as the 837I declares it for a value code (01, 02, 05) and a principal
;; diagnosis with its present-on-admission indicator (01, 02, 09), merged.
(def ^:private hi (segment-schema "HI" (composite 1 2 5 9)))

;; A composite whose first declared component is not component 01.
(def ^:private lead (segment-schema "LQ" (composite 2 3)))

;; ---------------------------------------------------------------------------
;; Reading and writing
;; ---------------------------------------------------------------------------

(defn- interchange [& segments]
  (str "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
       "*260924*1200*^*00501*000000001*0*T*:~"
       "GS*PI*SENDER*RECEIVER*20260924*1200*1*X*005010X210~"
       "ST*275*0001*005010X210~"
       (apply str (map #(str % "~") segments))
       "SE*" (+ 2 (count segments)) "*0001~GE*1*1~IEA*1*000000001~"))

(defn- parse [schema ^String edi]
  (with-open [r (.createEDIStreamReader serde/default-input-factory
                                        (ByteArrayInputStream. (.getBytes edi "US-ASCII")))]
    ((serde/make-parser schema) r)))

(defn- unparse
  ([schema data] (unparse serde/default-output-factory schema data))
  ([^EDIOutputFactory factory schema data]
   (let [out (ByteArrayOutputStream.)]
     (with-open [w (.createEDIStreamWriter factory out)]
       ((serde/make-unparser schema) w data))
     (String. (.toByteArray out) "US-ASCII"))))

(def ^:private body-path [:groups 0 :transactions 0 :body])

(defn- written
  "The body segment the writer produces for `data`, as text without its
   terminator, written inside an envelope parsed from a real interchange so
   the ISA the writer takes its delimiters from is well formed. SE01 counts
   the body segment the envelope did not have; the writer checks it."
  ([body data] (written serde/default-output-factory body data))
  ([factory body data]
   (let [schema (interchange-schema body)
         envelope (-> (parse schema (interchange))
                      (assoc-in [:groups 0 :transactions 0 :se :e1] "3"))
         text (unparse factory schema (assoc-in envelope body-path data))
         tag (-> body m/schema m/properties :segment-id)]
     (some #(when (.startsWith ^String % (str tag "*")) %)
           (map #(.trim ^String %) (.split ^String text "~"))))))

(defn- read-back
  "What a reader makes of the body segment the writer produced for `data`."
  [body data]
  (let [schema (interchange-schema body)]
    (get-in (parse schema (interchange (written body data))) body-path)))

;; ---------------------------------------------------------------------------

(deftest a-component-after-a-hole-is-written-at-its-own-position
  (testing "the 275's STC01. Component 03 is not declared, so 04 follows one
            empty component. Written without it, LOI lands in component 03."
    (let [data {:e01 {:c01 "R4" :c02 "18842-5" :c04 "LOI"}}]
      (is (= "STC*R4:18842-5::LOI" (written stc data)))
      (is (= data (read-back stc data))
          "and it reads back as the component it was written as"))))

(deftest a-hole-spanning-several-components-is-padded-by-its-width
  (testing "the 837I's value code: components 03 and 04 are not declared, so
            the amount at 05 follows two empty components. Written without
            them, `HI*BE:80:4` reads the amount as the C022-03 date format
            qualifier."
    (let [data {:e01 {:c01 "BE" :c02 "80" :c05 "4"}}]
      (is (= "HI*BE:80:::4" (written hi data)))
      (is (= data (read-back hi data)))))
  (testing "the present-on-admission indicator at 09 sits behind two holes and
            one declared component carrying no value; each is written empty."
    (let [data {:e01 {:c01 "ABK" :c02 "A01" :c09 "Y"}}]
      (is (= "HI*ABK:A01:::::::Y" (written hi data)))
      (is (= data (read-back hi data)))))
  (testing "both holes filled around values on either side of them."
    (let [data {:e01 {:c01 "BE" :c02 "80" :c05 "4" :c09 "Y"}}]
      (is (= "HI*BE:80:::4::::Y" (written hi data)))
      (is (= data (read-back hi data))))))

(deftest a-composite-whose-first-declared-component-is-not-01
  (testing "the hole is at the front: component 02 follows one empty
            component 01. Written without it, 02 and 03 each move one early."
    (let [data {:e01 {:c02 "A" :c03 "B"}}]
      (is (= "LQ*:A:B" (written lead data)))
      (is (= data (read-back lead data))))))

(deftest what-follows-the-composite-is-unmoved
  (testing "padding is inside the composite, so the element after it keeps
            its position."
    (let [data {:e01 {:c01 "R4" :c02 "18842-5" :c04 "LOI"} :e02 "X"}]
      (is (= "STC*R4:18842-5::LOI*X" (written stc data)))
      (is (= data (read-back stc data))))))

(deftest nothing-is-written-after-the-last-component-sent
  (testing "a hole behind the last component carrying a value is trailing, not
            interior, and X12 sends no delimiters for it."
    (is (= "STC*R4:18842-5" (written stc {:e01 {:c01 "R4" :c02 "18842-5"}})))
    (is (= "HI*BE:80:::4" (written hi {:e01 {:c01 "BE" :c02 "80" :c05 "4"}}))
        "09 is declared after 05, and nothing is written for it or for the
         holes before it")
    (is (= "HI*BK:A01" (written hi {:e01 {:c01 "BK" :c02 "A01"}})))
    (is (= "STC*R4*X" (written stc {:e01 {:c01 "R4"} :e02 "X"}))
        "a composite left with only component 01 travels without a separator,
         and the element after it still follows"))
  (testing "a composite with a leading hole and nothing after it."
    (is (= "LQ*:A" (written lead {:e01 {:c02 "A"}})))))

(deftest trailing-empty-components-are-trimmed-by-the-factory
  (testing "the writer emits an empty component for every declared position it
            has no value for, trailing ones included, and leaves trimming to
            StAEDI. The default factory is what makes that safe."
    (is (true? (.getProperty ^EDIOutputFactory serde/default-output-factory
                             EDIOutputFactory/TRUNCATE_EMPTY_ELEMENTS))))
  (testing "a factory without TRUNCATE_EMPTY_ELEMENTS writes every declared
            position out, components and elements alike. This is StAEDI's
            behaviour, not a padding defect: the interior hole is the same
            either way."
    (let [untruncated (doto (EDIOutputFactory/newFactory)
                        (.setProperty EDIOutputFactory/PRETTY_PRINT true))]
      (is (= "STC*R4:18842-5::*"
             (written untruncated stc {:e01 {:c01 "R4" :c02 "18842-5"}})))
      (is (= "STC*R4:18842-5::LOI*"
             (written untruncated stc {:e01 {:c01 "R4" :c02 "18842-5" :c04 "LOI"}}))))))

(deftest decimal-and-an-wire-format
  (let [body [:map {:type :segment :segment-id "NTE"}
              [:number {:sequence 1} 'decimal?]
              [:text {:sequence 2} :string]
              [:comp {:sequence 3}
               [:map {:type :composite}
                [:number {:sequence 1} 'decimal?]
                [:text {:sequence 2} :string]
                [:end {:sequence 3} :string]]]
              [:end {:sequence 4} :string]]]
    (doseq [[number expected] [[1E+2M "100"] [1E-7M "0.0000001"]
                               [-1E+2M "-100"] [0M "0"] [1.20M "1.20"]]]
      (is (= (str "NTE*" expected "*  A  B*" expected ":  X:Y*Z")
             (written body {:number number :text "  A  B   "
                            :comp {:number number :text "  X   " :end "Y"}
                            :end "Z"}))))
    (is (= "NTE***::Y*Z"
           (written body {:text "   " :comp {:text "   " :end "Y"} :end "Z"})))
    (is (= "NTE**A\t*::Y*Z"
           (written body {:text "A\t " :comp {:end "Y"} :end "Z"}))))
  (is (= "NTE*A  *Z"
         (written [:map {:type :segment :segment-id "NTE"}
                   [:id {:sequence 1} [:string {:type "ID"}]]
                   [:end {:sequence 2} :string]]
                  {:id "A  " :end "Z"}))))
