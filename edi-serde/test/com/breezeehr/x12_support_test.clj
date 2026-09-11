(ns com.breezeehr.x12-support-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.breezeehr.x12-support :as support]
            [malli.core :as m]
            [tech.v3.dataset :as ds]))

(defn segment-row [area sequence id level loop-id]
  {"Area" area "Sequence" sequence "Segment ID" id
   "Loop Level" (str level) "Loop Identifier" loop-id
   "Requirement" "M" "Maximum Use" "1" "Loop Repeat" ">1"})

(defn table-spec [rows]
  {"SETDETL.TXT" (ds/->dataset rows)
   "SEGDETL.TXT" (ds/->dataset
                   (mapv (fn [id]
                           {"Segment ID" id "Sequence" "01"
                            "Data Element Number" "1"})
                         (distinct (map #(get % "Segment ID") rows))))
   "ELEHEAD.TXT" (ds/->dataset [{"Data Element Number" "1"
                                 "Data Element Name" "Value"}])
   "COMHEAD.TXT" (ds/->dataset [{"Composite Data Element Number" "unused"
                                 "Composite Name" "Unused"}])
   "CONDETL.TXT" (ds/->dataset
                   (mapv #(assoc (select-keys % ["Area" "Sequence"])
                                 "Segment ID" nil "Usage" "1" "Record Type" "B"
                                 "Reference Designator" nil "Composite Sequence" nil)
                         rows))
   "CONTEXT.TXT" (ds/->dataset
                   (mapv #(assoc (select-keys % ["Area" "Sequence"])
                                 "Note Type" "A" "Record Type" "B"
                                 "Note" (get % "Segment ID")
                                 "Reference Designator" nil "Composite Sequence" nil)
                         rows))
   :elements {"1" {"Data Element Number" "1" "Data Element Type" "AN"
                    "Minimum Length" "1" "Maximum Length" "20"}}})

(defn segment-paths [schema]
  (letfn [(walk [form path]
            (cond
              (= :sequential (first form)) (walk (last form) path)
              (= :segment (:type (second form))) [[path (:segment-id (second form))]]
              :else (mapcat (fn [[key & entry]]
                              (let [child (last entry)
                                    unwrapped (if (= :sequential (first child))
                                                (last child) child)]
                                (walk child (cond-> path
                                              (= :loop (:type (second unwrapped)))
                                              (conj key)))))
                            (drop 2 form))))]
    (vec (walk (m/form schema) []))))

(deftest repeated-sequences-use-their-own-area
  (let [rows [(segment-row "1" "010" "ST" 0 nil)
              (segment-row "2" "010" "NM1" 0 nil)
              (segment-row "3" "010" "SE" 0 nil)]
        spec (table-spec rows)
        expected [:st :nm1 :se]]
    (is (= expected (mapv first (m/entries (support/make-message spec)))))
    (binding [support/*context-data* (select-keys spec ["CONDETL.TXT" "CONTEXT.TXT"])]
      (is (= expected (mapv first ((support/process-segments spec)
                                  (get spec "SETDETL.TXT"))))))))

(deftest nested-and-sibling-loops-retain-order-and-parentage
  (let [rows [(segment-row "1" "010" "ST" 0 nil)
              (segment-row "2" "020" "HL" 1 "1000")
              (assoc (segment-row "2" "030" "NM1" 2 "1100") "Loop Repeat" "1")
              (segment-row "2" "040" "N3" 2 nil)
              (assoc (segment-row "2" "050" "REF" 2 "1200") "Requirement" "O")
              (segment-row "2" "060" "DTP" 1 nil)
              (segment-row "2" "070" "CLM" 1 "2000")
              (segment-row "3" "010" "SE" 0 nil)]
        schema (support/make-message (table-spec rows))
        form (m/form schema)
        outer (last (second (drop 2 form)))
        loop-form (last outer)]
    (is (= [[[] "ST"] [["1000"] "HL"] [["1000" "1100"] "NM1"]
            [["1000" "1100"] "N3"] [["1000" "1200"] "REF"]
            [["1000"] "DTP"] [["2000"] "CLM"] [[] "SE"]]
           (segment-paths schema)))
    (testing "loop cardinality and optionality remain part of the schema"
      (is (= :sequential (first outer)))
      (is (= :map (first (last (second (drop 2 loop-form))))))
      (is (= {:optional true} (second (nth (drop 2 loop-form) 2)))))))

(deftest binary-elements-produce-byte-array-schemas
  (let [spec (assoc-in (table-spec [(segment-row "2" "010" "BIN" 0 nil)])
                       [:elements "1" "Data Element Type"] "B")
        schema (support/make-message spec)]
    (is (m/validate schema {:bin {:value-01 (byte-array [0 42 126 -1])}}))
    (is (not (m/validate schema {:bin {:value-01 "not binary"}})))
    (is (some #(and (map? %) (= "B" (:edi/data-type %)))
              (tree-seq coll? seq (m/form schema))))))
