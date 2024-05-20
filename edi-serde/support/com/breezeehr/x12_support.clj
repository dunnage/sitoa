(ns com.breezeehr.x12-support
  (:require [clojure.java.io :as io]
            [malli.core :as m]
            [net.cgrand.xforms :as xforms]
            [tech.v3.dataset :as ds]
            [malli.experimental.time :as mtime]
            [tech.v3.dataset.join :as ds-join])
  (:import (clojure.lang IReduceInit)
           (java.nio.file Files LinkOption Path)
           (java.util Iterator)))

(defn iterator-reducible
  "Similar to `clojure.core/iterator-seq`,
   but returns something reducible instead of a lazy-seq."
  [^Iterator iter]
  (reify IReduceInit
    (reduce [_ f init]
      (loop [it iter
             state init]
        (if (reduced? state)
          @state
          (if (.hasNext it)
            (recur it (f state (.next it)))
            state))))))
(defn partition-dataset-by [dataset partitioner]
  (eduction
    (comp
      (partition-by partitioner)
      (map (fn [x]
             (transduce (map identity) (ds/mapseq-rf) x))))
    (ds/mapseq-reader dataset)))

(defn make-process-element [spec]

  (let [elements (get spec :elements)]
    (fn inner-make-process-element [segment {segid "Segment ID"
                                             compid "Composite Data Element Number"
                                             seq   "Sequence" :as element}]
      [(if segid
         (format "%s%02d" segid seq)
         (format "%s-%02d" compid seq))
       (case (get element "Data Element Type")
         "ID" [:enum :temp]
         "AN" [:string {:min (get element "Minimum Length")
                        :max (get element "Maximum Length")}]
         "DT" :time/local-date
         "TM" :time/local-time
         "R" [decimal?]
         "N0" :int
         "N2" :int
         "Compostite" (into [:map]
                            (keep (fn [subitem]
                                    (inner-make-process-element segment (into subitem (get elements (str (get subitem "Data Element Number")))))))
                            (ds/mapseq-reader (get element "items"))))])))

(defn process-segment [spec]
  (let [context (get spec "CONTEXT.TXT")
        segment-detail (get spec "SEGDETL.TXT")
        elements (get spec :elements)
        composites (get spec :composites)
        process-element (make-process-element spec)]
    (fn [{s           "Sequence" segid "Segment ID" max-use "Maximum Use"
          requirement "Requirement"
          :as x}]
      (let [els (-> (ds/filter segment-detail (fn [y]
                                                (= (get x "Segment ID")
                                                   (get y "Segment ID"))))
                    (ds/sort-by-column "Sequence"))
            ctx (-> (ds/filter context (fn [y]
                                         (and (= (get x "Area")
                                                 (get y "Area"))
                                              (= (get x "Sequence")
                                                 (get y "Sequence"))
                                              (= "A"
                                                 (get y "Note Type"))
                                              (= "B"
                                                 (get y "Record Type")))
                                         ))
                    (ds/sort-by-column "Sequence"))]
        ;(prn ctx)
        [(if-some [seg-name (xforms/some (keep (fn [{x "Note"}] x)) (-> ctx ds/mapseq-reader))]
           (-> seg-name
               (clojure.string/replace #"/|," "")
               (clojure.string/split #"\s")
               (->>
                 (mapv #(.toLowerCase ^String %))
                 (interpose "-")
                 (clojure.string/join "")))
           segid)
               (case requirement
                 "M" {}
                 "O" {:optional true})
               (case max-use
                 1 (into [:map {:type :segment
                                :segment-id segid}]
                         (keep (fn [{el-num "Data Element Number" :as el}]
                                (process-element x (if-some [seg (get elements el-num)]
                                                     (into el seg)
                                                     (assoc el "Data Element Type" "Compostite"
                                                               "items" (get composites el-num) )))))
                         (ds/mapseq-reader els))
                 [:sequential
                  (into [:map {:type :segment
                               :segment-id segid}]
                        (keep (fn [{el-num "Data Element Number" :as el}]
                                (process-element x (if-some [seg (get elements el-num)]
                                                     (into el seg)
                                                     (assoc el "Data Element Type" "Compostite"
                                                               "items" (get composites el-num) )))))
                        (ds/mapseq-reader els))])]))))

(defn process-segments [spec]
  (let [ps (process-segment spec)]
    (fn [segment-ds]
      (into []
            (map ps)
            (ds/mapseq-reader segment-ds)))))


(defn process-loop [spec ps]
  (let [single-ps (process-segment spec)]
    (fn process-loop-inner [loop-ds
         level]
      (let [{s           "Sequence" loopid "Loop Identifier"
             requirement "Requirement" loop-repeat "Loop Repeat"
             :as first-seg}
            (ds/row-at loop-ds 0)]
        [loopid
         (case requirement
           "M" {}
           "O" {:optional true})
         (case loop-repeat
           ">1"
           [:sequential
            (-> [:map {:type :loop}]
                (conj (single-ps first-seg))
                (into (comp
                        (mapcat (fn [ds]
                                  (let [first-row (ds/row-at ds 0)]
                                    (if (get first-row "Loop Identifier")
                                      [(process-loop-inner ds (inc level))]
                                      (ps ds)))
                                  )))
                      (partition-dataset-by
                        (ds/drop-rows loop-ds [0])
                        #(= level (get % "Loop Level")))))]
           "1"
           (-> [:map {:type :loop}]
               (conj (single-ps first-seg))
               (into (comp
                       (mapcat (fn [ds]
                                 (let [first-row (ds/row-at ds 0)]
                                   (if (get first-row "Loop Identifier")
                                     [(process-loop-inner ds (inc level))]
                                     (ps ds)))
                                 )))
                     (partition-dataset-by
                       (ds/drop-rows loop-ds [0])
                       #(= level (get % "Loop Level")))))
           [:sequential
            (-> [:map {:type :loop}]
                (conj (single-ps first-seg))
                (into (comp
                        (mapcat (fn [ds]
                                  (let [first-row (ds/row-at ds 0)]
                                    (if (get first-row "Loop Identifier")
                                      [(process-loop-inner ds (inc level))]
                                      (ps ds)))
                                  )))
                      (partition-dataset-by
                        (ds/drop-rows loop-ds [0])
                        #(= level (get % "Loop Level")))))])]))))

(defn process-areas [spec]
  (let [ps (process-segments spec)
        pl (process-loop spec ps)
        ]
    (fn [area-ds]
      (let [first-row (ds/row-at area-ds 0)]
        (if (get first-row "Loop Identifier")
          [(pl area-ds 1)]
          (ps area-ds))))))

(defn make-message [{tx-set "SETDETL.TXT" :as spec}]
  (-> [:map]
      (into (comp
              (mapcat (process-areas spec)))
            (partition-dataset-by tx-set #(get % "Area")))
            (m/schema {:registry (merge (m/default-schemas) (mtime/schemas))})))

(def files {"SETHEAD.TXT"
            ["Transaction Set ID", "Transaction Set Name", "Functional Group ID"]
            "SETDETL.TXT"
            ["Set Transaction ID", "Area", "Sequence", "Segment ID", "Requirement",
             "Maximum Use", "Loop Level", "Loop Repeat", "Loop Identifier"]
            "SEGHEAD.TXT"
            ["Segment Name", "Segment ID"]
            "SEGDETL.TXT"
            ["Segment ID", "Sequence", "Data Element Number", "Requirement", "Repeat"]
            "COMHEAD.TXT"
            ["Element Number Composite Data", "Composite Name"]
            "COMDETL.TXT"
            ["Composite Data Element Number", "Sequence", "Data Element Number", "Requirement"]
            "ELEHEAD.TXT"
            ["Element Data Number", "Data Element Name"]
            "ELEDETL.TXT"
            ["Data Element Number", "Data Element Type", "Minimum Length",
             "Maximum Length"]
            "CONDETL.TXT"
            ["Record Type", "Transaction Set ID", "Area", "Sequence", "Segment ID",
             "Reference Designator", "Composite ID", "Composite Sequence",
             "Data Element Number", "Code", "Table", "Position", "Usage"]
            "CONTEXT.TXT"
            ["Record Type", "Transaction Set ID", "Area", "Sequence", "Segment ID",
             "Reference Designator", "Composite ID", "Composite Sequence",
             "Data Element Number", "Code", "Table", "Position", "Note Number",
             "Note Type", "Note"]

            })
(defn index-composites [{composite-detail "COMDETL.TXT" :as spec}]
  (assoc spec :composites
              (into {}
                    (map (fn [[k v]]
                           [k (ds/sort-by-column v "Sequence")]))
                    (ds/group-by-column composite-detail "Composite Data Element Number"))))

(defn index-dataelements [{element-detail "ELEDETL.TXT" :as spec}]
  (assoc spec :elements
              (into {}
                    (map (fn [x]
                           [(get x "Data Element Number")
                            x]))
                    (ds/mapseq-reader element-detail))))

(defn load-spec [path]
  (let [p (Path/of path (make-array String 0))]
    (->  {}
      (into
            (keep (fn [[^String f cols]]
                    (let [spath (.resolve p (.toLowerCase f))]
                      (when (Files/exists spath (make-array LinkOption 0))
                        [f (ds/->dataset (.toFile spath)
                                         {:file-type   :csv
                                          :header-row? false
                                          :key-fn      (into {} (map-indexed (fn [idx v]
                                                                               [(str "column-" idx) v]))
                                                             cols)})]))))
            files)
         (update
           "ELEDETL.TXT" ds/column-map  "Data Element Number"  str :string ["Data Element Number"])
         index-dataelements
         index-composites)

    ))

(comment

  (def temp (ds/->dataset "specs/x12products/005010X279 Health Care Eligibility Benefit Inquiry and Response/270/seghead.txt"
                          {:file-type :csv}))
  (def spec (load-spec "specs/x12products/005010X279 Health Care Eligibility Benefit Inquiry and Response/270"))
  (make-message spec)
  )


(comment

  )