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

(def ^:dynamic *context-data* nil)
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

(defn snake-case-str [x]
  (-> x
      (clojure.string/replace #"/|," "")
      (clojure.string/split #"\s")
      (->>
        (mapv #(.toLowerCase ^String %))
        (interpose "-")
        (clojure.string/join ""))))

(defn snake-case-keyword [x]
  (-> (snake-case-str x)
      keyword))

(defn partition-dataset-by [dataset partitioner]
  (eduction
    (comp
      (partition-by partitioner)
      (map (fn [x]
             (transduce (map identity) (ds/mapseq-rf) x))))
    (ds/mapseq-reader dataset)))

(defn format-sequence-number [x seq]
  (format "%s-%02d" x seq))
(defn check1 [xform data]
  (transduce
    (comp xform
          (distinct))
    (fn ([] [])
      ([acc]
       (when (> (count acc) 1)
         (throw (ex-info "too many "
                         {:dups acc
                          :data data})))
       data)
      ([acc x ]
       (conj acc x)))
    data))

(defn make-process-element [spec]

  (let [elements (get spec :elements)
        CONDETL (get spec "CONDETL.TXT")
        CONTEXT (get spec "CONTEXT.TXT")
        ELEHEAD (get spec "ELEHEAD.TXT")]
    (fn inner-make-process-element [segment
                                    {segid "Segment ID"
                                             compid "Composite Data Element Number"
                                     dtype "Data Element Type" :as element}
                                    composite]
      ;(prn element)
      (let [main (if composite
                   composite
                   element)
            seq (get main "Sequence")
            context (if composite
                      (ds/filter CONTEXT
                                 (fn [y]

                                   (and
                                     (= (get segment "Area")
                                        (get y "Area"))
                                     (= (get segment "Sequence")
                                        (get y "Sequence"))
                                     (= (get segment "Sequence")
                                        (get y "Sequence"))
                                     (= (get element "Sequence")
                                        (get y "Reference Designator"))
                                     (= (get composite "Sequence")
                                        (get y "Composite Sequence"))
                                     #_(= (get element "Data Element Number")
                                          (get y "Data Element Number"))
                                     (= "F"
                                        (get y "Note Type")))))
                      (ds/filter CONTEXT
                                 (fn [y]

                                   (and
                                     (= (get segment "Area")
                                        (get y "Area"))
                                     (= (get segment "Sequence")
                                        (get y "Sequence"))
                                     (= (get element "Sequence")
                                        (get y "Reference Designator"))
                                     (= nil
                                        (get y "Composite Sequence"))
                                     #_(= (get element "Data Element Number")
                                          (get y "Data Element Number"))
                                     (= "F"
                                        (get y "Note Type"))))))
            usage (if composite
                    (ds/filter CONDETL
                               (fn [y]
                                 (and
                                   (= (get segment "Area")
                                      (get y "Area"))
                                   (= (get segment "Sequence")
                                      (get y "Sequence"))
                                   (= (get element "Sequence")
                                      (get y "Reference Designator"))
                                   (= (get composite "Sequence")
                                      (get y "Composite Sequence"))
                                   (= "D"
                                      (get y "Record Type")))))
                    (ds/filter CONDETL
                               (fn [y]

                                 (and
                                   (= (get segment "Area")
                                      (get y "Area"))
                                   (= (get segment "Sequence")
                                      (get y "Sequence"))
                                   (= (get element "Sequence")
                                      (get y "Reference Designator"))
                                   (= "C"
                                      (get y "Record Type"))))))
            usagex (-> usage
                       ds/mapseq-reader
                       (->> (xforms/some (keep #(get % "Usage")))))
            _ (prn segment)
            _ (prn element)
            context-name (some-> context
                                 (ds/mapseq-reader)
                                 (->>
                                   (check1 (keep #(get % "Note")))
                                   (xforms/some (keep #(get % "Note"))))
                                 snake-case-str
                                 (format-sequence-number  seq))
            elname (some-> (ds/filter ELEHEAD
                                  (fn [y]
                                    (and
                                      (= (get element "Data Element Number")
                                         (str (get y "Data Element Number"))))))
                       (ds/mapseq-reader)
                       (->> (xforms/some (keep #(get % "Data Element Name")))))
            fallback-name (if segid
                            (format "%s%02d" segid seq)
                            (format "%s-%02d" compid seq))]
        ;(prn  elname)
        ;(prn context-name)
        ;(prn element)
        (when-not (= 8 usagex)
          [(cond context-name
                 (keyword context-name)
                 ;elname
                 ;(snake-case-keyword elname)
                 :default
                 (snake-case-keyword fallback-name))
           (cond-> {:sequence (if composite
                                (get composite "Sequence")
                                (get element "Sequence"))}
                   (case usagex
                     nil true
                     (1 5) false
                     (2 3 4 6 7 8) true)
                   (assoc :optional  true))
           (case (if composite
                   (get composite "Data Element Type")
                   (get element "Data Element Type"))
             "ID" (let [items (if composite
                                (ds/filter CONDETL
                                           (fn [y]
                                             (and
                                               (= (get segment "Area")
                                                  (get y "Area"))
                                               (= (get segment "Sequence")
                                                  (get y "Sequence"))
                                               (= (get element "Sequence")
                                                  (get y "Reference Designator"))
                                               (= (get composite "Sequence")
                                                  (get y "Composite Sequence"))
                                               (= "E"
                                                  (get y "Record Type")))))
                                (ds/filter CONDETL
                                           (fn [y]

                                             (and
                                               (= (get segment "Area")
                                                  (get y "Area"))
                                               (= (get segment "Sequence")
                                                  (get y "Sequence"))
                                               (= (get element "Data Element Number")
                                                  (get y "Data Element Number"))
                                               (= "E"
                                                  (get y "Record Type"))))))]
                    #_(prn (get segment "Area")
                           (get segment "Sequence")
                           (get element "Data Element Number"))
                    #_(prn (ds/filter CONDETL
                                      (fn [y]
                                        (and
                                          (= (get segment "Area")
                                             (get y "Area"))
                                          (= (get segment "Sequence")
                                             (get y "Sequence"))
                                          (= (get element "Data Element Number")
                                             (get y "Data Element Number"))
                                          ))))
                    #_(when (zero? (ds/row-count items))
                        (prn element))
                    (if (zero? (ds/row-count items))
                      (if segid
                        [:string {:type "ID"
                                  :min  (get main "Minimum Length")
                                  :max  (get main "Maximum Length")}]
                        [:enum :composite])
                      (into [:enum]
                            (keep (fn [x]
                                    (get x "Code")))
                            (ds/mapseq-reader
                              items))))
             "AN" [:string {:min (get main "Minimum Length")
                            :max (get main "Maximum Length")}]
             "DT" :time/local-date
             "TM" :time/local-time
             "R" 'decimal?
             "N0" :int
             "N2" :int
             "Composite" (into [:map]
                                (keep (fn [subitem]
                                        (inner-make-process-element segment element (into subitem (get elements (str (get subitem "Data Element Number")))))))
                                (ds/mapseq-reader (get element "items"))))])))))

(defn process-segment [spec]
  (let [segment-detail (get spec "SEGDETL.TXT")
        elements (get spec :elements)
        composites (get spec :composites)
        process-element (make-process-element spec)]
    (fn [{s           "Sequence" segid "Segment ID" max-use "Maximum Use"
          requirement "Requirement" loop-repeat "Loop Repeat"
          :as x}]
      (when-some [usage (->> (ds/filter-column (get *context-data* "CONDETL.TXT") "Segment ID" nil?)
                             ds/mapseq-reader
                             (xforms/some (keep #(get % "Usage"))))]

        (prn x)
        (let [context (get *context-data* "CONTEXT.TXT")
              els (-> (ds/filter segment-detail (fn [y]
                                                  (= (get x "Segment ID")
                                                     (get y "Segment ID"))))
                      (ds/sort-by-column "Sequence"))
              ctx (-> (ds/filter context (fn [y]
                                           (and (= "A"
                                                   (get y "Note Type"))
                                                (= "B"
                                                   (get y "Record Type")))
                                           ))
                      (ds/sort-by-column "Sequence"))
              ]
          (prn segid)
          (prn usage)
          (prn context)
          [(if-some [seg-name (xforms/some (keep (fn [{x "Note"}] x)) (-> ctx ds/mapseq-reader))]
             (snake-case-keyword seg-name)
             segid)
           (case requirement
             "M" {}
             "O" {:optional true})
           (case max-use
             1 (into [:map {:type       :segment
                            :segment-id segid}]
                     (keep (fn [{el-num "Data Element Number" :as el}]
                             (process-element x (if-some [seg (get elements el-num)]
                                                  (into el seg)
                                                  (assoc el "Data Element Type" "Composite"
                                                            "items" (get composites el-num)))
                                              nil)))
                     (ds/mapseq-reader els))
             [:sequential
              (into [:map {:type       :segment
                           :segment-id segid}]
                    (keep (fn [{el-num "Data Element Number" :as el}]
                            (process-element x (if-some [seg (get elements el-num)]
                                                 (into el seg)
                                                 (assoc el "Data Element Type" "Composite"
                                                           "items" (get composites el-num)))
                                             nil)))
                    (ds/mapseq-reader els))])])))))

(defn process-segments [spec]
  (let [ps (process-segment spec)]
    (fn [segment-ds]
      (into []
            (keep (fn [{seq-num "Sequence"  :as segment}]
                    (prn seq-num)
                   (binding [*context-data* (-> *context-data*
                                                (update "CONDETL.TXT" ds/filter-column "Sequence" #(= (long %) seq-num))
                                                (update "CONTEXT.TXT" ds/filter-column "Sequence" #(= (long %) seq-num)))]
                     (ps segment))) )
            (ds/mapseq-reader segment-ds)))))


(defn process-loop [spec ps]
  (let [single-ps (process-segment spec)]
    (fn process-loop-inner [loop-ds
         level]
      (let [{s           "Sequence" loopid "Loop Identifier"
             requirement "Requirement" loop-repeat "Loop Repeat"
             :as first-seg}
            (ds/row-at loop-ds 0)]
        (let [first-segment (let [{seq-num "Sequence"} first-seg]
                              (assert (integer? seq-num))
                              (binding [*context-data* (-> *context-data*
                                                           (update "CONDETL.TXT" ds/filter-column "Sequence" #(= (long %) seq-num))
                                                           (update "CONTEXT.TXT" ds/filter-column "Sequence" #(= (long %) seq-num)))]
                                (single-ps first-seg)))
              other-segments (not-empty (into [] (comp
                                                   (mapcat (fn [ds]
                                                             (let [first-row (ds/row-at ds 0)]
                                                               (if (get first-row "Loop Identifier")
                                                                 [(process-loop-inner ds (inc level))]
                                                                 (ps ds)))
                                                             )))
                                              (partition-dataset-by
                                                (ds/drop-rows loop-ds [0])
                                                #(= level (get % "Loop Level")))))
              inner-map (cond-> [:map {:type :loop}]
                                first-segment
                                (conj first-segment)
                                other-segments
                                (into other-segments))]
          [loopid
           (case requirement
             "M" {}
             "O" {:optional true})
           (case loop-repeat
             ">1"
             [:sequential
              inner-map]
             "1"
             inner-map
             [:sequential
              inner-map])])))))

(defn process-areas [spec]
  (let [ps (process-segments spec)
        pl (process-loop spec ps)
        ]
    (fn [area-ds]
      (let [{area "Area" :as first-row} (ds/row-at area-ds 0)]
        (binding [*context-data* (-> spec
                                     (select-keys ["CONDETL.TXT"
                                                   "CONTEXT.TXT"])
                                     (update "CONDETL.TXT" ds/filter-column "Area" #(= (long %) area))
                                     (update "CONTEXT.TXT" ds/filter-column "Area" #(= % area)))]
          ;(prn area)
          ;(prn *context-data*)
          (if (get first-row "Loop Identifier")
            [(pl area-ds 1)]
            (ps area-ds)))))))

(defn make-message [{tx-set "SETDETL.TXT" :as spec}]
  (-> [:map]
      (into (comp
              (mapcat (process-areas spec)))
            (partition-dataset-by tx-set #(get % "Area")))
      (m/schema {:registry (merge (m/default-schemas) (mtime/schemas))})
      ))

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
            ["Data Element Number", "Data Element Name"]
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
         (update
           "CONDETL.TXT" ds/column-map  "Data Element Number"  str :string ["Data Element Number"])
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