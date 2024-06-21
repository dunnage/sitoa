(ns com.breezeehr.x12-support
  (:require [clojure.java.io :as io]
            [malli.core :as m]
            [net.cgrand.xforms :as xforms]
            [tech.v3.dataset :as ds]
            [malli.experimental.time :as mtime]
            [tech.v3.dataset.join :as ds-join])
  (:import (clojure.lang IReduceInit)
           (java.nio.file Files LinkOption Path)
           (java.util ArrayList Iterator)))

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
      (clojure.string/replace #"\(|\)" "")
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

(defn take-while-dataset [dataset pred]
  (transduce
    (take-while pred)
    (ds/mapseq-rf)
    (ds/mapseq-reader dataset)))

(defn drop-while-dataset [dataset pred]
  (transduce
    (drop-while pred)
    (ds/mapseq-rf)
    (ds/mapseq-reader dataset)))


(defn next-sibling-loop-row [ds level]
  (let [on (volatile! false)]
    (xforms/some
      (map-indexed
        (fn [i {ll "Loop Level" lid "Loop Identifier"}]
          ;skip until after loops
          (when (and (not @on) (not (and lid (= level (parse-long ll)))))
            (vreset! on true))
          (when (and @on lid (= level (parse-long ll)))
            i)))
      (ds/mapseq-reader ds))))

(defn format-sequence-number [x seq]
  (format "%s-%s" x seq))
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

(defn not-empty-dataset [ds]
  (when-not (zero? (ds/row-count ds))
    ds))

(defn make-process-nocontext-element [spec]

  (let [elements (get spec :elements)
        ELEHEAD (get spec "ELEHEAD.TXT")
        COMHEAD (get spec "COMHEAD.TXT")]
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
            generic-name (if (get element "items")
                           (do
                             (some-> (ds/filter COMHEAD
                                                (fn [y]
                                                  (and
                                                    (= (get element "Data Element Number")
                                                       (get y "Composite Data Element Number")))))
                                     (ds/mapseq-reader)
                                     (->> (xforms/some (keep #(get % "Composite Name"))))
                                     snake-case-str
                                     (format-sequence-number  seq)))
                           (some-> (ds/filter ELEHEAD
                                              (fn [y]
                                                (and
                                                  (= (get element "Data Element Number")
                                                     (get y "Data Element Number")))))
                                   (ds/mapseq-reader)
                                   (->> (xforms/some (keep #(get % "Data Element Name"))))
                                   snake-case-str
                                   (format-sequence-number  seq)))]
        [(cond generic-name
               (keyword generic-name)
               :default
               (throw (ex-info "should not hit" {}))
               #_(snake-case-keyword fallback-name))
         (cond-> {:sequence (parse-long (if composite
                                          (get composite "Sequence")
                                          (get element "Sequence")))}
                 true
                 (assoc :optional true))
         (case (get main "Data Element Type")
           nil [:string {:data-element-number (get main "Data Element Number")
                         :min (some-> (get main "Minimum Length") parse-long)
                          :max (some-> (get main "Maximum Length") parse-long)}]
           "ID" [:string {:min (some-> (get main "Minimum Length") parse-long)
                          :max (some-> (get main "Maximum Length") parse-long)}]
           "AN" [:string {:min (some-> (get main "Minimum Length") parse-long)
                          :max (some-> (get main "Maximum Length") parse-long)}]
           "DT" :time/local-date
           "TM" :time/local-time
           "R" 'decimal?
           "N0" :int
           "N2" [:int {:shift 100}]
           "Composite" (into [:map {:type :composite}]
                             (keep (fn [subitem]
                                     (inner-make-process-element segment element (into subitem (get elements (get subitem "Data Element Number"))))))
                             (ds/mapseq-reader (get element "items"))))]))))

(defn make-process-element [spec]

  (let [elements (get spec :elements)
        CONDETL (get spec "CONDETL.TXT")
        CONTEXT (get spec "CONTEXT.TXT")
        ELEHEAD (get spec "ELEHEAD.TXT")
        COMHEAD (get spec "COMHEAD.TXT")]
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
                       (->>
                         (check1  (keep #(get % "Usage")))
                         (xforms/some (keep #(get % "Usage")))))
            context-name (some-> context
                                 (ds/mapseq-reader)
                                 (->>
                                   (check1 (keep #(get % "Note")))
                                   (xforms/some (keep #(get % "Note"))))
                                 snake-case-str
                                 (format-sequence-number  seq))
            generic-name (if (get element "items")
                           (do
                             (some-> (ds/filter COMHEAD
                                                (fn [y]
                                                  (and
                                                    (= (get element "Data Element Number")
                                                       (get y "Composite Data Element Number")))))
                                     (ds/mapseq-reader)
                                     (->> (xforms/some (keep #(get % "Composite Name"))))
                                     snake-case-str
                                     (format-sequence-number  seq)))
                           (some-> (ds/filter ELEHEAD
                                              (fn [y]
                                                (and
                                                  (= (get element "Data Element Number")
                                                     (get y "Data Element Number")))))
                                   (ds/mapseq-reader)
                                   (->> (xforms/some (keep #(get % "Data Element Name"))))
                                   snake-case-str
                                   (format-sequence-number  seq)))
            #_#_fallback-name (if segid
                            (format "%s%02d" segid seq)
                            (format "%s-%02d" compid seq))]
        ;(prn  elname)
        ;(prn generic-name)
        ;(prn element)
        (when-not (= "8" usagex)
          [(cond context-name
                 (keyword context-name)
                 generic-name
                 (keyword generic-name)
                 :default
                 (throw (ex-info "should not hit" {}))
                 #_(snake-case-keyword fallback-name))
           (cond-> {:sequence (parse-long (if composite
                                            (get composite "Sequence")
                                            (get element "Sequence")))}
                   (case usagex
                     nil true
                     ("1" "5") false
                     ("2" "3" "4" "6" "7" "8") true)
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
                                  :min  (some-> (get main "Minimum Length") parse-long)
                                  :max  (some-> (get main "Maximum Length") parse-long)}]
                        [:enum :composite])
                      (into [:enum]
                            (keep (fn [x]
                                    (get x "Code")))
                            (ds/mapseq-reader
                              items))))
             "AN" [:string {:min (some-> (get main "Minimum Length") parse-long)
                            :max (some-> (get main "Maximum Length") parse-long)}]
             "DT" :time/local-date
             "TM" :time/local-time
             "R" 'decimal?
             "N0" :int
             "N2" [:int {:shift 100}]
             "Composite" (into [:map {:type :composite}]
                                (keep (fn [subitem]
                                        (inner-make-process-element segment element (into subitem (get elements (get subitem "Data Element Number"))))))
                                (ds/mapseq-reader (get element "items"))))])))))


(defn process-nocontext-segment [spec]
  (let [segment-detail (get spec "SEGDETL.TXT")
        segment-head (get spec "SEGHEAD.TXT")
        elements (get spec :elements)
        composites (get spec :composites)
        process-element (make-process-nocontext-element spec)]
    (fn [{segid "Segment ID" segname "Segment Name":as x}]
      (let [els (-> (ds/filter segment-detail (fn [y]
                                                (= (get x "Segment ID")
                                                   (get y "Segment ID"))))
                    (ds/sort-by-column "Sequence"))
            ]
        ;(prn segid)
        ;(prn usage)
        ;(prn context)
        [(snake-case-keyword segname)
         (into [:map {:type       :segment
                      :segment-id segid}]
               (keep (fn [{el-num "Data Element Number" :as el}]
                       (process-element x (if-some [seg (get elements el-num)]
                                            (into el seg)
                                            (assoc el "Data Element Type" "Composite"
                                                      "items" (get composites el-num)))
                                        nil)))
               (ds/mapseq-reader els))]))))

(defn process-segment [spec]
  (let [segment-detail (get spec "SEGDETL.TXT")
        elements (get spec :elements)
        composites (get spec :composites)
        process-element (make-process-element spec)]
    (fn [{s           "Sequence" segid "Segment ID" max-use "Maximum Use"
          requirement "Requirement" loop-repeat "Loop Repeat"
          :as x}]
      #_(when  (= segid "LE")
        (prn x))
      (when-some [usage (->> (ds/filter-column (get *context-data* "CONDETL.TXT") "Segment ID" nil?)
                             ds/mapseq-reader
                             (xforms/some (keep #(get % "Usage"))))]

        ;(prn segid usage)
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
          ;(prn segid)
          ;(prn usage)
          ;(prn context)
          [(if-some [seg-name (xforms/some (keep (fn [{x "Note"}] x)) (-> ctx ds/mapseq-reader))]
             (snake-case-keyword seg-name)
             segid)
           (case requirement
             "M" {}
             "O" {:optional true})
           (case max-use
             "1" (into [:map {:type       :segment
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
                    #_(prn (get segment "Area") seq-num (get segment "Segment ID"))
                    #_(prn (-> (get *context-data* "CONDETL.TXT" )
                             (ds/filter-column "Sequence" #(= % seq-num))))
                   (binding [*context-data* (-> *context-data*
                                                (update "CONDETL.TXT" ds/filter-column "Sequence" #(= % seq-num))
                                                (update "CONTEXT.TXT" ds/filter-column "Sequence" #(= % seq-num)))]
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
        ;(prn loop-ds)
        (let [first-segment (let [{seq-num "Sequence" area "Area"} first-seg]
                              (binding [*context-data* (-> *context-data*
                                                           (update "CONDETL.TXT" ds/filter-column "Sequence" #(= % seq-num))
                                                           (update "CONTEXT.TXT" ds/filter-column "Sequence" #(= % seq-num))
                                                           (update "CONDETL.TXT" ds/filter-column "Area" #(= % area))
                                                           (update "CONTEXT.TXT" ds/filter-column "Area" #(= % area)))]
                                (single-ps first-seg)))
              ;_ (prn first-seg)
              sibling-idx (next-sibling-loop-row loop-ds level)
              other-segments (not-empty (-> []
                                            (into (comp
                                                       (mapcat (fn [ds]
                                                                 (let [first-row (ds/row-at ds 0)]
                                                                    ;(prn :chunk (ds/filter-column ds "Segment ID" #(= % "LE")))
                                                                   (if (get first-row "Loop Identifier")
                                                                     [(process-loop-inner ds (parse-long (get first-row "Loop Level")))]
                                                                     (let [{seq-num "Sequence" area "Area"} first-row]
                                                                       (binding [*context-data* (-> *context-data*
                                                                                                    ;(update "CONDETL.TXT" ds/filter-column "Sequence" #(= % seq-num))
                                                                                                    ;(update "CONTEXT.TXT" ds/filter-column "Sequence" #(= % seq-num))
                                                                                                    (update "CONDETL.TXT" ds/filter-column "Area" #(= % area))
                                                                                                    (update "CONTEXT.TXT" ds/filter-column "Area" #(= % area)))]
                                                                         (ps ds)))))
                                                                 )))

                                                  (partition-dataset-by
                                                    (if sibling-idx
                                                      (ds/select-rows loop-ds (range 1 sibling-idx))
                                                      (take-while-dataset
                                                        (ds/drop-rows loop-ds [0])
                                                        #(>= (parse-long (get % "Loop Level")) level)))
                                                    #(= level (parse-long (get % "Loop Level")))))
                                            (into (when sibling-idx
                                                    (let [ds (ds/select-rows loop-ds (range sibling-idx (ds/row-count loop-ds)))]
                                                      ;(prn :left-over (ds/filter-column ds "Segment ID" #(= % "LE")))
                                                      (when-some [first-row (ds/row-at ds 0)]
                                                        (assert (get first-row "Loop Identifier"))
                                                        [(process-loop-inner ds (parse-long (get first-row "Loop Level")))]))))
                                            ;trailers
                                            #_(into (let [area (get first-seg "Area")
                                                        ds (some-> (if sibling-idx
                                                                     (ds/select-rows loop-ds (range sibling-idx (ds/row-count loop-ds)))
                                                                     (drop-while-dataset
                                                                       (ds/drop-rows loop-ds [0])
                                                                       #(>= (parse-long (get % "Loop Level")) level)))
                                                                   #_(drop-while-dataset
                                                                     #(> (parse-long (get % "Loop Level")) level))
                                                                   not-empty-dataset
                                                                   (ds/filter-column
                                                                     "Area"
                                                                     #(= % area)))]
                                                    (when (and ds (not (zero? (ds/row-count ds))))
                                                      (when-some [first-row (ds/row-at ds 0)]
                                                        (prn :trailing-chunk (ds/filter-column ds "Segment ID" #(= % "LE")))
                                                        (if (get first-row "Loop Identifier")
                                                          [(process-loop-inner ds (parse-long (get first-row "Loop Level")))]
                                                          (let [{seq-num "Sequence" area "Area"} first-row]
                                                            (binding [*context-data* (-> *context-data*
                                                                                         ;(update "CONDETL.TXT" ds/filter-column "Sequence" #(= % seq-num))
                                                                                         ;(update "CONTEXT.TXT" ds/filter-column "Sequence" #(= % seq-num))
                                                                                         (update "CONDETL.TXT" ds/filter-column "Area" #(= % area))
                                                                                         (update "CONTEXT.TXT" ds/filter-column "Area" #(= % area)))]
                                                              (ps ds))))))))))
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
        pl (process-loop spec ps)]
    (fn [area-ds]
      (let [first-row (ds/row-at area-ds 0)]
        (binding [*context-data* (-> spec
                                     (select-keys ["CONDETL.TXT"
                                                   "CONTEXT.TXT"])
                                     ;(update "CONDETL.TXT" ds/filter-column "Area" #(= % area))
                                     ;(update "CONTEXT.TXT" ds/filter-column "Area" #(= % area))
                                     )]
          ;(prn area)
          ;(prn *context-data*)
          (if (get first-row "Loop Identifier")
            [(pl area-ds 1)]
            (ps area-ds)))))))

(defn make-message [{tx-set "SETDETL.TXT" :as spec}]
  (-> [:map {:type :transaction}]
      (into (comp
              (mapcat (process-areas spec)))
            (partition-dataset-by tx-set #(> (parse-long (get % "Loop Level")) 0)))
      (m/schema {:registry (merge (m/default-schemas) (mtime/schemas))})
      ))

(def files {"SETHEAD.TXT"
            ["Transaction Set ID", "Transaction Set Name", "Functional Group ID"]
            "SETDETL.TXT"
            ["Set Transaction ID", "Area", "Sequence", "Segment ID", "Requirement",
             "Maximum Use", "Loop Level", "Loop Repeat", "Loop Identifier"]
            "SEGHEAD.TXT"
            ["Segment ID" "Segment Name"]
            "SEGDETL.TXT"
            ["Segment ID", "Sequence", "Data Element Number", "Requirement", "Repeat"]
            "COMHEAD.TXT"
            ["Composite Data Element Number", "Composite Name"]
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
                                          :parser-fn :string
                                          :header-row? false
                                          :key-fn      (into {} (map-indexed (fn [idx v]
                                                                               [(str "column-" idx) v]))
                                                             cols)})]))))
            files)
         #_(update
           "ELEDETL.TXT" ds/column-map  "Data Element Number"  str :string ["Data Element Number"])
         #_(update
           "CONDETL.TXT" ds/column-map  "Data Element Number"  str :string ["Data Element Number"])
         index-dataelements
         index-composites)

    ))
(defn functional-group-schema [transaction {seghead "SEGHEAD.TXT" :as spec}]
  (let [ps (process-nocontext-segment spec)]
    (-> [:map {:type :group}]
      (into
        (map ps)
        (-> seghead
            (ds/filter-column "Segment ID" #{"GE"})
            ds/mapseq-reader))
      (conj [:transaction transaction])
        (into
          (map ps)
          (-> seghead
              (ds/filter-column "Segment ID" #{"GS"})
              ds/mapseq-reader))))
  )
(defn interchange-schema [transaction {seghead "SEGHEAD.TXT" :as spec}]
  (let [ps (process-nocontext-segment spec)]
    (-> [:map {:type :interchange}]
        (into
          (map ps )
          (-> seghead
              (ds/filter-column "Segment ID" #{"ISA"})
              ds/mapseq-reader))
        (conj [:functional-groups [:sequential (functional-group-schema transaction spec)]])
        (into
          (map ps )
          (-> seghead
              (ds/filter-column "Segment ID" #{"IEA"})
              ds/mapseq-reader))
        ))

  )

(comment

  (def full-ds (load-spec "specs/x12products/005010 Table Data"))
  (interchange-schema :any full-ds)
  (def temp (ds/->dataset "specs/x12products/005010X279 Health Care Eligibility Benefit Inquiry and Response/270/seghead.txt"
                          {:file-type :csv}))L
  (def spec (load-spec "specs/x12products/005010X279 Health Care Eligibility Benefit Inquiry and Response/270"))
  (def spec (load-spec "specs/x12products/005010X279 Health Care Eligibility Benefit Inquiry and Response/271"))
  (make-message spec)
  (interchange-schema (make-message spec) full-ds)
  )


(comment

  )