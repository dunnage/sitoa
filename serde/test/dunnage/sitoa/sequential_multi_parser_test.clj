(ns dunnage.sitoa.sequential-multi-parser-test
  "Coverage for a :multi child under :sequential.

  `-sequential-parser` picks the per-element parser with a `case` on the child's
  deref-all type. That `case` has a trailing default clause: the `#_(:map)`
  elides only the TEST, leaving its expression dangling as the default. So a
  :multi child did not throw — it silently took the single-tag path instead of
  the choice path, and the element never parsed.

  The NCPDP SCRIPT shape mirrored here is

    [:NextQuestionCondition {:optional true} [:sequential {:min 1} [:ref :script/NumericType]]]

  where :script/NumericType is an :or of tagged tuples. Converting such an :or
  into a discriminated :multi must not change how the value parses."
  (:require [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.parser :as parser]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [malli.core :as m])
  (:import (java.io StringReader)))

(def ^:private or-choice
  [:or
   [:tuple [:enum :SingleComparison] :string]
   [:tuple [:enum :RangeComparison] :string]])

(def ^:private multi-choice
  [:multi {:dispatch first}
   [:SingleComparison [:tuple [:enum :SingleComparison] :string]]
   [:RangeComparison [:tuple [:enum :RangeComparison] :string]]])

(defn- ref-schema
  "Registry reference site, as the SCRIPT registry emits it."
  [choice]
  (m/schema
   [:schema {:registry   {:test/Root   [:map {:closed true}
                                        [:NextQuestionCondition {:optional true}
                                         [:sequential {:min 1} [:ref :test/Choice]]]]
                          :test/Choice choice}
             :topElement "Root"}
    :test/Root]
   xml-primitives/external-registry))

(defn- inline-schema
  "Same shape with the choice inlined rather than referenced."
  [choice]
  (m/schema
   [:schema {:registry   {:test/Root [:map {:closed true}
                                      [:NextQuestionCondition {:optional true}
                                       [:sequential {:min 1} choice]]]}
             :topElement "Root"}
    :test/Root]
   xml-primitives/external-registry))

(defn- parse [schema xml]
  (with-open [r (StringReader. xml)]
    ((parser/xml-parser schema) (parser/make-stream-reader {} r))))

(defn- doc [& bodies]
  (str "<?xml version=\"1.0\"?><Root>"
       (apply str (map #(str "<NextQuestionCondition>" % "</NextQuestionCondition>") bodies))
       "</Root>"))

(def ^:private single (doc "<SingleComparison>a</SingleComparison>"))
(def ^:private repeated (doc "<SingleComparison>a</SingleComparison>"
                             "<RangeComparison>b</RangeComparison>"
                             "<SingleComparison>c</SingleComparison>"))

(deftest multi-under-sequential-parses
  "A :multi reached through [:sequential {:min 1} [:ref k]] parses each repeated
  wrapper element and dispatches on the inner tag."
  (is (= {:NextQuestionCondition [[:SingleComparison "a"]]}
         (parse (ref-schema multi-choice) single)))
  (is (= {:NextQuestionCondition [[:SingleComparison "a"]
                                  [:RangeComparison "b"]
                                  [:SingleComparison "c"]]}
         (parse (ref-schema multi-choice) repeated))))

(deftest multi-under-sequential-matches-or
  "or->multi conversion must be parse-neutral: the discriminated :multi yields
  exactly what the :or it replaced yields, referenced and inlined alike."
  (doseq [[label ->schema] [["ref" ref-schema] ["inline" inline-schema]]]
    (testing label
      (doseq [[what xml] [["single" single] ["repeated" repeated]]]
        (testing what
          (is (= (parse (->schema or-choice) xml)
                 (parse (->schema multi-choice) xml))))))))

(deftest multi-under-sequential-consumes-wrapper-end-tag
  "Each repeated element must be fully consumed, closing tag included, or the
  enclosing element fails to exit."
  (is (some? (parse (ref-schema multi-choice) repeated))
      "a trailing </NextQuestionCondition> left on the reader throws 'expected to exit :Root'"))
