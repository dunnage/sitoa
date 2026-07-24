(ns dunnage.sitoa.in-regex-unparser-test
  "Focused coverage for dual-mode in-regex? unparser paths:
  leaf string (regex), :cat of tuples, dual-mode :ref cache, and :multi value-mode."
  (:require [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.unparser :as unparser]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [malli.core :as m]))

(defn- schema [registry-root]
  (m/schema
   [:schema {:registry {:test/Root registry-root}
             :topElement "Root"}
    :test/Root]
   xml-primitives/external-registry))

(defn- unparse [sch data]
  ((unparser/xml-string-unparser sch) data))

(deftest cat-of-strings-leaf-in-regex
  "Value-wrapped :cat of plain :string forces string-unparser with in-regex? true
  (pos-based leaf path that writes characters and advances pos)."
  (let [sch (schema
             [:map {:closed true}
              [:val {}
               [:map {:closed true :xml/value-wrapped true}
                [:xml/value {} [:cat {} :string :string]]]]])
        xml (unparse sch {:val {:xml/value ["hello" "world"]}})]
    (is (re-find #"<val>helloworld</val>" xml))
    (is (re-find #"<Root>" xml))))

(deftest cat-of-tuples-value-wrapped
  "Public API always starts with in-regex? false; value-wrapped :cat of two
  tagged tuples exercises in-regex tuple + value-mode string children."
  (let [sch (schema
             [:map {:closed true}
              [:val {}
               [:map {:closed true :xml/value-wrapped true}
                [:xml/value {}
                 [:cat {}
                  [:tuple {} [:enum :a] :string]
                  [:tuple {} [:enum :b] :string]]]]]])
        xml (unparse sch {:val {:xml/value [[:a "x"] [:b "y"]]}})]
    (is (re-find #"<a>x</a>" xml))
    (is (re-find #"<b>y</b>" xml))
    (is (re-find #"<val><a>x</a><b>y</b></val>" xml))))

(deftest dual-mode-ref-map-child-and-cat
  "Same registry type as a map field (value-mode) and under :cat of :ref
  (in-regex mode). Dual-mode cache key [kw in-regex?] must not collapse."
  (let [sch (m/schema
             [:schema
              {:registry
               {:test/Item [:tuple {} [:enum :item] :string]
                :test/Root
                [:map {:closed true}
                 [:one {} [:ref :test/Item]]
                 [:parts {}
                  [:map {:closed true :xml/value-wrapped true}
                   [:xml/value {}
                    [:cat {} [:ref :test/Item] [:ref :test/Item]]]]]]}
               :topElement "Root"}
              :test/Root]
             xml-primitives/external-registry)
        data {:one [:item "solo"]
              :parts {:xml/value [[:item "a"] [:item "b"]]}}
        xml (unparse sch data)]
    (is (re-find #"<one><item>solo</item></one>" xml))
    (is (re-find #"<parts><item>a</item><item>b</item></parts>" xml))))

(deftest dual-mode-ref-map-child-and-star
  "Same registry type as map child and under [:* [:ref :T]]."
  (let [sch (m/schema
             [:schema
              {:registry
               {:test/Item [:tuple {} [:enum :item] :string]
                :test/Root
                [:map {:closed true}
                 [:one {} [:ref :test/Item]]
                 [:parts {}
                  [:map {:closed true :xml/value-wrapped true}
                   [:xml/value {} [:* {} [:ref :test/Item]]]]]]}
               :topElement "Root"}
              :test/Root]
             xml-primitives/external-registry)
        data {:one [:item "solo"]
              :parts {:xml/value [[:item "a"] [:item "b"] [:item "c"]]}}
        xml (unparse sch data)]
    (is (re-find #"<one><item>solo</item></one>" xml))
    (is (re-find #"<parts><item>a</item><item>b</item><item>c</item></parts>" xml))))

(deftest multi-value-mode-document-style
  ":multi as a map element body (in-regex? false) dispatches and unparses."
  (let [sch (schema
             [:map {:closed true}
              [:val {}
               [:multi {:dispatch first}
                [:a [:tuple {} [:enum :a] :string]]
                [:b [:tuple {} [:enum :b] :string]]]]])
        xml-a (unparse sch {:val [:a "x"]})
        xml-b (unparse sch {:val [:b "y"]})]
    (is (re-find #"<val><a>x</a></val>" xml-a))
    (is (re-find #"<val><b>y</b></val>" xml-b))
    (is (not (re-find #"<b>" xml-a)))
    (is (not (re-find #"<a>" xml-b)))))
