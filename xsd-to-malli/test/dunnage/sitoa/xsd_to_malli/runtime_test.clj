(ns dunnage.sitoa.xsd-to-malli.runtime-test
  "The runtime assembles compiled forms the same way the XSOM pipeline does.

  Every helper that is a port asserts parity against the function it was ported
  from, including the private ones, because a silent drift there would move
  parse results rather than merely reshape forms."
  (:require [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [dunnage.sitoa.xsd-to-malli.runtime :as rt]
            [malli.core :as m]))

(def ^:private attrs-map
  [:map {:closed true}
   [:createdBy {:xml/attr true :optional true} [:ref :org.w3.www.2001.XMLSchema/string]]
   [:version {:xml/attr true :optional true} [:ref :types.example/codeType]]])

(def ^:private content-map
  [:map {:closed true}
   [:id [:ref :org.w3.www.2001.XMLSchema/string]]
   [:note {:optional true} [:ref :org.w3.www.2001.XMLSchema/string]]])

;; ---------------------------------------------------------------------------
;; Shapes
;; ---------------------------------------------------------------------------

(deftest shapes-locate-attributes-and-content
  (testing ":merge - attributes plus map content"
    (let [form [:merge {} attrs-map content-map]]
      (is (= :merge (rt/shape-of form)))
      (is (= (vec (drop 2 attrs-map)) (rt/attrs-of form :merge)))
      (is (= content-map (rt/content-of form :merge)))))

  (testing ":merge with several content parts keeps them together"
    (let [form [:merge {} attrs-map content-map [:map {:closed true} [:extra :string]]]]
      (is (= [:merge {} content-map [:map {:closed true} [:extra :string]]]
             (rt/content-of form :merge)))))

  (testing "a :merge of content maps only is content, not attributes"
    (is (= :content-only (rt/shape-of [:merge {} content-map content-map]))))

  (testing ":value-wrapped"
    (let [form [:map {:closed true :xml/value-wrapped true}
                [:currency {:xml/attr true} [:ref :types.example/codeType]]
                [:xml/value {} :org.w3.www.2001.XMLSchema/decimal]]]
      (is (= :value-wrapped (rt/shape-of form)))
      (is (= [[:currency {:xml/attr true} [:ref :types.example/codeType]]]
             (rt/attrs-of form :value-wrapped)))
      (is (= :org.w3.www.2001.XMLSchema/decimal (rt/content-of form :value-wrapped)))))

  (testing ":attrs-only, :empty and :content-only"
    (is (= :attrs-only (rt/shape-of attrs-map)))
    (is (nil? (rt/content-of attrs-map :attrs-only)))
    (is (= :empty (rt/shape-of [:map {:empty true}])))
    (is (nil? (rt/content-of [:map {:empty true}] :empty)))
    (is (= :content-only (rt/shape-of content-map)))
    (is (= content-map (rt/content-of content-map :content-only)))
    (is (= [] (rt/attrs-of content-map :content-only))))

  (testing "properties dropped by m/form do not confuse the accessors"
    (is (= content-map (rt/content-of [:merge attrs-map content-map] :merge)))
    (is (= (vec (drop 2 attrs-map)) (rt/attrs-of [:merge attrs-map content-map] :merge)))))

;; ---------------------------------------------------------------------------
;; Ports
;; ---------------------------------------------------------------------------

(deftest simplify-fields-matches-the-oracle
  (let [row [:id {} [:ref :org.w3.www.2001.XMLSchema/string]]
        cases [[]
               [row]
               [row [:note {:optional true} :string]]
               [content-map]
               [content-map [:map {:closed true} [:detail :string]]]
               [[:merge {} content-map content-map] [:map {:closed true} [:detail :string]]]
               [content-map row]
               [row content-map row]]]
    (doseq [props [{:closed true} {:closed true :xml/in-seq-ex true}]
            vals cases]
      (testing (pr-str props vals)
        (is (= (transduce (map identity) (bs/simplify-fields props) vals)
               (rt/combine-fields props vals)))))))

(deftest occurrence-wrappers-match-the-oracle
  (doseq [[min-occurs max-occurs] [[1 1] [0 1] [1 -1] [0 -1] [2 5] [1 3]]
          msch [[:tuple {} [:enum :x] :string] content-map :xml/hiccup]]
    (testing (pr-str [min-occurs max-occurs msch])
      (is (= (@#'bs/regex-occurrence min-occurs max-occurs msch)
             (rt/regex-occurrence min-occurs max-occurs msch)))))
  (is (= (@#'bs/mark-map-in-seq-ex content-map) (rt/mark-map-in-seq-ex content-map)))
  (is (= (@#'bs/mark-map-in-seq-ex :xml/hiccup) (rt/mark-map-in-seq-ex :xml/hiccup))))

(deftest choice-promotion-and-value-wrapping-match-the-oracle
  (doseq [form [[:or [:tuple {} [:enum :a] :string]]
                [:? [:or [:tuple {} [:enum :a] :string]]]
                [:sequential [:or [:tuple {} [:enum :a] :string]]]
                [:cat {} [:tuple {} [:enum :a] :string]]
                :xml/hiccup]]
    (testing (pr-str form)
      (is (= (@#'bs/promote-value-choice-to-alt form) (rt/promote-value-choice-to-alt form)))
      (is (= (@#'bs/value-wrap attrs-map form) (rt/value-wrap attrs-map form)))))

  (testing "all-maps? never holds, exactly as in the oracle"
    (doseq [form [[:cat {} content-map content-map] [:cat {}] [:cat {} :string]]]
      (is (= (bs/all-maps? form) (rt/all-maps? form)))
      (is (false? (rt/all-maps? form))))))

(deftest assembly-matches-the-oracle-combination-rules
  (testing "attributes plus simple content is value-wrapped"
    (is (= [:map {:closed true :xml/value-wrapped true}
            [:currency {:xml/attr true} :string]
            [:xml/value {} :org.w3.www.2001.XMLSchema/decimal]]
           (rt/assemble-complex {:attrs [[:currency {:xml/attr true} :string]]
                                 :simple :org.w3.www.2001.XMLSchema/decimal}))))
  (testing "attributes plus map content is a :merge"
    (is (= [:merge {} attrs-map content-map]
           (rt/assemble-complex {:attrs (vec (drop 2 attrs-map)) :content content-map}))))
  (testing "attributes plus merge content flattens into one :merge"
    (is (= [:merge {} attrs-map content-map content-map]
           (rt/assemble-complex {:attrs (vec (drop 2 attrs-map))
                                 :content [:merge {} content-map content-map]}))))
  (testing "attributes plus seqex content is value-wrapped"
    (is (= [:map {:closed true :xml/value-wrapped true}
            [:createdBy {:xml/attr true :optional true} [:ref :org.w3.www.2001.XMLSchema/string]]
            [:version {:xml/attr true :optional true} [:ref :types.example/codeType]]
            [:xml/value {} [:cat {} content-map]]]
           (rt/assemble-complex {:attrs (vec (drop 2 attrs-map))
                                 :content [:cat {} content-map]}))))
  (testing "mixed content collapses to hiccup, and beats the content particle"
    (is (= [:xml/value {} :xml/hiccup]
           (peek (rt/assemble-complex {:attrs [[:a {:xml/attr true} :string]]
                                       :mixed? true
                                       :content content-map})))))
  (testing "no attributes"
    (is (= content-map (rt/assemble-complex {:content content-map})))
    (is (= :string (rt/assemble-complex {:simple :string})))
    (is (= [:map {:empty true}] (rt/assemble-complex {:empty? true})))
    (is (nil? (rt/assemble-complex {}))))
  (testing "attributes and empty content is the attribute map"
    (is (= attrs-map (rt/assemble-complex {:attrs (vec (drop 2 attrs-map)) :empty? true})))))

(deftest empty-property-maps-are-dropped-like-m-form-drops-them
  (is (= [:cat [:tuple [:enum :a] :string]]
         (rt/drop-empty-props [:cat {} [:tuple {} [:enum :a] :string]])))
  (is (= [:map {:closed true} [:id :string]]
         (rt/drop-empty-props [:map {:closed true} [:id {} :string]])))
  (testing "enum values are data, not schema properties"
    (is (= [:enum "a" "b"] (rt/drop-empty-props [:enum {} "a" "b"])))))

;; ---------------------------------------------------------------------------
;; Derivation
;; ---------------------------------------------------------------------------

(def ^:private base-form
  "What a base type's namespace exports as `sch`."
  [:merge {} attrs-map content-map])

(def ^:private base-registry
  {:t/Base base-form
   :types.example/codeType [:and [:enum "A" "B"] :org.w3.www.2001.XMLSchema/token]})

(defn- build [registry k]
  (m/deref (m/deref (m/schema [:schema {:registry (merge xml-primitives/xmlschema-registry registry)} k]
                              xml-primitives/external-registry))))

(deftest derivation-builds-on-the-base-schema
  (testing "an extension appends rows and keeps the base's"
    (let [derived (reify m/IntoSchema
                    (-into-schema [_ _ _ options]
                      (rt/derive-complex
                       {:base base-form :base-shape :merge
                        :content-source base-form :content-shape :merge
                        :mode :splice-map
                        :splice-props {:closed true}
                        :own-content [:map {:closed true} [:detail :string]]
                        :attrs [[:priority {:xml/attr true :optional true} :string]]
                        :drop-attrs #{}}
                       options)))
          form (m/form (build (assoc base-registry :t/Derived derived) :t/Derived))]
      (is (= [:merge
              [:map {:closed true}
               [:createdBy {:xml/attr true :optional true} [:ref :org.w3.www.2001.XMLSchema/string]]
               [:version {:xml/attr true :optional true} [:ref :types.example/codeType]]
               [:priority {:xml/attr true :optional true} :string]]
              content-map
              [:map {:closed true} [:detail :string]]]
             form))))

  (testing "a restriction inherits attributes, narrows one and replaces the content"
    (let [derived (reify m/IntoSchema
                    (-into-schema [_ _ _ options]
                      (rt/derive-complex
                       {:base base-form :base-shape :merge
                        :mode :own
                        :own-content [:map {:closed true} [:id :string]]
                        :attrs [[:version {:xml/attr true} [:ref :types.example/codeType]]]
                        :drop-attrs #{:createdBy}}
                       options)))
          form (m/form (build (assoc base-registry :t/Derived derived) :t/Derived))]
      (is (= [:merge
              [:map {:closed true} [:version {:xml/attr true} [:ref :types.example/codeType]]]
              [:map {:closed true} [:id :string]]]
             form))))

  (testing "simple content is taken from the base when the derivation does not narrow it"
    (let [price [:map {:closed true :xml/value-wrapped true}
                 [:currency {:xml/attr true} :string]
                 [:xml/value :org.w3.www.2001.XMLSchema/decimal]]
          derived (reify m/IntoSchema
                    (-into-schema [_ _ _ options]
                      (rt/derive-complex
                       {:base price :base-shape :value-wrapped
                        :mode :none :simple :from-base
                        :attrs [[:currency {:xml/attr true} :string]]
                        :drop-attrs #{}}
                       options)))]
      (is (= price (m/form (build {:t/Price price :t/Derived derived} :t/Derived)))))))

(deftest realized-registries-can-be-closed
  (let [derived (reify m/IntoSchema
                  (-into-schema [_ _ _ options]
                    (rt/derive-complex
                     {:base base-form :base-shape :merge
                      :content-source base-form :content-shape :merge
                      :mode :splice-map
                      :splice-props {:closed true}
                      :own-content [:map {:closed true} [:detail :string]]
                      :attrs [] :drop-attrs #{}}
                     options)))
        registry (merge xml-primitives/xmlschema-registry
                        (assoc base-registry :t/Derived derived))
        realized (rt/realize-registry registry)]
    (testing "every value becomes a plain schema, reified ones included"
      (is (= (set (keys registry)) (set (keys realized))))
      (is (every? m/schema? (vals realized))))
    (testing "and the result feeds closed-make-schema, which the raw registry cannot"
      (is (some? (xml-primitives/closed-make-schema realized :t/Derived))))))
