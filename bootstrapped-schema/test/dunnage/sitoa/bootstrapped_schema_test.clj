(ns dunnage.sitoa.bootstrapped-schema-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [dunnage.sitoa.bootstrapped-schema :as bs
             :refer [xsd->schema xsd->registry serialize-schema serialize-registry
                     raw-xsd->schema trim-registry-for-top-types
                     leading-tag-dispatch keywordize-leading-tag
                     or->multi or->multi-keys
                     parse-xsd value-form->seq-form derive-seq-registry]]
            [malli.core :as m]
            [malli.util :as mu]
            [malli.transform :as mt]
            [dunnage.sitoa.xml-primitives :as xml-primitives]
            [clojure.java.io :as io]
            [malli.generator :as mg]))

;; All fixtures below are synthetic: sitoa is public and carries no
;; NCPDP-derived data. Parsed-value fixtures use the FLAT chunk shapes the serde
;; parser actually produces.

(def ^:private multi-props
  {:dispatch      leading-tag-dispatch
   :decode/string {:enter keywordize-leading-tag}})

;; 1. leading-tag-dispatch

(deftest leading-tag-dispatch-test
  (testing "flat tuple"
    (is (= :Alpha (leading-tag-dispatch [:Alpha "v"]))))
  (testing "flat chunk"
    (is (= :Alpha (leading-tag-dispatch [[:Alpha "v"] [:Extension "e"]]))))
  (testing "nested chunk (generality; the parser splices chunks flat)"
    (is (= :Alpha (leading-tag-dispatch [[[:Alpha "v"]] [:Extension "e"]]))))
  (testing "string tag has not been decoded yet"
    (is (nil? (leading-tag-dispatch ["Alpha" "v"]))))
  (testing "no leading tag at all"
    (is (nil? (leading-tag-dispatch [])))
    (is (nil? (leading-tag-dispatch nil)))
    (is (nil? (leading-tag-dispatch :Alpha)))
    (is (nil? (leading-tag-dispatch "Alpha")))))

;; 2. keywordize-leading-tag

(deftest keywordize-leading-tag-test
  (testing "tag at [0]"
    (is (= [:Alpha "v"] (keywordize-leading-tag ["Alpha" "v"]))))
  (testing "tag at [0 0]"
    (is (= [[:Alpha "v"] [:Extension "e"]]
           (keywordize-leading-tag [["Alpha" "v"] [:Extension "e"]]))))
  (testing "already a keyword"
    (is (= [:Alpha "v"] (keywordize-leading-tag [:Alpha "v"]))))
  (testing "passthrough"
    (is (= "Alpha" (keywordize-leading-tag "Alpha")))
    (is (nil? (keywordize-leading-tag nil)))
    (is (= [] (keywordize-leading-tag [])))))

;; 3. Pattern A: tuple-only choice

(def ^:private pattern-a
  [:or
   [:tuple [:enum :Alpha] [:ref :test/AlphaType]]
   [:tuple {:documentation "The beta arm."} [:enum :Beta] [:ref :test/BetaType]]
   [:tuple {} [:enum :Gamma] [:ref :test/GammaType]]])

(deftest or->multi-tuple-choice-test
  (testing "arms are preserved verbatim, including empty and documented props"
    (is (= [:multi multi-props
            [:Alpha [:tuple [:enum :Alpha] [:ref :test/AlphaType]]]
            [:Beta [:tuple {:documentation "The beta arm."} [:enum :Beta] [:ref :test/BetaType]]]
            [:Gamma [:tuple {} [:enum :Gamma] [:ref :test/GammaType]]]]
           (or->multi pattern-a))))
  (testing "branch order follows arm order"
    (is (= [:Alpha :Beta :Gamma]
           (mapv first (drop 2 (or->multi pattern-a)))))))

;; 4. Pattern B: :cat arm led by an :alt

(def ^:private optional-note
  [:? [:tuple [:enum :Note] [:ref :test/NoteType]]])

(def ^:private any-extensions
  [:* [:tuple [:enum :Extension] [:ref :test/ExtensionType]]])

(def ^:private alpha-tuple [:tuple [:enum :Alpha] [:ref :test/AlphaType]])
(def ^:private beta-cat
  [:cat
   [:tuple [:enum :Beta] [:ref :test/BetaType]]
   [:tuple [:enum :BetaDetail] [:ref :test/BetaDetailType]]])
(def ^:private delta-tuple [:tuple [:enum :Delta] [:ref :test/DeltaType]])

(def ^:private pattern-b
  [:or
   [:cat [:alt alpha-tuple beta-cat] optional-note any-extensions]
   delta-tuple])

(deftest or->multi-alt-split-test
  (testing "one branch per :alt member, sharing the arm's tail"
    (is (= [:multi multi-props
            [:Alpha [:cat alpha-tuple optional-note any-extensions]]
            [:Beta [:cat beta-cat optional-note any-extensions]]
            [:Delta delta-tuple]]
           (or->multi pattern-b))))
  (testing "the tail is shared structurally, not rebuilt"
    (let [[alpha beta] (drop 2 (or->multi pattern-b))]
      (is (identical? optional-note (nth (second alpha) 2)))
      (is (identical? optional-note (nth (second beta) 2)))
      (is (identical? any-extensions (nth (second alpha) 3)))))
  (testing "a nested :cat member stays nested"
    (is (= :cat (first (nth (second (nth (or->multi pattern-b) 3)) 1)))))
  (testing "an empty tail keeps the [:cat member] wrapper"
    (is (= [:multi multi-props
            [:Alpha [:cat alpha-tuple]]
            [:Beta [:cat beta-cat]]
            [:Delta delta-tuple]]
           (or->multi [:or [:cat [:alt alpha-tuple beta-cat]] delta-tuple]))))
  (testing "the :cat arm's own properties are carried onto every branch"
    (is (= [:multi multi-props
            [:Alpha [:cat {:xml/group true} alpha-tuple any-extensions]]
            [:Beta [:cat {:xml/group true} beta-cat any-extensions]]]
           (or->multi [:or [:cat {:xml/group true} [:alt alpha-tuple beta-cat] any-extensions]]))))
  (testing "an empty property map on the :alt is dropped losslessly"
    (is (= [:multi multi-props
            [:Alpha [:cat alpha-tuple]]
            [:Beta [:cat beta-cat]]]
           (or->multi [:or [:cat [:alt {} alpha-tuple beta-cat]]]))))
  (testing "a :cat arm not led by an :alt is kept verbatim"
    (is (= [:multi multi-props
            [:Alpha [:cat alpha-tuple any-extensions]]]
           (or->multi [:or [:cat alpha-tuple any-extensions]])))))

;; 5. Properties and options

(deftest or->multi-props-and-opts-test
  (testing "choice properties are preserved and the injected keys win"
    (is (= [:multi (assoc multi-props :documentation "A choice.")
            [:Alpha [:tuple [:enum :Alpha] :string]]]
           (or->multi [:or {:documentation "A choice." :dispatch :stale}
                       [:tuple [:enum :Alpha] :string]]))))
  (testing ":decode-string? false omits the decode hook"
    (is (= [:multi {:dispatch leading-tag-dispatch}
            [:Alpha [:tuple [:enum :Alpha] :string]]]
           (or->multi [:or [:tuple [:enum :Alpha] :string]]
                      {:decode-string? false})))))

;; 6. Throw matrix

(defn- conversion-error
  "ex-data :reason for a conversion that is expected to fail."
  ([form] (conversion-error form nil))
  ([form opts]
   (try
     (or->multi form opts)
     ::no-throw
     (catch clojure.lang.ExceptionInfo e
       (:reason (ex-data e))))))

(deftest or->multi-throw-test
  (testing "simple-type unions are not discriminable"
    (is (= :not-discriminable (conversion-error [:or :string :int])))
    (is (= :not-discriminable (conversion-error [:or [:ref :test/AlphaType] delta-tuple])))
    (is (= :not-discriminable (conversion-error [:or [:map [:Alpha :string]] delta-tuple])))
    (is (= :not-discriminable (conversion-error [:or [:sequential delta-tuple]])))
    (is (= :not-discriminable (conversion-error [:or :test/AlphaType]))))
  (testing "a :cat arm with no fixed leading tag"
    (is (= :not-discriminable (conversion-error [:or [:cat optional-note any-extensions]])))
    (is (= :not-discriminable
           (conversion-error [:or [:cat [:repeat {:min 0 :max 2} delta-tuple] any-extensions]])))
    (is (= :not-discriminable
           (conversion-error [:or [:cat [:map [:Alpha :string]] any-extensions]])))
    (is (= :not-discriminable (conversion-error [:or [:cat [:ref :test/AlphaType]]])))
    (is (= :empty-cat (conversion-error [:or [:cat]]))))
  (testing "the tag enum must hold exactly one tag"
    (is (= :multi-tag-enum (conversion-error [:or [:tuple [:enum :Alpha :Beta] :string]])))
    (is (= :multi-tag-enum (conversion-error [:or [:tuple [:enum] :string]])))
    (is (= :not-enum-tagged (conversion-error [:or [:tuple :keyword :string]]))))
  (testing "duplicate branch keys are reported"
    (let [form [:or
                [:tuple [:enum :Alpha] :string]
                [:cat [:alt [:tuple [:enum :Alpha] :int] delta-tuple]]]]
      (is (= :duplicate-branch-keys (conversion-error form)))
      (is (= [:Alpha]
             (try (or->multi form)
                  (catch clojure.lang.ExceptionInfo e (:duplicates (ex-data e))))))))
  (testing "the input must be a choice form"
    (is (= :not-a-choice (conversion-error delta-tuple)))
    (is (= :not-a-choice (conversion-error [:cat alpha-tuple])))
    (is (= :not-a-choice (conversion-error :test/AlphaType))))
  (testing "an :alt carrying properties cannot be split"
    (is (= :alt-properties
           (conversion-error [:or [:cat [:alt {:documentation "d"} alpha-tuple delta-tuple]]]))))
  (testing ":alt-headed input producing a :cat branch"
    (let [form [:alt alpha-tuple beta-cat]]
      (is (= :seqex-branches (conversion-error form)))
      (is (= [:multi multi-props [:Alpha alpha-tuple] [:Beta beta-cat]]
             (or->multi form {:seqex-branches :allow})))))
  (testing ":or-headed input producing a :cat branch is always fine"
    (is (= [:multi multi-props [:Alpha alpha-tuple] [:Beta beta-cat]]
           (or->multi [:or alpha-tuple beta-cat])))))

;; 7. :alt-headed input converts like its :or twin

(deftest or->multi-alt-head-test
  (let [tuple-only [:or [:tuple [:enum :Alpha] :string] [:tuple [:enum :Delta] :int]]]
    (is (= (or->multi tuple-only)
           (or->multi (assoc tuple-only 0 :alt))))))

;; or->multi-keys

(deftest or->multi-keys-test
  (let [registry {:test/Choice   pattern-a
                  :test/Untouched [:map [:Alpha :string]]}]
    (testing "only the named keys are converted"
      (is (= (assoc registry :test/Choice (or->multi pattern-a))
             (or->multi-keys registry [:test/Choice]))))
    (testing "opts are passed through"
      (is (= [:multi {:dispatch leading-tag-dispatch}]
             (take 2 (:test/Choice (or->multi-keys registry [:test/Choice]
                                                   {:decode-string? false}))))))
    (testing "absent keys throw by default"
      (is (thrown? clojure.lang.ExceptionInfo (or->multi-keys registry [:test/Absent])))
      (is (= :missing-key
             (try (or->multi-keys registry [:test/Absent])
                  (catch clojure.lang.ExceptionInfo e (:reason (ex-data e)))))))
    (testing ":missing :skip leaves the registry alone"
      (is (= registry (or->multi-keys registry [:test/Absent] {:missing :skip}))))))

;; 8. Validation equivalence

(def ^:private test-registry
  (merge (m/default-schemas)
         (mu/schemas)
         xml-primitives/xmlschema-custom))

(defn- schema* [form]
  (m/schema form {:registry test-registry}))

(def ^:private tuple-choice
  [:or
   [:tuple [:enum :Alpha] :string]
   [:tuple [:enum :Beta] :int]])

(def ^:private seqex-choice
  [:or
   [:cat [:tuple [:enum :Alpha] :string] [:* [:tuple [:enum :Extension] :string]]]
   [:tuple [:enum :Delta] :string]])

(deftest validation-equivalence-test
  (testing "tuple choice"
    (let [original  (schema* tuple-choice)
          converted (schema* (or->multi tuple-choice))]
      (doseq [v [[:Alpha "x"] [:Beta 1]]]
        (is (true? (m/validate original v)) (pr-str v))
        (is (true? (m/validate converted v)) (pr-str v)))
      (doseq [v [[:Zeta "x"] [:Alpha 1] [:Beta "x"] [] ["Alpha" "x"] "nope" :nope 7 nil]]
        (is (false? (m/validate original v)) (pr-str v))
        (is (false? (m/validate converted v)) (pr-str v)))))
  (testing "seqex choice on flat chunk values"
    (let [original  (schema* seqex-choice)
          converted (schema* (or->multi seqex-choice))]
      (doseq [v [[[:Alpha "x"]] [[:Alpha "x"] [:Extension "e"]] [:Delta "d"]]]
        (is (true? (m/validate original v)) (pr-str v))
        (is (true? (m/validate converted v)) (pr-str v)))
      (doseq [v [[[:Zeta "x"]] [[:Alpha 1]] [[:Alpha "x"] [:Zeta "e"]]]]
        (is (false? (m/validate original v)) (pr-str v))
        (is (false? (m/validate converted v)) (pr-str v)))))
  (testing "the string transformer keywordizes the leading tag before dispatch"
    (let [converted (schema* (or->multi tuple-choice))]
      (is (= [:Alpha "x"] (m/decode converted ["Alpha" "x"] (mt/string-transformer))))
      (is (true? (m/validate converted (m/decode converted ["Alpha" "x"]
                                                 (mt/string-transformer)))))))
  (testing "the string transformer reaches a nested leading tag"
    (let [converted (schema* (or->multi seqex-choice))]
      (is (= [[:Alpha "x"] [:Extension "e"]]
             (m/decode converted [["Alpha" "x"] [:Extension "e"]]
                       (mt/string-transformer))))))
  (testing "an unknown tag is invalid, not an exception"
    (let [converted (schema* (or->multi tuple-choice))]
      (is (false? (m/validate converted [:Zeta "x"])))
      (is (some? (m/explain converted [:Zeta "x"]))))))
(defn- xsd [name] (io/file "dev-resources" name))

(defn- own-entries [registry]
  (remove (fn [[k v]] (= v (get xml-primitives/xmlschema-registry k))) registry))

(deftest builtin-types-never-become-registry-entries
  (testing "a schema in its own namespace keeps the builtin seed and nothing else"
    (let [registry (xsd->registry {:default-ns "fop"} (bs/parse-xsd (xsd "fop.xsd")))]
      (is (= 275 (count registry)))
      (is (empty? (filter (comp bs/xsd-builtin-kw? key) (own-entries registry))))))
  (testing "a reference to a builtin stays bare so the seeded entry resolves it"
    (is (= :org.w3.www.2001.XMLSchema/NMTOKEN
           (bs/wrap-ref-np :org.w3.www.2001.XMLSchema/NMTOKEN)))
    (is (= [:ref :fop/length_Type] (bs/wrap-ref-np :fop/length_Type))))
  (testing "anyType is a builtin with no mapping, so it inlines as open content"
    (is (= :xml/hiccup (bs/wrap-ref-np :org.w3.www.2001.XMLSchema/anyType)))
    (is (= :xml/hiccup (bs/wrap-ref-np :org.w3.www.2001.XMLSchema/anyType-seq)))))

(deftest the-schema-for-schemas-declares-into-the-builtin-namespace
  ;; XMLSchema.xsd's targetNamespace IS the XML Schema namespace: its types are
  ;; declarations, not builtins, so they must reach the registry and must be
  ;; referenced through [:ref ...] - the type graph is cyclic and a bare keyword
  ;; reference would expand forever.
  (let [xsom (bs/parse-xsd (xsd "XMLSchema.xsd"))
        registry (xsd->registry {:default-ns "xsd"} xsom)
        own (own-entries registry)]
    (is (pos? (count own)))
    (is (every? (comp #{"org.w3.www.2001.XMLSchema"} namespace key) own))
    (testing "the seeded builtins survive alongside them"
      (is (= :string (get registry :org.w3.www.2001.XMLSchema/string)))
      (is (not (contains? registry :org.w3.www.2001.XMLSchema/anyType))))
    (testing "the cyclic registry resolves into a schema"
      (is (some? (xsd->schema {:default-ns "xsd"} (xsd "XMLSchema.xsd")))))))
(deftest value-form->seq-form-test
  (let [registry {:t/Group [:map {:closed true} [:A {} :string]]
                  :t/Color [:enum {} "red" "green"]}
        tuple-a  [:tuple {} [:enum :A] :string]
        tuple-b  [:tuple {} [:enum :B] :string]]
    (testing "minOccurs=0 arms and content hoist :?"
      (is (= [:? [:alt [:? tuple-a] tuple-b]]
             (value-form->seq-form registry
                                   [:or {:xml/min 0}
                                    [:tuple {:xml/min 0} [:enum :A] :string]
                                    tuple-b]))))
    (testing "repeated arms map back onto :+ :* :repeat"
      (is (= [:alt [:+ tuple-a] [:* tuple-a] [:repeat {:min 2, :max 4} tuple-a]]
             (value-form->seq-form registry
                                   [:or
                                    [:sequential {:min 1} tuple-a]
                                    [:sequential {:min 0} tuple-a]
                                    [:sequential {:min 2 :max 4} tuple-a]]))))
    (testing "a union :or of simple types stays untouched"
      (is (= [:or :string [:ref :t/Color]]
             (value-form->seq-form registry [:or :string [:ref :t/Color]]))))
    (testing "refs to element content rename to the -seq form"
      (is (= [:ref :t/Group-seq]
             (value-form->seq-form registry [:ref :t/Group]))))
    (testing "entry maps gain :xml/in-seq-ex, nested group maps do not"
      (is (= [:map {:closed true :xml/in-seq-ex true} [:A {} :string]]
             (value-form->seq-form registry [:map {:closed true} [:A {} :string]])))
      (is (= [:merge {}
              [:map {:closed true :xml/in-seq-ex true} [:A {} :string]]
              [:map {:closed true :xml/group true} [:B {} :string]]]
             (value-form->seq-form registry
                                   [:merge {}
                                    [:map {:closed true} [:A {} :string]]
                                    [:map {:closed true :xml/group true} [:B {} :string]]]))))
    (testing "value-wrapped content converts under :xml/value"
      (is (= [:map {:closed true :xml/value-wrapped true}
              [:id {:xml/attr true :optional true} :string]
              [:xml/value {} [:? [:alt tuple-a tuple-b]]]]
             (value-form->seq-form registry
                                   [:map {:closed true :xml/value-wrapped true}
                                    [:id {:xml/attr true :optional true} :string]
                                    [:xml/value {} [:alt {:xml/min 0} tuple-a tuple-b]]]))))))

(defn- split-dual-registry
  "Split a dual-mode registry into its value half and its *-seq half,
  excluding the xml-primitives base entries."
  [registry]
  (let [base (set (keys xml-primitives/xmlschema-registry))
        own  (into {} (remove (comp base key)) registry)
        seq-keys (into #{}
                       (filter (fn [k]
                                 (and (str/ends-with? (name k) "-seq")
                                      (contains? own (keyword (namespace k)
                                                              (subs (name k) 0 (- (count (name k)) 4)))))))
                       (keys own))]
    {:seq-part (select-keys own seq-keys)
     :value-part (apply dissoc own seq-keys)}))

(deftest derive-seq-registry-matches-emitted-test
  (doseq [[default-ns resource] [["fop" "fop.xsd"]
                                 ["junit" "JUnit.xsd"]]]
    (testing resource
      (let [registry (xsd->registry {:default-ns default-ns} (parse-xsd (io/resource resource)))
            {:keys [seq-part value-part]} (split-dual-registry registry)]
        (is (pos? (count seq-part)))
        (is (= seq-part (derive-seq-registry value-part)))))))

(comment
  (set! *print-namespace-maps* false)
  (xsd->schema {:default-ns "xsd"} (io/resource "XMLSchema.xsd"))
  (with-open [writer (io/writer "resources/fop.edn")]
    (fipp.edn/pprint (m/form (xsd->schema {:default-ns "fop"} (io/resource "fop.xsd"))) {:writer writer}))

  (def message-schema (m/schema (xsd->registry {:default-ns "script"} (io/resource "NCPDP_2023011/transport.xsd"))
                                {:registry (merge
                                            (m/default-schemas)
                                            (mu/schemas)
                                            xml-primitives/xmlschema-custom)}))

  (->> (mg/generate message-schema) #_(m/explain message-schema))

  (serialize-registry
   (-> (xsd->schema {:default-ns "script"} (io/resource "NCPDP_2023011/transport.xsd"))
       (mu/update-properties update :registry trim-registry-for-top-types [:script/MessageType]))
   "script_registry.edn")
  (serialize-registry
   (-> (xsd->schema {:default-ns "directory"} (io/resource "Directory/62/directory6.2.xsd"))
       (mu/update-properties update :registry trim-registry-for-top-types [:directory/DirectoryMessageType]))
   "directory_registry.edn")
  (serialize-schema (xsd->schema {:default-ns "spl"} (io/resource "spl/spl.xsd")) "spl.edn"))
