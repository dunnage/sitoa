(ns dunnage.sitoa.xsd-to-malli.compiler-test
  "The compiler reproduces the XSOM pipeline's registry exactly.

  Every compiled type carries a flattened form alongside the value that gets
  emitted. The flattened form is what these tests compare against the oracle:
  if it matches key for key, the compiler read the schema documents the way
  XSOM did, and whatever the emitted derivation code then has to reproduce is
  pinned down. equivalence-test checks that it does."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.bootstrapped-schema :as bs]
            [dunnage.sitoa.schema-namespaces :as sn]
            [dunnage.sitoa.xsd-to-malli.compiler :as compiler]
            [dunnage.sitoa.xsd-to-malli.oracle :as oracle]
            [dunnage.sitoa.xsd-to-malli.support :as support])
  (:import (com.sun.xml.xsom XSSimpleType)))

(def ^:private out-root "target/compiler-test")

(defn- write-fixture! [dir name content]
  (let [f (io/file out-root dir name)]
    (io/make-parents f)
    (spit f content)
    f))

(defn- ex-type [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))

(defn- canon-arms [top-type]
  (update-vals (into {} (drop 2 top-type)) sn/canonicalize-form))

;; ---------------------------------------------------------------------------
;; Built-in datatypes
;; ---------------------------------------------------------------------------

(deftest the-builtin-datatype-table-matches-xsom
  (let [schema-set (oracle/parse-xsd-offline (oracle/xsd "XMLSchema.xsd"))
        schema (.getSchema schema-set "http://www.w3.org/2001/XMLSchema")
        checked (atom 0)]
    (doseq [[local expected] (sort compiler/builtin-datatypes)
            :let [^XSSimpleType t (.getSimpleType schema local)]
            :when t]
      (swap! checked inc)
      (testing local
        (is (= (keyword (str (.getVariety t))) (:variety expected)))
        (is (= (some-> t .getPrimitiveType .getName) (:primitive expected)))
        (is (= (.isPrimitive t) (:primitive? expected)))
        (is (= (some-> t .getBaseType .getName) (:base expected)))
        (testing "and its inline form is what -mtype produces"
          (is (= (bs/-mtype t {:default-ns "xsd"}) (:form expected))))))
    (testing "the table covers every built-in XSOM defines"
      (is (= 45 @checked))
      (is (= 49 (count compiler/builtin-datatypes))))
    (testing "the four XSOM has no definition for are still listed"
      (is (= #{"anyType" "dayTimeDuration" "untypedAtomic" "yearMonthDuration"}
             (into #{} (remove #(some? (.getSimpleType schema %)))
                   (keys compiler/builtin-datatypes)))))))

;; ---------------------------------------------------------------------------
;; Registry equality with the oracle
;; ---------------------------------------------------------------------------

(defn- registry-report [fixture]
  (let [{:keys [compiled oracle]} @fixture
        mine (sn/canonicalize-registry (compiler/flat-registry compiled))
        theirs (sn/canonicalize-registry (:registry oracle))
        ks (into (sorted-set) (concat (keys mine) (keys theirs)))]
    {:keys-equal (= (set (keys mine)) (set (keys theirs)))
     :count (count ks)
     :differing (into [] (remove (fn [k] (= (get mine k) (get theirs k)))) ks)}))

(deftest the-compiled-registry-equals-the-oracle-registry
  (doseq [fixture support/all-fixtures]
    (testing (:name @fixture)
      (let [{:keys [keys-equal count differing]} (registry-report fixture)]
        (is keys-equal)
        (is (pos? count))
        (is (= [] differing))))))

(deftest the-compiled-top-type-equals-the-oracle-top-type
  (doseq [fixture support/all-fixtures]
    (testing (:name @fixture)
      (is (= (canon-arms (:top-type (:oracle @fixture)))
             (canon-arms (:flat-top-type (:compiled @fixture))))))))

(deftest the-fixtures-are-substantial
  (testing "the comparison is not vacuously passing on empty registries"
    (is (= 230 (count (:registry (:compiled @support/fop)))))
    (is (= 56 (count (drop 2 (:top-type (:compiled @support/fop))))))
    (is (= 114 (count (:registry (:compiled @support/xmlschema)))))
    (is (= 14 (count (:registry (:compiled @support/multifile)))))
    (is (= 4 (count (:registry (:compiled @support/junit)))))))

;; ---------------------------------------------------------------------------
;; Derivation is modelled, not flattened
;; ---------------------------------------------------------------------------

(defn- emitted [fixture k] (:emit (get (:registry (:compiled @fixture)) k)))

(deftest derived-types-emit-a-plan-instead-of-a-flattened-form
  (let [plan (fn [k] (:plan (emitted support/multifile k)))]
    (testing "complexContent extension splices the base's content in map mode"
      (is (compiler/derived? (emitted support/multifile :types.example/ExtendedRecord)))
      (let [p (plan :types.example/ExtendedRecord)]
        (is (= :splice-map (:mode p)))
        (is (= 'types.example.BaseRecord/sch (:base p)))
        (is (= 'types.example.BaseRecord/sch (:content-source p)))
        (is (= [[:priority {:xml/attr true :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/int]]]
               (:attrs p)))
        (testing "and the -seq dual differs only in the properties of the spliced map"
          (is (= {:closed true :xml/in-seq-ex true}
                 (:splice-props (plan :types.example/ExtendedRecord-seq)))))))

    (testing "complexContent restriction folds: restriction restates, extension derives"
      (let [e (get (:registry (:compiled @support/multifile)) :types.example/StrictRecord)]
        (is (not (compiler/derived? (:emit e))))
        (is (= (:form e) (:emit e)))
        (testing "inherited attribute rows are resolved statically"
          (is (= [:map {:closed true}
                  [:version #:xml{:attr true} [:ref :types.example/codeType]]
                  [:createdBy {:xml/attr true :optional true}
                   [:ref :org.w3.www.2001.XMLSchema/string]]]
                 (second (rest (:emit e))))))))

    (testing "simpleContent restriction folds with the base's value type resolved statically"
      (let [e (get (:registry (:compiled @support/multifile)) :types.example/UsdPrice)]
        (is (not (compiler/derived? (:emit e))))
        (is (= (:form e) (:emit e)))
        (is (= [:map {:closed true :xml/value-wrapped true}
                [:currency #:xml{:attr true} [:ref :types.example/codeType]]
                [:xml/value {} :org.w3.www.2001.XMLSchema/decimal]]
               (:emit e)))))

    (testing "simpleContent extension of a simple type is a reference, not code"
      (let [v (emitted support/multifile :types.example/Price)]
        (is (not (compiler/derived? v)))
        (is (= [:map {:closed true :xml/value-wrapped true}
                [:currency {:xml/attr true} [:ref :types.example/codeType]]
                [:xml/value {} :org.w3.www.2001.XMLSchema/decimal]]
               v))))

    (testing "nothing else in the fixture is derived - restrictions fold to data"
      (is (= #{:types.example/ExtendedRecord :types.example/ExtendedRecord-seq}
             (into #{}
                   (comp (filter (fn [[_ v]] (compiler/derived? (:emit v)))) (map key))
                   (:registry (:compiled @support/multifile))))))))

(deftest an-anonymous-derived-type-is-modelled-too
  (testing "JUnit's testsuites element nests an extension inside an anonymous type"
    (let [arm (second (some (fn [[tag arm]] (when (= tag :testsuites) [tag arm]))
                            (drop 2 (:top-type (:compiled @support/junit)))))
          derived (->> (tree-seq coll? seq arm) (filter compiler/derived?) first)]
      (is (some? derived))
      (is (= 'junit.testsuite/sch (:base (:plan derived))))
      (is (= '#{junit.testsuite} (:requires derived))))))

(deftest xmlschema-exercises-both-splice-modes
  (let [plans (into {}
                    (comp (filter (fn [[_ v]] (compiler/derived? (:emit v))))
                          (map (fn [[k v]] [k (:plan (:emit v))])))
                    (:registry (:compiled @support/xmlschema)))
        modes (frequencies (map :mode (vals plans)))]
    (is (= 28 (count plans)))
    (testing "every extension mode the compiler has is reached"
      (is (= #{:splice-map :splice-cat :base :own} (set (keys modes))))
      (is (every? pos? (vals modes))))
    (testing "a seqex splice takes the base's -seq content"
      (is (= 'org.w3.www.2001.XMLSchema.annotated/sch-seq
             (:content-source (get plans :org.w3.www.2001.XMLSchema/complexType)))))
    (testing "restrictions fold even here: topLevelComplexType restates complexType"
      (let [e (get (:registry (:compiled @support/xmlschema))
                   :org.w3.www.2001.XMLSchema/topLevelComplexType)]
        (is (not (compiler/derived? (:emit e))))
        (testing "while anonymous extensions nested in its content stay code"
          (is (some compiler/derived? (tree-seq coll? seq (:emit e)))))))))

;; ---------------------------------------------------------------------------
;; Loud failures
;; ---------------------------------------------------------------------------

(deftest an-extension-repeating-a-base-tag-becomes-a-sequence-not-a-map
  ;; The counterexample for the map-mode splice: the oracle compiles a
  ;; duplicated tag as a sequence of two maps rather than one map with the base
  ;; row replaced, because its map-mode analysis carries a first-wins tag set
  ;; across both particles. The compiler reproduces the analysis, so the plan
  ;; that would silently replace the base row is never reached.
  (let [f (write-fixture!
           "duptag" "main.xsd"
           "<?xml version=\"1.0\"?>
<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"
           xmlns:t=\"urn:example:dup\" targetNamespace=\"urn:example:dup\">
  <xs:complexType name=\"Base\">
    <xs:sequence><xs:element name=\"item\" type=\"xs:string\"/></xs:sequence>
  </xs:complexType>
  <xs:complexType name=\"Derived\">
    <xs:complexContent>
      <xs:extension base=\"t:Base\">
        <xs:sequence><xs:element name=\"item\" type=\"xs:string\" maxOccurs=\"unbounded\"/></xs:sequence>
      </xs:extension>
    </xs:complexContent>
  </xs:complexType>
</xs:schema>")
        compiled (support/compile-schemas f "dup")
        derived (:emit (get (:registry compiled) :dup.example/Derived))
        oracle-registry (bs/xsd->registry {:default-ns "dup"}
                                          (oracle/parse-xsd-offline f))]
    (is (compiler/derived? derived))
    (is (= :splice-cat (:mode (:plan derived))))
    (is (= 'dup.example.Base/sch-seq (:content-source (:plan derived))))
    (testing "and the flattened form still equals the oracle's"
      (is (= (sn/canonicalize-form (get oracle-registry :dup.example/Derived))
             (sn/canonicalize-form (get-in (:registry compiled)
                                           [:dup.example/Derived :form])))))))

(deftest declaring-both-foo-and-foo-seq-is-refused
  (let [f (write-fixture!
           "seqcollision" "main.xsd"
           "<?xml version=\"1.0\"?>
<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"
           targetNamespace=\"urn:example:collide\">
  <xs:complexType name=\"Foo\"><xs:sequence/></xs:complexType>
  <xs:complexType name=\"Foo-seq\"><xs:sequence/></xs:complexType>
</xs:schema>")]
    (is (= :xsd-to-malli/seq-name-collision
           (ex-type #(support/compile-schemas f "collide"))))))

(deftest a-type-reference-that-resolves-to-nothing-is-refused
  (let [f (write-fixture!
           "missing" "main.xsd"
           "<?xml version=\"1.0\"?>
<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"
           xmlns:t=\"urn:example:missing\" targetNamespace=\"urn:example:missing\">
  <xs:element name=\"root\" type=\"t:Absent\"/>
</xs:schema>")]
    (is (= :xsd-to-malli/unresolved-type
           (ex-type #(support/compile-schemas f "missing"))))))
