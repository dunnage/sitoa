(ns dunnage.sitoa.xml-primitives-test
  (:require [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.xml-primitives :as xp]
            [malli.core :as m])
  (:import (java.nio ByteBuffer)
           (java.time LocalDate OffsetDateTime Year)))

(defn- valid? [type-kw x]
  (m/validate (xp/make-schema xp/xmlschema-registry type-kw) x))

(deftest builtin-types-validate
  (testing "strings, numbers and booleans"
    (is (valid? :org.w3.www.2001.XMLSchema/string "a"))
    (is (valid? :org.w3.www.2001.XMLSchema/decimal 1.5M))
    (is (not (valid? :org.w3.www.2001.XMLSchema/decimal 1.5)))
    (is (valid? :org.w3.www.2001.XMLSchema/int 3))
    (is (valid? :org.w3.www.2001.XMLSchema/boolean true)))
  (testing "dates and times, from malli.experimental.time"
    (is (valid? :org.w3.www.2001.XMLSchema/date (LocalDate/of 2026 10 19)))
    (is (valid? :org.w3.www.2001.XMLSchema/dateTime (OffsetDateTime/parse "2026-10-19T12:00:00Z")))
    (is (not (valid? :org.w3.www.2001.XMLSchema/date "2026-10-19"))))
  (testing "sitoa's own types"
    (is (valid? :org.w3.www.2001.XMLSchema/gYear (Year/of 2026)))
    (is (valid? :org.w3.www.2001.XMLSchema/base64Binary (ByteBuffer/wrap (byte-array 2))))
    (is (valid? :org.w3.www.2001.XMLSchema/duration [:any "hiccup"]))))

(deftest start-type-can-change
  (let [s (xp/make-schema (assoc xp/xmlschema-registry ::id :org.w3.www.2001.XMLSchema/string) ::id)]
    (is (m/validate s "x"))
    (is (m/validate (xp/update-start-type s :org.w3.www.2001.XMLSchema/int) 1))))
