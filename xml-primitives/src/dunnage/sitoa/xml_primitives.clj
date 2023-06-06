(ns dunnage.sitoa.xml-primitives
  (:require [malli.core :as m]
            [clojure.test.check.generators :as gen]
            [malli.transform :as mt]
            [malli.util :as mu]
            [com.breezeehr.malli-java-time :as mjt]
            [malli.experimental.time]
            malli.experimental.time.generator
            com.breezeehr.java-time-printing)
  (:import (java.time LocalDateTime LocalDate LocalTime OffsetDateTime ZoneId ZoneOffset)))


(defn -string->bigdec [x]
  (if (string? x)
    (try
      (bigdec x)
      (catch Exception _e x))
    x))

(def xmlschema-custom
  {:decimal         (m/-simple-schema {:type          :decimal,
                                       :pred          decimal?
                                       :type-properties
                                       {:decode/string -string->bigdec
                                        :encode/string mt/-any->string}})})

(def external-registry {:registry (merge
                                    (m/default-schemas)
                                    (mu/schemas)
                                    xmlschema-custom
                                    (malli.experimental.time/schemas))})
(def xmlschema-registry
  {
   :org.w3.www.2001.XMLSchema/QName              :string ;javax.xml.namespace.QName
   :org.w3.www.2001.XMLSchema/NOTATION           :string ;javax.xml.namespace.QName
   :org.w3.www.2001.XMLSchema/float              :double
   :org.w3.www.2001.XMLSchema/double             :double
   :org.w3.www.2001.XMLSchema/decimal            :decimal
   :org.w3.www.2001.XMLSchema/anyURI,            :string
   :org.w3.www.2001.XMLSchema/boolean            :boolean ;(do (onlywhitespacefacet x) [:boolean {}]) ;"boolean, java.lang.Boolean"
   :org.w3.www.2001.XMLSchema/base64Binary       :any ;(m/-simple-schema {:type :bytes, :pred bytes?}) ;byte[]
   :org.w3.www.2001.XMLSchema/hexBinary          :any ;(m/-simple-schema {:type :bytes, :pred bytes?}) ;byte[]
   :org.w3.www.2001.XMLSchema/date,              :time/local-date ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/dateTime,          :time/offset-date-time ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/time,              :time/local-time ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/duration           :any ;javax.xml.datatype.Duration
   :org.w3.www.2001.XMLSchema/dayTimeDuration    :any ;javax.xml.datatype.Duration
   :org.w3.www.2001.XMLSchema/yearMonthDuration  :any ;javax.xml.datatype.Duration
   :org.w3.www.2001.XMLSchema/gDay,              :any ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/gMonth,            :any ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/gMonthDay,         :any ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/gYear,             :any ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/gYearMonth,        :any ;javax.xml.datatype.XMLGregorianCalendar
   :org.w3.www.2001.XMLSchema/integer            :int ;!!!!java.math.BigInteger
   :org.w3.www.2001.XMLSchema/nonPositiveInteger :int ;!!!!java.math.BigInteger
   :org.w3.www.2001.XMLSchema/negativeInteger    :int ;!!!!java.math.BigInteger
   :org.w3.www.2001.XMLSchema/long               :int ;"long, java.lang.Long"
   :org.w3.www.2001.XMLSchema/int                :int ;"int, java.lang.Integer"
   :org.w3.www.2001.XMLSchema/short              :int ;"short, java.lang.Short"
   :org.w3.www.2001.XMLSchema/byte               :int ;"byte, java.lang.Byte"
   :org.w3.www.2001.XMLSchema/nonNegativeInteger :int ;!!!!java.math.BigInteger
   :org.w3.www.2001.XMLSchema/unsignedLong       :int ;!!!!java.math.BigInteger
   :org.w3.www.2001.XMLSchema/unsignedInt        :int ;long
   :org.w3.www.2001.XMLSchema/unsignedShort      :int ;int
   :org.w3.www.2001.XMLSchema/unsignedByte       :int ;short
   :org.w3.www.2001.XMLSchema/positiveInteger    :int ;!!!!java.math.BigInteger
   :org.w3.www.2001.XMLSchema/string,            :string
   :org.w3.www.2001.XMLSchema/normalizedString,  :string
   :org.w3.www.2001.XMLSchema/token,             :string
   :org.w3.www.2001.XMLSchema/language,          :string
   :org.w3.www.2001.XMLSchema/NMTOKEN,           :string
   :org.w3.www.2001.XMLSchema/Name,              :string
   :org.w3.www.2001.XMLSchema/NCName,            :string
   :org.w3.www.2001.XMLSchema/ID,                :string
   :org.w3.www.2001.XMLSchema/IDREF,             :string
   :org.w3.www.2001.XMLSchema/ENTITY,            :string
   :org.w3.www.2001.XMLSchema/untypedAtomic,     :string
   :org.w3.www.2001.XMLSchema/anySimpleType      :string #_[:or
                                                            :org.w3.www.2001.XMLSchema/QName
                                                            :org.w3.www.2001.XMLSchema/NOTATION
                                                            :org.w3.www.2001.XMLSchema/float
                                                            :org.w3.www.2001.XMLSchema/double
                                                            :org.w3.www.2001.XMLSchema/decimal
                                                            :org.w3.www.2001.XMLSchema/anyURI
                                                            :org.w3.www.2001.XMLSchema/boolean
                                                            :org.w3.www.2001.XMLSchema/base64Binary
                                                            :org.w3.www.2001.XMLSchema/hexBinary
                                                            :org.w3.www.2001.XMLSchema/date
                                                            :org.w3.www.2001.XMLSchema/dateTime
                                                            :org.w3.www.2001.XMLSchema/time
                                                            :org.w3.www.2001.XMLSchema/duration
                                                            :org.w3.www.2001.XMLSchema/dayTimeDuration
                                                            :org.w3.www.2001.XMLSchema/yearMonthDuration
                                                            :org.w3.www.2001.XMLSchema/gDay
                                                            :org.w3.www.2001.XMLSchema/gMonth
                                                            :org.w3.www.2001.XMLSchema/gMonthDay
                                                            :org.w3.www.2001.XMLSchema/gYear
                                                            :org.w3.www.2001.XMLSchema/gYearMonth
                                                            :org.w3.www.2001.XMLSchema/integer
                                                            :org.w3.www.2001.XMLSchema/nonPositiveInteger
                                                            :org.w3.www.2001.XMLSchema/negativeInteger
                                                            :org.w3.www.2001.XMLSchema/long
                                                            :org.w3.www.2001.XMLSchema/int
                                                            :org.w3.www.2001.XMLSchema/short
                                                            :org.w3.www.2001.XMLSchema/byte
                                                            :org.w3.www.2001.XMLSchema/nonNegativeInteger
                                                            :org.w3.www.2001.XMLSchema/unsignedLong
                                                            :org.w3.www.2001.XMLSchema/unsignedInt
                                                            :org.w3.www.2001.XMLSchema/unsignedShort
                                                            :org.w3.www.2001.XMLSchema/unsignedByte
                                                            :org.w3.www.2001.XMLSchema/positiveInteger
                                                            :org.w3.www.2001.XMLSchema/string
                                                            :org.w3.www.2001.XMLSchema/normalizedString
                                                            :org.w3.www.2001.XMLSchema/token
                                                            :org.w3.www.2001.XMLSchema/language
                                                            :org.w3.www.2001.XMLSchema/NMTOKEN
                                                            :org.w3.www.2001.XMLSchema/Name
                                                            :org.w3.www.2001.XMLSchema/NCName
                                                            :org.w3.www.2001.XMLSchema/ID
                                                            :org.w3.www.2001.XMLSchema/IDREF
                                                            :org.w3.www.2001.XMLSchema/ENTITY
                                                            :org.w3.www.2001.XMLSchema/untypedAtomic]
   })

(defn make-schema [malli-registry start-type ]
  (m/schema [:schema {:registry
                      malli-registry}
             start-type]
            external-registry))

(defn closed-make-schema [malli-registry start-type]
  (m/schema [:schema {:registry
                      (reduce-kv
                        (fn [acc k v]
                          (assoc acc k (mu/closed-schema v)))
                        {}
                        malli-registry)}
             start-type]
            external-registry))

(defn update-start-type [schema start-type]
  (make-schema (-> schema m/properties :registry) start-type))

(defn closed-update-start-type [schema start-type]
  (closed-make-schema (-> schema m/properties :registry) start-type))