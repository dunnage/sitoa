(ns dunnage.sitoa.unparser-test
  (:require [clojure.test :refer :all]
            [dunnage.sitoa.unparser :refer :all]
            [dunnage.sitoa.bootstrapped-schema :as schema]
            [dunnage.sitoa.xml-primitives :as xml-primitives ]
            [clojure.java.io :as io]
            [malli.util :as mu]
            [malli.core :as m]))


(comment
  (def message-schema (schema/xsd->schema {:default-ns "script"} (io/resource "NCPDP_20170715/transport.xsd")))
  (def simple-message-schema  (m/-set-children message-schema (-> message-schema m/children first m/children first vector)))


  (def up (xml-unparser simple-message-schema))
  (let [s (StringWriter.)
        up (xml-unparser simple-message-schema)]
    (with-open [s s #_(sink (io/file "testout.xml"))]
      (with-open [w ^XMLStreamWriter (make-stream-writer {} s)]
        ; (up "AP" w)
        #_(up {:Text "hi"
               :Code "hi"} w)
        (up [:Message {:DatatypesVersion   "20170715",
                       :TransportVersion   "20170715",
                       :TransactionDomain  "SCRIPT",
                       :TransactionVersion "20170715",
                       :StructuresVersion  "20170715",
                       :ECLVersion         "20170715",
                       :Header
                       {:To                    {:Qualifier "P", :xml/value  "7701630"},
                        :From                  {:Qualifier "C", :xml/value  "77777777"},
                        :MessageID             "1234567",
                        :SentTime              (LocalDateTime/parse "2010-10-01T08:15:22"),
                        :Security
                        {:UsernameToken
                         {:Password {:Type "PasswordDigest", :value "String"},
                          :Created  (ZonedDateTime/parse "2001-12-17T09:30:47Z")},
                         :Sender {:SecondaryIdentification "PASSWORDA"}},
                        :SenderSoftware
                        {:SenderSoftwareDeveloper      "MDLITE",
                         :SenderSoftwareProduct        "443",
                         :SenderSoftwareVersionRelease "2.1"},
                        :PrescriberOrderNumber "110088"},
                       :Body
                       [:NewRx
                        {:Patient
                         [:HumanPatient
                          {:Identification {:SocialSecurity "333445555"},
                           :Name           {:LastName "SMITH", :FirstName "MARY"},
                           :Gender         "F",
                           :DateOfBirth    [:Date (LocalDate/parse "1954-12-25")],
                           :Address
                           {:AddressLine1  "45 EAST ROAD SW",
                            :City          "CLANCY",
                            :StateProvince "WI",
                            :PostalCode    "54999",
                            :CountryCode   "US"},
                           :CommunicationNumbers
                           {:PrimaryTelephone {:Number "6512551122"}}}],
                         :Pharmacy
                         {:Identification       {:NCPDPID "7701630", :NPI "7878787878"},
                          :BusinessName         "MAIN STREET PHARMACY",
                          :CommunicationNumbers {:PrimaryTelephone {:Number "6152205656"}}},
                         :Prescriber
                         [:NonVeterinarian
                          {:Identification {:NPI "666666666"},
                           :Name           {:LastName "JONES", :FirstName "MARK"},
                           :Address
                           {:AddressLine1  "211 CENTRAL ROAD",
                            :City          "JONESVILLE",
                            :StateProvince "TN",
                            :PostalCode    "37777",
                            :CountryCode   "US"},
                           :CommunicationNumbers
                           {:PrimaryTelephone {:Number "6152219800"}}}],
                         :MedicationPrescribed
                         {:DrugCoded
                          {:Strength
                           {:StrengthValue         "240",
                            :StrengthForm          {:Code "C42998"},
                            :StrengthUnitOfMeasure {:Code "C28253"}}},
                          :DrugDescription       "CALAN SR 240MG",
                          :WrittenDate           [:Date (LocalDate/parse "2010-10-01")],
                          :RxFillIndicator       "All Fill Statuses Except Transferred",
                          :NumberOfRefills       "1",
                          :PrescriberCheckedREMS "A",
                          :DaysSupply            "30",
                          :Substitutions         "0",
                          :Quantity
                          {:Value                 "60",
                           :CodeListQualifier     "38",
                           :QuantityUnitOfMeasure {:Code "C48542"}},
                          :Sig                   [[:SigText "TAKE ONE TABLET TWO TIMES A DAY UNTIL GONE"]]}}]}] w)
        ))
    (println (.toString s)))
  )
