(ns
 org.w3.www.2001.XMLSchema.topLevelComplexType
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NCName
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/boolean
   :org.w3.www.2001.XMLSchema/complexTypeModel-seq
   :org.w3.www.2001.XMLSchema/derivationSet
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:map
  {:closed true, :xml/value-wrapped true}
  [:abstract
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:block
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/derivationSet]]
  [:defaultAttributesApply
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:final
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/derivationSet]]
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:mixed
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:name {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/NCName]]
  [:xml/value
   [:cat
    [:?
     [:tuple
      [:enum :annotation]
      [:map
       {:closed true, :xml/value-wrapped true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:xml/value
        [:*
         [:alt
          [:tuple
           [:enum :appinfo]
           [:map
            {:closed true, :xml/value-wrapped true}
            [:source
             {:xml/attr true, :optional true}
             [:ref :org.w3.www.2001.XMLSchema/anyURI]]
            [:xml/value :xml/hiccup]]]
          [:tuple
           [:enum :documentation]
           [:map
            {:closed true, :xml/value-wrapped true}
            [:source
             {:xml/attr true, :optional true}
             [:ref :org.w3.www.2001.XMLSchema/anyURI]]
            [:org.w3.www.XML.1998.namespace/lang
             {:xml/attr true, :optional true}
             [:or
              [:and
               [:re
                "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
               :org.w3.www.2001.XMLSchema/token]
              [:enum ""]]]
            [:xml/value :xml/hiccup]]]]]]]]]
    [:ref :org.w3.www.2001.XMLSchema/complexTypeModel-seq]]]])
(def
 sch-seq
 [:map
  {:closed true, :xml/value-wrapped true}
  [:abstract
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:block
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/derivationSet]]
  [:defaultAttributesApply
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:final
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/derivationSet]]
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:mixed
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:name {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/NCName]]
  [:xml/value
   [:cat
    [:?
     [:tuple
      [:enum :annotation]
      [:map
       {:closed true, :xml/value-wrapped true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:xml/value
        [:*
         [:alt
          [:tuple
           [:enum :appinfo]
           [:map
            {:closed true, :xml/value-wrapped true}
            [:source
             {:xml/attr true, :optional true}
             [:ref :org.w3.www.2001.XMLSchema/anyURI]]
            [:xml/value :xml/hiccup]]]
          [:tuple
           [:enum :documentation]
           [:map
            {:closed true, :xml/value-wrapped true}
            [:source
             {:xml/attr true, :optional true}
             [:ref :org.w3.www.2001.XMLSchema/anyURI]]
            [:org.w3.www.XML.1998.namespace/lang
             {:xml/attr true, :optional true}
             [:or
              [:and
               [:re
                "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
               :org.w3.www.2001.XMLSchema/token]
              [:enum ""]]]
            [:xml/value :xml/hiccup]]]]]]]]]
    [:ref :org.w3.www.2001.XMLSchema/complexTypeModel-seq]]]])
