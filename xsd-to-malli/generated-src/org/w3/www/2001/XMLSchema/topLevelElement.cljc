(ns
 org.w3.www.2001.XMLSchema.topLevelElement
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NCName
   :org.w3.www.2001.XMLSchema/QName
   :org.w3.www.2001.XMLSchema/altType
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/blockSet
   :org.w3.www.2001.XMLSchema/boolean
   :org.w3.www.2001.XMLSchema/derivationSet
   :org.w3.www.2001.XMLSchema/identityConstraint-seq
   :org.w3.www.2001.XMLSchema/localComplexType
   :org.w3.www.2001.XMLSchema/localSimpleType
   :org.w3.www.2001.XMLSchema/string
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
   [:ref :org.w3.www.2001.XMLSchema/blockSet]]
  [:default
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/string]]
  [:final
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/derivationSet]]
  [:fixed
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/string]]
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:name {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/NCName]]
  [:nillable
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:substitutionGroup
   {:xml/attr true, :optional true}
   [:sequential :org.w3.www.2001.XMLSchema/QName]]
  [:type
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/QName]]
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
    [:?
     [:alt
      [:tuple
       [:enum :simpleType]
       [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]
      [:tuple
       [:enum :complexType]
       [:ref :org.w3.www.2001.XMLSchema/localComplexType]]]]
    [:*
     [:tuple
      [:enum :alternative]
      [:ref :org.w3.www.2001.XMLSchema/altType]]]
    [:* [:ref :org.w3.www.2001.XMLSchema/identityConstraint-seq]]]]])
(def
 sch-seq
 [:map
  {:closed true, :xml/value-wrapped true}
  [:abstract
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:block
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/blockSet]]
  [:default
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/string]]
  [:final
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/derivationSet]]
  [:fixed
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/string]]
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:name {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/NCName]]
  [:nillable
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/boolean]]
  [:substitutionGroup
   {:xml/attr true, :optional true}
   [:sequential :org.w3.www.2001.XMLSchema/QName]]
  [:type
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/QName]]
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
    [:?
     [:alt
      [:tuple
       [:enum :simpleType]
       [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]
      [:tuple
       [:enum :complexType]
       [:ref :org.w3.www.2001.XMLSchema/localComplexType]]]]
    [:*
     [:tuple
      [:enum :alternative]
      [:ref :org.w3.www.2001.XMLSchema/altType]]]
    [:* [:ref :org.w3.www.2001.XMLSchema/identityConstraint-seq]]]]])
