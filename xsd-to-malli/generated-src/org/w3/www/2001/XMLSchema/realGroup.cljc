(ns
 org.w3.www.2001.XMLSchema.realGroup
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NCName
   :org.w3.www.2001.XMLSchema/QName
   :org.w3.www.2001.XMLSchema/all
   :org.w3.www.2001.XMLSchema/allNNI
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/explicitGroup
   :org.w3.www.2001.XMLSchema/nonNegativeInteger
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:map
  {:closed true, :xml/value-wrapped true}
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:maxOccurs
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/allNNI]]
  [:minOccurs
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/nonNegativeInteger]]
  [:name
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/NCName]]
  [:ref
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
      [:tuple [:enum :all] [:ref :org.w3.www.2001.XMLSchema/all]]
      [:tuple
       [:enum :choice]
       [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
      [:tuple
       [:enum :sequence]
       [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]]]]]])
(def
 sch-seq
 [:map
  {:closed true, :xml/value-wrapped true}
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:maxOccurs
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/allNNI]]
  [:minOccurs
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/nonNegativeInteger]]
  [:name
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/NCName]]
  [:ref
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
      [:tuple [:enum :all] [:ref :org.w3.www.2001.XMLSchema/all]]
      [:tuple
       [:enum :choice]
       [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
      [:tuple
       [:enum :sequence]
       [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]]]]]])
