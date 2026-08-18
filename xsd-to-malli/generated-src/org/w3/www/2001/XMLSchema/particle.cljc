(ns
 org.w3.www.2001.XMLSchema.particle
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NMTOKEN
   :org.w3.www.2001.XMLSchema/all
   :org.w3.www.2001.XMLSchema/allNNI
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/explicitGroup
   :org.w3.www.2001.XMLSchema/groupRef
   :org.w3.www.2001.XMLSchema/localElement
   :org.w3.www.2001.XMLSchema/namespaceList
   :org.w3.www.2001.XMLSchema/nonNegativeInteger
   :org.w3.www.2001.XMLSchema/qnameList
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:or
  [:tuple
   [:enum :element]
   [:ref :org.w3.www.2001.XMLSchema/localElement]]
  [:tuple [:enum :group] [:ref :org.w3.www.2001.XMLSchema/groupRef]]
  [:tuple [:enum :all] [:ref :org.w3.www.2001.XMLSchema/all]]
  [:tuple
   [:enum :choice]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
  [:tuple
   [:enum :sequence]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
  [:tuple
   [:enum :any]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:maxOccurs
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/allNNI]]
     [:minOccurs
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/nonNegativeInteger]]
     [:namespace
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/namespaceList]]
     [:notNamespace {:xml/attr true, :optional true} :string]
     [:notQName
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/qnameList]]
     [:processContents
      {:xml/attr true, :optional true}
      [:and
       [:enum "skip" "lax" "strict"]
       :org.w3.www.2001.XMLSchema/NMTOKEN]]]
    [:map
     {:closed true}
     [:annotation
      {:optional true}
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
            [:xml/value :xml/hiccup]]]]]]]]]]]])
(def
 sch-seq
 [:alt
  [:tuple
   [:enum :element]
   [:ref :org.w3.www.2001.XMLSchema/localElement]]
  [:tuple [:enum :group] [:ref :org.w3.www.2001.XMLSchema/groupRef]]
  [:tuple [:enum :all] [:ref :org.w3.www.2001.XMLSchema/all]]
  [:tuple
   [:enum :choice]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
  [:tuple
   [:enum :sequence]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
  [:tuple
   [:enum :any]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:maxOccurs
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/allNNI]]
     [:minOccurs
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/nonNegativeInteger]]
     [:namespace
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/namespaceList]]
     [:notNamespace {:xml/attr true, :optional true} :string]
     [:notQName
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/qnameList]]
     [:processContents
      {:xml/attr true, :optional true}
      [:and
       [:enum "skip" "lax" "strict"]
       :org.w3.www.2001.XMLSchema/NMTOKEN]]]
    [:map
     {:closed true}
     [:annotation
      {:optional true}
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
            [:xml/value :xml/hiccup]]]]]]]]]]]])
