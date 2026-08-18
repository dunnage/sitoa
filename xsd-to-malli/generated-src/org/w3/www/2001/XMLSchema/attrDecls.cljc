(ns
 org.w3.www.2001.XMLSchema.attrDecls
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NMTOKEN
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/attribute
   :org.w3.www.2001.XMLSchema/attributeGroupRef
   :org.w3.www.2001.XMLSchema/namespaceList
   :org.w3.www.2001.XMLSchema/qnameListA
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:cat
  [:*
   [:alt
    [:tuple
     [:enum :attribute]
     [:ref :org.w3.www.2001.XMLSchema/attribute]]
    [:tuple
     [:enum :attributeGroup]
     [:ref :org.w3.www.2001.XMLSchema/attributeGroupRef]]]]
  [:?
   [:tuple
    [:enum :anyAttribute]
    [:merge
     [:map
      {:closed true}
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:namespace
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/namespaceList]]
      [:notNamespace {:xml/attr true, :optional true} :string]
      [:notQName
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/qnameListA]]
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
             [:xml/value :xml/hiccup]]]]]]]]]]]]])
(def
 sch-seq
 [:cat
  [:*
   [:alt
    [:tuple
     [:enum :attribute]
     [:ref :org.w3.www.2001.XMLSchema/attribute]]
    [:tuple
     [:enum :attributeGroup]
     [:ref :org.w3.www.2001.XMLSchema/attributeGroupRef]]]]
  [:?
   [:tuple
    [:enum :anyAttribute]
    [:merge
     [:map
      {:closed true}
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:namespace
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/namespaceList]]
      [:notNamespace {:xml/attr true, :optional true} :string]
      [:notQName
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/qnameListA]]
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
             [:xml/value :xml/hiccup]]]]]]]]]]]]])
