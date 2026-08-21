(ns
 org.w3.www.2001.XMLSchema.schemaTop
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NCName
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/public
   :org.w3.www.2001.XMLSchema/redefinable
   :org.w3.www.2001.XMLSchema/redefinable-seq
   :org.w3.www.2001.XMLSchema/token
   :org.w3.www.2001.XMLSchema/topLevelAttribute
   :org.w3.www.2001.XMLSchema/topLevelElement})
(def
 sch
 [:or
  [:ref :org.w3.www.2001.XMLSchema/redefinable]
  [:tuple
   [:enum :element]
   [:ref :org.w3.www.2001.XMLSchema/topLevelElement]]
  [:tuple
   [:enum :attribute]
   [:ref :org.w3.www.2001.XMLSchema/topLevelAttribute]]
  [:tuple
   [:enum :notation]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:name {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/NCName]]
     [:public
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/public]]
     [:system
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/anyURI]]]
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
  [:ref :org.w3.www.2001.XMLSchema/redefinable-seq]
  [:tuple
   [:enum :element]
   [:ref :org.w3.www.2001.XMLSchema/topLevelElement]]
  [:tuple
   [:enum :attribute]
   [:ref :org.w3.www.2001.XMLSchema/topLevelAttribute]]
  [:tuple
   [:enum :notation]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:name {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/NCName]]
     [:public
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/public]]
     [:system
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/anyURI]]]
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
