(ns
 org.w3.www.2001.XMLSchema.noFixedFacet
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/anySimpleType
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:merge
  [:map
   {:closed true}
   [:id
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/ID]]
   [:value
    {:xml/attr true}
    [:ref :org.w3.www.2001.XMLSchema/anySimpleType]]]
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
          [:xml/value :xml/hiccup]]]]]]]]]])
(def
 sch-seq
 [:merge
  [:map
   {:closed true}
   [:id
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/ID]]
   [:value
    {:xml/attr true}
    [:ref :org.w3.www.2001.XMLSchema/anySimpleType]]]
  [:map
   {:xml/in-seq-ex true, :closed true}
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
          [:xml/value :xml/hiccup]]]]]]]]]])
