(ns
 org.w3.www.2001.XMLSchema.attribute
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NCName
   :org.w3.www.2001.XMLSchema/NMTOKEN
   :org.w3.www.2001.XMLSchema/QName
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/boolean
   :org.w3.www.2001.XMLSchema/formChoice
   :org.w3.www.2001.XMLSchema/localSimpleType
   :org.w3.www.2001.XMLSchema/string
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:merge
  [:map
   {:closed true}
   [:default
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/string]]
   [:fixed
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/string]]
   [:form
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/formChoice]]
   [:id
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/ID]]
   [:inheritable
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/boolean]]
   [:name
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/NCName]]
   [:ref
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/QName]]
   [:targetNamespace
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/anyURI]]
   [:type
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/QName]]
   [:use
    {:xml/attr true, :optional true}
    [:and
     [:enum "prohibited" "optional" "required"]
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
          [:xml/value :xml/hiccup]]]]]]]]]
  [:map
   {:closed true}
   [:simpleType
    {:optional true}
    [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]])
(def
 sch-seq
 [:merge
  [:map
   {:closed true}
   [:default
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/string]]
   [:fixed
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/string]]
   [:form
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/formChoice]]
   [:id
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/ID]]
   [:inheritable
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/boolean]]
   [:name
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/NCName]]
   [:ref
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/QName]]
   [:targetNamespace
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/anyURI]]
   [:type
    {:xml/attr true, :optional true}
    [:ref :org.w3.www.2001.XMLSchema/QName]]
   [:use
    {:xml/attr true, :optional true}
    [:and
     [:enum "prohibited" "optional" "required"]
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
          [:xml/value :xml/hiccup]]]]]]]]]
  [:map
   {:closed true}
   [:simpleType
    {:optional true}
    [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]])
