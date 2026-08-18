(ns
 org.w3.www.2001.XMLSchema.simpleDerivation
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/QName
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/localSimpleType
   :org.w3.www.2001.XMLSchema/simpleRestrictionModel-seq
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:or
  [:tuple
   [:enum :restriction]
   [:map
    {:closed true, :xml/value-wrapped true}
    [:base
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/QName]]
    [:id
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/ID]]
    [:xml/value
     [:cat
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
              [:xml/value :xml/hiccup]]]]]]]]]
      [:ref :org.w3.www.2001.XMLSchema/simpleRestrictionModel-seq]]]]]
  [:tuple
   [:enum :list]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:itemType
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/QName]]]
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
      [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]]]
  [:tuple
   [:enum :union]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:memberTypes
      {:xml/attr true, :optional true}
      [:sequential :org.w3.www.2001.XMLSchema/QName]]]
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
      [:sequential
       {:min 1}
       [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]]]]])
(def
 sch-seq
 [:alt
  [:tuple
   [:enum :restriction]
   [:map
    {:closed true, :xml/value-wrapped true}
    [:base
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/QName]]
    [:id
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/ID]]
    [:xml/value
     [:cat
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
              [:xml/value :xml/hiccup]]]]]]]]]
      [:ref :org.w3.www.2001.XMLSchema/simpleRestrictionModel-seq]]]]]
  [:tuple
   [:enum :list]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:itemType
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/QName]]]
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
      [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]]]
  [:tuple
   [:enum :union]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:memberTypes
      {:xml/attr true, :optional true}
      [:sequential :org.w3.www.2001.XMLSchema/QName]]]
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
      [:sequential
       {:min 1}
       [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]]]]])
