(ns
 org.w3.www.2001.XMLSchema.identityConstraint
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NCName
   :org.w3.www.2001.XMLSchema/QName
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/keybase
   :org.w3.www.2001.XMLSchema/token
   :org.w3.www.2001.XMLSchema/xpathDefaultNamespace})
(def
 sch
 [:or
  [:tuple [:enum :unique] [:ref :org.w3.www.2001.XMLSchema/keybase]]
  [:tuple [:enum :key] [:ref :org.w3.www.2001.XMLSchema/keybase]]
  [:tuple
   [:enum :keyref]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:name
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/NCName]]
     [:ref
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/QName]]
     [:refer
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
     {:closed true, :optional-group true}
     [:selector
      {:optional true, :required-in-group true}
      [:merge
       [:map
        {:closed true}
        [:id
         {:xml/attr true, :optional true}
         [:ref :org.w3.www.2001.XMLSchema/ID]]
        [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
        [:xpathDefaultNamespace
         {:xml/attr true, :optional true}
         [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
       [:map
        {:closed true}
        [:annotation
         {:optional true, :required-in-group true}
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
               [:xml/value :xml/hiccup]]]]]]]]]]]
     [:field
      {:optional true, :required-in-group true}
      [:sequential
       {:min 1}
       [:merge
        [:map
         {:closed true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
         [:xpathDefaultNamespace
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
        [:map
         {:closed true}
         [:annotation
          {:optional true, :required-in-group true}
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
                [:xml/value :xml/hiccup]]]]]]]]]]]]]]]])
(def
 sch-seq
 [:alt
  [:tuple [:enum :unique] [:ref :org.w3.www.2001.XMLSchema/keybase]]
  [:tuple [:enum :key] [:ref :org.w3.www.2001.XMLSchema/keybase]]
  [:tuple
   [:enum :keyref]
   [:merge
    [:map
     {:closed true}
     [:id
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/ID]]
     [:name
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/NCName]]
     [:ref
      {:xml/attr true, :optional true}
      [:ref :org.w3.www.2001.XMLSchema/QName]]
     [:refer
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
     {:closed true, :optional-group true}
     [:selector
      {:optional true, :required-in-group true}
      [:merge
       [:map
        {:closed true}
        [:id
         {:xml/attr true, :optional true}
         [:ref :org.w3.www.2001.XMLSchema/ID]]
        [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
        [:xpathDefaultNamespace
         {:xml/attr true, :optional true}
         [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
       [:map
        {:closed true}
        [:annotation
         {:optional true, :required-in-group true}
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
               [:xml/value :xml/hiccup]]]]]]]]]]]
     [:field
      {:optional true, :required-in-group true}
      [:sequential
       {:min 1}
       [:merge
        [:map
         {:closed true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
         [:xpathDefaultNamespace
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
        [:map
         {:closed true}
         [:annotation
          {:optional true, :required-in-group true}
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
                [:xml/value :xml/hiccup]]]]]]]]]]]]]]]])
