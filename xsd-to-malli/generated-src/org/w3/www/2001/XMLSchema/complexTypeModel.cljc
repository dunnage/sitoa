(ns
 org.w3.www.2001.XMLSchema.complexTypeModel
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NMTOKEN
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/assertions-seq
   :org.w3.www.2001.XMLSchema/attrDecls-seq
   :org.w3.www.2001.XMLSchema/boolean
   :org.w3.www.2001.XMLSchema/complexRestrictionType
   :org.w3.www.2001.XMLSchema/extensionType
   :org.w3.www.2001.XMLSchema/simpleExtensionType
   :org.w3.www.2001.XMLSchema/simpleRestrictionType
   :org.w3.www.2001.XMLSchema/token
   :org.w3.www.2001.XMLSchema/typeDefParticle-seq
   :org.w3.www.2001.XMLSchema/wildcard})
(def
 sch
 [:or
  [:tuple
   [:enum :simpleContent]
   [:map
    {:closed true, :xml/value-wrapped true}
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
      [:alt
       [:tuple
        [:enum :restriction]
        [:ref :org.w3.www.2001.XMLSchema/simpleRestrictionType]]
       [:tuple
        [:enum :extension]
        [:ref :org.w3.www.2001.XMLSchema/simpleExtensionType]]]]]]]
  [:tuple
   [:enum :complexContent]
   [:map
    {:closed true, :xml/value-wrapped true}
    [:id
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/ID]]
    [:mixed
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/boolean]]
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
      [:alt
       [:tuple
        [:enum :restriction]
        [:ref :org.w3.www.2001.XMLSchema/complexRestrictionType]]
       [:tuple
        [:enum :extension]
        [:ref :org.w3.www.2001.XMLSchema/extensionType]]]]]]]
  [:cat
   [:?
    [:tuple
     [:enum :openContent]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:mode
        {:xml/attr true, :optional true}
        [:and
         [:enum "none" "interleave" "suffix"]
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
       [:any
        {:optional true}
        [:ref :org.w3.www.2001.XMLSchema/wildcard]]]]]]
   [:? [:ref :org.w3.www.2001.XMLSchema/typeDefParticle-seq]]
   [:ref :org.w3.www.2001.XMLSchema/attrDecls-seq]
   [:ref :org.w3.www.2001.XMLSchema/assertions-seq]]])
(def
 sch-seq
 [:alt
  [:tuple
   [:enum :simpleContent]
   [:map
    {:closed true, :xml/value-wrapped true}
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
      [:alt
       [:tuple
        [:enum :restriction]
        [:ref :org.w3.www.2001.XMLSchema/simpleRestrictionType]]
       [:tuple
        [:enum :extension]
        [:ref :org.w3.www.2001.XMLSchema/simpleExtensionType]]]]]]]
  [:tuple
   [:enum :complexContent]
   [:map
    {:closed true, :xml/value-wrapped true}
    [:id
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/ID]]
    [:mixed
     {:xml/attr true, :optional true}
     [:ref :org.w3.www.2001.XMLSchema/boolean]]
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
      [:alt
       [:tuple
        [:enum :restriction]
        [:ref :org.w3.www.2001.XMLSchema/complexRestrictionType]]
       [:tuple
        [:enum :extension]
        [:ref :org.w3.www.2001.XMLSchema/extensionType]]]]]]]
  [:cat
   [:?
    [:tuple
     [:enum :openContent]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:mode
        {:xml/attr true, :optional true}
        [:and
         [:enum "none" "interleave" "suffix"]
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
       [:any
        {:optional true}
        [:ref :org.w3.www.2001.XMLSchema/wildcard]]]]]]
   [:? [:ref :org.w3.www.2001.XMLSchema/typeDefParticle-seq]]
   [:ref :org.w3.www.2001.XMLSchema/attrDecls-seq]
   [:ref :org.w3.www.2001.XMLSchema/assertions-seq]]])
