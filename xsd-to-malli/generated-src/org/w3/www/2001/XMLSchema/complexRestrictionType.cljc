(ns
 org.w3.www.2001.XMLSchema.complexRestrictionType
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NMTOKEN
   :org.w3.www.2001.XMLSchema/QName
   :org.w3.www.2001.XMLSchema/anyURI
   :org.w3.www.2001.XMLSchema/assertions-seq
   :org.w3.www.2001.XMLSchema/attrDecls-seq
   :org.w3.www.2001.XMLSchema/token
   :org.w3.www.2001.XMLSchema/typeDefParticle-seq
   :org.w3.www.2001.XMLSchema/wildcard})
(def
 sch
 [:map
  {:closed true, :xml/value-wrapped true}
  [:base {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/QName]]
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
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
       [:ref :org.w3.www.2001.XMLSchema/typeDefParticle-seq]]]]
    [:ref :org.w3.www.2001.XMLSchema/attrDecls-seq]
    [:ref :org.w3.www.2001.XMLSchema/assertions-seq]]]])
(def
 sch-seq
 [:map
  {:closed true, :xml/value-wrapped true}
  [:base {:xml/attr true} [:ref :org.w3.www.2001.XMLSchema/QName]]
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
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
       [:ref :org.w3.www.2001.XMLSchema/typeDefParticle-seq]]]]
    [:ref :org.w3.www.2001.XMLSchema/attrDecls-seq]
    [:ref :org.w3.www.2001.XMLSchema/assertions-seq]]]])
