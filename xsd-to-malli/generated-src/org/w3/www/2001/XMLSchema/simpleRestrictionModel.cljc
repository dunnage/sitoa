(ns
 org.w3.www.2001.XMLSchema.simpleRestrictionModel
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def deps #{:org.w3.www.2001.XMLSchema/localSimpleType})
(def
 sch
 [:cat
  [:?
   [:tuple
    [:enum :simpleType]
    [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]
  [:* [:alt [:tuple [:enum :facet] :xml/hiccup] [:xml/hiccup]]]])
(def
 sch-seq
 [:cat
  [:?
   [:tuple
    [:enum :simpleType]
    [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]
  [:* [:alt [:tuple [:enum :facet] :xml/hiccup] [:xml/hiccup]]]])
