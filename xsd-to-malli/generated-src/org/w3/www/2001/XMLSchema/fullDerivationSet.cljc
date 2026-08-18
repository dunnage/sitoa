(ns
 org.w3.www.2001.XMLSchema.fullDerivationSet
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/derivationControl
   :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:or
  [:and [:enum "#all"] :org.w3.www.2001.XMLSchema/token]
  [:sequential
   [:and
    [:enum "extension" "restriction" "list" "union"]
    :org.w3.www.2001.XMLSchema/derivationControl]]])
