(ns
 org.w3.www.2001.XMLSchema.qnameList
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/QName :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:sequential
  [:or
   :org.w3.www.2001.XMLSchema/QName
   [:and
    [:enum "##defined" "##definedSibling"]
    :org.w3.www.2001.XMLSchema/token]]])
