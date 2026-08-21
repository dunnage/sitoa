(ns
 org.w3.www.2001.XMLSchema.allNNI
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/NMTOKEN
   :org.w3.www.2001.XMLSchema/decimal})
(def
 sch
 [:or
  :org.w3.www.2001.XMLSchema/decimal
  [:and [:enum "unbounded"] :org.w3.www.2001.XMLSchema/NMTOKEN]])
