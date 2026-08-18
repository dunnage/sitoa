(ns
 org.w3.www.2001.XMLSchema.xpathDefaultNamespace
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/anyURI :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:or
  :org.w3.www.2001.XMLSchema/anyURI
  [:and
   [:enum "##defaultNamespace" "##targetNamespace" "##local"]
   :org.w3.www.2001.XMLSchema/token]])
