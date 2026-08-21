(ns
 org.w3.www.2001.XMLSchema.basicNamespaceList
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/anyURI :org.w3.www.2001.XMLSchema/token})
(def
 sch
 [:sequential
  [:or
   :org.w3.www.2001.XMLSchema/anyURI
   [:and
    [:enum "##targetNamespace" "##local"]
    :org.w3.www.2001.XMLSchema/token]]])
