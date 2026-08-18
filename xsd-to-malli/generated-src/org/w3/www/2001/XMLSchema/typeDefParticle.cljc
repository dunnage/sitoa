(ns
 org.w3.www.2001.XMLSchema.typeDefParticle
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/all
   :org.w3.www.2001.XMLSchema/explicitGroup
   :org.w3.www.2001.XMLSchema/groupRef})
(def
 sch
 [:or
  [:tuple [:enum :group] [:ref :org.w3.www.2001.XMLSchema/groupRef]]
  [:tuple [:enum :all] [:ref :org.w3.www.2001.XMLSchema/all]]
  [:tuple
   [:enum :choice]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
  [:tuple
   [:enum :sequence]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]])
(def
 sch-seq
 [:alt
  [:tuple [:enum :group] [:ref :org.w3.www.2001.XMLSchema/groupRef]]
  [:tuple [:enum :all] [:ref :org.w3.www.2001.XMLSchema/all]]
  [:tuple
   [:enum :choice]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]
  [:tuple
   [:enum :sequence]
   [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]])
