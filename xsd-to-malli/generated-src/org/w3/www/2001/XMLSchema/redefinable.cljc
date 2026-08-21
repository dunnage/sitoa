(ns
 org.w3.www.2001.XMLSchema.redefinable
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/namedAttributeGroup
   :org.w3.www.2001.XMLSchema/namedGroup
   :org.w3.www.2001.XMLSchema/topLevelComplexType
   :org.w3.www.2001.XMLSchema/topLevelSimpleType})
(def
 sch
 [:or
  [:tuple
   [:enum :simpleType]
   [:ref :org.w3.www.2001.XMLSchema/topLevelSimpleType]]
  [:tuple
   [:enum :complexType]
   [:ref :org.w3.www.2001.XMLSchema/topLevelComplexType]]
  [:tuple [:enum :group] [:ref :org.w3.www.2001.XMLSchema/namedGroup]]
  [:tuple
   [:enum :attributeGroup]
   [:ref :org.w3.www.2001.XMLSchema/namedAttributeGroup]]])
(def
 sch-seq
 [:alt
  [:tuple
   [:enum :simpleType]
   [:ref :org.w3.www.2001.XMLSchema/topLevelSimpleType]]
  [:tuple
   [:enum :complexType]
   [:ref :org.w3.www.2001.XMLSchema/topLevelComplexType]]
  [:tuple [:enum :group] [:ref :org.w3.www.2001.XMLSchema/namedGroup]]
  [:tuple
   [:enum :attributeGroup]
   [:ref :org.w3.www.2001.XMLSchema/namedAttributeGroup]]])
