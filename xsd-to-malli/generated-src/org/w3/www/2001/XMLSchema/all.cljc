(ns
 org.w3.www.2001.XMLSchema.all
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def
 deps
 #{:org.w3.www.2001.XMLSchema/ID
   :org.w3.www.2001.XMLSchema/NMTOKEN
   :org.w3.www.2001.XMLSchema/allModel
   :org.w3.www.2001.XMLSchema/allModel-seq
   :org.w3.www.2001.XMLSchema/decimal})
(def
 sch
 [:map
  {:closed true, :xml/value-wrapped true}
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:maxOccurs
   {:xml/attr true, :optional true}
   [:or
    :org.w3.www.2001.XMLSchema/decimal
    [:and [:enum "unbounded"] :org.w3.www.2001.XMLSchema/NMTOKEN]]]
  [:minOccurs
   {:xml/attr true, :optional true}
   :org.w3.www.2001.XMLSchema/decimal]
  [:xml/value [:ref :org.w3.www.2001.XMLSchema/allModel]]])
(def
 sch-seq
 [:map
  {:closed true, :xml/value-wrapped true}
  [:id
   {:xml/attr true, :optional true}
   [:ref :org.w3.www.2001.XMLSchema/ID]]
  [:maxOccurs
   {:xml/attr true, :optional true}
   [:or
    :org.w3.www.2001.XMLSchema/decimal
    [:and [:enum "unbounded"] :org.w3.www.2001.XMLSchema/NMTOKEN]]]
  [:minOccurs
   {:xml/attr true, :optional true}
   :org.w3.www.2001.XMLSchema/decimal]
  [:xml/value [:ref :org.w3.www.2001.XMLSchema/allModel-seq]]])
