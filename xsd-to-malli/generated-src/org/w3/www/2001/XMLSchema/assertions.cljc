(ns
 org.w3.www.2001.XMLSchema.assertions
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit.")
(def deps #{:org.w3.www.2001.XMLSchema/assertion})
(def
 sch
 [:map
  {:closed true}
  [:assert
   {:optional true}
   [:sequential {:min 1} [:ref :org.w3.www.2001.XMLSchema/assertion]]]])
(def
 sch-seq
 [:map
  {:xml/in-seq-ex true, :closed true}
  [:assert
   {:optional true}
   [:sequential {:min 1} [:ref :org.w3.www.2001.XMLSchema/assertion]]]])
