(ns
 dunnage.sitoa.xsd-meta
 "Generated from XSD by dunnage.sitoa.schema-namespaces. Do not edit."
 (:require
  [dunnage.sitoa.xml-primitives :as xml-primitives]
  [org.w3.www.2001.XMLSchema.all]
  [org.w3.www.2001.XMLSchema.allModel]
  [org.w3.www.2001.XMLSchema.allNNI]
  [org.w3.www.2001.XMLSchema.altType]
  [org.w3.www.2001.XMLSchema.annotated]
  [org.w3.www.2001.XMLSchema.assertion]
  [org.w3.www.2001.XMLSchema.assertions]
  [org.w3.www.2001.XMLSchema.attrDecls]
  [org.w3.www.2001.XMLSchema.attribute]
  [org.w3.www.2001.XMLSchema.attributeGroup]
  [org.w3.www.2001.XMLSchema.attributeGroupRef]
  [org.w3.www.2001.XMLSchema.basicNamespaceList]
  [org.w3.www.2001.XMLSchema.blockSet]
  [org.w3.www.2001.XMLSchema.complexRestrictionType]
  [org.w3.www.2001.XMLSchema.complexType]
  [org.w3.www.2001.XMLSchema.complexTypeModel]
  [org.w3.www.2001.XMLSchema.composition]
  [org.w3.www.2001.XMLSchema.derivationControl]
  [org.w3.www.2001.XMLSchema.derivationSet]
  [org.w3.www.2001.XMLSchema.element]
  [org.w3.www.2001.XMLSchema.explicitGroup]
  [org.w3.www.2001.XMLSchema.extensionType]
  [org.w3.www.2001.XMLSchema.facet]
  [org.w3.www.2001.XMLSchema.formChoice]
  [org.w3.www.2001.XMLSchema.fullDerivationSet]
  [org.w3.www.2001.XMLSchema.group]
  [org.w3.www.2001.XMLSchema.groupRef]
  [org.w3.www.2001.XMLSchema.identityConstraint]
  [org.w3.www.2001.XMLSchema.intFacet]
  [org.w3.www.2001.XMLSchema.keybase]
  [org.w3.www.2001.XMLSchema.localComplexType]
  [org.w3.www.2001.XMLSchema.localElement]
  [org.w3.www.2001.XMLSchema.localSimpleType]
  [org.w3.www.2001.XMLSchema.namedAttributeGroup]
  [org.w3.www.2001.XMLSchema.namedGroup]
  [org.w3.www.2001.XMLSchema.namespaceList]
  [org.w3.www.2001.XMLSchema.nestedParticle]
  [org.w3.www.2001.XMLSchema.noFixedFacet]
  [org.w3.www.2001.XMLSchema.numFacet]
  [org.w3.www.2001.XMLSchema.openAttrs]
  [org.w3.www.2001.XMLSchema.particle]
  [org.w3.www.2001.XMLSchema.public]
  [org.w3.www.2001.XMLSchema.qnameList]
  [org.w3.www.2001.XMLSchema.qnameListA]
  [org.w3.www.2001.XMLSchema.realGroup]
  [org.w3.www.2001.XMLSchema.redefinable]
  [org.w3.www.2001.XMLSchema.reducedDerivationControl]
  [org.w3.www.2001.XMLSchema.restrictionType]
  [org.w3.www.2001.XMLSchema.schemaTop]
  [org.w3.www.2001.XMLSchema.simpleDerivation]
  [org.w3.www.2001.XMLSchema.simpleDerivationSet]
  [org.w3.www.2001.XMLSchema.simpleExplicitGroup]
  [org.w3.www.2001.XMLSchema.simpleExtensionType]
  [org.w3.www.2001.XMLSchema.simpleRestrictionModel]
  [org.w3.www.2001.XMLSchema.simpleRestrictionType]
  [org.w3.www.2001.XMLSchema.simpleType]
  [org.w3.www.2001.XMLSchema.specialNamespaceList]
  [org.w3.www.2001.XMLSchema.topLevelAttribute]
  [org.w3.www.2001.XMLSchema.topLevelComplexType]
  [org.w3.www.2001.XMLSchema.topLevelElement]
  [org.w3.www.2001.XMLSchema.topLevelSimpleType]
  [org.w3.www.2001.XMLSchema.typeDefParticle]
  [org.w3.www.2001.XMLSchema.typeDerivationControl]
  [org.w3.www.2001.XMLSchema.wildcard]
  [org.w3.www.2001.XMLSchema.xpathDefaultNamespace]))
(def
 registry
 (merge
  xml-primitives/xmlschema-registry
  {:org.w3.www.2001.XMLSchema/all org.w3.www.2001.XMLSchema.all/sch,
   :org.w3.www.2001.XMLSchema/all-seq org.w3.www.2001.XMLSchema.all/sch-seq,
   :org.w3.www.2001.XMLSchema/allModel org.w3.www.2001.XMLSchema.allModel/sch,
   :org.w3.www.2001.XMLSchema/allModel-seq org.w3.www.2001.XMLSchema.allModel/sch-seq,
   :org.w3.www.2001.XMLSchema/allNNI org.w3.www.2001.XMLSchema.allNNI/sch,
   :org.w3.www.2001.XMLSchema/altType org.w3.www.2001.XMLSchema.altType/sch,
   :org.w3.www.2001.XMLSchema/altType-seq org.w3.www.2001.XMLSchema.altType/sch-seq,
   :org.w3.www.2001.XMLSchema/annotated org.w3.www.2001.XMLSchema.annotated/sch,
   :org.w3.www.2001.XMLSchema/annotated-seq org.w3.www.2001.XMLSchema.annotated/sch-seq,
   :org.w3.www.2001.XMLSchema/assertion org.w3.www.2001.XMLSchema.assertion/sch,
   :org.w3.www.2001.XMLSchema/assertion-seq org.w3.www.2001.XMLSchema.assertion/sch-seq,
   :org.w3.www.2001.XMLSchema/assertions org.w3.www.2001.XMLSchema.assertions/sch,
   :org.w3.www.2001.XMLSchema/assertions-seq org.w3.www.2001.XMLSchema.assertions/sch-seq,
   :org.w3.www.2001.XMLSchema/attrDecls org.w3.www.2001.XMLSchema.attrDecls/sch,
   :org.w3.www.2001.XMLSchema/attrDecls-seq org.w3.www.2001.XMLSchema.attrDecls/sch-seq,
   :org.w3.www.2001.XMLSchema/attribute org.w3.www.2001.XMLSchema.attribute/sch,
   :org.w3.www.2001.XMLSchema/attribute-seq org.w3.www.2001.XMLSchema.attribute/sch-seq,
   :org.w3.www.2001.XMLSchema/attributeGroup org.w3.www.2001.XMLSchema.attributeGroup/sch,
   :org.w3.www.2001.XMLSchema/attributeGroup-seq org.w3.www.2001.XMLSchema.attributeGroup/sch-seq,
   :org.w3.www.2001.XMLSchema/attributeGroupRef org.w3.www.2001.XMLSchema.attributeGroupRef/sch,
   :org.w3.www.2001.XMLSchema/attributeGroupRef-seq org.w3.www.2001.XMLSchema.attributeGroupRef/sch-seq,
   :org.w3.www.2001.XMLSchema/basicNamespaceList org.w3.www.2001.XMLSchema.basicNamespaceList/sch,
   :org.w3.www.2001.XMLSchema/blockSet org.w3.www.2001.XMLSchema.blockSet/sch,
   :org.w3.www.2001.XMLSchema/complexRestrictionType org.w3.www.2001.XMLSchema.complexRestrictionType/sch,
   :org.w3.www.2001.XMLSchema/complexRestrictionType-seq org.w3.www.2001.XMLSchema.complexRestrictionType/sch-seq,
   :org.w3.www.2001.XMLSchema/complexType org.w3.www.2001.XMLSchema.complexType/sch,
   :org.w3.www.2001.XMLSchema/complexType-seq org.w3.www.2001.XMLSchema.complexType/sch-seq,
   :org.w3.www.2001.XMLSchema/complexTypeModel org.w3.www.2001.XMLSchema.complexTypeModel/sch,
   :org.w3.www.2001.XMLSchema/complexTypeModel-seq org.w3.www.2001.XMLSchema.complexTypeModel/sch-seq,
   :org.w3.www.2001.XMLSchema/composition org.w3.www.2001.XMLSchema.composition/sch,
   :org.w3.www.2001.XMLSchema/composition-seq org.w3.www.2001.XMLSchema.composition/sch-seq,
   :org.w3.www.2001.XMLSchema/derivationControl org.w3.www.2001.XMLSchema.derivationControl/sch,
   :org.w3.www.2001.XMLSchema/derivationSet org.w3.www.2001.XMLSchema.derivationSet/sch,
   :org.w3.www.2001.XMLSchema/element org.w3.www.2001.XMLSchema.element/sch,
   :org.w3.www.2001.XMLSchema/element-seq org.w3.www.2001.XMLSchema.element/sch-seq,
   :org.w3.www.2001.XMLSchema/explicitGroup org.w3.www.2001.XMLSchema.explicitGroup/sch,
   :org.w3.www.2001.XMLSchema/explicitGroup-seq org.w3.www.2001.XMLSchema.explicitGroup/sch-seq,
   :org.w3.www.2001.XMLSchema/extensionType org.w3.www.2001.XMLSchema.extensionType/sch,
   :org.w3.www.2001.XMLSchema/extensionType-seq org.w3.www.2001.XMLSchema.extensionType/sch-seq,
   :org.w3.www.2001.XMLSchema/facet org.w3.www.2001.XMLSchema.facet/sch,
   :org.w3.www.2001.XMLSchema/facet-seq org.w3.www.2001.XMLSchema.facet/sch-seq,
   :org.w3.www.2001.XMLSchema/formChoice org.w3.www.2001.XMLSchema.formChoice/sch,
   :org.w3.www.2001.XMLSchema/fullDerivationSet org.w3.www.2001.XMLSchema.fullDerivationSet/sch,
   :org.w3.www.2001.XMLSchema/group org.w3.www.2001.XMLSchema.group/sch,
   :org.w3.www.2001.XMLSchema/group-seq org.w3.www.2001.XMLSchema.group/sch-seq,
   :org.w3.www.2001.XMLSchema/groupRef org.w3.www.2001.XMLSchema.groupRef/sch,
   :org.w3.www.2001.XMLSchema/groupRef-seq org.w3.www.2001.XMLSchema.groupRef/sch-seq,
   :org.w3.www.2001.XMLSchema/identityConstraint org.w3.www.2001.XMLSchema.identityConstraint/sch,
   :org.w3.www.2001.XMLSchema/identityConstraint-seq org.w3.www.2001.XMLSchema.identityConstraint/sch-seq,
   :org.w3.www.2001.XMLSchema/intFacet org.w3.www.2001.XMLSchema.intFacet/sch,
   :org.w3.www.2001.XMLSchema/intFacet-seq org.w3.www.2001.XMLSchema.intFacet/sch-seq,
   :org.w3.www.2001.XMLSchema/keybase org.w3.www.2001.XMLSchema.keybase/sch,
   :org.w3.www.2001.XMLSchema/keybase-seq org.w3.www.2001.XMLSchema.keybase/sch-seq,
   :org.w3.www.2001.XMLSchema/localComplexType org.w3.www.2001.XMLSchema.localComplexType/sch,
   :org.w3.www.2001.XMLSchema/localComplexType-seq org.w3.www.2001.XMLSchema.localComplexType/sch-seq,
   :org.w3.www.2001.XMLSchema/localElement org.w3.www.2001.XMLSchema.localElement/sch,
   :org.w3.www.2001.XMLSchema/localElement-seq org.w3.www.2001.XMLSchema.localElement/sch-seq,
   :org.w3.www.2001.XMLSchema/localSimpleType org.w3.www.2001.XMLSchema.localSimpleType/sch,
   :org.w3.www.2001.XMLSchema/localSimpleType-seq org.w3.www.2001.XMLSchema.localSimpleType/sch-seq,
   :org.w3.www.2001.XMLSchema/namedAttributeGroup org.w3.www.2001.XMLSchema.namedAttributeGroup/sch,
   :org.w3.www.2001.XMLSchema/namedAttributeGroup-seq org.w3.www.2001.XMLSchema.namedAttributeGroup/sch-seq,
   :org.w3.www.2001.XMLSchema/namedGroup org.w3.www.2001.XMLSchema.namedGroup/sch,
   :org.w3.www.2001.XMLSchema/namedGroup-seq org.w3.www.2001.XMLSchema.namedGroup/sch-seq,
   :org.w3.www.2001.XMLSchema/namespaceList org.w3.www.2001.XMLSchema.namespaceList/sch,
   :org.w3.www.2001.XMLSchema/nestedParticle org.w3.www.2001.XMLSchema.nestedParticle/sch,
   :org.w3.www.2001.XMLSchema/nestedParticle-seq org.w3.www.2001.XMLSchema.nestedParticle/sch-seq,
   :org.w3.www.2001.XMLSchema/noFixedFacet org.w3.www.2001.XMLSchema.noFixedFacet/sch,
   :org.w3.www.2001.XMLSchema/noFixedFacet-seq org.w3.www.2001.XMLSchema.noFixedFacet/sch-seq,
   :org.w3.www.2001.XMLSchema/numFacet org.w3.www.2001.XMLSchema.numFacet/sch,
   :org.w3.www.2001.XMLSchema/numFacet-seq org.w3.www.2001.XMLSchema.numFacet/sch-seq,
   :org.w3.www.2001.XMLSchema/openAttrs org.w3.www.2001.XMLSchema.openAttrs/sch,
   :org.w3.www.2001.XMLSchema/openAttrs-seq org.w3.www.2001.XMLSchema.openAttrs/sch-seq,
   :org.w3.www.2001.XMLSchema/particle org.w3.www.2001.XMLSchema.particle/sch,
   :org.w3.www.2001.XMLSchema/particle-seq org.w3.www.2001.XMLSchema.particle/sch-seq,
   :org.w3.www.2001.XMLSchema/public org.w3.www.2001.XMLSchema.public/sch,
   :org.w3.www.2001.XMLSchema/qnameList org.w3.www.2001.XMLSchema.qnameList/sch,
   :org.w3.www.2001.XMLSchema/qnameListA org.w3.www.2001.XMLSchema.qnameListA/sch,
   :org.w3.www.2001.XMLSchema/realGroup org.w3.www.2001.XMLSchema.realGroup/sch,
   :org.w3.www.2001.XMLSchema/realGroup-seq org.w3.www.2001.XMLSchema.realGroup/sch-seq,
   :org.w3.www.2001.XMLSchema/redefinable org.w3.www.2001.XMLSchema.redefinable/sch,
   :org.w3.www.2001.XMLSchema/redefinable-seq org.w3.www.2001.XMLSchema.redefinable/sch-seq,
   :org.w3.www.2001.XMLSchema/reducedDerivationControl org.w3.www.2001.XMLSchema.reducedDerivationControl/sch,
   :org.w3.www.2001.XMLSchema/restrictionType org.w3.www.2001.XMLSchema.restrictionType/sch,
   :org.w3.www.2001.XMLSchema/restrictionType-seq org.w3.www.2001.XMLSchema.restrictionType/sch-seq,
   :org.w3.www.2001.XMLSchema/schemaTop org.w3.www.2001.XMLSchema.schemaTop/sch,
   :org.w3.www.2001.XMLSchema/schemaTop-seq org.w3.www.2001.XMLSchema.schemaTop/sch-seq,
   :org.w3.www.2001.XMLSchema/simpleDerivation org.w3.www.2001.XMLSchema.simpleDerivation/sch,
   :org.w3.www.2001.XMLSchema/simpleDerivation-seq org.w3.www.2001.XMLSchema.simpleDerivation/sch-seq,
   :org.w3.www.2001.XMLSchema/simpleDerivationSet org.w3.www.2001.XMLSchema.simpleDerivationSet/sch,
   :org.w3.www.2001.XMLSchema/simpleExplicitGroup org.w3.www.2001.XMLSchema.simpleExplicitGroup/sch,
   :org.w3.www.2001.XMLSchema/simpleExplicitGroup-seq org.w3.www.2001.XMLSchema.simpleExplicitGroup/sch-seq,
   :org.w3.www.2001.XMLSchema/simpleExtensionType org.w3.www.2001.XMLSchema.simpleExtensionType/sch,
   :org.w3.www.2001.XMLSchema/simpleExtensionType-seq org.w3.www.2001.XMLSchema.simpleExtensionType/sch-seq,
   :org.w3.www.2001.XMLSchema/simpleRestrictionModel org.w3.www.2001.XMLSchema.simpleRestrictionModel/sch,
   :org.w3.www.2001.XMLSchema/simpleRestrictionModel-seq org.w3.www.2001.XMLSchema.simpleRestrictionModel/sch-seq,
   :org.w3.www.2001.XMLSchema/simpleRestrictionType org.w3.www.2001.XMLSchema.simpleRestrictionType/sch,
   :org.w3.www.2001.XMLSchema/simpleRestrictionType-seq org.w3.www.2001.XMLSchema.simpleRestrictionType/sch-seq,
   :org.w3.www.2001.XMLSchema/simpleType org.w3.www.2001.XMLSchema.simpleType/sch,
   :org.w3.www.2001.XMLSchema/simpleType-seq org.w3.www.2001.XMLSchema.simpleType/sch-seq,
   :org.w3.www.2001.XMLSchema/specialNamespaceList org.w3.www.2001.XMLSchema.specialNamespaceList/sch,
   :org.w3.www.2001.XMLSchema/topLevelAttribute org.w3.www.2001.XMLSchema.topLevelAttribute/sch,
   :org.w3.www.2001.XMLSchema/topLevelAttribute-seq org.w3.www.2001.XMLSchema.topLevelAttribute/sch-seq,
   :org.w3.www.2001.XMLSchema/topLevelComplexType org.w3.www.2001.XMLSchema.topLevelComplexType/sch,
   :org.w3.www.2001.XMLSchema/topLevelComplexType-seq org.w3.www.2001.XMLSchema.topLevelComplexType/sch-seq,
   :org.w3.www.2001.XMLSchema/topLevelElement org.w3.www.2001.XMLSchema.topLevelElement/sch,
   :org.w3.www.2001.XMLSchema/topLevelElement-seq org.w3.www.2001.XMLSchema.topLevelElement/sch-seq,
   :org.w3.www.2001.XMLSchema/topLevelSimpleType org.w3.www.2001.XMLSchema.topLevelSimpleType/sch,
   :org.w3.www.2001.XMLSchema/topLevelSimpleType-seq org.w3.www.2001.XMLSchema.topLevelSimpleType/sch-seq,
   :org.w3.www.2001.XMLSchema/typeDefParticle org.w3.www.2001.XMLSchema.typeDefParticle/sch,
   :org.w3.www.2001.XMLSchema/typeDefParticle-seq org.w3.www.2001.XMLSchema.typeDefParticle/sch-seq,
   :org.w3.www.2001.XMLSchema/typeDerivationControl org.w3.www.2001.XMLSchema.typeDerivationControl/sch,
   :org.w3.www.2001.XMLSchema/wildcard org.w3.www.2001.XMLSchema.wildcard/sch,
   :org.w3.www.2001.XMLSchema/wildcard-seq org.w3.www.2001.XMLSchema.wildcard/sch-seq,
   :org.w3.www.2001.XMLSchema/xpathDefaultNamespace org.w3.www.2001.XMLSchema.xpathDefaultNamespace/sch}))
(def
 top-type
 (into
  [:multi {:dispatch first}]
  [[:all [:tuple [:enum :all] [:ref :org.w3.www.2001.XMLSchema/all]]]
   [:annotation
    [:tuple
     [:enum :annotation]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:xml/value
       [:*
        [:alt
         [:tuple
          [:enum :appinfo]
          [:map
           {:closed true, :xml/value-wrapped true}
           [:source
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/anyURI]]
           [:xml/value :xml/hiccup]]]
         [:tuple
          [:enum :documentation]
          [:map
           {:closed true, :xml/value-wrapped true}
           [:source
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/anyURI]]
           [:org.w3.www.XML.1998.namespace/lang
            {:xml/attr true, :optional true}
            [:or
             [:and
              [:re
               "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
              :org.w3.www.2001.XMLSchema/token]
             [:enum ""]]]
           [:xml/value :xml/hiccup]]]]]]]]]
   [:any
    [:tuple
     [:enum :any]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:maxOccurs
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/allNNI]]
       [:minOccurs
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/nonNegativeInteger]]
       [:namespace
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/namespaceList]]
       [:notNamespace {:xml/attr true, :optional true} :string]
       [:notQName
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/qnameList]]
       [:processContents
        {:xml/attr true, :optional true}
        [:and
         [:enum "skip" "lax" "strict"]
         :org.w3.www.2001.XMLSchema/NMTOKEN]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:anyAttribute
    [:tuple
     [:enum :anyAttribute]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:namespace
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/namespaceList]]
       [:notNamespace {:xml/attr true, :optional true} :string]
       [:notQName
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/qnameListA]]
       [:processContents
        {:xml/attr true, :optional true}
        [:and
         [:enum "skip" "lax" "strict"]
         :org.w3.www.2001.XMLSchema/NMTOKEN]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:appinfo
    [:tuple
     [:enum :appinfo]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:source
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/anyURI]]
      [:xml/value :xml/hiccup]]]]
   [:assertion
    [:tuple
     [:enum :assertion]
     [:ref :org.w3.www.2001.XMLSchema/assertion]]]
   [:attribute
    [:tuple
     [:enum :attribute]
     [:ref :org.w3.www.2001.XMLSchema/topLevelAttribute]]]
   [:attributeGroup
    [:tuple
     [:enum :attributeGroup]
     [:ref :org.w3.www.2001.XMLSchema/namedAttributeGroup]]]
   [:choice
    [:tuple
     [:enum :choice]
     [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]]
   [:complexContent
    [:tuple
     [:enum :complexContent]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:mixed
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/boolean]]
      [:xml/value
       [:cat
        [:map
         {:xml/in-seq-ex true, :closed true}
         [:annotation
          {:optional true}
          [:map
           {:closed true, :xml/value-wrapped true}
           [:id
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/ID]]
           [:xml/value
            [:*
             [:alt
              [:tuple
               [:enum :appinfo]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:xml/value :xml/hiccup]]]
              [:tuple
               [:enum :documentation]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:org.w3.www.XML.1998.namespace/lang
                 {:xml/attr true, :optional true}
                 [:or
                  [:and
                   [:re
                    "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                   :org.w3.www.2001.XMLSchema/token]
                  [:enum ""]]]
                [:xml/value :xml/hiccup]]]]]]]]]
        [:alt
         [:tuple
          [:enum :restriction]
          [:ref :org.w3.www.2001.XMLSchema/complexRestrictionType]]
         [:tuple
          [:enum :extension]
          [:ref :org.w3.www.2001.XMLSchema/extensionType]]]]]]]]
   [:complexType
    [:tuple
     [:enum :complexType]
     [:ref :org.w3.www.2001.XMLSchema/topLevelComplexType]]]
   [:defaultOpenContent
    [:tuple
     [:enum :defaultOpenContent]
     [:merge
      [:map
       {:closed true}
       [:appliesToEmpty
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/boolean]]
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:mode
        {:xml/attr true, :optional true}
        [:and
         [:enum "interleave" "suffix"]
         :org.w3.www.2001.XMLSchema/NMTOKEN]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]
      [:map
       {:closed true}
       [:any [:ref :org.w3.www.2001.XMLSchema/wildcard]]]]]]
   [:documentation
    [:tuple
     [:enum :documentation]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:source
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/anyURI]]
      [:org.w3.www.XML.1998.namespace/lang
       {:xml/attr true, :optional true}
       [:or
        [:and
         [:re
          "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
         :org.w3.www.2001.XMLSchema/token]
        [:enum ""]]]
      [:xml/value :xml/hiccup]]]]
   [:element
    [:tuple
     [:enum :element]
     [:ref :org.w3.www.2001.XMLSchema/topLevelElement]]]
   [:enumeration
    [:tuple
     [:enum :enumeration]
     [:ref :org.w3.www.2001.XMLSchema/noFixedFacet]]]
   [:explicitTimezone
    [:tuple
     [:enum :explicitTimezone]
     [:merge
      [:map
       {:closed true}
       [:fixed
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/boolean]]
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:value
        {:xml/attr true}
        [:and
         [:enum "optional" "required" "prohibited"]
         :org.w3.www.2001.XMLSchema/NMTOKEN]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:facet [:tuple [:enum :facet] :xml/hiccup]]
   [:field
    [:tuple
     [:enum :field]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
       [:xpathDefaultNamespace
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:fractionDigits
    [:tuple
     [:enum :fractionDigits]
     [:ref :org.w3.www.2001.XMLSchema/numFacet]]]
   [:group
    [:tuple
     [:enum :group]
     [:ref :org.w3.www.2001.XMLSchema/namedGroup]]]
   [:import
    [:tuple
     [:enum :import]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:namespace
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/anyURI]]
       [:schemaLocation
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/anyURI]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:include
    [:tuple
     [:enum :include]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:schemaLocation
        {:xml/attr true}
        [:ref :org.w3.www.2001.XMLSchema/anyURI]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:key
    [:tuple [:enum :key] [:ref :org.w3.www.2001.XMLSchema/keybase]]]
   [:keyref
    [:tuple
     [:enum :keyref]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:name
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/NCName]]
       [:ref
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/QName]]
       [:refer
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/QName]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]
      [:map
       {:closed true, :optional-group true}
       [:selector
        {:optional true, :required-in-group true}
        [:merge
         [:map
          {:closed true}
          [:id
           {:xml/attr true, :optional true}
           [:ref :org.w3.www.2001.XMLSchema/ID]]
          [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
          [:xpathDefaultNamespace
           {:xml/attr true, :optional true}
           [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
         [:map
          {:closed true}
          [:annotation
           {:optional true, :required-in-group true}
           [:map
            {:closed true, :xml/value-wrapped true}
            [:id
             {:xml/attr true, :optional true}
             [:ref :org.w3.www.2001.XMLSchema/ID]]
            [:xml/value
             [:*
              [:alt
               [:tuple
                [:enum :appinfo]
                [:map
                 {:closed true, :xml/value-wrapped true}
                 [:source
                  {:xml/attr true, :optional true}
                  [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                 [:xml/value :xml/hiccup]]]
               [:tuple
                [:enum :documentation]
                [:map
                 {:closed true, :xml/value-wrapped true}
                 [:source
                  {:xml/attr true, :optional true}
                  [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                 [:org.w3.www.XML.1998.namespace/lang
                  {:xml/attr true, :optional true}
                  [:or
                   [:and
                    [:re
                     "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                    :org.w3.www.2001.XMLSchema/token]
                   [:enum ""]]]
                 [:xml/value :xml/hiccup]]]]]]]]]]]
       [:field
        {:optional true, :required-in-group true}
        [:sequential
         {:min 1}
         [:merge
          [:map
           {:closed true}
           [:id
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/ID]]
           [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
           [:xpathDefaultNamespace
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
          [:map
           {:closed true}
           [:annotation
            {:optional true, :required-in-group true}
            [:map
             {:closed true, :xml/value-wrapped true}
             [:id
              {:xml/attr true, :optional true}
              [:ref :org.w3.www.2001.XMLSchema/ID]]
             [:xml/value
              [:*
               [:alt
                [:tuple
                 [:enum :appinfo]
                 [:map
                  {:closed true, :xml/value-wrapped true}
                  [:source
                   {:xml/attr true, :optional true}
                   [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                  [:xml/value :xml/hiccup]]]
                [:tuple
                 [:enum :documentation]
                 [:map
                  {:closed true, :xml/value-wrapped true}
                  [:source
                   {:xml/attr true, :optional true}
                   [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                  [:org.w3.www.XML.1998.namespace/lang
                   {:xml/attr true, :optional true}
                   [:or
                    [:and
                     [:re
                      "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                     :org.w3.www.2001.XMLSchema/token]
                    [:enum ""]]]
                  [:xml/value :xml/hiccup]]]]]]]]]]]]]]]]
   [:length
    [:tuple
     [:enum :length]
     [:ref :org.w3.www.2001.XMLSchema/numFacet]]]
   [:list
    [:tuple
     [:enum :list]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:itemType
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/QName]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]
      [:map
       {:closed true}
       [:simpleType
        {:optional true}
        [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]]]]
   [:maxExclusive
    [:tuple
     [:enum :maxExclusive]
     [:ref :org.w3.www.2001.XMLSchema/facet]]]
   [:maxInclusive
    [:tuple
     [:enum :maxInclusive]
     [:ref :org.w3.www.2001.XMLSchema/facet]]]
   [:maxLength
    [:tuple
     [:enum :maxLength]
     [:ref :org.w3.www.2001.XMLSchema/numFacet]]]
   [:minExclusive
    [:tuple
     [:enum :minExclusive]
     [:ref :org.w3.www.2001.XMLSchema/facet]]]
   [:minInclusive
    [:tuple
     [:enum :minInclusive]
     [:ref :org.w3.www.2001.XMLSchema/facet]]]
   [:minLength
    [:tuple
     [:enum :minLength]
     [:ref :org.w3.www.2001.XMLSchema/numFacet]]]
   [:notation
    [:tuple
     [:enum :notation]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:name
        {:xml/attr true}
        [:ref :org.w3.www.2001.XMLSchema/NCName]]
       [:public
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/public]]
       [:system
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/anyURI]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:openContent
    [:tuple
     [:enum :openContent]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:mode
        {:xml/attr true, :optional true}
        [:and
         [:enum "none" "interleave" "suffix"]
         :org.w3.www.2001.XMLSchema/NMTOKEN]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]
      [:map
       {:closed true}
       [:any
        {:optional true}
        [:ref :org.w3.www.2001.XMLSchema/wildcard]]]]]]
   [:override
    [:tuple
     [:enum :override]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:schemaLocation
       {:xml/attr true}
       [:ref :org.w3.www.2001.XMLSchema/anyURI]]
      [:xml/value
       [:cat
        [:?
         [:tuple
          [:enum :annotation]
          [:map
           {:closed true, :xml/value-wrapped true}
           [:id
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/ID]]
           [:xml/value
            [:*
             [:alt
              [:tuple
               [:enum :appinfo]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:xml/value :xml/hiccup]]]
              [:tuple
               [:enum :documentation]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:org.w3.www.XML.1998.namespace/lang
                 {:xml/attr true, :optional true}
                 [:or
                  [:and
                   [:re
                    "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                   :org.w3.www.2001.XMLSchema/token]
                  [:enum ""]]]
                [:xml/value :xml/hiccup]]]]]]]]]
        [:* [:ref :org.w3.www.2001.XMLSchema/schemaTop-seq]]]]]]]
   [:pattern
    [:tuple
     [:enum :pattern]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:value
        {:xml/attr true}
        [:ref :org.w3.www.2001.XMLSchema/string]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:redefine
    [:tuple
     [:enum :redefine]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:schemaLocation
       {:xml/attr true}
       [:ref :org.w3.www.2001.XMLSchema/anyURI]]
      [:xml/value
       [:*
        [:alt
         [:tuple
          [:enum :annotation]
          [:map
           {:closed true, :xml/value-wrapped true}
           [:id
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/ID]]
           [:xml/value
            [:*
             [:alt
              [:tuple
               [:enum :appinfo]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:xml/value :xml/hiccup]]]
              [:tuple
               [:enum :documentation]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:org.w3.www.XML.1998.namespace/lang
                 {:xml/attr true, :optional true}
                 [:or
                  [:and
                   [:re
                    "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                   :org.w3.www.2001.XMLSchema/token]
                  [:enum ""]]]
                [:xml/value :xml/hiccup]]]]]]]]
         [:ref :org.w3.www.2001.XMLSchema/redefinable]]]]]]]
   [:restriction
    [:tuple
     [:enum :restriction]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:base
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/QName]]
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:xml/value
       [:cat
        [:map
         {:xml/in-seq-ex true, :closed true}
         [:annotation
          {:optional true}
          [:map
           {:closed true, :xml/value-wrapped true}
           [:id
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/ID]]
           [:xml/value
            [:*
             [:alt
              [:tuple
               [:enum :appinfo]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:xml/value :xml/hiccup]]]
              [:tuple
               [:enum :documentation]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:org.w3.www.XML.1998.namespace/lang
                 {:xml/attr true, :optional true}
                 [:or
                  [:and
                   [:re
                    "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                   :org.w3.www.2001.XMLSchema/token]
                  [:enum ""]]]
                [:xml/value :xml/hiccup]]]]]]]]]
        [:ref :org.w3.www.2001.XMLSchema/simpleRestrictionModel-seq]]]]]]
   [:schema
    [:tuple
     [:enum :schema]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:attributeFormDefault
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/formChoice]]
      [:blockDefault
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/blockSet]]
      [:defaultAttributes
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/QName]]
      [:elementFormDefault
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/formChoice]]
      [:finalDefault
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/fullDerivationSet]]
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:targetNamespace
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/anyURI]]
      [:version
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/token]]
      [:xpathDefaultNamespace
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]
      [:org.w3.www.XML.1998.namespace/lang
       {:xml/attr true, :optional true}
       [:or
        [:and
         [:re
          "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
         :org.w3.www.2001.XMLSchema/token]
        [:enum ""]]]
      [:xml/value
       [:cat
        [:* [:ref :org.w3.www.2001.XMLSchema/composition-seq]]
        [:?
         [:map
          {:xml/in-seq-ex true, :closed true}
          [:defaultOpenContent
           [:merge
            [:map
             {:closed true}
             [:appliesToEmpty
              {:xml/attr true, :optional true}
              [:ref :org.w3.www.2001.XMLSchema/boolean]]
             [:id
              {:xml/attr true, :optional true}
              [:ref :org.w3.www.2001.XMLSchema/ID]]
             [:mode
              {:xml/attr true, :optional true}
              [:and
               [:enum "interleave" "suffix"]
               :org.w3.www.2001.XMLSchema/NMTOKEN]]]
            [:map
             {:closed true}
             [:annotation
              {:optional true}
              [:map
               {:closed true, :xml/value-wrapped true}
               [:id
                {:xml/attr true, :optional true}
                [:ref :org.w3.www.2001.XMLSchema/ID]]
               [:xml/value
                [:*
                 [:alt
                  [:tuple
                   [:enum :appinfo]
                   [:map
                    {:closed true, :xml/value-wrapped true}
                    [:source
                     {:xml/attr true, :optional true}
                     [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                    [:xml/value :xml/hiccup]]]
                  [:tuple
                   [:enum :documentation]
                   [:map
                    {:closed true, :xml/value-wrapped true}
                    [:source
                     {:xml/attr true, :optional true}
                     [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                    [:org.w3.www.XML.1998.namespace/lang
                     {:xml/attr true, :optional true}
                     [:or
                      [:and
                       [:re
                        "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                       :org.w3.www.2001.XMLSchema/token]
                      [:enum ""]]]
                    [:xml/value :xml/hiccup]]]]]]]]]
            [:map
             {:closed true}
             [:any [:ref :org.w3.www.2001.XMLSchema/wildcard]]]]]
          [:annotation
           {:optional true}
           [:sequential
            {:min 1}
            [:map
             {:closed true, :xml/value-wrapped true}
             [:id
              {:xml/attr true, :optional true}
              [:ref :org.w3.www.2001.XMLSchema/ID]]
             [:xml/value
              [:*
               [:alt
                [:tuple
                 [:enum :appinfo]
                 [:map
                  {:closed true, :xml/value-wrapped true}
                  [:source
                   {:xml/attr true, :optional true}
                   [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                  [:xml/value :xml/hiccup]]]
                [:tuple
                 [:enum :documentation]
                 [:map
                  {:closed true, :xml/value-wrapped true}
                  [:source
                   {:xml/attr true, :optional true}
                   [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                  [:org.w3.www.XML.1998.namespace/lang
                   {:xml/attr true, :optional true}
                   [:or
                    [:and
                     [:re
                      "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                     :org.w3.www.2001.XMLSchema/token]
                    [:enum ""]]]
                  [:xml/value :xml/hiccup]]]]]]]]]]]
        [:*
         [:cat
          [:ref :org.w3.www.2001.XMLSchema/schemaTop-seq]
          [:*
           [:tuple
            [:enum :annotation]
            [:map
             {:closed true, :xml/value-wrapped true}
             [:id
              {:xml/attr true, :optional true}
              [:ref :org.w3.www.2001.XMLSchema/ID]]
             [:xml/value
              [:*
               [:alt
                [:tuple
                 [:enum :appinfo]
                 [:map
                  {:closed true, :xml/value-wrapped true}
                  [:source
                   {:xml/attr true, :optional true}
                   [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                  [:xml/value :xml/hiccup]]]
                [:tuple
                 [:enum :documentation]
                 [:map
                  {:closed true, :xml/value-wrapped true}
                  [:source
                   {:xml/attr true, :optional true}
                   [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                  [:org.w3.www.XML.1998.namespace/lang
                   {:xml/attr true, :optional true}
                   [:or
                    [:and
                     [:re
                      "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                     :org.w3.www.2001.XMLSchema/token]
                    [:enum ""]]]
                  [:xml/value :xml/hiccup]]]]]]]]]]]]]]]]
   [:selector
    [:tuple
     [:enum :selector]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:xpath {:xml/attr true} :org.w3.www.2001.XMLSchema/token]
       [:xpathDefaultNamespace
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/xpathDefaultNamespace]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:sequence
    [:tuple
     [:enum :sequence]
     [:ref :org.w3.www.2001.XMLSchema/explicitGroup]]]
   [:simpleContent
    [:tuple
     [:enum :simpleContent]
     [:map
      {:closed true, :xml/value-wrapped true}
      [:id
       {:xml/attr true, :optional true}
       [:ref :org.w3.www.2001.XMLSchema/ID]]
      [:xml/value
       [:cat
        [:map
         {:xml/in-seq-ex true, :closed true}
         [:annotation
          {:optional true}
          [:map
           {:closed true, :xml/value-wrapped true}
           [:id
            {:xml/attr true, :optional true}
            [:ref :org.w3.www.2001.XMLSchema/ID]]
           [:xml/value
            [:*
             [:alt
              [:tuple
               [:enum :appinfo]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:xml/value :xml/hiccup]]]
              [:tuple
               [:enum :documentation]
               [:map
                {:closed true, :xml/value-wrapped true}
                [:source
                 {:xml/attr true, :optional true}
                 [:ref :org.w3.www.2001.XMLSchema/anyURI]]
                [:org.w3.www.XML.1998.namespace/lang
                 {:xml/attr true, :optional true}
                 [:or
                  [:and
                   [:re
                    "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                   :org.w3.www.2001.XMLSchema/token]
                  [:enum ""]]]
                [:xml/value :xml/hiccup]]]]]]]]]
        [:alt
         [:tuple
          [:enum :restriction]
          [:ref :org.w3.www.2001.XMLSchema/simpleRestrictionType]]
         [:tuple
          [:enum :extension]
          [:ref :org.w3.www.2001.XMLSchema/simpleExtensionType]]]]]]]]
   [:simpleType
    [:tuple
     [:enum :simpleType]
     [:ref :org.w3.www.2001.XMLSchema/topLevelSimpleType]]]
   [:totalDigits
    [:tuple
     [:enum :totalDigits]
     [:merge
      [:map
       {:closed true}
       [:fixed
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/boolean]]
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:value
        {:xml/attr true}
        [:ref :org.w3.www.2001.XMLSchema/positiveInteger]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]
   [:union
    [:tuple
     [:enum :union]
     [:merge
      [:map
       {:closed true}
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:memberTypes
        {:xml/attr true, :optional true}
        [:sequential :org.w3.www.2001.XMLSchema/QName]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]
      [:map
       {:closed true}
       [:simpleType
        {:optional true}
        [:sequential
         {:min 1}
         [:ref :org.w3.www.2001.XMLSchema/localSimpleType]]]]]]]
   [:unique
    [:tuple [:enum :unique] [:ref :org.w3.www.2001.XMLSchema/keybase]]]
   [:whiteSpace
    [:tuple
     [:enum :whiteSpace]
     [:merge
      [:map
       {:closed true}
       [:fixed
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/boolean]]
       [:id
        {:xml/attr true, :optional true}
        [:ref :org.w3.www.2001.XMLSchema/ID]]
       [:value
        {:xml/attr true}
        [:and
         [:enum "preserve" "replace" "collapse"]
         :org.w3.www.2001.XMLSchema/NMTOKEN]]]
      [:map
       {:closed true}
       [:annotation
        {:optional true}
        [:map
         {:closed true, :xml/value-wrapped true}
         [:id
          {:xml/attr true, :optional true}
          [:ref :org.w3.www.2001.XMLSchema/ID]]
         [:xml/value
          [:*
           [:alt
            [:tuple
             [:enum :appinfo]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:xml/value :xml/hiccup]]]
            [:tuple
             [:enum :documentation]
             [:map
              {:closed true, :xml/value-wrapped true}
              [:source
               {:xml/attr true, :optional true}
               [:ref :org.w3.www.2001.XMLSchema/anyURI]]
              [:org.w3.www.XML.1998.namespace/lang
               {:xml/attr true, :optional true}
               [:or
                [:and
                 [:re
                  "([a-zA-Z]{2}|[iI]-[a-zA-Z]+|[xX]-[a-zA-Z]{1,8})(-[a-zA-Z]{1,8})*"]
                 :org.w3.www.2001.XMLSchema/token]
                [:enum ""]]]
              [:xml/value :xml/hiccup]]]]]]]]]]]]]))
(defn
 make-schema
 ([] (make-schema top-type))
 ([start-type] (xml-primitives/make-schema registry start-type)))
(defn
 closed-make-schema
 ([] (closed-make-schema top-type))
 ([start-type]
  (xml-primitives/closed-make-schema registry start-type)))
