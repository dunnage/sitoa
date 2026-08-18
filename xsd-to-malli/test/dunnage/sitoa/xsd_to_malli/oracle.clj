(ns dunnage.sitoa.xsd-to-malli.oracle
  "The XSOM pipeline, as this suite's equivalence oracle.

  bootstrapped-schema arrives transitively through serde, so the oracle is on
  the TEST classpath for free - nothing under src/ may touch it. The one thing
  that has to change is the entity resolver: bs/parse-xsd fetches
  XMLSchema.xsd's xs:import over the network, and this suite is offline. The
  resolver below points that system id at the checked-in catalog copy;
  bootstrapped-schema itself is untouched."
  (:require [clojure.java.io :as io])
  (:import (com.sun.xml.xsom.parser AnnotationContext AnnotationParser AnnotationParserFactory XSOMParser)
           (java.io File)
           (javax.xml.parsers SAXParserFactory)
           (org.xml.sax ContentHandler EntityResolver ErrorHandler InputSource SAXParseException)))

(def xml-xsd (io/file "dev-resources/catalog/xml.xsd"))

(def catalog
  {"http://www.w3.org/2001/xml.xsd" xml-xsd
   "http://www.w3.org/XML/1998/namespace" xml-xsd})

(defn xsd
  "A schema document from bootstrapped-schema's dev-resources. They are
  referenced by path: dev-resources do not cross a :local/root classpath."
  [name]
  (io/file ".." "bootstrapped-schema" "dev-resources" name))

(defn parse-xsd-offline
  "bs/parse-xsd with an entity resolver, and with the same annotation parser so
  the resulting XSSchemaSet is what the oracle would have built."
  ^com.sun.xml.xsom.XSSchemaSet [^File f]
  (let [parser (XSOMParser. (SAXParserFactory/newDefaultInstance))]
    (.setErrorHandler parser (reify ErrorHandler
                               (^void warning [_ ^SAXParseException x] (prn x))
                               (^void error [_ ^SAXParseException x] (prn x))
                               (^void fatalError [_ ^SAXParseException x] (prn x))))
    (.setEntityResolver parser (reify EntityResolver
                                 (resolveEntity [_ _public-id system-id]
                                   (when (and system-id
                                              (.endsWith ^String system-id "/2001/xml.xsd"))
                                     (InputSource. (io/input-stream xml-xsd))))))
    (.setAnnotationParser parser
                          (reify AnnotationParserFactory
                            (create [_]
                              (push-thread-bindings {#'clojure.xml/*stack* nil
                                                     #'clojure.xml/*current* (struct clojure.xml/element)
                                                     #'clojure.xml/*state* :between
                                                     #'clojure.xml/*sb* nil})
                              (proxy [AnnotationParser] []
                                (getContentHandler [^AnnotationContext _context
                                                    ^String _parent
                                                    ^ErrorHandler _errors
                                                    ^EntityResolver _entities]
                                  ^ContentHandler clojure.xml/content-handler)
                                (getResult [_old]
                                  (let [result clojure.xml/*current*]
                                    (pop-thread-bindings)
                                    (into [] (mapcat :content) (:content result))))))))
    (.parse parser f)
    (.getResult parser)))
