(ns dunnage.sitoa.xsd-to-malli.resolver
  "Resolution of the schemaLocation references XSOM used to follow on its own.

  XSOM fetched absolute schemaLocation URLs over the network. This module is
  the replacement, and it deliberately has no network code path at all: an
  absolute http(s) location is only ever satisfied from a caller-supplied
  catalog, so a test can never reach the wire by accident.

  Known capability gap versus XSOM: xs:import with no schemaLocation is only
  satisfiable through a catalog entry for the imported namespace. XSOM also
  satisfies it from documents already in the schema set being built (a common
  idiom in circular-import schema pairs); this loader resolves every import
  eagerly and throws :xsd-to-malli/unresolvable-import instead. The failure
  names the namespace and the catalog keys, and mapping the namespace in the
  catalog is the workaround."
  (:require [clojure.java.io :as io])
  (:import (java.io File)
           (java.net URI URISyntaxException URL)))

(defprotocol SchemaResolver
  (resolve-schema [this base-uri namespace-uri location]
    "Resolve a schemaLocation reference found in the document at `base-uri`.

    `namespace-uri` is the xs:import namespace attribute, nil for xs:include.
    `location` is the schemaLocation string, which xs:import may omit.

    Returns {:uri java.net.URI, :open (fn [] java.io.Reader)} or throws
    ex-info. The :open thunk is called more than once per document - the
    prefix pre-pass and the parse each consume a stream - so every call must
    return a fresh Reader."))

(defn ->uri
  "Canonical URI for a File, URL, URI or string. Canonical means absolute and
  normalized, so that two spellings of one file share a graph key. A string
  without a scheme is a file path, which is how catalog entries and load
  targets are usually written."
  ^URI [x]
  (cond
    (instance? URI x) (.normalize ^URI x)
    (instance? File x) (.normalize (.toURI (.getAbsoluteFile ^File x)))
    (instance? URL x) (.normalize (.toURI ^URL x))
    (string? x) (let [uri (.normalize (URI. ^String x))]
                  (if (.getScheme uri)
                    uri
                    (.normalize (.toURI (.getAbsoluteFile (io/file x))))))
    :else (throw (ex-info "cannot be read as a URI"
                          {:type :xsd-to-malli/bad-uri :value x :class (class x)}))))

(defn- file-uri? [^URI uri]
  (= "file" (.getScheme uri)))

(defn- network-uri? [^URI uri]
  (contains? #{"http" "https"} (.getScheme uri)))

(defn source
  "A {:uri :open} source for a File, URL, URI or string, without going through
  a resolver. Used for the document a load starts from."
  [x]
  (let [uri (->uri x)]
    (when (network-uri? uri)
      (throw (ex-info "a load cannot start from a network URI"
                      {:type :xsd-to-malli/no-network :uri (str uri)})))
    (when (and (file-uri? uri) (not (.exists (io/file uri))))
      (throw (ex-info "schema document does not exist"
                      {:type :xsd-to-malli/missing-schema-document :uri (str uri)})))
    {:uri uri :open (fn [] (io/reader uri))}))

(defn- catalog-source [target]
  (let [uri (->uri target)]
    {:uri uri :open (fn [] (io/reader target))}))

(defn- parse-location ^URI [location base-uri]
  (try
    (URI. ^String location)
    (catch URISyntaxException e
      (throw (ex-info "schemaLocation is not a URI"
                      {:type :xsd-to-malli/bad-location
                       :location location
                       :base-uri (str base-uri)}
                      e)))))

(defn catalog-resolver
  "Resolver backed by a local catalog.

  `catalog` maps a lookup string - an absolute schemaLocation URL, or a
  namespace URI for an xs:import that omits schemaLocation - to a File, URL,
  URI or path string holding the document. Resolution rules:

  1. no location: look the namespace up in the catalog; a miss is fatal.
  2. location resolving to an absolute http(s) URL: look the resolved URL, the
     raw location and then the namespace up in the catalog; a miss is fatal
     and never falls back to the network.
  3. otherwise: resolve against the including document's URI, the standard
     relative rule, and open the result directly."
  ([] (catalog-resolver {}))
  ([catalog]
   (reify SchemaResolver
     (resolve-schema [_ base-uri namespace-uri location]
       (if (nil? location)
         (if-some [target (get catalog namespace-uri)]
           (catalog-source target)
           (throw (ex-info "xs:import without schemaLocation and no catalog entry for its namespace"
                           {:type :xsd-to-malli/unresolvable-import
                            :namespace namespace-uri
                            :base-uri (str base-uri)
                            :catalog (vec (sort (keys catalog)))})))
         (let [raw (parse-location location base-uri)
               resolved (.normalize (if base-uri (.resolve (->uri base-uri) raw) raw))]
           (cond
             (network-uri? resolved)
             (if-some [target (or (get catalog (str resolved))
                                  (get catalog location)
                                  (get catalog namespace-uri))]
               (catalog-source target)
               (throw (ex-info "absolute schemaLocation is not in the catalog and the resolver never uses the network"
                               {:type :xsd-to-malli/no-network
                                :location location
                                :resolved (str resolved)
                                :namespace namespace-uri
                                :base-uri (str base-uri)
                                :catalog (vec (sort (keys catalog)))})))

             (and (file-uri? resolved) (not (.exists (io/file resolved))))
             (throw (ex-info "schema document does not exist"
                             {:type :xsd-to-malli/missing-schema-document
                              :location location
                              :uri (str resolved)
                              :base-uri (str base-uri)}))

             :else
             {:uri resolved :open (fn [] (io/reader resolved))})))))))
