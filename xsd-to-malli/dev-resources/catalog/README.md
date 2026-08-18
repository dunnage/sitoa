# Local schema catalog

Documents in this directory back the offline resolver catalog used by the test
suite. Tests never reach the network: an absolute `schemaLocation` URL that the
catalog does not map raises `:xsd-to-malli/no-network`.

## xml.xsd

- Source URL: <https://www.w3.org/2001/xml.xsd>
- Retrieved: 2026-08-17
- SHA-256: `61960fb3131e38022caad5360e2f33a3382578ab3c80cd58bd74320ede61b20c`
- Bytes: 8836
- Target namespace: `http://www.w3.org/XML/1998/namespace`
- Verbatim copy, no modifications.

Why it is here: `dev-resources/XMLSchema.xsd` in the sibling `bootstrapped-schema`
module carries

```xml
<xs:import namespace="http://www.w3.org/XML/1998/namespace"
           schemaLocation="http://www.w3.org/2001/xml.xsd"/>
```

and the compiled result depends on the real declarations in that document (the
`xml:lang` union in particular), so a stub would not reproduce the oracle. The
catalog maps both the URL and the namespace URI to this file.

### License

This document includes material copied from
<https://www.w3.org/2001/xml.xsd>. Copyright (c) World Wide Web Consortium
(<https://www.w3.org/>). The source document carries no year in its own text,
so none is asserted here.

It is redistributed under the W3C Software and Document License - 2023 version,
<https://www.w3.org/copyright/software-license/>, which permits copying,
modification and distribution for any purpose without fee, provided this notice
is included in a location viewable to users of the redistributed work. License
text verified on 2026-08-17. No modifications were made to the document.

## Regenerating the checked-in meta-schema (`../../generated-src`)

The same recipe is recorded at the top of
`src/dunnage/sitoa/xsd_to_malli/loader.clj`; keep the two in sync.

```
# from the worktree's sitoa directory
rm -rf xsd-to-malli/generated-src
cd bootstrapped-schema
clojure -M:dev -e '
(require (quote [dunnage.sitoa.schema-namespaces :as sn]) (quote [clojure.java.io :as io]))
(def res (sn/xsd->namespaces! {:default-ns "xsd"
                               :out-dir "../xsd-to-malli/generated-src"
                               :entry-ns (quote dunnage.sitoa.xsd-meta)}
                              (io/file "dev-resources/XMLSchema.xsd")))
(println "files:" (count (:files res)) "included:" (count (:included res)))
(System/exit 0)'
# expected: files: 66 included: 114
git diff --stat xsd-to-malli/generated-src   # expected: empty for an unchanged input
```

Regeneration uses the XSOM path deliberately (a one-time, dev-time bootstrap)
and needs NETWORK access, because XSOM resolves XMLSchema.xsd's `xs:import` of
`http://www.w3.org/2001/xml.xsd` itself. Tests never do. After regenerating,
diff the document W3C serves against this catalog copy
(`curl -s https://www.w3.org/2001/xml.xsd`) and update the copy, the SHA-256
above and the retrieval date if W3C published a new revision.
