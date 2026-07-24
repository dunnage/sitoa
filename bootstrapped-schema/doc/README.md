# Bootstrapped Schema Documentation

`bootstrapped-schema` turns **XML Schema (XSD)** into **Malli** schemas that sitoa's serde layer can parse and unparse.

It uses [XSOM](https://javaee.github.io/jaxb-v2/) to parse XSD, walks the component model, and emits Malli forms annotated with XML-specific properties (`:xml/attr`, `:xml/value-wrapped`, `:xml/in-seq-ex`, etc.). Named types become registry entries under namespaced keywords derived from target namespaces.

## Docs in this folder

| Document | Description |
|----------|-------------|
| [concepts.md](concepts.md) | Core ideas: namespaces, refs, content models, value wrapping, sequence entry |
| [generation.md](generation.md) | Pipeline, public API, registry building, serialization, trimming |
| [xsd-mapping.md](xsd-mapping.md) | How XSD constructs map to Malli forms |
| [schema-properties.md](schema-properties.md) | Map/entry properties the parser and unparser understand |

## Where it sits in sitoa

```
XSD file(s)
    │
    ▼
bootstrapped-schema   (this module: XSD → Malli + registry)
    │
    ▼
xml-primitives        (built-in XSD type registry, custom schemas)
    │
    ▼
serde                 (streaming XML parse / unparse from Malli)
```

## Quick start

```clojure
(require '[dunnage.sitoa.bootstrapped-schema :as bs]
         '[clojure.java.io :as io]
         '[malli.core :as m])

;; Compile an XSD into a Malli schema (with registry of named types)
(def schema
  (bs/xsd->schema {:default-ns "script"}
                  (io/resource "NCPDP_2023011/transport.xsd")))

;; Inspect the form
(m/form schema)

;; Write a sorted EDN dump for inspection or offline use
(bs/serialize-schema schema "script.edn")
```

Context map:

- **`:default-ns`** — string used when a declaration has no (or empty) target namespace. Becomes the keyword namespace for type and element names.

## Related modules

- **`xml-primitives`** — maps W3C XML Schema built-in types to Malli/Java types and provides `make-schema`.
- **`serde`** — builds StAX parsers/unparsers from the generated Malli schemas.
