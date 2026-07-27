# Malli Generation Pipeline

How `dunnage.sitoa.bootstrapped-schema` turns an XSD file into a Malli schema.

## Overview

```
┌─────────────┐     XSOMParser      ┌──────────────┐
│  XSD file   │ ──────────────────► │  XSSchemaSet │
└─────────────┘                     └──────┬───────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                      ▼
             iterateTypes           iterateModelGroupDecls  iterateElementDecls
                    │                      │                      │
                    └──────────┬───────────┘                      │
                               ▼                                  ▼
                        xsd->registry                       xsd->top-type
                     (named :ref targets)                 (:multi of roots)
                               │                                  │
                               └──────────────┬───────────────────┘
                                              ▼
                                    xml-primitives/make-schema
                                              │
                                              ▼
                                    Malli [:schema {:registry …} top]
```

## Entry points

### `parse-xsd`

```clojure
(parse-xsd f)  ;; → XSSchemaSet
```

- Builds an `XSOMParser` with the default SAX factory
- Installs error handlers that print SAX issues
- Custom **annotation parser** captures `xs:documentation` / related content via `clojure.xml` handlers
- Returns the XSOM schema set used by all higher-level functions

`f` is anything XSOM accepts (typically a resource, file, or URL).

### `xsd->registry`

```clojure
(xsd->registry {:default-ns "script"} schema-set)
```

Builds a map of namespaced keyword → Malli form:

1. Start from `xml-primitives/xmlschema-registry` (W3C builtins).
2. For each type in the schema (non-primitive simple types filtered out where appropriate):
   - If `-seq-possible?`, register `-seq` key with `(-mtype % seq-context)`.
   - Register normal key with `(-mtype % context)`.
3. Same pattern for **global** model group declarations.
4. Skip re-registering names under `org.w3.www.2001.XMLSchema`.

### `xsd->top-type`

```clojure
(xsd->top-type context schema-set)
```

Collects global element declarations into:

```clojure
[:multi {:dispatch first}
 [tag [:tuple {} [:enum tag] content-schema]]
 …]
```

so documents can start with any global root element.

### `xsd->schema`

```clojure
(xsd->schema context f)
```

Full pipeline:

1. `(parse-xsd f)`
2. `(xsd->registry context schema)`
3. `(xsd->top-type context schema)`
4. `(xml-primitives/make-schema registry top-type)`

Returns a compiled Malli schema with the registry installed on the `:schema` wrapper, plus default/util/custom/time schemas from `xml-primitives/external-registry`.

### `raw-xsd->schema`

```clojure
(raw-xsd->schema context f)
```

Same structure as `xsd->schema`, but returns the **unevaluated form**:

```clojure
[:schema {:registry registry} top-type]
```

Useful for serialization, debugging, or applying transforms before `m/schema`.

## Complex type conversion (`-mtype` on `XSComplexType`)

For each complex type:

1. Collect **attributes** → `complex-attrs-map` (closed `:map` with `:xml/attr` entries).
2. Inspect content type:
   - simple content → recurse with `-mtype`
   - empty → empty map or attrs-only map
   - particle → `handle-toplevel-particle`
3. Combine with rules (simplified):

| Attributes | Content | Result |
|------------|---------|--------|
| yes | simple | value-wrap attrs + simple |
| yes | mixed | value-wrap attrs + `:xml/hiccup` |
| yes | `:map` / `:merge` particle | `[:merge {} attrs content]` |
| yes | other particle | value-wrap attrs + content |
| yes | none | attrs map only |
| no | simple / complex | content only |
| no | empty | `[:map {:empty true}]` or attrs map |

`handle-toplevel-particle` sets `:sequence true` so occurrence wrappers use `:?` / `:*` / `:+` / `:repeat` rather than bare `:sequential` where appropriate.

## Model groups and particles

### `handle-model-group`

| Compositor | Strategy |
|------------|----------|
| **sequence** | If all children are uniquely named elements (and nested sequences): closed `:map` (with `:xml/in-seq-ex` when already in sequence context). Else: `[:cat …]` of `group-particle` results. Single `xs:any` → `:xml/hiccup`. |
| **choice** | `:or` or `:alt` (if parent is sequence context) of wrapped particles. Single any → hiccup. |
| **all** | Map if uniquely named; otherwise falls through to a compositor-shaped form. |

### `group-particle` / `wrap-regex`

Each particle’s term (element, nested group, group ref, wildcard) is converted, then occurrence constraints are applied via `wrap-regex`. Maps under optional/repeat wrappers get **`:xml/in-seq-ex`** so serde does not advance past the first child start tag incorrectly.

### Element decls

```clojure
(handle-element-decl context el)
;; → [:tuple {} [:enum :LocalName] type-ref-or-inline]
```

Used for choices, cats, and multi roots.

## Simple types

### Atomic restrictions

- Primitive → keyword under `org.w3.www.2001.XMLSchema/…`
- Numbers, dates, binaries → that primitive keyword (mapped later by xml-primitives)
- Strings with facets → `malli-string-primitive` (`:string` / `:enum` / `:re` / `:and`)

### List

```clojure
[:sequential item-schema]
```

or for some builtin list types, `:string`.

### Union

```clojure
[:or member1 member2 …]
```

Restriction varieties named `"union"` currently throw (`"union is not supported yet"`) on the restriction path; `XSUnionSimpleType` uses the `:or` form above.

## Registry trimming

Large XSDs produce huge registries. To keep only types reachable from chosen roots:

```clojure
(trim-registry-for-top-types registry [:script/MessageType])
```

Walks Malli forms collecting `:ref` targets (and schema references), repeatedly selecting keys until closure.

Typical use with serialization:

```clojure
(-> (xsd->schema {:default-ns "script"} (io/resource "…/transport.xsd"))
    (mu/update-properties update :registry
                          trim-registry-for-top-types [:script/MessageType])
    (serialize-registry "script_registry.edn"))
```

## Serialization

| Function | Output |
|----------|--------|
| `serialize-schema` | Full `m/form` of the schema with **sorted** registry keys (fipp EDN) |
| `serialize-registry` | Sorted map of registry key → form only (brace-wrapped EDN object) |

Sorting keeps diffs stable for committed schema dumps.

## Example: NCPDP SCRIPT

```clojure
(ns user
  (:require [dunnage.sitoa.bootstrapped-schema :as bs]
            [malli.core :as m]
            [malli.util :as mu]
            [malli.generator :as mg]
            [clojure.java.io :as io]))

(def schema
  (bs/xsd->schema {:default-ns "script"}
                  (io/resource "NCPDP_2023011/transport.xsd")))

;; Generate sample data (may be large / constrained by patterns)
(comment
  (mg/generate schema))

;; Persist for offline use
(bs/serialize-schema schema "script.edn")

;; Smaller registry for one root type
(bs/serialize-registry
 (-> schema
     (mu/update-properties update :registry
                           bs/trim-registry-for-top-types [:script/MessageType]))
 "script_registry.edn")
```

## Example: raw form then compile later

```clojure
(def form
  (bs/raw-xsd->schema {:default-ns "fop"} (io/resource "fop.xsd")))

;; form is data — edit, trim, or merge registries before:
(def schema
  (m/schema form dunnage.sitoa.xml-primitives/external-registry))
```

## Dev resources

Under `bootstrapped-schema/dev-resources/` (on the classpath with the `:dev` alias):

- `XMLSchema.xsd` — W3C schema for schemas
- `fop.xsd`, `JUnit.xsd` — smaller fixtures
- `NCPDP_2023011/*.xsd` — multi-file pharmacy SCRIPT schemas

Parse with `io/resource` when the alias path includes `dev-resources`.

## Dependencies of note

| Library | Role |
|---------|------|
| `org.glassfish.jaxb/xsom` | XSD object model |
| `metosin/malli` | Schema forms, walk, registry |
| `xml-primitives` | Builtin type map, `make-schema` |
| `fipp` | Pretty EDN serialization |

## Failure modes and limits

- **Union via restriction variety** — not fully supported on the restriction path
- **Choice / all edge cases** — assert on unexpected sequence nesting or value-sequences on nested sequences
- **SAX / annotation errors** — printed via the error handler; check console output
- **Huge schemas** — prefer `trim-registry-for-top-types` before shipping EDN

For construct-level mapping tables, see [xsd-mapping.md](xsd-mapping.md).