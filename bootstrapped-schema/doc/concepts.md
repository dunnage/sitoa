# Concepts

This document explains the mental model behind XSD → Malli generation: how names are chosen, how structure is represented in Clojure data, and why certain Malli properties exist.

## Why generate Malli from XSD?

XSD describes message shapes (elements, attributes, compositors, facets). Malli describes the same shapes as data schemas that:

1. Validate and generate sample data
2. Drive streaming XML parsers and unparsers in **serde**
3. Stay inspectable as EDN (serializable registries)

Generation is the bridge: one source of truth (XSD) produces schemas that both humans and the runtime can use.

## Namespaced keywords from URIs

XSD types and declarations live in **target namespaces** (URIs). Those become Clojure keyword namespaces via `uri->ns`:

| URI style | Example | Keyword namespace |
|-----------|---------|-------------------|
| HTTP(S) host + path | `http://www.w3.org/2001/XMLSchema` | `org.w3.www.2001.XMLSchema` |
| URN | `urn:hl7-org:v3` | reverse of colon-separated parts joined with `.` |

Then:

```clojure
;; Type named "string" in the XML Schema namespace
:org.w3.www.2001.XMLSchema/string

;; With context {:default-ns "script"} and no target NS
:script/MessageType
```

Helpers:

- **`->nskw`** — namespaced keyword for a named declaration (type, group, …)
- **`->nskw-seq`** — same name with a `-seq` suffix for the *sequence-context* variant
- **`->kw`** — local name only (used for element tags on maps / tuples)

W3C XML Schema built-ins are **not** wrapped in `[:ref …]`; everything else usually is.

## Registry and `[:ref …]`

Global types and model groups are registered by namespaced keyword. References use Malli `:ref`:

```clojure
[:ref :script/SomeComplexType]
```

Two registry entries can exist per complex type / group:

| Key | Context | Purpose |
|-----|---------|---------|
| `:ns/TypeName` | normal | Standalone element content (typical map / value-wrapped form) |
| `:ns/TypeName-seq` | `:sequence true` | Form used when the type appears inside a sequence compositor |

The sequence variant is needed because the same XSD type may be entered differently when nested under `xs:sequence` / `xs:choice` particles (see [xml/in-seq-ex](schema-properties.md#xmlin-seq-ex)).

Primitives from `org.w3.www.2001.XMLSchema` are seeded from **`xml-primitives/xmlschema-registry`** and skipped when re-emitting from the XSD (they are not re-defined from the schema document).

## Data shapes for XML

Generated schemas describe Clojure data that mirrors XML structure:

### Element as map entry

```clojure
;; <Patient><Name>Ada</Name></Patient>  (simplified)
{:Name "Ada"}
```

Map keys are element local names (or namespaced keys for namespaced attributes). Child element schemas sit in the third position of each map entry:

```clojure
[:map {:closed true}
 [:Name {} :string]
 [:Age {:optional true} :int]]
```

### Attributes

Attribute map entries carry `{:xml/attr true}`:

```clojure
[:id {:xml/attr true :optional true} :string]
```

In data they appear as ordinary map keys alongside children; serde treats them as XML attributes, not child elements.

### Tagged choices / multi roots

Top-level element decls become a Malli `:multi` on the first of a tuple:

```clojure
[:multi {:dispatch first}
 [:Message
  [:tuple {} [:enum :Message] [:ref :script/MessageType]]]
 ...]
```

Choice particles often become `:or` or `:alt` of `[:tuple [:enum :tag] content]`.

### Sequences and regex schemas

Inside sequence compositors, occurrence constraints map to Malli sequence expressions:

| minOccurs | maxOccurs | Malli |
|-----------|-----------|--------|
| 1 | 1 | bare schema |
| 0 | 1 | `[:? …]` |
| 1 | unbounded | `[:+ …]` |
| 0 | unbounded | `[:* …]` |
| n | m | `[:repeat {:min n :max m} …]` |

Outside pure sequence context, repeated particles may become `[:sequential …]` instead (value-sequence on map fields).

### Hiccup for wildcards and mixed content

Wildcards (`xs:any`) and many mixed-content models use the custom type **`:xml/hiccup`**: a vector tree in hiccup style for fidelity when structure is open-ended.

## Value wrapping

When a complex type has **attributes plus non-map content** (simple content, mixed content, or non-mergeable particle content), the generator does not invent a fake child key for the body. It:

1. Puts attributes on a closed map
2. Sets **`:xml/value-wrapped true`** on the map
3. Adds a synthetic entry **`:xml/value`** for the content schema

```clojure
[:map {:closed true :xml/value-wrapped true}
 [:Qualifier {:xml/attr true :optional true} :string]
 [:xml/value {} :string]]
```

Data:

```clojure
{:Qualifier "P" :xml/value "7701630"}
```

See [schema-properties.md](schema-properties.md) for parser behavior (including mixed hiccup bodies).

## Map vs merge

Optional nested sequences and grouped fields may produce multiple `:map` fragments. **`simplify-fields`** combines them:

- Consecutive field entries stay on one `:map`
- Nested map/merge fragments become `[:merge {} map-a map-b …]`

This keeps closed maps and optional groups composable without flattening incorrectly.

## Optional groups

When a particle has `minOccurs="0"` and is itself a sequence group, children inherit **`:optional-group`** context. Map entries get:

```clojure
{:optional true :required-in-group true}
```

Meaning: the group as a whole may be absent, but if the group is present, “required-in-group” fields are expected. (Consumers / serde use `:optional` primarily; `:required-in-group` documents XSD intent.)

## Anonymous vs global types

- **Global / named** types → registry key + `[:ref …]` (or the keyword itself for XML Schema builtins)
- **Local / anonymous** types → inlined Malli form via `-mtype`

`anon-type?` treats local types and types without a usable name as anonymous.

## Facets on strings

Restrictions of `xs:string` collect facets into Malli constraints:

| Facet | Effect |
|-------|--------|
| `enumeration` | `[:enum …]` (optional `:value-documentation` on props) |
| `minLength` / `maxLength` / `length` | `:string` props `:min` / `:max` |
| `pattern` | `[:re …]` (with a few XSD name-pattern special cases) |
| `whiteSpace` | ignored for schema shape |

Multiple constraints combine under `[:and …]` when needed.

## Documentation from annotations

`xs:annotation` / `xs:documentation` content is attached when present:

- Complex type map: `:documentation`
- Elements: `:documentation` on the entry
- Attributes: `:attr-documentation`
- Enum values: `:value-documentation` map on the enum props

## Protocol: `MalliXML`

Conversion is driven by protocol **`MalliXML`**:

| Method | Role |
|--------|------|
| `-mtype` | Full Malli form for this component in `context` |
| `-seq-possible?` | Whether a `-seq` registry entry should be emitted |
| `-seq-ref` | Reference form (plain or `-seq`) for this context |

Implemented for complex types, restriction/list/union simple types, and model group decls.

## Context map

Throughout generation, a small **context** map threads options:

| Key | Meaning |
|-----|---------|
| `:default-ns` | Fallback keyword namespace |
| `:sequence` | Inside a sequence/choice particle path; affects refs and regex wrapping |
| `:optional-group` | Parent sequence had minOccurs 0 |
| `:compositor` | `"sequence"` or `"choice"` (and related) |

## Downstream consumption

Generated schemas are not only for validation. **serde** walks the same forms and properties to:

- Bind attributes vs elements
- Enter maps at parent start tag or at first child (`:xml/in-seq-ex`)
- Parse mixed/simple bodies via `:xml/value`
- Discriminate choices and multi roots by element name

Understanding the concepts above is enough to read a generated `.edn` dump and predict parse results.