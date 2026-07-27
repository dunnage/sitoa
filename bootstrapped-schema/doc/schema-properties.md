# Schema Properties

Properties attached to Malli maps and map entries during generation. **serde** reads these when building StAX parsers and unparsers. They are ordinary Malli properties (second position of `:map` / entry vectors).

## Map-level properties

### `:closed`

```clojure
[:map {:closed true} …]
```

Almost all generated element maps are closed: unknown keys are invalid. Optional groups may set `:optional-group true` alongside.

### `:xml/value-wrapped`

```clojure
[:map {:closed true :xml/value-wrapped true}
 [:attr {:xml/attr true} :string]
 [:xml/value {} content-schema]]
```

**Meaning:** this complex type has XML attributes (and/or empty structure) plus a **body** that is not a normal set of sibling element keys.

**Data shape:**

```clojure
{:attr "x" :xml/value body}
```

**Parser behavior (serde):**

1. Read attributes into the map.
2. Parse body with the `:xml/value` child schema.
3. For `:xml/hiccup` bodies, parse **content children only** so the parent tag/attrs are not duplicated inside `:xml/value`.
4. Empty / self-closing body may omit `:xml/value` when optional or empty-capable.

**When generated:**

- Attributes + simple content
- Attributes + mixed content (`:xml/hiccup`)
- Attributes + non-`:map`/`:merge` particle content

### `:xml/in-seq-ex`

```clojure
[:map {:closed true :xml/in-seq-ex true}
 [:tr {} …]
 …]
```

**Meaning:** this map is entered at the **first child element start tag**, not by advancing past a parent start tag that wraps only this map.

**Why:** sequence/choice arms and nested tables (e.g. header/body rows) are often parsed while the reader is already positioned on the first child. Without this flag, the map parser would call “next tag” once and skip the first child.

**When generated:**

- Maps under `:?` / `:*` / `:+` / `:repeat` (`mark-map-in-seq-ex`)
- Maps built for sequences already in `:sequence` context
- Choice arms forced into sequence context for multi-element arms

### `:documentation`

Human-readable text from `xs:documentation` on the complex type.

### `:empty`

```clojure
[:map {:empty true}]
```

Empty complex content with no attributes.

### `:optional-group`

Set on maps produced for optional nested sequences (`minOccurs="0"` on a group). Documents that the whole field group may be absent.

### Internal generation marker `:x`

Used only while reducing fields in `simplify-fields` (placeholder on open maps). Stripped in the completing step; should not appear in final schemas.

## Map-entry properties

Entry shape: `[key props child-schema]`.

### `:xml/attr`

```clojure
[:id {:xml/attr true :optional true} :string]
```

Key is an **XML attribute**, not a child element. Parser fills it from the attribute map on the start tag; unparser writes it as an attribute.

### `:optional`

Element or attribute may be omitted. From `minOccurs="0"` or attribute `use` not required; also forced for entries inside optional groups.

### `:required-in-group`

With `:optional true`, marks fields that XSD requires **if** their optional parent group is present. Primarily documentary for tooling; generation sets it when `:optional-group` context is active.

### `:documentation` / `:attr-documentation`

- **`:documentation`** — element-level annotation text  
- **`:attr-documentation`** — attribute-level annotation text  

### `:xml/value` (special key, not a prop)

The key **`:xml/value`** is a synthetic child of value-wrapped maps. Its entry props are often `{}`. The child schema is the body type (simple type, hiccup, cat, or, etc.).

## Enum properties

```clojure
[:enum {:value-documentation {"A" ["…"]}} "A" "B"]
```

`:value-documentation` maps enumeration literals to documentation strings extracted from annotations on facets.

## Sequence / string properties

Not XML-specific, but commonly emitted:

| Location | Props |
|----------|--------|
| `:string` | `:min`, `:max` from length facets |
| `:sequential` | `:min`, `:max` from occurs |
| `:repeat` | `:min`, `:max` |

## Custom types (from xml-primitives)

These appear as types, not map props:

| Type | Predicate / role |
|------|------------------|
| `:xml/hiccup` | `vector?` — open XML fragments |
| `:xml/base64Binary` | `ByteBuffer` |
| `:xml/hexBinary` | `ByteBuffer` |
| `:decimal` | `decimal?` with string codecs |
| `:time/*` | java.time wrappers |

## Mental checklist when reading a generated schema

1. **`:xml/attr`** → attribute on the element that owns this map  
2. **`:xml/value-wrapped`** → look for `:xml/value` body; data is not “all child keys”  
3. **`:xml/in-seq-ex`** → parser is already on the first child when this map starts  
4. **`:optional`** → may be missing  
5. **`:closed true`** → extra keys fail validation  
6. **`[:ref :ns/Name-seq]`** → sequence-context variant of a type  

## Example (conceptual)

```clojure
[:map {:closed true :xml/value-wrapped true}
 [:nullFlavor {:xml/attr true :optional true} :string]
 [:xml/value {}
  [:*
   [:map {:closed true :xml/in-seq-ex true}
    [:streetAddressLine {} :string]
    [:city {:optional true} :string]]]]]
```

- Outer element: attributes + repeating structured children in the body  
- Body optional/repeat uses sequence-entry maps  
- Parsed data:

```clojure
{:nullFlavor "UNK"
 :xml/value [{:streetAddressLine "1 Main"}
             {:streetAddressLine "Suite 2" :city "X"}]}
```

(Exact nesting depends on the XSD; this illustrates property interaction only.)
