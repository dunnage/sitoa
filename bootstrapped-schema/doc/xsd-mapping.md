# XSD → Malli Mapping

Reference for how XML Schema constructs become Malli forms in `bootstrapped-schema`.

## Built-in simple types

Resolved via `xml-primitives/xmlschema-registry` (keywords under `org.w3.www.2001.XMLSchema`).

| XSD type | Malli / custom type | Notes |
|----------|---------------------|--------|
| `string`, `normalizedString`, `token`, `language`, `NMTOKEN`, `Name`, `NCName`, `ID`, `IDREF`, `ENTITY` | `:string` | |
| `boolean` | `:boolean` | |
| `float`, `double` | `:double` | |
| `decimal` | `:decimal` | custom schema, BigDecimal |
| `integer` and friends | `:int` | several are conceptually BigInteger in XSD |
| `date` | `:time/local-date` | |
| `dateTime` | `:time/offset-date-time` | |
| `time` | `:time/local-time` | |
| `dayTimeDuration` | `:time/duration` | |
| `yearMonthDuration` | `:time/period` | |
| `gYear`, `gYearMonth`, `gMonth`, `gMonthDay` | time/* custom types | |
| `gDay`, `duration` | `:xml/hiccup` | no direct java.time mapping |
| `base64Binary`, `hexBinary` | `:xml/base64Binary`, `:xml/hexBinary` | `ByteBuffer` |
| `anyURI`, `QName`, `NOTATION` | `:string` | |
| `anySimpleType` | `:string` | simplified |

During generation, primitives usually appear as **namespaced keywords** (e.g. `:org.w3.www.2001.XMLSchema/dateTime`), which resolve through the registry when the schema is compiled with `make-schema`.

## Restrictions and facets

| XSD | Malli |
|-----|--------|
| `xs:restriction` base string + facets | `:string` props, and/or `[:enum]`, `[:re]`, often under `[:and …]` |
| `enumeration` | `[:enum {} "a" "b" …]` with optional docs map |
| `minLength` / `maxLength` / `length` | `:min` / `:max` on `:string` |
| `pattern` | `[:re "…"]` (special-case rewrites for XSD name patterns) |
| `whiteSpace` | no structural effect |
| Numeric/date primitives under restriction | often the primitive keyword (facets not always reified) |

## List and union

| XSD | Malli |
|-----|--------|
| `xs:list` / list variety | `[:sequential item]` |
| Builtin `IDREFS`, `ENTITIES`, `NMTOKENS` | sometimes `:string` |
| `xs:union` (`XSUnionSimpleType`) | `[:or m1 m2 …]` |

## Complex types

| XSD content | Malli pattern |
|-------------|----------------|
| Attributes only | `[:map {:closed true} attr-entries…]` |
| Empty content | attrs map, or `[:map {:empty true}]` |
| Simple content + attrs | value-wrapped map + `[:xml/value … simple]` |
| Complex content as element map | `[:map {:closed true} …]` or `[:merge {} attrs child-map]` |
| Mixed content + attrs | value-wrapped + `[:xml/value {} :xml/hiccup]` |
| Non-map particle + attrs | value-wrapped + particle form as `:xml/value` |

### Attribute use

```xml
<xs:attribute name="id" type="xs:ID" use="optional"/>
```

```clojure
[:id {:xml/attr true :optional true}
 [:ref :org.w3.www.2001.XMLSchema/ID]]
```

Namespaced attributes become namespaced keys (`uri->ns` + local name).

## Elements

| XSD | Malli |
|-----|--------|
| Local element in map-shaped sequence | `[:LocalName props type]` map entry |
| Element in choice / cat / multi | `[:tuple {} [:enum :LocalName] type]` |
| Global element (root) | branch of top-level `:multi` |
| `minOccurs="0"` | `:optional true` on map entry |
| `maxOccurs > 1` or unbounded (map field) | `[:sequential {:min … :max …} type]` |
| Same under sequence regex context | `:*` / `:+` / `:repeat` wrappers |

Type reference:

- Named global type → `[:ref :ns/Type]` or `[:ref :ns/Type-seq]` in sequence context
- Anonymous type → inlined form

## Compositors

### Sequence (`xs:sequence`)

| Situation | Malli |
|-----------|--------|
| Distinct element names, no repeated value-sequences on children | `:map` (`:closed true`) |
| Same, nested under sequence context | map with `:xml/in-seq-ex true` |
| Ordered heterogeneous particles / repeats | `[:cat {} p1 p2 …]` |
| Single `xs:any` | `:xml/hiccup` |

### Choice (`xs:choice`)

| Situation | Malli |
|-----------|--------|
| Typical | `:or` of particles, or `:alt` when parent context is sequence |
| Single any | `:xml/hiccup` |

### All (`xs:all`)

Prefer map when every particle is a uniquely named element; otherwise compositor-shaped fallback.

## Occurrence on particles (sequence context)

Applied by `wrap-regex`:

| minOccurs | maxOccurs | Wrapper |
|-----------|-----------|---------|
| 0 | 0 | omitted (`nil`) |
| 1 | 1 | none |
| 0 | 1 | `[:? schema]` |
| 1 | unbounded | `[:+ schema]` |
| 0 | unbounded | `[:* schema]` |
| other | other | `[:repeat {:min n :max m} schema]` |

Maps inside these wrappers are marked `:xml/in-seq-ex`.

## Wildcards

| XSD | Malli |
|-----|--------|
| `xs:any` / wildcard term | `[:xml/hiccup]` or bare `:xml/hiccup` |

## Model groups

| XSD | Malli |
|-----|--------|
| Global `xs:group` | registry entries `:ns/Name` and optionally `:ns/Name-seq` |
| Group reference | `[:ref …]` via `-seq-ref` |
| Local / anonymous group | inlined `handle-model-group` |

## Top-level document schema

```clojure
[:schema
 {:registry {… all named types and groups …}}
 [:multi {:dispatch first}
  [:RootA [:tuple {} [:enum :RootA] [:ref :ns/RootAType]]]
  [:RootB …]]]
```

Compiled with `xml-primitives/make-schema`, which merges default Malli schemas, util schemas, custom XML types, and experimental time schemas.

## Optional group flags

Nested sequence with `minOccurs="0"`:

```clojure
[:Child {:optional true :required-in-group true} child-schema]
```

and parent map props may include `:optional-group true`.

## Annotations

| Source | Property |
|--------|----------|
| Type documentation | `:documentation` on map props |
| Element documentation | `:documentation` on entry props |
| Attribute documentation | `:attr-documentation` |
| Enumeration value docs | `:value-documentation` on enum props |

## Identity examples

### Simple element sequence → map

```xml
<xs:complexType name="Person">
  <xs:sequence>
    <xs:element name="first" type="xs:string"/>
    <xs:element name="last" type="xs:string" minOccurs="0"/>
  </xs:sequence>
  <xs:attribute name="id" type="xs:string"/>
</xs:complexType>
```

Roughly:

```clojure
[:merge {}
 [:map {:closed true}
  [:id {:xml/attr true :optional true}
   :org.w3.www.2001.XMLSchema/string]]
 [:map {:closed true}
  [:first {} :org.w3.www.2001.XMLSchema/string]
  [:last {:optional true} :org.w3.www.2001.XMLSchema/string]]]
```

(Exact merge/layout depends on attribute handling; both attrs and children end up as one logical closed shape after merge.)

### Attr + simple content → value-wrapped

```xml
<xs:complexType name="Coded">
  <xs:simpleContent>
    <xs:extension base="xs:string">
      <xs:attribute name="code" type="xs:string"/>
    </xs:extension>
  </xs:simpleContent>
</xs:complexType>
```

```clojure
[:map {:closed true :xml/value-wrapped true}
 [:code {:xml/attr true :optional true} …]
 [:xml/value {} :org.w3.www.2001.XMLSchema/string]]
```

### Choice of elements

```xml
<xs:choice>
  <xs:element name="A" type="xs:string"/>
  <xs:element name="B" type="xs:int"/>
</xs:choice>
```

```clojure
[:or
 [:tuple {} [:enum :A] :org.w3.www.2001.XMLSchema/string]
 [:tuple {} [:enum :B] :org.w3.www.2001.XMLSchema/int]]
```

(or `:alt` in sequence context)

## What is *not* fully mapped

- Full facet application on non-string primitives
- All XSD identity constraints (`key`, `keyref`, `unique`)
- Full `xs:redefine` / advanced schema composition quirks beyond XSOM’s model
- Restriction-path union variety (throws)

When in doubt, generate with `raw-xsd->schema` or `serialize-schema` and inspect the EDN for the types you care about.