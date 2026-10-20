# sitoa
Malli XML bindings

## Modules

| Module | Clojars | What it does |
|---|---|---|
| `xml-primitives` | `io.github.dunnage.sitoa/xml-primitives` | XML Schema built-in types as malli schemas |
| `bootstrapped-schema` | `io.github.dunnage.sitoa/bootstrapped-schema` | Generates malli schemas, or Clojure namespaces, from XSD |
| `serde` | `io.github.dunnage.sitoa/serde` | Streaming XML parser and unparser driven by those schemas |
| `edi-serde` | `io.github.dunnage.sitoa/edi-serde` | Schema-driven X12 parsing and writing with malli and StAEDI |

```clojure
{:deps {io.github.dunnage.sitoa/serde {:mvn/version "0.1.0"}}}
```

Every module is also usable as a git dependency, with `:deps/root` naming the
module directory. The Maven and git coordinates share the same lib names.

`serde` brings `xml-primitives` but not `bootstrapped-schema`. XSD generation
(XSOM, JAXB) is only needed when you generate schemas, so add
`bootstrapped-schema` where you do that, for example under a build alias.
`edi-serde` stands alone. Its X12 helpers in `support/`, which use
tech.ml.dataset, are for tests and REPL work (`-M:support`) and are not in the
jar.

## Releasing

From the repository root, `build.clj` releases `xml-primitives`,
`bootstrapped-schema`, `serde` and `edi-serde` at one version, in dependency
order. Each
module refers to its siblings with `:local/root`. Each pom names them at the
release version instead.

```bash
clojure -T:build jars      # target/<module>-<version>.jar and pom for each
clojure -T:build install   # into ~/.m2, to try them locally
CLOJARS_USERNAME=<username> CLOJARS_PASSWORD=<deploy token> clojure -T:build deploy
```

Set `version` in `build.clj` first. `deploy` refuses to run with uncommitted
changes. It needs a Clojars account with the `io.github.dunnage.sitoa` group.
Tag the released commit `v<version>`.

From GitHub Actions, pushing a tag `v<version>` that matches `build.clj`
runs `.github/workflows/release.yml`. That tests the released modules and
deploys them. It reads the repository secrets `CLOJARS_USERNAME` and
`CLOJARS_PASSWORD`, the latter a Clojars deploy token.
`.github/workflows/ci.yml` tests every module on JDK 21 and 25 on each push to
`main` and on each pull request.
