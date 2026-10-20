(ns build
  "Releases sitoa's library modules to Clojars as io.github.dunnage.sitoa/<module>,
  all at one version, in dependency order. Run from the repository root:

    clojure -T:build jars      target/<module>-<version>.jar and pom for each
    clojure -T:build install   all of them into ~/.m2, to try them locally
    clojure -T:build deploy    all of them to Clojars

  deploy reads CLOJARS_USERNAME and CLOJARS_PASSWORD (a Clojars deploy token)
  from the environment and refuses to run with uncommitted changes. Bump
  version below for each release, and tag the commit v<version> afterwards.

  Within the repository the modules refer to each other with :local/root,
  which a pom cannot express. Each pom names its siblings at this release's
  version instead. edi-serde is released separately (com.breezeehr/edi-serde)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def version "0.1.0")
(def group "io.github.dunnage.sitoa")
(def repo "https://github.com/dunnage/sitoa")

;; dependency order: each module may only depend on the ones before it
(def modules
  [{:module "xml-primitives"
    :description "sitoa: XML Schema built-in types as malli schemas, shared by sitoa's other modules."}
   {:module "bootstrapped-schema"
    :description "sitoa: generate malli schemas (and Clojure namespaces) from XSD, for sitoa's XML serde."}
   {:module "serde"
    :description "sitoa: streaming XML parser and unparser driven by malli schemas."}])

(defn- lib [module] (symbol group module))

(defn- path [& parts] (.getAbsolutePath ^java.io.File (apply io/file parts)))

(defn- class-dir [module] (path "target" module "classes"))

(defn- jar-file [module] (path "target" (format "%s-%s.jar" module version)))

(defn- sibling? [lib-sym] (= group (namespace lib-sym)))

(defn- release-basis
  "The module's basis with each sibling, a :local/root within this repository,
  replaced by its Maven coordinate at this release's version. write-pom lists
  the basis's top-level libs, so the pom then names the siblings properly."
  [basis]
  (update basis :libs
          (fn [libs]
            (reduce-kv (fn [m l coord]
                         (if (and (sibling? l) (empty? (:dependents coord)))
                           (assoc m l {:mvn/version version})
                           m))
                       libs libs))))

(defn- check-maven-only! [module basis]
  (let [bad (into (sorted-map)
                  (remove (fn [[l coord]] (or (sibling? l) (contains? coord :mvn/version))))
                  (:deps basis))]
    (when (seq bad)
      (throw (ex-info (str module " depends on artifacts a pom cannot name: " (pr-str bad))
                      {:module module :deps bad})))))

(defn- jar-module [{:keys [module description]}]
  (b/with-project-root module
    (let [basis (b/create-basis {:project "deps.edn"})
          _ (check-maven-only! module basis)
          cd (class-dir module)]
      (b/delete {:path cd})
      (b/write-pom {:class-dir cd
                    :lib       (lib module)
                    :version   version
                    :basis     (release-basis basis)
                    :src-dirs  ["src"]
                    :scm       {:url                 repo
                                :connection          (str "scm:git:" repo ".git")
                                :developerConnection "scm:git:ssh://git@github.com/dunnage/sitoa.git"
                                :tag                 (b/git-process {:git-args "rev-parse HEAD"})}
                    :pom-data  [[:description description]
                                [:url repo]
                                [:licenses
                                 [:license
                                  [:name "Eclipse Public License 1.0"]
                                  [:url "https://www.eclipse.org/legal/epl-v10.html"]]]]})
      (b/copy-dir {:src-dirs   (:paths basis)
                   :target-dir cd
                   :ignores    [".*~$" "^#.*#$" "^\\.#.*" "^.DS_Store$" "^\\.keep$"]})
      (b/copy-file {:src "LICENSE" :target (str cd "/META-INF/sitoa/LICENSE")})
      (b/jar {:class-dir cd :jar-file (jar-file module)})
      (println "Built" (str (lib module) " " version) "->" (jar-file module))
      basis)))

(defn jars
  "Build every module's jar and pom."
  [_]
  (b/delete {:path "target"})
  (into {} (map (fn [m] [(:module m) (jar-module m)])) modules))

(defn install
  "Install every module into the local Maven repository."
  [opts]
  (let [bases (jars opts)]
    (doseq [{:keys [module]} modules]
      (b/install {:basis     (release-basis (get bases module))
                  :lib       (lib module)
                  :version   version
                  :jar-file  (jar-file module)
                  :class-dir (class-dir module)})
      (println "Installed" (lib module) version))))

(defn deploy
  "Deploy every module to Clojars, in dependency order."
  [opts]
  (let [dirty (b/git-process {:git-args "status --porcelain"})]
    (when-not (str/blank? dirty)
      (throw (ex-info "Refusing to deploy with uncommitted changes; the pom records HEAD." {:status dirty}))))
  (jars opts)
  (doseq [{:keys [module]} modules]
    (dd/deploy {:installer :remote
                :artifact  (jar-file module)
                :pom-file  (b/pom-path {:lib (lib module) :class-dir (class-dir module)})})
    (println "Deployed" (lib module) version)))
