(ns dunnage.sitoa.xsd-to-malli.determinism-test
  "Regeneration into a clean directory is byte-identical.

  The generator has to be reproducible for the same reason the v1 emitter does:
  a checked-in tree that shifts between runs makes every review a diff of
  noise. Attribute uses now come from this project's own attributeGroup
  expansion rather than from XSOM's identity-hash iteration order, so document
  order is the thing under test."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [dunnage.sitoa.xsd-to-malli.support :as support])
  (:import (java.io File)
           (java.security MessageDigest)))

(defn- delete-tree! [^File f]
  (when (.isDirectory f)
    (run! delete-tree! (.listFiles f)))
  (.delete f))

(defn- sha256 [^File f]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (apply str (map #(format "%02x" %) (.digest digest (java.nio.file.Files/readAllBytes (.toPath f)))))))

(defn- tree-digest
  "Relative path -> content digest, for every file under `dir`."
  [dir]
  (let [root (.toPath (io/file dir))]
    (into (sorted-map)
          (comp (filter #(.isFile ^File %))
                (map (fn [^File f] [(str (.relativize root (.toPath f))) (sha256 f)])))
          (file-seq (io/file dir)))))

(defn- regenerate [name xsd default-ns entry-ns]
  (let [dirs (mapv #(str support/out-root "/determinism/" name "/" %) ["a" "b"])]
    (doseq [dir dirs]
      (delete-tree! (io/file dir))
      (support/emit-into! xsd default-ns dir entry-ns))
    (mapv tree-digest dirs)))

(deftest regeneration-is-byte-identical
  (doseq [[name xsd default-ns entry-ns expected-files]
          [["multifile" support/multifile-xsd "multi" 'dunnage.sitoa.gen.det.multifile 9]
           ["xmlschema" (dunnage.sitoa.xsd-to-malli.oracle/xsd "XMLSchema.xsd") "xsd"
            'dunnage.sitoa.gen.det.xmlschema 66]]]
    (testing name
      (let [[a b] (regenerate name xsd default-ns entry-ns)]
        (is (= expected-files (count a)))
        (is (= (set (keys a)) (set (keys b))))
        (is (= a b))))))

(deftest a-derived-type-file-is-stable-across-runs
  (testing "the plan literals are ordered, not merely present"
    (let [[a b] (regenerate "stability" support/multifile-xsd "multi"
                            'dunnage.sitoa.gen.det.stability)]
      (is (= (get a "types/example/ExtendedRecord.cljc")
             (get b "types/example/ExtendedRecord.cljc")))
      (is (= (get a "dunnage/sitoa/gen/det/stability.cljc")
             (get b "dunnage/sitoa/gen/det/stability.cljc"))))))
