let meta = #{
    name: "fix-in-regex",
    description: "Implement near-term in-regex? correctness fixes in serde unparser and verify with tests",
    phases: [
        #{ title: "Implement", detail: "apply near-term in-regex fixes in unparser.clj" },
        #{ title: "Tests", detail: "add focused tests for leaf/ref/multi/map disc paths" },
        #{ title: "Verify", detail: "run serde tests and adversarial code review" },
        #{ title: "Report", detail: "summarize changes and residual risks" },
    ],
    when_to_use: "Fix unparser in-regex? cursor protocol, ref dual-mode cache, multi, map disc, and noisy logs",
};

// Near-term recommended actions from adversarial review of in-regex? logic.
// Workspace root is the sitoa monorepo; primary file is serde/src/dunnage/sitoa/unparser.clj

phase("Implement");

let implement_prompt = "";
implement_prompt += "You are implementing near-term correctness fixes for the unparser `in-regex?` ";
implement_prompt += "protocol in the sitoa monorepo. Work in the existing workspace (no new branch required).\n\n";
implement_prompt += "PRIMARY FILE: serde/src/dunnage/sitoa/unparser.clj\n";
implement_prompt += "Also read: serde/src/dunnage/sitoa/parser.clj (make-tag-discriminator / map disc for reference).\n\n";
implement_prompt += "Implement ALL of the following fixes carefully. Read the full unparser before editing.\n\n";
implement_prompt += "## 1. Leaf unparsers must use the cursor\n";
implement_prompt += "When in-regex? is true, leaf unparsers currently write `data` instead of `(nth data pos)`. ";
implement_prompt += "Discriminators already use `(nth data pos)`. Fix consistency for:\n";
implement_prompt += "- string-unparser, boolean-unparser\n";
implement_prompt += "- base64-binary-unparser, hex-binary-unparser\n";
implement_prompt += "- duration/period/year/year-month/month-day/month unparsers\n";
implement_prompt += "- string-encode-unparser, offset-datetime-unparser\n";
implement_prompt += "hiccup-unparser already does (nth data pos) — keep that pattern.\n";
implement_prompt += "Guard bounds: only write when pos is in range; still advance carefully.\n\n";
implement_prompt += "## 2. Ref caches must be dual-mode\n";
implement_prompt += "ensure-discriminator-ref and ensure-unparser-ref currently key only by registry keyword, ";
implement_prompt += "so the first in-regex? mode wins forever. Key the cache by both the type key AND mode, ";
implement_prompt += "e.g. cache key [kw in-regex?] or a namespaced composite. Compile delays with the matching mode.\n\n";
implement_prompt += "## 3. -ref-discriminator arity in regex mode\n";
implement_prompt += "Today -ref-discriminator always returns (fn [data] ...) and never passes pos. ";
implement_prompt += "When in-regex? is true, return (fn [data pos] (@sub data pos)) with a sub that is the regex-mode disc. ";
implement_prompt += "When false, keep unary value-mode. Same idea for -ref-unparser: regex mode must call ";
implement_prompt += "(@sub data pos w) and return the new pos.\n\n";
implement_prompt += "## 4. :multi in regex mode\n";
implement_prompt += "Fix -multi-discriminator and -multi-unparser:\n";
implement_prompt += "- Non-regex disc: (fn [data] (and (vector? data) (tags (dispatch data)))) — do NOT call (f data 0).\n";
implement_prompt += "- Regex disc: (fn [data pos] ...) inspect (nth data pos), advance pos by 1 on match else stay.\n";
implement_prompt += "- Regex unparser: dispatch on (nth data pos), call child with the slot value (not whole data), ";
implement_prompt += "return (inc pos). Never use (reduced ...) outside of reduce.\n";
implement_prompt += "If multi children need false in-regex child unparsers, pass the item at pos, not the vector.\n\n";
implement_prompt += "## 5. Align map regex discrimination with tags (parser-like)\n";
implement_prompt += "Parser -map-discriminator uses non-attr element tags (see parser.clj). ";
implement_prompt += "In-regex -map-discriminator currently only checks map? + required keys (attrs mixed in). ";
implement_prompt += "Improve it: treat required non-attr, non-:xml/value keys as the identity signal when present; ";
implement_prompt += "prefer matching when the item is a map and either (a) has all required non-attr keys that exist, ";
implement_prompt += "or (b) if no required element keys, still require map? and any required attrs. ";
implement_prompt += "Do not break existing IVL/in-seq-ex map arms. Keep assert (not in-seq-ex) for non-regex maps.\n\n";
implement_prompt += "## 6. Gate noisy disc logs\n";
implement_prompt += "Hot-path log/info in discriminators and cat/alt/or unparsers is expensive. ";
implement_prompt += "Change those to log/debug, or wrap with a comment that they are debug-only. Prefer log/debug.\n\n";
implement_prompt += "## Constraints\n";
implement_prompt += "- Do not change public API of xml-unparser / xml-string-unparser.\n";
implement_prompt += "- Do not rewrite generation/bootstrapped-schema unless a tiny prop is required.\n";
implement_prompt += "- Preserve existing working round-trips (OffsetDateTime, value-wrapped hiccup, xsi:type).\n";
implement_prompt += "- Prefer minimal surgical edits; no drive-by refactors.\n";
implement_prompt += "- After edits, re-read the changed functions for arity consistency (disc/unparser pairs).\n";

let impl = agent(implement_prompt, #{
    label: "implement-in-regex-fixes",
    capability_mode: "all",
    agent_type: "general-purpose",
});

if impl == () || !impl.success {
    complete(#{
        summary: "Implementation agent failed",
        impl_success: false,
    });
}

phase("Tests");

let tests_prompt = "";
tests_prompt += "Add focused unit tests for the in-regex? unparser fixes in the sitoa serde module.\n\n";
tests_prompt += "Workspace: sitoa monorepo. Prefer extending serde/test/dunnage/sitoa/verify_offsetdatetime_test.clj ";
tests_prompt += "OR create serde/test/dunnage/sitoa/in_regex_unparser_test.clj if cleaner.\n\n";
tests_prompt += "Read serde/src/dunnage/sitoa/unparser.clj (current code after implement agent) and existing tests.\n\n";
tests_prompt += "Add tests that cover:\n";
tests_prompt += "1) Leaf in-regex string (or encode) path: build a tiny schema that forces cat of two strings ";
tests_prompt += "if feasible via -xml-unparser- with true, OR test discriminators/unparsers privately if needed. ";
tests_prompt += "If full public API always starts with in-regex false, construct internal schema forms like ";
tests_prompt += "[:cat {} :string :string] and use dunnage.sitoa.unparser/-xml-unparser- or xml-string-unparser ";
tests_prompt += "with a wrapper map/tuple so data is a vector of slots unparsed through cat.\n";
tests_prompt += "Practical approach: schema\n";
tests_prompt += "  [:map {:closed true :xml/value-wrapped true}\n";
tests_prompt += "   [:xml/value {} [:cat {} [:tuple {} [:enum :a] :string] [:tuple {} [:enum :b] :string]]]]\n";
tests_prompt += "  with data {:xml/value [[:a \"x\"] [:b \"y\"]]} and assert XML contains both elements.\n";
tests_prompt += "2) Dual-mode ref: same registry type used as map child (value) and under [:* [:ref :T]] if possible; ";
tests_prompt += "at minimum compile a schema with [:ref :T] inside :cat and as a map field and unparse both without throw.\n";
tests_prompt += "3) :multi non-regex still works (document-style) if already tested, skip if redundant.\n";
tests_prompt += "4) Keep existing verify_offsetdatetime_test and mixed hiccup tests green.\n\n";
tests_prompt += "Run tests from serde with something like:\n";
tests_prompt += "  cd serde && clojure -M:test -e \"(require 'clojure.test) ...\" \n";
tests_prompt += "Inspect serde/deps.edn for the correct test alias. Fix failures you introduce.\n";
tests_prompt += "Do not commit; leave files for the parent. Report which tests were added and command results.\n";

let tests = agent(tests_prompt, #{
    label: "add-in-regex-tests",
    capability_mode: "all",
    agent_type: "general-purpose",
});

phase("Verify");

let verify_jobs = [
    #{
        prompt: "In the sitoa monorepo, run the serde test suite thoroughly after in-regex unparser changes. "
            + "Inspect serde/deps.edn for aliases. Run all relevant tests under serde/test. "
            + "If tests fail, fix unparser or tests with minimal changes until green, or report blockers with stack traces. "
            + "Also run a quick sanity compile of unparser.clj. Report pass/fail counts and any remaining failures.",
        label: "run-serde-tests",
        capability_mode: "all",
        agent_type: "general-purpose",
    },
    #{
        prompt: "Adversarially review the CURRENT uncommitted changes to serde/src/dunnage/sitoa/unparser.clj "
            + "(and any new tests) related to in-regex? fixes. Use git diff and read_file. "
            + "Check specifically: (1) all leaf in-regex unparsers use (nth data pos); "
            + "(2) ref caches key by mode; (3) ref disc/unparser pass pos in regex mode; "
            + "(4) multi regex/non-regex arities; (5) map disc improvements do not break in-seq-ex; "
            + "(6) logs demoted. List residual bugs with file:line evidence. "
            + "Do not edit files unless you find a critical clear bug you can fix in <20 lines — prefer report.",
        label: "adversarial-review",
        capability_mode: "all",
        agent_type: "general-purpose",
    },
];

let verify_results = parallel(verify_jobs);

phase("Report");

let report_prompt = "";
report_prompt += "Synthesize a final report for the in-regex? near-term fix workflow.\n\n";
report_prompt += "Gather facts with git status, git diff --stat, and reading key hunks in ";
report_prompt += "serde/src/dunnage/sitoa/unparser.clj and any new test files.\n\n";
report_prompt += "Write a markdown report covering:\n";
report_prompt += "1. What was changed (bullet list mapped to the 6 recommended actions)\n";
report_prompt += "2. Tests added and test run results\n";
report_prompt += "3. Residual risks / follow-ups (from adversarial review)\n";
report_prompt += "4. Suggested commit message (do NOT commit)\n\n";
report_prompt += "Return the full report as your final message. Optionally write it to a file under ";
report_prompt += "serde/ only if useful; prefer the message body.\n";

let report = agent(report_prompt, #{
    label: "final-report",
    capability_mode: "all",
    agent_type: "general-purpose",
});

let test_ok = tests != () && tests.success;
let v0 = if verify_results.len() > 0 { verify_results[0] } else { () };
let v1 = if verify_results.len() > 1 { verify_results[1] } else { () };
let verify_ok = v0 != () && v0.success;
let review_ok = v1 != () && v1.success;
let report_ok = report != () && report.success;

let summary = "in-regex near-term fixes workflow finished";
if !test_ok {
    summary = "completed with test-agent issues";
}
if !verify_ok {
    summary = "completed with test-run issues";
}

complete(#{
    summary: summary,
    implement_success: true,
    tests_success: test_ok,
    verify_success: verify_ok,
    review_success: review_ok,
    report_success: report_ok,
});
