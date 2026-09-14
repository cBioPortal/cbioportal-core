# Required preprocessing checks

`validateData.py`, `validateStudies.py`, and `metaImport.py` reject studies that
need OncoTree label updates, CNA duplicate-gene resolution, or missing generated
case lists. Validation never rewrites study files. No marker proving that a
script ran is required: already-correct data passes.

## OncoTree

When sample clinical data contains `ONCOTREE_CODE`, nonempty codes must exist in
the selected reference, and `CANCER_TYPE` / `CANCER_TYPE_DETAILED` must match its
`mainType` / `name`. Unknown codes, missing label columns and stale labels are
errors. Missing/NA codes remain allowed; existing labels for unclassified samples
are preserved. This is the strict audit policy from curation-tools PR #77;
its default Datahub mode preserves existing labels, so correcting drift requires
reviewing its audit and using `oncotree_apply.py --force`. Remap retired codes
before applying; do not erase unknown codes to make validation pass.

For reproducible or disconnected runs, save the OncoTree `tumorTypes` JSON array
used during preprocessing, then pass the same snapshot:

```sh
python scripts/importer/validateData.py -s STUDY -p PORTAL_INFO \
  --oncotree-file /references/oncotree.json -v
```

All three entry points accept `--oncotree-file` and `--oncotree-version`. Without
an explicit file, `-p PORTAL_INFO` uses `PORTAL_INFO/oncotree.json`. Otherwise the
validator fetches `https://oncotree.mskcc.org/api/tumorTypes` once per validation
instance, using `oncotree_latest_stable` unless a version is supplied. The reference
is loaded only when needed. Its source and SHA-256 are logged at INFO level.
Fetch/read/schema failure is an error, not a skipped check, and is not retried
for every row. An explicit snapshot takes precedence over the version argument.

`-n` disables portal-dependent checks, not OncoTree checks. Supply a snapshot for
an entirely disconnected run. Gene alias collision coverage still requires the
portal gene and alias reference data; `-n` cannot establish full gene-resolution
parity. Use `-p` with the same reference data used for preprocessing.

## CNA

Duplicate rows resolving to the same gene are errors for discrete and continuous
CNA matrices. Resolution reuses core's existing gene/alias resolver; this is not
a second alias implementation copied from curation-tools PR #75. Unrelated
expression/methylation duplicate warning behavior is unchanged. Both compatible
and conflicting duplicates fail: inspect conflicts before running the merger.

The curation merger documents differences from Java for ambiguous symbols,
miRNA aliases and fallback rules. These checks do not claim to resolve those
upstream gaps or validate exact byte-for-byte transformation output.

## Case lists

`case_list_config.tsv` and the read-only parsing helpers are derived from
curation-tools PR #76 at `d8526de87d4e38e0badf9666cf8e75477aecd87f` (AGPL-3.0).
The bundled config determines which additional lists are required for its
recognized staging filenames. Parsing preserves case-insensitive filename
lookup, sequenced-sample overrides, TCGA normalization and union/intersection
rules. Reference samples are cached for the duration of a study check.

A nonempty generated list missing from the study is an error. Existing stable
IDs count even under different filenames; virtual `_all` from
`add_global_case_list: true` also counts. Core's existing case-list checks still
validate references and metadata. Existing curated memberships, custom lists,
descriptions and ordering are not replaced or compared against mutation events:
a sample with no mutation events can still have been sequenced. That matches the
generator's gap-fill policy rather than inventing an overwrite policy.

The read-only helper exposes `missing_generated_case_lists()` for reuse. There
is no runtime dependency on unmerged curation PRs. Future generator changes must
update this vendored policy and parity tests together. Unlike the upstream loop,
a three-way intersection stays empty after any disjoint pair; it must not
restart from the third input.

Performance: generator-compatible IDs are collected during the existing UTF-8
scan and reused after data-file validation. Files not visited by a validator
still use the standalone parser. Sequenced-sample sidecars retain precedence.
`validateStudies.py` lazily saves the first successfully fetched OncoTree
reference in a private temporary directory shared by its child validators.
The directory is removed after the batch. Explicit snapshots keep precedence;
independent CLI invocations should use the same `--oncotree-file` for a pinned
reference. Network failures still fail each affected study's validation.
