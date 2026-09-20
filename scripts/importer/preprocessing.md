# Required preprocessing checks

`validateData.py`, `validateStudies.py`, and `metaImport.py` check the conditions
that remain when applicable preprocessing is skipped. Validation never rewrites
study files. No marker proving that a script ran is required: already-correct
data passes. Script execution and input hashes belong in workflow provenance.

| Preprocessing step | Validation gate |
| --- | --- |
| CNA merging | Duplicate resolved genes and malformed Entrez identifiers are errors. Unresolved genes are warnings. |
| Supplemental clinical merging | Unreferenced top-level clinical files and multiple sample/patient attribute files are errors; existing clinical header, attribute and ID checks still apply. |
| Same-profile MAF fusion | Unreferenced top-level mutation files and repeated profile stable IDs are errors. Called and uncalled profiles with separate metadata remain separate. |
| MAF deduplication | Duplicate mutations on the existing eight-column key are errors. |
| Metadata gap-fill | Clinical, mutation and timeline staging files need a referencing metadata file of the correct type. Declared missing files also fail the ordinary file checks. |
| Case-list gap-fill | Missing required/nonempty generated lists are errors. Called mutation samples missing from the sequenced list are checked after reading mutation data. |
| OncoTree audit | Retired/unknown codes and populated conflicting labels are errors for curation; blank optional labels are not filled or rejected. |

Existing supported MAF formats are retained: the eight-column duplicate check
applies when all eight columns are present. This does not add a blanket requirement
for all optional MAF columns. Explicit importer filtering remains in effect.

## OncoTree

When sample clinical data contains `ONCOTREE_CODE`, nonempty codes must exist in
the selected reference. Populated `CANCER_TYPE` / `CANCER_TYPE_DETAILED` values
must match its `mainType` / `name`. Unknown codes and stale populated labels are
errors. Missing/NA codes and absent/blank/NA optional labels remain allowed;
existing labels for unclassified samples are preserved. Findings follow
cmo-pipelines PR #1394's read-only audit at `83f6e0e`, also retaining the core
validator's bracketed unavailable-value conventions. Codes and labels are trimmed
before comparison. Do not automatically rewrite labels or erase unknown codes
to make validation pass: report them for manual curation.

Running an audit cannot make a stale annotation current: the data is unchanged.
A regression therefore checks that audit and validator agree on the finding,
that neither edits input bytes, and that a reviewed correction clears the error.

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
Malformed nonempty Entrez identifiers (non-integer, non-positive or outside the
supported 32-bit range) remain errors. Unknown identifiers and unresolved genes
are warnings: per the public rollout policy, import may continue with those rows
omitted. An unresolved identifier alone does not fail a study, and validation
does not rewrite or remove the input row. Existing errors for missing identifiers
or ambiguous official symbols remain unchanged. Duplicate resolved genes remain
errors, including alias collisions. Equivalent integer
spellings (for example `001` and `1`) collide. Blank Entrez values can still
resolve through a valid gene symbol. Full resolution checks require portal
references; `-n` does not prove unknown-gene or alias-collision coverage.

The curation merger documents differences from Java for ambiguous symbols,
miRNA aliases and fallback rules. These checks do not claim to resolve those
upstream gaps or validate exact byte-for-byte transformation output.

## Case lists

`case_list_config.tsv` and the read-only parsing helpers are derived from
curation-tools PR #76 at `d8526de87d4e38e0badf9666cf8e75477aecd87f` (AGPL-3.0).
The parser includes cmo-pipelines PR #1394 parity for `Sample_ID` and `Sample_Id`
headers, blank data/sidecar lines, deterministic file lookup, and trailing fields.
The bundled config adds canonical `data_mutations.txt` alternatives to the legacy
mutation filenames. Synchronize the generator's deployed config to this same
candidate config before preparing new inputs; do not assume an older EC2 config
already contains those entries.

The bundled config determines which additional lists are required for its
recognized staging filenames. Parsing preserves case-insensitive filename
lookup, sequenced-sample overrides, TCGA normalization and union/intersection
rules. Reference samples are cached for the duration of a study check.

A nonempty generated list missing from the study is an error. Existing stable
IDs count even under different filenames. A study-local, nonempty curated list
with the expected category (except generic `other`) also satisfies the generated
role, even with a different stable ID. This does not bypass metadata or sample-ID
validation. An unrelated list occupying a required output filename is a conflict,
not permission to overwrite it. Virtual `_all` from
`add_global_case_list: true` also counts. Core's existing case-list checks still
validate references and metadata. Existing curated memberships, custom lists,
descriptions and ordering are not replaced or compared against mutation events:
a sample with no mutation events can still have been sequenced. That matches the
generator's gap-fill policy rather than inventing an overwrite policy.
Called mutation samples that are present in input but absent from `_sequenced`
are errors. This comparison runs after mutation-file validation, excludes the
separate uncalled profile, and resets per-study state between validations.

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

Timeline metadata: top-level `data_timeline.txt`, `data_timeline_*.txt`, and their
`.tsv` equivalents (case-insensitive) must be referenced by a timeline meta file
(`genetic_alteration_type: CLINICAL`, `datatype: TIMELINE`). Metadata filenames
need not match the data filename; resolved `data_filename` paths establish the
reference. Missing references are errors. Archived subdirectories, hidden files,
editor backups, and files with unrelated names are not inferred as timeline data.

The same reference-based coverage rule applies to top-level `data_clinical.txt`,
`data_clinical_*.txt` (`.tsv` also), and `data_mutations.txt`,
`data_mutations_*.txt` (`.tsv`/`.maf` also), case-insensitively. Correct metadata
may have any filename. A supplemental clinical file cannot bypass the single
sample/patient profile rule merely by adding a second metadata file. Do not fuse
explicitly distinct mutation profiles to satisfy this check. Arbitrarily named
unreferenced files and archived subdirectories cannot be reliably inferred as
import inputs and are outside this filename-based check.

## Regression and pinned-tool verification

`bash test_scripts.sh` includes omission/correction fixtures, already-clean
controls, alternate metadata names, distinct called/uncalled profiles, exact
duplicate identity, source-byte preservation, case-list completeness and state
isolation, and unchanged non-CNA warning policy.

To additionally exercise the actual pinned scripts used for public preprocessing:

```sh
PREPROCESS_TOOLS_DIR=/path/to/pinned-tools bash test_scripts.sh
```

That directory must contain cmo-pipelines PR1394 scripts (`83f6e0e`), CNA PR75
(`9843d670`), clinical/metadata PR78 (`92fdb9e1`), and MAF-fusion PR79 (`51e36fac`).
The integration fixture fails with each applicable preprocessing omission, runs
the six transforming tools using the candidate case-list config, then passes.
It separately compares the seventh, read-only OncoTree audit to validator output.
Without that environment variable this external-tool test is explicitly skipped.
It does not download tools or use the network. A valid fixture without a virtual
`_all` list exercises physical-list generation; the validator still rejects a
duplicate physical `_all` when `add_global_case_list: true` already defines one.
