# Preprocessing omission verification — 2026-09-20

Baseline: PR183 revision `9155561e67230b669535bde807fc63c599c3a836`.
Candidate: the commit adding this report on `codex/validator-hot-paths`.
Python 3.13.5, Linux x86_64, isolated environment with repository
`requirements.txt`. Both revisions include the prior hot-path optimizations
and generic-assay uniqueness improvement (`e6cc508`).

Full Python suite: baseline **206 tests, OK (1 skipped)**; candidate **223 tests,
OK (1 skipped)**, including the actual pinned preprocessing-tool integration.
The additional 17 tests cover omission/correction and clean controls for the
seven steps, case-list membership timing and state isolation, supported sample
headers, CNA identifiers, and numeric alias IDs (which previously crashed).
See `scripts/importer/preprocessing.md` for the coverage matrix and scope limits.

## Paired performance checks

Input: Datahub `0cc9138746c08b304f8dac92c31983e0ef44af1d`, study
`brca_tcga_pan_can_atlas_2018`. Preserve the original header and the first N
complete data rows; do not run the preprocessor on these benchmark inputs.

| File | Rows | Sample bytes | SHA-256 |
| --- | ---: | ---: | --- |
| `data_methylation_hm450.txt` | 2,000 | 14,313,577 | `a14a276ccf9910c8d152b5e56787892a5a3dd66d5efbb066e3f7859d7a549bad` |
| `data_mutations.txt` | 20,000 | 21,401,316 | `b0bf993100e7b80cbd9b5ff6a2447cc13ca41ca1e0d38c23027e0a8e78318681` |

Run the existing `validate_generic_assay.py` and `validate_mutations.py`
benchmarks with `PYTHONPATH` pointing at each checkout's `scripts/` directory.
Use three alternating baseline/candidate pairs in the same environment, without
cProfile. All runs parsed the complete row sample; ordered diagnostic hashes
matched between revisions (matrix: no diagnostics; mutations: 42,876 warnings).

| Median elapsed seconds | Baseline | Candidate |
| --- | ---: | ---: |
| Matrix (776 sample columns) | 0.494 | 0.481 |
| Mutations | 0.959 | 0.941 |

These measurements check for regression, not a new speedup claim. Host load
varies, and this is not a whole-collection or candidate-image benchmark.

## Real-study spot checks

Historical results below used candidate `a964d5b`, before the subsequent user
policy change restoring unresolved CNA identifiers to warnings. Those findings
are no longer blocking by themselves; duplicate resolved CNA genes, malformed
Entrez IDs and OncoTree errors remain blocking. See `preprocessing.md` for the
current policy. Do not use these historical pass/fail results as a new manifest.

Checks used the batch's frozen public-blue gene/alias tables and OncoTree
`oncotree_2025_10_03`. Cancer-type, gene-set and gene-panel references were not
supplied, so those checks were explicitly skipped. These are targeted source
checks, **not** the planned complete candidate-image validation gate.

| Study | Raw → prepared result |
| --- | --- |
| `gbc_mskcc_2022` | 99 duplicate-CNA errors removed; 6 unresolved CNA genes remain blocking. |
| `blca_mskcc_solit_2012` | Both inputs pass with warnings. |
| `prad_cdk12_mskcc_2020` | 10 duplicate-CNA errors removed; 4 unresolved CNA genes and 2 OncoTree label findings remain blocking. |

This demonstrates that executing preprocessing does not guarantee validation
acceptance. Findings are retained for curation/exclusion; no records or
annotations were changed to force a pass. No database imports or traffic changes
were performed.

Detailed local evidence (not a runtime dependency):
`/home/james/work/cbio-validator-checks-20260920-GFSEMK7M/verification.json`,
`representative-results.json`, and per-study/test logs. Prepared artifacts and
frozen references are from batch `20260920-MoxUyglU`.
