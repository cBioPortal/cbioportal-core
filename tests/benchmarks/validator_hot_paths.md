# Validator hot-path benchmarks

Baseline: `83c9dce` (preprocessing checks plus generic-assay uniqueness fix
`e6cc508`). Python 3.12.7, macOS arm64. These isolate file validation with
portal checks skipped; they do not measure full study or batch runtime.

Inputs: first 20,000 complete data rows plus header from Datahub commit
`0cc9138746c08b304f8dac92c31983e0ef44af1d`:

- `public/thym_tcga_pan_can_atlas_2018/data_methylation_hm450.txt`:
  123 sample columns, 23,464,320 bytes; sample SHA-256
  `ed01aafc40f45ef1c4d721dff4a6e08e15a66d2f2a201cee039716d42b8eff8e`.
- `public/msk_impact_50k_2026/data_mutations.txt`: 7,843,070 bytes; sample SHA-256
  `c465d87975a66034c9a32fee49077535006002a448f1e10c0df4bf48dc1f0230`.

The baseline cProfile matrix run spent 4.96 of 10.44 seconds in per-cell
`re.match` and `re.sub` calls. Mutation profiling showed 1,087,371 `list.index`
calls across 20,000 rows. Header-bound function and duplicate-key lookups now
happen once per file; other mutation helper lookups are unchanged.

Three alternating unprofiled before/after runs:

| File | Median before | Median after | Before range | After range |
| --- | ---: | ---: | ---: | ---: |
| matrix | 9.584s | 3.228s | 7.428–11.542s | 2.554–8.183s |
| mutations | 1.578s | 0.759s | 1.376–2.033s | 0.706–0.865s |

Host load varied substantially. These samples establish an improvement, not a
production speedup guarantee. Compare unprofiled runs; cProfile adds overhead.
Every run parsed all 20,000 rows and preserved ordered WARNING/ERROR hashes:
matrix had no diagnostics; mutations had two warnings and 19,770 unique keys.
The active Codespace validation run was not modified or restarted.

Reproduce from a repository root using the same input and Python environment:

```sh
PYTHONPATH=scripts python tests/benchmarks/validate_generic_assay.py MATRIX_SAMPLE
PYTHONPATH=scripts python tests/benchmarks/validate_mutations.py MUTATION_SAMPLE
```

Both scripts accept `--profile output.prof`; inspect with Python's `pstats`.
The generic-assay benchmark already exists on the baseline. Copy the mutation
benchmark to an external location and point PYTHONPATH at each checkout when
comparing revisions. `--oncotree-file` pins the clinical reference for full-study
measurements; the microbenchmarks do not load it.
