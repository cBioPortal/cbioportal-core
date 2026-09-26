# cbioportal-core

Welcome to the cBioPortal Core GitHub page!

This repository contains the Java code used for the cBioPortal importer + scripts that allow end users to interact with it.

For documentation and usage instructions on the cBioPortal importer, please see here: https://docs.cbioportal.org/data-loading/

If you are a developer and want to help contribute to the cBioPortal importer codebase, please see here: https://docs.cbioportal.org/data-loading/data-loading-for-developers/

## WSI imports

Whole-slide images (WSI) are imported as standard resource data: matched slides
are `WSI_SAMPLE` rows in the sample resource file, unmatched slides are
`WSI_PATIENT` rows in the patient resource file, and both carry
`TYPE=WHOLE_SLIDE_IMAGE` with the slide hierarchy, timing and serving metadata
as JSON. `validateData.py` checks those rows against the WSI contract, and
`metaImport.py` loads them with the other resource files. See
[`docs/wsi-study-format.md`](docs/wsi-study-format.md).

Legacy `meta_wsi.txt`/`data_wsi.txt` pairs (format v2 or v3) are no longer
imported; convert them offline with `scripts/importer/convertWsiToResources.py`.
Given `--study-dir`, it also merges the six `WSI_*` slide-count clinical
attributes into copies of the study's clinical sample and patient files. Format
v2 has no slide timing columns; the converter takes them from the study's
pathology timeline file (found through `--study-dir`, or given with
`--timeline-file`), which is only read. The native WSI
tables and the `ImportWsiData` Java entry point are deprecated but retained.
Thumbnail artifacts and slide metadata must still be prepared by an upstream
artifact-generation/export pipeline; core does not generate thumbnails or write
the object store. Pathology procedure timing shown on the patient timeline is
imported separately as standard clinical timeline data.
