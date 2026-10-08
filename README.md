# cbioportal-core

Welcome to the cBioPortal Core GitHub page!

This repository contains the Java code used for the cBioPortal importer + scripts that allow end users to interact with it.

For documentation and usage instructions on the cBioPortal importer, please see here: https://docs.cbioportal.org/data-loading/

If you are a developer and want to help contribute to the cBioPortal importer codebase, please see here: https://docs.cbioportal.org/data-loading/data-loading-for-developers/

## WSI imports

Whole-slide images (WSI) are imported as standard resource data: matched slides
are `WSI_SAMPLE` rows in the sample resource file, unmatched slides are
`WSI_PATIENT` rows in the patient resource file, and both carry
`TYPE=WHOLE_SLIDE_IMAGE` with the slide hierarchy and serving metadata
as JSON. `validateData.py` checks those rows against the WSI contract, and
`metaImport.py` loads them with the other resource files. See
[`docs/wsi-study-format.md`](docs/wsi-study-format.md).

Legacy `meta_wsi.txt`/`data_wsi.txt` pairs are no longer imported; convert
them offline with `scripts/importer/convertWsiToResources.py`, which accepts
only format v4 (30 columns, ending with the opaque `SLIDE_KEY` and
`SEALED_SOURCE`; slide-timing columns, if present, are ignored for now). Files
that still have `IMAGE_ID`, `SOURCE_URL` or `THUMBNAIL_URL` columns (format v3
and older) are rejected. Given
`--study-dir`, it also merges the six `WSI_*` slide-count clinical
attributes into copies of the study's clinical sample and patient files. Viewer
links and public metadata identify slides only by `slide_key`. The pathology
image ID and the object URIs that embed it are never stored in the study files
or the database: the upstream pipeline seals them into `SEALED_SOURCE`, which
only the tile server can open, and servable slides carry it in the private
`wsi_serving` metadata. The data provider is
responsible for de-identifying free-text values before export. WSI data is
stored only as resource data; core has no native WSI tables or importer.
Thumbnail artifacts and slide metadata must still be prepared by an upstream
artifact-generation/export pipeline; core does not generate thumbnails or write
the object store. Pathology procedure events shown on the patient timeline are
imported separately as standard clinical timeline data.
