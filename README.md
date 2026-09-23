# cbioportal-core

Welcome to the cBioPortal Core GitHub page!

This repository contains the Java code used for the cBioPortal importer + scripts that allow end users to interact with it.

For documentation and usage instructions on the cBioPortal importer, please see here: https://docs.cbioportal.org/data-loading/

If you are a developer and want to help contribute to the cBioPortal importer codebase, please see here: https://docs.cbioportal.org/data-loading/data-loading-for-developers/

## WSI imports

The WSI importer is documented in [`docs/wsi-study-format.md`](docs/wsi-study-format.md).
cBioPortal core is the sole ClickHouse writer for WSI snapshots. Thumbnail
artifacts and the completed `meta_wsi.txt`/`data_wsi.txt` snapshot must be
prepared by an upstream artifact-generation/export pipeline before
`metaImport.py` runs. That pipeline is deployment-specific; it may use
Databricks or another implementation. Core does not generate thumbnails or
write the object store. Pathology procedure timing is imported separately as
standard `PATHOLOGY SLIDES` clinical timeline data.
