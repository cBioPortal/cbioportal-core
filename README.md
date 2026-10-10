# cbioportal-core

Welcome to the cBioPortal Core GitHub page!

This repository contains the Java code used for the cBioPortal importer + scripts that allow end users to interact with it.

For documentation and usage instructions on the cBioPortal importer, please see here: https://docs.cbioportal.org/data-loading/

If you are a developer and want to help contribute to the cBioPortal importer codebase, please see here: https://docs.cbioportal.org/data-loading/data-loading-for-developers/

## WSI imports

Whole-slide images (WSI) are imported as standard resource data
(`WSI_SAMPLE`/`WSI_PATIENT` rows with `TYPE=WHOLE_SLIDE_IMAGE`). Legacy
`meta_wsi.txt`/`data_wsi.txt` pairs are not imported; convert them offline with
`scripts/importer/convertWsiToResources.py`. See
[`docs/wsi-study-format.md`](docs/wsi-study-format.md).
