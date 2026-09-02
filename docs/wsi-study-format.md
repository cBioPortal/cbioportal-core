# WSI study import

cBioPortal core is the sole database writer for whole-slide-image (WSI)
studies. The Databricks/export pipeline produces a normal study directory with
`meta_wsi.txt` and `data_wsi.txt`; the tile server only serves the source and
thumbnail artifacts returned by cBioPortal.

## Upstream artifact publication

Before the study files are exported, a separate scheduled thumbnail process
must read eligible slide inventory/source rows, generate master thumbnails,
write them to the S3/Dell ECS-compatible object store, and populate
`cdsi_prod.pathology_data_mining.slide_thumbnail_registry`. Each successful
registry row carries `artifact_uri`, `tile_metadata_json`, `width`, `height`,
and `content_type`. The Databricks canonical-association query joins the latest
successful registry row, computes `can_serve_tiles`, and exports the artifact
fields below into `data_wsi.txt`.

The scheduled thumbnail process is outside cBioPortal core and outside the
frontend. The frontend is a read-only consumer. The tile-server
`app/thumbnail_worker.py` on-demand CLI may write an object-store JPEG for
development or controlled remediation, but it does not update the registry and
must not be used as the production publication mechanism. A canonical refresh
must wait for the thumbnail batch completion watermark, and successful legacy
rows missing `tile_metadata_json` must be regenerated before export.

## Metadata

```text
cancer_study_identifier: <study stable id>
genetic_alteration_type: PATHOLOGY_SLIDES
datatype: WSI
data_filename: data_wsi.txt
format_version: 2
```

The importer rejects unsupported format versions. A study may contain one WSI
pair. The data file follows the normal cBioPortal five-row preamble: four
comment rows, followed by this exact header:

```text
PATIENT_ID  REFERENCE_SAMPLE_ID  SAMPLE_ID  IMAGE_ID  PART_KEY  PART_NUMBER  PART_DESIGNATOR  PART_TYPE  PART_DESCRIPTION  SUBSPECIALTY  PATH_DX_TITLE  BLOCK_KEY  BLOCK_NUMBER  BLOCK_LABEL  MATCH_LEVEL  SPECIMEN_KEY  STAIN_NAME  STAIN_GROUP  IS_HNE  IS_IHC  MAGNIFICATION  FILE_SIZE_BYTES  BARCODE  SLIDE_TYPE  CAN_SERVE_TILES  SOURCE_URL  TILE_METADATA_JSON  THUMBNAIL_URL  THUMBNAIL_WIDTH  THUMBNAIL_HEIGHT  THUMBNAIL_CONTENT_TYPE
```

Values are tab-delimited. Timing is represented by the standard
`PATHOLOGY SLIDES` clinical timeline event, not by the WSI hierarchy file.
Every canonical slide with an available timeline date is represented in that
event stream. Slides whose resolved stain flags are neither H&E nor IHC use
the `Other` subtype and an all-slides linkout. Rows without a usable timeline
date remain in the WSI hierarchy but cannot be placed on the timeline.
Required values are `PATIENT_ID`, `IMAGE_ID`,
`PART_KEY`, `BLOCK_KEY`, `MATCH_LEVEL`, `SPECIMEN_KEY`, `IS_HNE`, `IS_IHC`,
and `CAN_SERVE_TILES`. `MATCH_LEVEL` is `BLOCK`, `PART`, or `UNMATCHED`;
matched rows require `SAMPLE_ID`, while unmatched rows leave it blank.

`IMAGE_ID` is unique within a study. Repeated part and block keys must carry
the same descriptive values. Stable patient, sample, and reference-sample IDs
must resolve to the study, and a sample/reference sample must belong to the
row's patient. `TILE_METADATA_JSON` must be a JSON object when present. URLs
must be absolute. When `CAN_SERVE_TILES=TRUE`, source URL, tile metadata,
thumbnail URL, positive dimensions, and thumbnail content type are mandatory.
The importer requires the declared media type to match the thumbnail URI
extension (`.jpg`/`.jpeg` → `image/jpeg`, `.png` → `image/png`); mismatches are
rejected before a row can be loaded.
Non-servable rows have those artifact columns stored as null. The importer
rejects MRNs, absolute dates, labelled identifiers, unsafe URI components, and
URI prefixes outside the configured `WSI_ALLOWED_SOURCE_PREFIXES` and
`WSI_ALLOWED_THUMBNAIL_PREFIXES` environment variables. Production must set
both allowlists; development may explicitly include `file:///app/testdata/`.

The importer assumes these values were already materialized by the upstream
Databricks/export pipeline. It does not discover source slides, generate
thumbnails, read `slide_thumbnail_registry`, or write the object store.

## Import commands

Full study import:

```bash
metaImport.py -s /path/to/study
```

WSI snapshots are loaded as part of a full study import into the inactive
blue/green database. WSI is not supported by incremental (`metaImport.py -d`)
imports. WSI loading runs after clinical sample definitions and before a full
study is marked `AVAILABLE`.

During the WSI load, the importer also writes sample-level clinical attributes
(`WSI_SAMPLE_SLIDE_COUNT`, `WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT`, and
`WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT`) and authoritative patient-level
attributes (`WSI_PATIENT_SLIDE_COUNT`, `WSI_PATIENT_PART_MATCHED_SLIDE_COUNT`,
and `WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT`). Study View uses the patient-level
values so pagination cannot produce partial totals, and falls back to summing
sample values for older studies. Existing count attributes are preserved when a
generated WSI count clinical file was loaded earlier in the same full import.

## ClickHouse snapshot

The importer resolves the stable identifiers to internal IDs and normalizes
the flat file into these tables:

- `wsi_patient`
- `wsi_part`
- `wsi_block`
- `wsi_slide`
- `wsi_slide_placement`
The five tables are provisioned by the cBioPortal backend schema. Core does not
create or migrate production tables. Study deletion removes the snapshot rows.
The importer is insert-only and must run against a fresh inactive database;
discard and rebuild that database after a failed or repeated WSI import.

The backend schema also provisions the additive `wsi_slide_by_access`
ClickHouse projection. It is ordered by `(cancer_study_id, image_id)` for the
authenticated slide-access lookup. A production rebuild must materialize the
projection before the new database is promoted; the importer itself does not
perform live schema changes.

The former tile-server ClickHouse loader is not supported; all WSI loads must
use the standard cBioPortal importer.
