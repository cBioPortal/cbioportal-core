# WSI study import

cBioPortal core is the sole database writer for whole-slide-image (WSI)
studies. An upstream artifact-generation/export pipeline produces a normal
study directory with `meta_wsi.txt` and `data_wsi.txt`; the tile server only
serves the source and thumbnail artifacts returned by cBioPortal. The upstream
pipeline is deployment-specific and may use Databricks, scripts, or another
service; Databricks is not required by cBioPortal core.

## Upstream artifact publication

Before the study files are imported, an upstream artifact-generation/export
pipeline must read or receive the eligible slide inventory and source rows,
generate the required master thumbnails and tile metadata, store the artifacts
where the deployment's tile-serving layer can access them, and materialize the
artifact fields below into `data_wsi.txt`. The pipeline must set
`CAN_SERVE_TILES` consistently with the availability of the required source,
tile-metadata, and thumbnail fields. An implementation may maintain a
registry, perform canonical-association queries, or use a completion watermark,
but those details are producer-specific and are not part of the cBioPortal core
contract.

The artifact-generation process is outside cBioPortal core and outside the
frontend. The frontend is a read-only consumer. A tile-serving deployment may
provide optional development or controlled-remediation tooling, but those
tools are not required by core and must not be used as the production
publication mechanism. The export must wait until artifact generation and
metadata publication are complete, and any producer-side metadata needed for
serving must be finalized before the study is imported.

## Metadata

```text
cancer_study_identifier: <study stable id>
genetic_alteration_type: PATHOLOGY_SLIDES
datatype: WSI
data_filename: data_wsi.txt
format_version: 3
```

The importer rejects unsupported format versions. A study may contain one WSI
pair. The data file follows the normal cBioPortal five-row preamble: four
comment rows, followed by this exact header:

```text
PATIENT_ID  REFERENCE_SAMPLE_ID  SAMPLE_ID  IMAGE_ID  PART_KEY  PART_NUMBER  PART_DESIGNATOR  PART_TYPE  PART_DESCRIPTION  SUBSPECIALTY  PATH_DX_TITLE  BLOCK_KEY  BLOCK_NUMBER  BLOCK_LABEL  MATCH_LEVEL  SPECIMEN_KEY  STAIN_NAME  STAIN_GROUP  IS_HNE  IS_IHC  MAGNIFICATION  FILE_SIZE_BYTES  BARCODE  SLIDE_TYPE  CAN_SERVE_TILES  SOURCE_URL  TILE_METADATA_JSON  THUMBNAIL_URL  THUMBNAIL_WIDTH  THUMBNAIL_HEIGHT  THUMBNAIL_CONTENT_TYPE  TIMELINE_START_DAYS  TIMELINE_DATE_STATUS  TIMELINE_DATE_KIND  TIMELINE_DATE_SOURCE  TIMELINE_DATE_REASON  TIMELINE_COORDINATE_SYSTEM  TIMEPOINT_SOURCE
```

Values are tab-delimited. Format v3 carries the de-identified relative timing
contract on every row. `TIMELINE_START_DAYS` is relative to the patient's first
tumor-sequencing sample; day zero is valid. `TIMELINE_DATE_KIND` is `RECORDED`,
`ESTIMATED`, or `UNDATED`, and `TIMELINE_DATE_SOURCE` and
`TIMELINE_DATE_REASON` preserve provenance. The coordinate system must be
`patient_first_tumor_sequencing_day_zero`. Missing procedure dates remain in the
WSI hierarchy and are represented by the adjacent undated UI section; they are
not converted into dated clinical events.
Required values are `PATIENT_ID`, `IMAGE_ID`,
`PART_KEY`, `BLOCK_KEY`, `MATCH_LEVEL`, `SPECIMEN_KEY`, `IS_HNE`, `IS_IHC`,
and `CAN_SERVE_TILES`. `MATCH_LEVEL` is `BLOCK`, `PART`, or `UNMATCHED`;
matched rows require `SAMPLE_ID`, while unmatched rows leave it blank.

`IMAGE_ID` is unique within a study. Repeated part and block keys must carry
the same descriptive values. Stable patient, sample, and reference-sample IDs
must resolve to the study, and a sample/reference sample must belong to the
row's patient. `TILE_METADATA_JSON` must be a JSON object when present. For
servable rows, it must contain positive `dimensions.width` and
`dimensions.height`, a positive `levels` count with one positive
`level_dimensions` entry per level, a nonnegative `max_zoom`, and a positive
`tile_size`; additional producer-defined fields are preserved. URLs
must be absolute and must not contain credentials, query strings, fragments, or
path traversal. When `CAN_SERVE_TILES=TRUE`, source URL, tile metadata,
thumbnail URL, positive dimensions, and an `image/*` thumbnail content type are
mandatory. Core does not require a particular URI scheme, source filename
extension, or thumbnail URI extension; those choices belong to the serving
layer. Deployments may restrict source and thumbnail roots with the
`WSI_ALLOWED_SOURCE_PREFIXES` and `WSI_ALLOWED_THUMBNAIL_PREFIXES` environment
variables.
Non-servable rows have those artifact columns stored as null. The importer
does not perform de-identification scanning; upstream publication pipelines are
responsible for removing protected health information and deployment-specific
identifiers before export. Production deployments should set both URI prefix
allowlists; development may explicitly include `file:///app/testdata/`.

The importer assumes these values were already materialized by the upstream
artifact-generation/export pipeline. It does not discover source slides,
generate thumbnails, read a producer-specific registry, or write the object
store.

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
- `wsi_slide_timing`
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
