# WSI study import

cBioPortal core is the sole database writer for whole-slide-image (WSI)
studies. Slides are imported as standard resource data: an upstream
artifact-generation/export pipeline produces a normal study directory whose
resource files describe the slides, `validateData.py` checks them, and
`metaImport.py` loads them with the rest of the study. The tile server only
serves the source and thumbnail artifacts named by the sealed source that
cBioPortal hands it in the access token. The upstream
pipeline is deployment-specific and may use Databricks, scripts, or another
service; Databricks is not required by cBioPortal core.

Legacy `meta_wsi.txt`/`data_wsi.txt` pairs are no longer
imported. Validation and import both fail for a study that still contains
`meta_wsi.txt`; convert it offline first (see
[Converting legacy files](#converting-legacy-files)).

## Upstream artifact publication

Before the study files are imported, an upstream artifact-generation/export
pipeline must read or receive the eligible slide inventory and source rows,
generate the required master thumbnails and tile metadata, store the artifacts
where the deployment's tile-serving layer can access them, seal each servable
slide's source (see [Sealed source](#sealed-source)), and materialize the
artifact fields below into the study files. The pipeline must set
`can_serve_tiles` consistently with the availability of the sealed source,
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

## Resource files

WSI slides use the standard resource definition, sample resource, and patient
resource file pairs:

- `WSI_SAMPLE` (resource type `SAMPLE`): slides matched to a sample
  (`match_level` `PART` or `BLOCK`), one row per slide in the sample resource
  file with columns `PATIENT_ID SAMPLE_ID RESOURCE_ID URL DISPLAY_NAME TYPE METADATA`.
- `WSI_PATIENT` (resource type `PATIENT`): unmatched slides, one row per slide
  in the patient resource file with columns
  `PATIENT_ID RESOURCE_ID URL DISPLAY_NAME TYPE METADATA`.

Both use `TYPE=WHOLE_SLIDE_IMAGE`, and the `WSI_SAMPLE`/`WSI_PATIENT` IDs are
reserved for that type. Files are raw tab-separated text: values are not
quoted, and no value may contain a tab or line break. `METADATA` is one compact
JSON object per row whose keys are the lower-cased format-v4 column names.
These public keys may be returned to the browser:

- strings: `slide_key`, `reference_sample_id`, `part_key`, `part_number`,
  `part_type`, `part_description`, `subspecialty`, `block_key`,
  `block_number`, `block_label`, `match_level`, `specimen_key`, `stain_name`,
  `stain_group`, `magnification`, `slide_type`;
- JSON booleans: `is_hne`, `is_ihc`, `can_serve_tiles`;
- JSON integers: `file_size_bytes`;

and one private key:

- `wsi_serving`, only for slides that can serve tiles: an object with
  `sealed_source`, `tile_metadata_json` (a JSON object, not a string),
  `thumbnail_width`, `thumbnail_height` (integers) and
  `thumbnail_content_type`. Slides that cannot serve tiles have no
  `wsi_serving`.

`slide_key` is the opaque per-slide key of the WSI de-identification contract
(`wsi-serving-v6`): 32 lowercase hex characters computed upstream from a
salted hash of the image ID, unique within the study. It is the only slide
identifier the browser sees. `wsi_serving` is private: the backend strips it
from generic resource responses and only reads it to build the authorized WSI
access token, into which it copies `sealed_source` verbatim.

The pathology image ID and the object URIs that embed it (`source_url`,
`thumbnail_url`) are not stored anywhere in cBioPortal, not even in
`wsi_serving`; they exist only inside the sealed source. `validateData.py`
rejects rows whose top-level metadata has `image_id`, `source_url`,
`thumbnail_url`, `barcode`, `part_designator`, `path_dx_title` or
`sealed_source`, and rows whose `wsi_serving` has `image_id`, `source_url` or
`thumbnail_url`. Missing keys are treated as absent values.

### Sealed source

`sealed_source` (`SEALED_SOURCE` in the legacy file) is produced by the
upstream pipeline with a key that only the pipeline and the tile server hold;
cBioPortal core and the backend cannot open it. It is unpadded base64url
(`A-Z`, `a-z`, `0-9`, `-`, `_`) of an AES-GCM nonce (12 bytes), the
ciphertext of the slide's image ID, tile source and thumbnail source, and the
tag (16 bytes), authenticated with the slide key. Core checks only its shape:
at most 4096 characters that decode to at least 29 bytes. It is required when
`can_serve_tiles` is true and must be absent or empty otherwise. Error
messages never echo it.

For every `WHOLE_SLIDE_IMAGE` row, `validateData.py` applies the same checks
as the legacy WSI validator described under [Legacy format](#legacy-format-v4):
required hierarchy keys, typed values, `match_level`
agreement with the file type (sample rows are matched, patient rows are
unmatched), `slide_type` being `H&E`, `IHC`, `Other`, or `Unknown` with
consistent stain flags (never both `is_hne` and `is_ihc`; `H&E` requires
`is_hne`, `IHC` requires `is_ihc`, `Other`/`Unknown` allow neither), as the
native `wsi_slide` table constraints required, `slide_key` (32 lowercase hex
characters) unique across both files of the study, consistent part and block
metadata, the sample and reference sample belonging to the row's patient, and
the `wsi_serving` shape and sealed-source rules. The row's `RESOURCE_ID` must be `WSI_SAMPLE` in sample files and
`WSI_PATIENT` in patient files.

Error messages for `WHOLE_SLIDE_IMAGE` rows never echo values; only the
column or JSON path is reported. Core does not scan free text (`DISPLAY_NAME`,
part and block descriptions, stain names, tile metadata and similar) for
protected health information or institution-specific identifiers such as
specimen accession numbers: the data provider is responsible for
de-identifying free text before export.

`URL` is the link opened from the Files & Links tab. The supported viewer link
is the standalone viewer route:

```text
<portal base URL>/wsi/patient/<patient ID>?studyId=<study ID>&slideKey=<slide key>
```

The base URL is absolute and includes any context path the portal is deployed
under; each path and query value is percent-encoded. The link never contains
the image ID or the sealed source. `DISPLAY_NAME` is a non-identifying label; the converter writes
`<stain name> · Specimen <part number> / Block <block number>` (falling back
to the slide type, and omitting missing parts).

## Converting legacy files

`scripts/importer/convertWsiToResources.py` converts a legacy pair offline. It
never contacts cBioPortal, a database, or the artifact store. It accepts only
[format v4](#legacy-format-v4), which carries the `SLIDE_KEY` and
`SEALED_SOURCE` on every row. Older formats are no longer converted: a meta
file with another `format_version`, or a data file that still has an
`IMAGE_ID`, `SOURCE_URL` or `THUMBNAIL_URL` column (format v3 and older), is
rejected without echoing any value; re-export such studies as format v4.

```bash
python scripts/importer/convertWsiToResources.py \
  --meta-wsi /path/to/legacy/meta_wsi.txt \
  --output-dir /path/to/converted \
  --portal-base-url https://portal.example.org/cbioportal \
  --study-dir /path/to/study
```

- `--meta-wsi` (required): the legacy meta file; its `data_filename` is read
  from the same directory.
- `--output-dir` (required): where the converted files are written.
- `--portal-base-url` (required): absolute `http`/`https` URL of the portal,
  optionally with a context path; a trailing slash is ignored. It builds the
  viewer links shown above.
- `--study-dir` (optional, recommended): the study the output will join. The
  converter merges the slide counts into copies of the study's clinical files
  (see [Slide counts](#slide-counts)). It must differ from `--output-dir`.

Rows are parsed like the retired native importer: leading `#` rows are
skipped, the header must match format v4 exactly, `MATCH_LEVEL` must agree
with `SAMPLE_ID`, `SLIDE_TYPE` and the stain flags must satisfy the native
`wsi_slide` constraints above, an `UNMATCHED` reference sample is dropped, and
serving fields are dropped when `CAN_SERVE_TILES=FALSE`. `SLIDE_KEY` is required, must be 32
lowercase hex characters and unique. `SEALED_SOURCE` must have the
[sealed-source shape](#sealed-source), is required when `CAN_SERVE_TILES=TRUE`
and must be empty otherwise; it is copied verbatim into `wsi_serving`. Error
messages name the column, never the value. Free-text cells are copied as
given; de-identifying them is the data provider's responsibility. Run
`validateData.py` on the study afterwards; the converter does not
re-implement the tile metadata contract.

Rows are streamed, so memory does not grow with the slide metadata. The output is staged
in a hidden `.<output dir name>.*.partial` directory next to `--output-dir`
and only moved into it when the whole conversion succeeds, so a failed run
writes nothing there.

The converter always writes the resource files; each pair is only written
when it has rows:

| Files | Content |
| --- | --- |
| `meta_resource_definition.txt`, `data_resource_definition.txt` | `WSI_SAMPLE` and/or `WSI_PATIENT` definitions, each with a `CUSTOM_METADATA` contract |
| `meta_resource_sample.txt`, `data_resource_sample.txt` | matched slides |
| `meta_resource_patient.txt`, `data_resource_patient.txt` | unmatched slides |

The `CUSTOM_METADATA` contract declares the per-slide identifier keys
(`slide_key`, `part_key`, `block_key`, `specimen_key` and
`reference_sample_id`) as `"filterable": false`. Nearly every slide has its own
value for these keys, so without the declaration the portal's resource table
would list every value as a filter option. The columns stay visible, searchable
and sortable.

With `--study-dir`, it also writes merged copies of the study's clinical sample
and patient files, under the study's own meta and data file names (for example
`meta_clinical_samples.txt`/`data_clinical_samples.txt`). The files are found
through their meta files (`datatype: SAMPLE_ATTRIBUTES` or
`PATIENT_ATTRIBUTES`); the meta files are copied unchanged. Copy the output
directory over the study to use them.

Without `--study-dir`, it writes the counts as standalone pairs instead:

| Files | Content |
| --- | --- |
| `meta_clinical_sample_wsi_counts.txt`, `data_clinical_sample_wsi_counts.txt` | sample slide counts (only when a slide is matched) |
| `meta_clinical_patient_wsi_counts.txt`, `data_clinical_patient_wsi_counts.txt` | patient slide counts |

A study may contain only one clinical sample file and one clinical patient
file, so the standalone pairs are only for studies without clinical files of
their own, or as input for merging by hand.

With `--study-dir`, the converter fails without writing anything if:

- a clinical file in the study already has any of the six `WSI_*` columns;
- a sample or patient with slides is missing from the clinical sample or
  patient file, or a sample belongs to a different patient there;
- the study has more than one clinical sample or clinical patient meta file;
- the study has no clinical patient file (every slide needs a patient count),
  or no clinical sample file while some slide is matched to a sample;
- a clinical file to merge lacks the four `#` attribute header rows;
- the study already has a resource definition, sample resource, or patient
  resource file.

### Slide counts

The count files carry the attributes the native importer used to write, as
`NUMBER` attributes with priority 1:

| Attribute | Display name |
| --- | --- |
| `WSI_SAMPLE_SLIDE_COUNT` | WSI Slides per Sample |
| `WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT` | WSI Slides per Sample, Part-matched |
| `WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT` | WSI Slides per Sample, Block-matched |
| `WSI_PATIENT_SLIDE_COUNT` | WSI Slides per Patient |
| `WSI_PATIENT_PART_MATCHED_SLIDE_COUNT` | WSI Slides per Patient, Part-matched |
| `WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT` | WSI Slides per Patient, Block-matched |

Each slide (`SLIDE_KEY`, unique within the study) counts once. Sample counts cover matched slides only, so only
samples with a matched slide get values. Patient counts include unmatched
slides, so every patient with a slide gets values. Part and block counts
follow `MATCH_LEVEL`, and zero is written for an entity that has values. In a
merged clinical file, the rows of samples or patients without slides get `NA`,
matching the native importer, which wrote no value for them. The merge appends
the six columns and their four header rows (display name, description,
`NUMBER`, priority `1`); every existing line, value and line ending is kept,
including comment and blank lines. Slides that
cannot serve tiles are counted. Study View uses the patient-level values so
pagination cannot produce partial totals. Because the counts are ordinary
clinical data, re-importing corrected files replaces them.

### Timeline

The converter does not produce timeline data. Pathology procedure events stay
in the study's existing clinical timeline files, which are imported unchanged.
Slides carry no timing in `METADATA`.

`PATHOLOGY SLIDES` timeline events reach the browser through the clinical
events API, so they must not carry real slide identifiers: `validateData.py`
and `ImportTimelineData` reject such events when they have a non-blank
`IMAGE_ID` or `IMAGE_IDS` column (again without echoing the value); slides
are addressed by their opaque key. `SPECIMEN`/`LINKOUT` should carry only the
opaque specimen key; other free-text columns are the data provider's
responsibility to de-identify.

## Legacy format v4

This is the converter's only input format.

### Legacy metadata

```text
cancer_study_identifier: <study stable id>
genetic_alteration_type: PATHOLOGY_SLIDES
datatype: WSI
data_filename: data_wsi.txt
format_version: 4
```

The converter rejects unsupported format versions. A legacy study has one WSI
pair. The data file follows the normal cBioPortal five-row preamble: four
comment rows, followed by this exact header:

```text
PATIENT_ID  REFERENCE_SAMPLE_ID  SAMPLE_ID  PART_KEY  PART_NUMBER  PART_DESIGNATOR  PART_TYPE  PART_DESCRIPTION  SUBSPECIALTY  PATH_DX_TITLE  BLOCK_KEY  BLOCK_NUMBER  BLOCK_LABEL  MATCH_LEVEL  SPECIMEN_KEY  STAIN_NAME  STAIN_GROUP  IS_HNE  IS_IHC  MAGNIFICATION  FILE_SIZE_BYTES  BARCODE  SLIDE_TYPE  CAN_SERVE_TILES  TILE_METADATA_JSON  THUMBNAIL_WIDTH  THUMBNAIL_HEIGHT  THUMBNAIL_CONTENT_TYPE  SLIDE_KEY  SEALED_SOURCE
```

Values are tab-delimited; a row has 30 columns. `SLIDE_KEY` is the opaque
slide key described under [Resource files](#resource-files) and
`SEALED_SOURCE` (last) the [sealed source](#sealed-source). Files that
still carry the seven slide-timing columns (`TIMELINE_START_DAYS` through
`TIMEPOINT_SOURCE`) between `THUMBNAIL_CONTENT_TYPE` and `SLIDE_KEY` (37
columns) are accepted, but those columns are ignored for now: they are neither
required, validated nor converted. A file with an `IMAGE_ID`, `SOURCE_URL`
or `THUMBNAIL_URL` column (format v3 and older) is rejected.
Required values are `PATIENT_ID`,
`PART_KEY`, `BLOCK_KEY`, `MATCH_LEVEL`, `SPECIMEN_KEY`, `IS_HNE`, `IS_IHC`,
`CAN_SERVE_TILES` and `SLIDE_KEY`. `MATCH_LEVEL` is `BLOCK`, `PART`, or `UNMATCHED`;
matched rows require `SAMPLE_ID`, while unmatched rows leave it blank.

`SLIDE_KEY` is unique within a study. Repeated part and block keys must carry
the same descriptive values. Stable patient, sample, and reference-sample IDs
must resolve to the study, and a sample/reference sample must belong to the
row's patient. `TILE_METADATA_JSON` must be a JSON object when present. For
servable rows, it must contain positive `dimensions.width` and
`dimensions.height`, a positive `levels` count with one positive
`level_dimensions` entry per level, a nonnegative `max_zoom`, and a positive
`tile_size`; additional producer-defined fields are preserved. When
`CAN_SERVE_TILES=TRUE`, the sealed source, tile metadata, thumbnail dimensions
between 1 and 8192, and an `image/*` thumbnail content type are mandatory;
when it is `FALSE`, `SEALED_SOURCE` must be empty. Core never sees the source
or thumbnail URIs, so it does not check them: the tile server validates the
URIs it unseals (scheme, allowlisted roots, extensions).
The converter drops the artifact columns for non-servable rows. Core does
not perform de-identification scanning of free text; the data provider
(upstream publication pipeline) is responsible for removing protected health
information and deployment-specific identifiers before export.

The converter and validator assume these values were already materialized by
the upstream artifact-generation/export pipeline. They do not discover source
slides, generate thumbnails, read a producer-specific registry, or write the
object store.

## ClickHouse storage

Resource rows are stored in `resource_data` and definitions in
`resource_definition`. Resource row IDs (`RESOURCE_DATA_ID`) come from the
importer's sequence, so they stay unique across importer processes.
Re-importing a resource file replaces the study's rows for the resource IDs in
that file, and study deletion removes them.

WSI data has no tables of its own: core neither writes nor deletes any
`wsi_*` table, and the backend schema has none from migration 3.7.0 on.
