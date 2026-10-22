# WSI study import

Whole-slide images (WSI) are imported as standard resource data. An upstream
pipeline produces a normal study directory whose resource files describe the
slides; `validateData.py` checks them and `metaImport.py` loads them with the
rest of the study. A study that contains a legacy `meta_wsi.txt` fails
validation and import; convert it first (see
[Converting legacy files](#converting-legacy-files)).

## Upstream pipeline

The pipeline is deployment-specific and outside cBioPortal core. Before the
study is exported it must generate each slide's thumbnail and tile metadata,
store the artifacts where the tile server can read them, seal each servable
slide's source (see [Sealed source](#sealed-source)) and write the fields
below into the study files, setting `can_serve_tiles` only for slides whose
artifacts are complete. Core does not discover slides, generate thumbnails or
write the artifact store.

## Resource files

WSI slides use the standard resource definition, sample resource, and patient
resource file pairs:

- `WSI_SAMPLE` (resource type `SAMPLE`): slides matched to a sample
  (`match_level` `PART` or `BLOCK`), one row per slide in the sample resource
  file with columns `PATIENT_ID SAMPLE_ID RESOURCE_ID URL DISPLAY_NAME METADATA`.
- `WSI_PATIENT` (resource type `PATIENT`): unmatched slides, one row per slide
  in the patient resource file with columns
  `PATIENT_ID RESOURCE_ID URL DISPLAY_NAME METADATA`.

A row is a slide row because of its `RESOURCE_ID`. Files are raw
tab-separated text: values are not quoted, and no value may contain a tab or
line break. `METADATA` is one compact JSON object per row whose keys are the
lower-cased [format-v4](#legacy-format-v4) column names. These public keys may
be returned to the browser:

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
  `thumbnail_content_type`.

`slide_key` is the opaque per-slide key of the WSI de-identification contract
(`wsi-serving-v6`): 12 lowercase hex characters computed upstream from a
salted hash of the image ID. It is the only slide identifier the browser sees.
The backend strips `wsi_serving` from generic resource responses and only
reads it to build the authorized WSI access token, into which it copies
`sealed_source` verbatim. The pathology image ID and the object URIs that
embed it are not stored anywhere in cBioPortal; they exist only inside the
sealed source.

### Row contract

`validateData.py` checks every `WSI_SAMPLE`/`WSI_PATIENT` row against these
rules, and the converter applies the same rules to each legacy row (both use
`convertWsiToResources.py`):

- `slide_key`, `part_key`, `block_key`, `match_level`, `specimen_key`,
  `is_hne`, `is_ihc` and `can_serve_tiles` are required, and every key has the
  JSON type listed above. Missing keys are absent values.
- `slide_key` is 12 lowercase hex characters and unique within the study,
  across both files.
- `part_key` and `block_key` contain no `?`; slides that share a part or
  block key carry the same part or block values.
- `match_level` is `BLOCK`, `PART` or `UNMATCHED`; matched slides have a
  `SAMPLE_ID` (sample file) and unmatched slides have none (patient file).
- `slide_type` is `H&E`, `IHC`, `Other` or `Unknown`. `H&E` requires
  `is_hne`, `IHC` requires `is_ihc`, `Other`/`Unknown` allow neither, and a
  slide is never both.
- `file_size_bytes` is not negative.
- All slides of a patient have the same `reference_sample_id`, or all omit it
  (`UNMATCHED` is not a value). The sample and reference sample belong to the
  row's patient in the clinical files.
- When `can_serve_tiles` is true, all five `wsi_serving` keys are set: a valid
  sealed source, tile metadata of schema version 2 (positive `dimensions`, one
  positive `level_dimensions` entry and one `level_downsamples` value per
  level, `max_zoom`, `tile_size`, `safe_min_level`,
  `tile_metadata_schema_version` 2, and the decode policy fields the tile
  server sets; other fields are kept), thumbnail dimensions between 1 and
  8192, and an `image/*` content type. When it is false, `sealed_source` is
  absent or empty.
- The public keys never include `image_id`, `source_url`, `thumbnail_url`,
  `barcode`, `part_designator`, `path_dx_title` or `sealed_source`, and
  `wsi_serving` never includes `image_id`, `source_url` or `thumbnail_url`.

WSI errors name the column or `METADATA` path, never the value. Core does not
scan free text (`DISPLAY_NAME`, part and block descriptions, stain names, tile
metadata and similar) for protected health information or
institution-specific identifiers: the data provider is responsible for
de-identifying free text before export.

### Sealed source

`sealed_source` (`SEALED_SOURCE` in the legacy file) is produced by the
upstream pipeline with a key that only the pipeline and the tile server hold;
cBioPortal core and the backend cannot open it. It is unpadded base64url
(`A-Z`, `a-z`, `0-9`, `-`, `_`) of an AES-GCM nonce (12 bytes), the
ciphertext of the slide's image ID, tile source and thumbnail source, and the
tag (16 bytes), authenticated with the slide key. Core checks only its shape:
at most 4096 characters that decode to at least 29 bytes. The tile server
validates the URIs it unseals.

### Viewer link

`URL` is the link opened from the Files & Links tab, the standalone viewer
route:

```text
<portal base URL>/wsi/patient/<patient ID>?studyId=<study ID>&slideKey=<slide key>
```

The base URL is absolute and includes any context path the portal is deployed
under; each path and query value is percent-encoded. `DISPLAY_NAME` is a
non-identifying label; the converter writes
`<stain name> · Part <part number> / Block <block number>` (falling back
to the slide type, and omitting missing parts).

## Converting legacy files

`scripts/importer/convertWsiToResources.py` converts a legacy pair offline. It
never contacts cBioPortal, a database, or the artifact store.

```bash
python scripts/importer/convertWsiToResources.py \
  --meta-wsi /path/to/legacy/meta_wsi.txt \
  --output-dir /path/to/converted \
  --portal-base-url https://portal.example.org/cbioportal
```

- `--meta-wsi`: the legacy meta file; its `data_filename` is read from the
  same directory.
- `--output-dir`: where the converted files are written.
- `--portal-base-url`: absolute `http`/`https` URL of the portal, optionally
  with a context path; a trailing slash is ignored. It builds the viewer
  links.

The converter fails on the first row that breaks the
[row contract](#row-contract). It drops `BARCODE`, `PART_DESIGNATOR` and
`PATH_DX_TITLE`, an `UNMATCHED` reference sample, and the `wsi_serving`
object of rows with `CAN_SERVE_TILES=FALSE`; it copies `SEALED_SOURCE`
verbatim. Rows are streamed, and the output is staged in a hidden
`.<output dir name>.*.partial` directory next to `--output-dir`, so a failed
run writes nothing there. Run `validateData.py` on the study afterwards.

Each pair is only written when it has rows:

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

### Timeline

The converter does not produce timeline data, and neither `data_wsi.txt` nor
`METADATA` carries slide timing. Slide timing lives only in the study's
pathology timeline file, which core's normal timeline import loads as
`PATHOLOGY SLIDES` clinical events. `PATHOLOGY SLIDES` events reach the browser through the clinical events
API, so `validateData.py` and `ImportTimelineData` reject such events with a
non-blank `IMAGE_ID` or `IMAGE_IDS` column (without echoing the value).
`SPECIMEN`/`LINKOUT` should carry only the opaque specimen key.

## Legacy format v4

This is the converter's only input format.

```text
cancer_study_identifier: <study stable id>
genetic_alteration_type: PATHOLOGY_SLIDES
datatype: WSI
data_filename: data_wsi.txt
format_version: 4
```

The data file has the normal cBioPortal preamble of four `#` rows, then this
exact header (tab-delimited, 30 columns):

```text
PATIENT_ID  REFERENCE_SAMPLE_ID  SAMPLE_ID  PART_KEY  PART_NUMBER  PART_DESIGNATOR  PART_TYPE  PART_DESCRIPTION  SUBSPECIALTY  PATH_DX_TITLE  BLOCK_KEY  BLOCK_NUMBER  BLOCK_LABEL  MATCH_LEVEL  SPECIMEN_KEY  STAIN_NAME  STAIN_GROUP  IS_HNE  IS_IHC  MAGNIFICATION  FILE_SIZE_BYTES  BARCODE  SLIDE_TYPE  CAN_SERVE_TILES  TILE_METADATA_JSON  THUMBNAIL_WIDTH  THUMBNAIL_HEIGHT  THUMBNAIL_CONTENT_TYPE  SLIDE_KEY  SEALED_SOURCE
```

`data_wsi.txt` carries no slide timing. Slide dates go in the study's
pathology timeline file and are imported as `PATHOLOGY SLIDES` clinical events
(see [Timeline](#timeline)). Any other header is rejected, including one with
slide-timing columns, as are other format versions and files with an
`IMAGE_ID`, `SOURCE_URL` or `THUMBNAIL_URL` column.
`PATIENT_ID` is required, booleans are `TRUE` or `FALSE`, and
`TILE_METADATA_JSON` is a JSON object when set.

## ClickHouse storage

Resource rows are stored in `resource` and definitions in
`resource_definition`. Resource row IDs (`internal_id`) come from the
importer's sequence, so they stay unique across importer processes.
Re-importing a resource file replaces the study's rows for the resource IDs in
that file, and study deletion removes them. WSI data has no tables of its own.
