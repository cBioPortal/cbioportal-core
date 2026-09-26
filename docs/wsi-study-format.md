# WSI study import

cBioPortal core is the sole database writer for whole-slide-image (WSI)
studies. Slides are imported as standard resource data: an upstream
artifact-generation/export pipeline produces a normal study directory whose
resource files describe the slides, `validateData.py` checks them, and
`metaImport.py` loads them with the rest of the study. The tile server only
serves the source and thumbnail artifacts returned by cBioPortal. The upstream
pipeline is deployment-specific and may use Databricks, scripts, or another
service; Databricks is not required by cBioPortal core.

Legacy `meta_wsi.txt`/`data_wsi.txt` pairs (format v2 or v3) are no longer
imported. Validation and import both fail for a study that still contains
`meta_wsi.txt`; convert it offline first (see
[Converting legacy files](#converting-legacy-files)).

## Upstream artifact publication

Before the study files are imported, an upstream artifact-generation/export
pipeline must read or receive the eligible slide inventory and source rows,
generate the required master thumbnails and tile metadata, store the artifacts
where the deployment's tile-serving layer can access them, and materialize the
artifact fields below into the study files. The pipeline must set
`can_serve_tiles` consistently with the availability of the required source,
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
JSON object per row whose keys are the lower-cased format-v3 column names:

- strings: `image_id`, `reference_sample_id`, `part_key`, `part_number`,
  `part_designator`, `part_type`, `part_description`, `subspecialty`,
  `path_dx_title`, `block_key`, `block_number`, `block_label`, `match_level`,
  `specimen_key`, `stain_name`, `stain_group`, `magnification`, `barcode`,
  `slide_type`, `timeline_date_status`, `timeline_date_kind`,
  `timeline_date_source`, `timeline_date_reason`,
  `timeline_coordinate_system`, `timepoint_source`;
- JSON booleans: `is_hne`, `is_ihc`, `can_serve_tiles`;
- JSON integers: `file_size_bytes`, `timeline_start_days`;
- `wsi_serving`: an object with `source_url`, `tile_metadata_json` (a JSON
  object, not a string), `thumbnail_url`, `thumbnail_width`,
  `thumbnail_height` (integers) and `thumbnail_content_type`. It is empty for
  slides that cannot serve tiles.

`wsi_serving` is private: the backend strips it from generic resource
responses and only returns it through the authorized WSI access endpoint.
Missing keys are treated as absent values.

For every `WHOLE_SLIDE_IMAGE` row, `validateData.py` applies the same checks
as the legacy WSI validator described under [Legacy format](#legacy-format-v3):
required hierarchy keys, typed values, consistent timing, `match_level`
agreement with the file type (sample rows are matched, patient rows are
unmatched), `slide_type` being `H&E`, `IHC`, `Other`, or `Unknown` with
consistent stain flags (never both `is_hne` and `is_ihc`; `H&E` requires
`is_hne`, `IHC` requires `is_ihc`, `Other`/`Unknown` allow neither), as the
native `wsi_slide` table constraints required, `image_id` unique across both files of the study, consistent part
and block metadata, the sample and reference sample belonging to the row's
patient, and the `wsi_serving` shape and URL safety rules. The row's
`RESOURCE_ID` must be `WSI_SAMPLE` in sample files and `WSI_PATIENT` in
patient files.

`URL` is the link opened from the Files & Links tab. The supported viewer link
is the standalone viewer route:

```text
<portal base URL>/wsi/patient/<patient ID>?studyId=<study ID>&imageId=<image ID>
```

The base URL is absolute and includes any context path the portal is deployed
under; each path and query value is percent-encoded.

## Converting legacy files

`scripts/importer/convertWsiToResources.py` converts a legacy pair offline. It
never contacts cBioPortal, a database, or the artifact store. It accepts
[format v3](#legacy-format-v3), which carries the slide timing on every row,
and [format v2](#legacy-format-v2), whose timing is filled from the study's
pathology timeline file (see [Format v2 timing](#format-v2-timing)).

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
  (see [Slide counts](#slide-counts)). It must differ from `--output-dir`. For
  format v2 input, the study's pathology timeline is found through its meta
  file: the `TIMELINE` clinical file whose header has an `IMAGE_IDS` column
  (more than one such file is an error).
- `--timeline-file` (format v2 only): the pathology timeline data file to take
  the slide timing from; overrides the one found in `--study-dir`. It is an
  error for format v3 input.
- `--day-zero-mismatch error|undated` (format v2 only, default `error`): what
  to do with timeline events whose day zero is not first tumor sequencing (see
  [Format v2 timing](#format-v2-timing)).

Format v2 input needs a timeline source: without `--timeline-file` and without
a pathology timeline in `--study-dir`, the conversion fails rather than
silently marking every slide undated.

Rows are parsed like the retired native importer: leading `#` rows are
skipped, the header must match the declared format version exactly, `MATCH_LEVEL` must agree
with `SAMPLE_ID`, `SLIDE_TYPE` and the stain flags must satisfy the native
`wsi_slide` constraints above, an `UNMATCHED` reference sample is dropped, a missing
`TIMEPOINT_SOURCE` is derived from the timing provenance, and serving fields
are dropped when `CAN_SERVE_TILES=FALSE`. Run `validateData.py` on the study
afterwards; the converter does not re-implement URL allowlists or the tile
metadata contract.

Rows are streamed, so memory does not grow with the slide metadata: a
1.74-million-row format v2 file (832 MB) with its 797,000-event timeline
converts in under two minutes with about 1 GB of memory. The output is staged
in a hidden `.<output dir name>.*.partial` directory next to `--output-dir`
and only moved into it when the whole conversion succeeds, so a failed run
writes nothing there.

The converter always writes the resource files; each pair is only written
when it has rows:

| Files | Content |
| --- | --- |
| `meta_resource_definition.txt`, `data_resource_definition.txt` | `WSI_SAMPLE` and/or `WSI_PATIENT` definitions |
| `meta_resource_sample.txt`, `data_resource_sample.txt` | matched slides |
| `meta_resource_patient.txt`, `data_resource_patient.txt` | unmatched slides |

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

Each `IMAGE_ID` counts once. Sample counts cover matched slides only, so only
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
in the study's existing clinical timeline files, which are imported unchanged;
for format v2 the pathology timeline is only read. The slide timing fields in
`METADATA` drive the WSI hierarchy (including the undated section) and do not
create clinical events.

### Format v2 timing

For format v2 input, each slide's seven timing fields come from the pathology
timeline events that list its `IMAGE_ID` in their `IMAGE_IDS` column (a JSON
array, which may be CSV-quoted as a whole, as some exports write it). The
event's `TIMEPOINT_SOURCE` states the day zero of its `START_DATE`; the
contract's only coordinate system is `patient_first_tumor_sequencing_day_zero`.

| Slide | `TIMELINE_START_DAYS` | `STATUS` | `KIND` | `SOURCE` | `REASON` | `TIMEPOINT_SOURCE` |
| --- | --- | --- | --- | --- | --- | --- |
| in an event whose `TIMEPOINT_SOURCE` ends with "relative to first tumor sequencing" | event `START_DATE` | `AVAILABLE` | `ESTIMATED` if the text says "estimated", else `RECORDED` | `PATHOLOGY_TIMELINE` | blank | event text |
| in an event with any other day zero, with `--day-zero-mismatch undated` | blank | `MISSING_REFERENCE_SEQUENCING_DATE` | as above | `PATHOLOGY_TIMELINE` | `NO_SEQUENCING_REFERENCE` | event text |
| in no event | blank | `MISSING_PROCEDURE_DATE` | `UNDATED` | `NO_VERIFIED_PROCEDURE_DATE` | `MISSING_PROCEDURE_DATE` | `MISSING_PROCEDURE_DATE` |

`TIMELINE_COORDINATE_SYSTEM` is always `patient_first_tumor_sequencing_day_zero`
(the prefix `TIMELINE_DATE_` is omitted from the column names above). The
undated row is the tuple the upstream export writes for slides without a
verified procedure date. `PATHOLOGY_TIMELINE` records that the date was taken
from the study's pathology timeline rather than from the upstream source
system, which a v2 file does not name.

An event whose `TIMEPOINT_SOURCE` does not place day zero at first tumor
sequencing (for example "Procedure date relative to first ICD-O diagnosis",
or an unrecognized text) cannot be expressed in the contract's coordinate
system, so its `START_DATE` is never copied. By default the conversion fails
and lists those sources with their event and image counts. With
`--day-zero-mismatch undated`, those slides keep their procedure-date
provenance without an offset, as in the second row above; they appear in the
WSI hierarchy's undated section. To date them, re-export the timeline relative
to first tumor sequencing.

The conversion also fails when an image is listed for a different patient than
its `data_wsi.txt` row, or in events that disagree on its `START_DATE` or
`TIMEPOINT_SOURCE` (listing it again with the same values is fine), or when an
event with `IMAGE_IDS` has a non-integer `START_DATE`. Images listed in the
timeline but absent from `data_wsi.txt` produce a warning; their events are
still imported unchanged with the timeline file.

## Legacy format v3

This is the converter's input format; [format v2](#legacy-format-v2) differs
only in lacking the timing columns.

### Legacy metadata

```text
cancer_study_identifier: <study stable id>
genetic_alteration_type: PATHOLOGY_SLIDES
datatype: WSI
data_filename: data_wsi.txt
format_version: 3
```

The converter rejects unsupported format versions. A legacy study has one WSI
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
The converter drops those artifact columns for non-servable rows. Core does
not perform de-identification scanning; upstream publication pipelines are
responsible for removing protected health information and deployment-specific
identifiers before export. Production deployments should set both URI prefix
allowlists; development may explicitly include `file:///app/testdata/`.

The converter and validator assume these values were already materialized by
the upstream artifact-generation/export pipeline. They do not discover source
slides, generate thumbnails, read a producer-specific registry, or write the
object store.

### Legacy format v2

`format_version: 2` files have the same metadata and preamble (four comment
rows, then the header) as format v3, and a header made of the first 31 format
v3 columns, from `PATIENT_ID` through `THUMBNAIL_CONTENT_TYPE`. The seven
timing columns (`TIMELINE_START_DAYS` through `TIMEPOINT_SOURCE`) are absent;
the converter fills them from the pathology timeline file as described in
[Format v2 timing](#format-v2-timing). Every other column follows the format v3
rules.

## ClickHouse storage

Resource rows are stored in `resource_data` and definitions in
`resource_definition`. Resource row IDs (`RESOURCE_DATA_ID`) come from the
importer's sequence, so they stay unique across importer processes.
Re-importing a resource file replaces the study's rows for the resource IDs in
that file, and study deletion removes them.

The native WSI tables (`wsi_patient`, `wsi_part`, `wsi_block`, `wsi_slide`,
`wsi_slide_placement`, `wsi_slide_timing`) and the `ImportWsiData` Java entry
point are deprecated but retained; nothing in the resource import path writes
them. Study deletion still removes any rows a study has in them. Retiring the
tables is a separate change.
