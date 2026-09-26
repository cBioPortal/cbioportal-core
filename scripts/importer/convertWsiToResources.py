#!/usr/bin/env python3
"""Convert a legacy WSI file pair (format v2 or v3) into standard cBioPortal study files.

The converter is deliberately offline: it never connects to cBioPortal, a
database, or an artifact store. It reads ``meta_wsi.txt``/``data_wsi.txt``,
applies the same row parsing and normalization as the retired native importer
(``ImportWsiData``), and writes:

* ``data_resource_definition.txt`` with the ``WSI_SAMPLE``/``WSI_PATIENT``
  definitions that have rows;
* ``data_resource_sample.txt`` for slides matched to a sample (``PART`` or
  ``BLOCK``) and ``data_resource_patient.txt`` for unmatched slides; each row
  links to the standalone viewer and carries the slide metadata as JSON;
* the six ``WSI_*`` slide-count attributes the native importer used to
  generate: with ``--study-dir``, merged into copies of the study's clinical
  sample and patient files (same file names); without it, as standalone
  ``data_clinical_sample_wsi_counts.txt``/``data_clinical_patient_wsi_counts.txt``
  pairs for studies without clinical files of their own or for hand merging;

each with its meta file. A generated data/meta pair is only written when it has rows.
Timeline files are not produced: existing clinical timeline files stay in the
study and are imported unchanged.

Format v3 carries the slide timing on every row. Format v2 has the same
columns without the seven timing columns; their values are filled from the
study's pathology timeline file (a ``TIMELINE`` clinical file with an
``IMAGE_IDS`` column), which is only read. See ``timeline_timing`` for the
mapping.

Rows are streamed, so large studies convert in bounded memory; output is
staged next to ``--output-dir`` and only moved there when the conversion
succeeds. The output is not validated here beyond what the native importer
checked while parsing; run ``validateData.py`` on the study after adding the
files.
"""

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote, urlparse


SAMPLE_RESOURCE_ID = "WSI_SAMPLE"
PATIENT_RESOURCE_ID = "WSI_PATIENT"
RESOURCE_TYPE = "WHOLE_SLIDE_IMAGE"

# Must stay identical to ImportWsiData.COLUMNS (format version 3).
COLUMNS = [
    "PATIENT_ID", "REFERENCE_SAMPLE_ID", "SAMPLE_ID", "IMAGE_ID",
    "PART_KEY", "PART_NUMBER", "PART_DESIGNATOR", "PART_TYPE",
    "PART_DESCRIPTION", "SUBSPECIALTY", "PATH_DX_TITLE", "BLOCK_KEY",
    "BLOCK_NUMBER", "BLOCK_LABEL", "MATCH_LEVEL", "SPECIMEN_KEY",
    "STAIN_NAME", "STAIN_GROUP", "IS_HNE", "IS_IHC", "MAGNIFICATION",
    "FILE_SIZE_BYTES", "BARCODE", "SLIDE_TYPE", "CAN_SERVE_TILES", "SOURCE_URL",
    "TILE_METADATA_JSON", "THUMBNAIL_URL", "THUMBNAIL_WIDTH",
    "THUMBNAIL_HEIGHT", "THUMBNAIL_CONTENT_TYPE", "TIMELINE_START_DAYS",
    "TIMELINE_DATE_STATUS", "TIMELINE_DATE_KIND", "TIMELINE_DATE_SOURCE",
    "TIMELINE_DATE_REASON", "TIMELINE_COORDINATE_SYSTEM", "TIMEPOINT_SOURCE",
]
# Format v2 is format v3 without the trailing timing columns.
TIMING_COLUMNS = COLUMNS[COLUMNS.index("TIMELINE_START_DAYS"):]
V2_COLUMNS = COLUMNS[:len(COLUMNS) - len(TIMING_COLUMNS)]
FORMAT_COLUMNS = {"2": V2_COLUMNS, "3": COLUMNS}

# Public metadata keys, emitted in lower case. Values the backend reads with
# JSONExtractString stay strings (e.g. PART_NUMBER, MAGNIFICATION).
PUBLIC_STRING_FIELDS = [
    "IMAGE_ID", "PART_KEY", "PART_NUMBER", "PART_DESIGNATOR", "PART_TYPE",
    "PART_DESCRIPTION", "SUBSPECIALTY", "PATH_DX_TITLE", "BLOCK_KEY",
    "BLOCK_NUMBER", "BLOCK_LABEL", "MATCH_LEVEL", "SPECIMEN_KEY", "STAIN_NAME",
    "STAIN_GROUP", "MAGNIFICATION", "BARCODE", "SLIDE_TYPE",
    "TIMELINE_DATE_STATUS", "TIMELINE_DATE_KIND", "TIMELINE_DATE_SOURCE",
    "TIMELINE_DATE_REASON", "TIMELINE_COORDINATE_SYSTEM",
]
SERVING_KEY = "wsi_serving"

MATCH_LEVELS = ("BLOCK", "PART", "UNMATCHED")
TIMELINE_STATUSES = ("AVAILABLE", "MISSING_PROCEDURE_DATE", "MISSING_REFERENCE_SEQUENCING_DATE")
TIMELINE_KINDS = ("RECORDED", "ESTIMATED", "UNDATED")
TIMELINE_COORDINATE_SYSTEM = "patient_first_tumor_sequencing_day_zero"
SLIDE_TYPES = ("H&E", "IHC", "Other", "Unknown")

# Format v2 timing from the pathology timeline (see timeline_timing).
TIMELINE_REQUIRED_COLUMNS = ("PATIENT_ID", "START_DATE", "TIMEPOINT_SOURCE", "IMAGE_IDS")
TIMELINE_DATE_SOURCE = "PATHOLOGY_TIMELINE"
NO_REFERENCE_REASON = "NO_SEQUENCING_REFERENCE"
# The timing of a slide in no timeline event: the upstream export's canonical
# undated tuple (TIMEPOINT_SOURCE is then derived from the reason).
UNDATED_TIMING = {
    "TIMELINE_START_DAYS": "",
    "TIMELINE_DATE_STATUS": "MISSING_PROCEDURE_DATE",
    "TIMELINE_DATE_KIND": "UNDATED",
    "TIMELINE_DATE_SOURCE": "NO_VERIFIED_PROCEDURE_DATE",
    "TIMELINE_DATE_REASON": "MISSING_PROCEDURE_DATE",
    "TIMELINE_COORDINATE_SYSTEM": TIMELINE_COORDINATE_SYSTEM,
    "TIMEPOINT_SOURCE": "",
}
SEQUENCING_DAY_ZERO = re.compile(r"\brelative to first tumor sequencing\s*$", re.IGNORECASE)
ESTIMATED_DATE = re.compile(r"\bestimated\b", re.IGNORECASE)
DAY_ZERO_MISMATCH_POLICIES = ("error", "undated")

# Names and descriptions must stay identical to ImportWsiData.insertSampleSlideCounts.
SAMPLE_COUNT_ATTRIBUTES = [
    ("WSI_SAMPLE_SLIDE_COUNT", "WSI Slides per Sample",
     "Associated pathology slide count for the sample."),
    ("WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT", "WSI Slides per Sample, Part-matched",
     "Associated pathology slides matched to a specimen part."),
    ("WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT", "WSI Slides per Sample, Block-matched",
     "Associated pathology slides matched to a specimen block."),
]
PATIENT_COUNT_ATTRIBUTES = [
    ("WSI_PATIENT_SLIDE_COUNT", "WSI Slides per Patient",
     "Associated pathology slide count for the patient."),
    ("WSI_PATIENT_PART_MATCHED_SLIDE_COUNT", "WSI Slides per Patient, Part-matched",
     "Associated pathology slides matched to a specimen part for the patient."),
    ("WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT", "WSI Slides per Patient, Block-matched",
     "Associated pathology slides matched to a specimen block for the patient."),
]
COUNT_ATTRIBUTE_IDS = frozenset(
    attribute[0] for attribute in SAMPLE_COUNT_ATTRIBUTES + PATIENT_COUNT_ATTRIBUTES)

DEFINITION_FILE = "data_resource_definition.txt"
SAMPLE_RESOURCE_FILE = "data_resource_sample.txt"
PATIENT_RESOURCE_FILE = "data_resource_patient.txt"
SAMPLE_COUNTS_FILE = "data_clinical_sample_wsi_counts.txt"
PATIENT_COUNTS_FILE = "data_clinical_patient_wsi_counts.txt"
RESOURCE_HEADER_SAMPLE = ["PATIENT_ID", "SAMPLE_ID", "RESOURCE_ID", "URL", "DISPLAY_NAME", "TYPE", "METADATA"]
RESOURCE_HEADER_PATIENT = ["PATIENT_ID", "RESOURCE_ID", "URL", "DISPLAY_NAME", "TYPE", "METADATA"]


class ConversionError(ValueError):
    """Raised when the legacy input cannot be converted faithfully."""


def read_meta(path):
    """Parse a cBioPortal ``key: value`` meta file."""
    values = {}
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ConversionError(f"{path}: cannot read meta file: {error}") from error
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if ":" not in line:
            raise ConversionError(f"{path}: invalid meta line: {line!r}")
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip()
    return values


def read_wsi_meta(path):
    meta = read_meta(path)
    if meta.get("genetic_alteration_type") != "PATHOLOGY_SLIDES" or meta.get("datatype") != "WSI":
        raise ConversionError(f"{path}: WSI metadata must use PATHOLOGY_SLIDES / WSI")
    if meta.get("format_version") not in FORMAT_COLUMNS:
        raise ConversionError(f"{path}: unsupported WSI format_version; expected 2 or 3")
    for field in ("cancer_study_identifier", "data_filename"):
        if not meta.get(field):
            raise ConversionError(f"{path}: {field} is required")
    return meta


def normalize_base_url(value):
    """Return the portal base URL without a trailing slash, or raise."""
    try:
        parsed = urlparse(value)
    except ValueError as error:
        raise ConversionError(f"--portal-base-url is not a valid URL: {value!r}") from error
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ConversionError(
            f"--portal-base-url must be an absolute http(s) URL, for example "
            f"https://portal.example.org or https://example.org/cbioportal: {value!r}")
    if parsed.query or parsed.fragment or parsed.params:
        raise ConversionError(f"--portal-base-url must not contain a query or fragment: {value!r}")
    return value.rstrip("/")


def viewer_url(base_url, study_id, patient_id, image_id):
    """Absolute link to the standalone viewer route for one slide."""
    return (f"{base_url}/wsi/patient/{quote(patient_id, safe='')}"
            f"?studyId={quote(study_id, safe='')}&imageId={quote(image_id, safe='')}")


def _iter_lines(path, what):
    """Yield (line number, text) for each line of a UTF-8 file, split on LF only.

    Splitting on LF alone (as the native importer did) keeps a stray CR inside a
    value in its row, so it is reported instead of silently starting a new row.
    """
    try:
        with open(path, "rb") as stream:
            for line_number, raw in enumerate(stream, start=1):
                if raw.endswith(b"\n"):
                    raw = raw[:-1]
                if raw.endswith(b"\r"):
                    raw = raw[:-1]
                try:
                    yield line_number, raw.decode("utf-8")
                except UnicodeDecodeError as error:
                    raise ConversionError(f"{path}: line {line_number}: {what} is not valid UTF-8") from error
    except OSError as error:
        raise ConversionError(f"{path}: cannot read {what}: {error}") from error


def iter_rows(data_path, columns=COLUMNS):
    """Stream the legacy data file: leading '#' rows, the exact header, then slide rows.

    Yields (line number, row dict with stripped values) and raises if the file
    has no slide rows.
    """
    lines = _iter_lines(data_path, "WSI data file")
    header = None
    for line_number, line in lines:
        if not line.startswith("#"):
            header = line
            break
    if header is None or header.split("\t") != columns:
        raise ConversionError(f"{data_path}: WSI data has an invalid header or column order")
    width = len(columns)
    found = False
    for line_number, line in lines:
        if line.startswith("#"):
            raise ConversionError(f"{data_path}: line {line_number}: WSI data row must not start with '#'")
        fields = line.split("\t")
        if len(fields) != width:
            raise ConversionError(
                f"{data_path}: line {line_number}: expected {width} columns, found {len(fields)}")
        fields = [field.strip() for field in fields]
        if not any(fields):
            raise ConversionError(f"{data_path}: line {line_number}: blank WSI row")
        found = True
        yield line_number, dict(zip(columns, fields))
    if not found:
        raise ConversionError(f"{data_path}: WSI data file contains no slide rows")


def read_rows(data_path, columns=COLUMNS):
    """Read every row of a legacy data file into a list (see iter_rows)."""
    return list(iter_rows(data_path, columns))


def _fail(line, message):
    raise ConversionError(f"line {line}: {message}")


def _require(row, field, line):
    if not row[field]:
        _fail(line, f"{field} is required")
    return row[field]


def _boolean(row, field, line):
    if row[field] not in ("TRUE", "FALSE"):
        _fail(line, f"{field} must be TRUE or FALSE")
    return row[field] == "TRUE"


def _optional_int(row, field, line):
    if not row[field]:
        return None
    try:
        return int(row[field])
    except ValueError:
        _fail(line, f"invalid {field}")


def _validate_timing(start_days, status, kind, source, reason, coordinate_system, line):
    # Mirrors ImportWsiData.validateTiming.
    if status not in TIMELINE_STATUSES:
        _fail(line, "invalid TIMELINE_DATE_STATUS")
    if kind not in TIMELINE_KINDS:
        _fail(line, "invalid TIMELINE_DATE_KIND")
    if not source:
        _fail(line, "TIMELINE_DATE_SOURCE is required")
    if coordinate_system != TIMELINE_COORDINATE_SYSTEM:
        _fail(line, "unsupported TIMELINE_COORDINATE_SYSTEM")
    if status == "AVAILABLE":
        if start_days is None or kind == "UNDATED" or reason:
            _fail(line, "AVAILABLE timing is inconsistent")
        return
    if start_days is not None:
        _fail(line, "non-AVAILABLE timing cannot have TIMELINE_START_DAYS")
    if status == "MISSING_PROCEDURE_DATE" and kind != "UNDATED":
        _fail(line, "missing procedure date must be UNDATED")
    if status == "MISSING_REFERENCE_SEQUENCING_DATE" and kind == "UNDATED":
        _fail(line, "missing reference date cannot be UNDATED")


def stain_flags_valid(slide_type, is_hne, is_ihc):
    """Mirror the native wsi_slide_stain_flags_valid CHECK constraint."""
    return (not (is_hne and is_ihc)
            and (slide_type != "H&E" or is_hne)
            and (slide_type != "IHC" or is_ihc)
            and (slide_type not in ("Other", "Unknown") or (not is_hne and not is_ihc)))


def derive_timepoint_source(kind, reason, source, status):
    # Mirrors ImportWsiData.deriveTimepointSource.
    if kind == "ESTIMATED":
        return "Verified estimated procedure date relative to first tumor sequencing"
    if kind == "RECORDED":
        return "Recorded procedure date relative to first tumor sequencing"
    return reason or source or status


def normalize_row(row, line):
    """Parse one row like ImportWsiData.normalize and return its metadata object."""
    metadata = {}
    for field in PUBLIC_STRING_FIELDS:
        if row[field]:
            metadata[field.lower()] = row[field]
    reference = row["REFERENCE_SAMPLE_ID"]
    if reference and reference.upper() != "UNMATCHED":
        metadata["reference_sample_id"] = reference
    metadata["is_hne"] = _boolean(row, "IS_HNE", line)
    metadata["is_ihc"] = _boolean(row, "IS_IHC", line)
    can_serve = _boolean(row, "CAN_SERVE_TILES", line)
    metadata["can_serve_tiles"] = can_serve
    # The native wsi_slide table enforced these as CHECK constraints
    # (wsi_slide_type_valid and wsi_slide_stain_flags_valid).
    if row["SLIDE_TYPE"] not in SLIDE_TYPES:
        _fail(line, "SLIDE_TYPE must be one of " + ", ".join(SLIDE_TYPES))
    if not stain_flags_valid(row["SLIDE_TYPE"], metadata["is_hne"], metadata["is_ihc"]):
        _fail(line, "IS_HNE/IS_IHC are inconsistent with SLIDE_TYPE " + row["SLIDE_TYPE"])

    file_size = _optional_int(row, "FILE_SIZE_BYTES", line)
    if file_size is not None:
        if file_size < 0:
            _fail(line, "FILE_SIZE_BYTES cannot be negative")
        metadata["file_size_bytes"] = file_size

    thumbnail_width = _optional_int(row, "THUMBNAIL_WIDTH", line)
    thumbnail_height = _optional_int(row, "THUMBNAIL_HEIGHT", line)
    for value in (thumbnail_width, thumbnail_height):
        if value is not None and not 0 <= value <= 0xFFFFFFFF:
            _fail(line, "thumbnail dimension is out of range")
    tile_metadata = None
    if row["TILE_METADATA_JSON"]:
        try:
            tile_metadata = json.loads(row["TILE_METADATA_JSON"])
        except ValueError:
            tile_metadata = None
        if not isinstance(tile_metadata, dict):
            _fail(line, "TILE_METADATA_JSON must be a JSON object")

    serving = {}
    if can_serve:
        for field in ("SOURCE_URL", "TILE_METADATA_JSON", "THUMBNAIL_URL", "THUMBNAIL_CONTENT_TYPE"):
            _require(row, field, line)
        for value in (thumbnail_width, thumbnail_height):
            if value is None or not 1 <= value <= 8192:
                _fail(line, "servable thumbnail dimensions must be between 1 and 8192")
        serving = {
            "source_url": row["SOURCE_URL"],
            "tile_metadata_json": tile_metadata,
            "thumbnail_url": row["THUMBNAIL_URL"],
            "thumbnail_width": thumbnail_width,
            "thumbnail_height": thumbnail_height,
            "thumbnail_content_type": row["THUMBNAIL_CONTENT_TYPE"],
        }
    # Non-servable rows carry no serving fields, as the native importer stored them as null.
    metadata[SERVING_KEY] = serving

    start_days = _optional_int(row, "TIMELINE_START_DAYS", line)
    _validate_timing(start_days, row["TIMELINE_DATE_STATUS"], row["TIMELINE_DATE_KIND"],
                     row["TIMELINE_DATE_SOURCE"], row["TIMELINE_DATE_REASON"],
                     row["TIMELINE_COORDINATE_SYSTEM"], line)
    if start_days is not None:
        metadata["timeline_start_days"] = start_days
    metadata["timepoint_source"] = row["TIMEPOINT_SOURCE"] or derive_timepoint_source(
        row["TIMELINE_DATE_KIND"], row["TIMELINE_DATE_REASON"],
        row["TIMELINE_DATE_SOURCE"], row["TIMELINE_DATE_STATUS"])
    return metadata


class SlideParser:
    """Normalize rows one at a time, applying the cross-row checks of ImportWsiData.normalize."""

    def __init__(self):
        self.image_ids = set()
        self.patient_references = {}
        self.parts = {}
        self.blocks = {}

    def parse(self, line, row):
        patient_id = _require(row, "PATIENT_ID", line)
        image_id = _require(row, "IMAGE_ID", line)
        if image_id in self.image_ids:
            _fail(line, f"IMAGE_ID is not unique: {image_id}")
        self.image_ids.add(image_id)
        part_key = _require(row, "PART_KEY", line)
        block_key = _require(row, "BLOCK_KEY", line)
        if "?" in part_key or "?" in block_key:
            _fail(line, "part/block keys cannot contain ?")
        match_level = row["MATCH_LEVEL"]
        if match_level not in MATCH_LEVELS:
            _fail(line, "invalid MATCH_LEVEL")
        sample_id = row["SAMPLE_ID"]
        if match_level == "UNMATCHED" and sample_id:
            _fail(line, "UNMATCHED rows cannot have SAMPLE_ID")
        if match_level != "UNMATCHED" and not sample_id:
            _fail(line, "matched rows require SAMPLE_ID")
        reference = row["REFERENCE_SAMPLE_ID"]
        reference = reference if reference and reference.upper() != "UNMATCHED" else None
        if self.patient_references.setdefault(patient_id, reference) != reference:
            _fail(line, "patient has conflicting reference samples")
        part = tuple(row[field] for field in (
            "PART_NUMBER", "PART_DESIGNATOR", "PART_TYPE",
            "PART_DESCRIPTION", "SUBSPECIALTY", "PATH_DX_TITLE"))
        if self.parts.setdefault((patient_id, part_key), part) != part:
            _fail(line, "conflicting part metadata")
        block = (row["BLOCK_NUMBER"], row["BLOCK_LABEL"])
        if self.blocks.setdefault((patient_id, part_key, block_key), block) != block:
            _fail(line, "conflicting block metadata")
        _require(row, "SPECIMEN_KEY", line)
        metadata = normalize_row(row, line)
        return {
            "patient_id": patient_id,
            "sample_id": sample_id or None,
            "image_id": image_id,
            "match_level": match_level,
            "metadata": metadata,
        }


def parse_slides(rows):
    """Normalize every row (see SlideParser)."""
    parser = SlideParser()
    return [parser.parse(line, row) for line, row in rows]


class SlideCounter:
    """Per-entity counts with the semantics of ImportWsiData.insertSampleSlideCounts.

    One count per IMAGE_ID. Sample counts cover matched slides only, so samples
    without a matched slide get no row; patient counts include unmatched slides,
    so every patient with a slide gets a row. Part/block counts follow
    MATCH_LEVEL and zeros are written for entities that have a row. Slides that
    cannot serve tiles are counted.
    """

    def __init__(self):
        self.by_sample = {}
        self.by_patient = {}

    def add(self, slide):
        targets = [self.by_patient.setdefault(slide["patient_id"], [0, 0, 0])]
        if slide["sample_id"] is not None:
            targets.append(self.by_sample.setdefault((slide["patient_id"], slide["sample_id"]), [0, 0, 0]))
        for counts in targets:
            counts[0] += 1
            if slide["match_level"] == "PART":
                counts[1] += 1
            elif slide["match_level"] == "BLOCK":
                counts[2] += 1


def count_slides(slides):
    """Return (by_sample, by_patient) counts for a list of parsed slides (see SlideCounter)."""
    counter = SlideCounter()
    for slide in slides:
        counter.add(slide)
    return counter.by_sample, counter.by_patient


def timeline_timing(start_days, timepoint_source, day_zero_mismatch="error"):
    """Return the format-v3 timing columns for a slide listed in a pathology timeline event.

    The event's ``TIMEPOINT_SOURCE`` names the day zero of its ``START_DATE``.
    When it is first tumor sequencing (the contract's coordinate system, e.g.
    "Recorded procedure date relative to first tumor sequencing"), the slide is
    dated: ``AVAILABLE``, ``START_DATE`` as ``TIMELINE_START_DAYS``, kind
    ``ESTIMATED`` when the text says the date is estimated and ``RECORDED``
    otherwise, date source ``PATHOLOGY_TIMELINE`` and the event's text as
    ``TIMEPOINT_SOURCE``.

    Any other day zero (e.g. "Procedure date relative to first ICD-O
    diagnosis") cannot be expressed in the contract's coordinate system, so the
    offset is not copied. Returns None when ``day_zero_mismatch`` is "error";
    with "undated" the slide keeps its procedure-date provenance without an
    offset: ``MISSING_REFERENCE_SEQUENCING_DATE`` with reason
    ``NO_SEQUENCING_REFERENCE``.
    """
    kind = "ESTIMATED" if ESTIMATED_DATE.search(timepoint_source) else "RECORDED"
    timing = {
        "TIMELINE_DATE_KIND": kind,
        "TIMELINE_DATE_SOURCE": TIMELINE_DATE_SOURCE,
        "TIMELINE_COORDINATE_SYSTEM": TIMELINE_COORDINATE_SYSTEM,
        "TIMEPOINT_SOURCE": timepoint_source,
    }
    if SEQUENCING_DAY_ZERO.search(timepoint_source):
        timing.update(TIMELINE_START_DAYS=str(start_days), TIMELINE_DATE_STATUS="AVAILABLE",
                      TIMELINE_DATE_REASON="")
        return timing
    if day_zero_mismatch != "undated":
        return None
    timing.update(TIMELINE_START_DAYS="", TIMELINE_DATE_STATUS="MISSING_REFERENCE_SEQUENCING_DATE",
                  TIMELINE_DATE_REASON=NO_REFERENCE_REASON)
    return timing


def _parse_image_ids(value, where):
    """Parse an IMAGE_IDS cell: a JSON array of IDs, optionally CSV-quoted as a whole."""
    text = value.strip()
    if not text:
        return []
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        text = text[1:-1].replace('""', '"')
    try:
        ids = json.loads(text)
    except ValueError:
        ids = None
    if not isinstance(ids, list) or not all(
            isinstance(image, str) or (isinstance(image, int) and not isinstance(image, bool))
            for image in ids):
        raise ConversionError(f"{where}: IMAGE_IDS must be a JSON array of image IDs: {value!r}")
    return [str(image).strip() for image in ids if str(image).strip()]


def read_timeline_index(path, day_zero_mismatch="error"):
    """Index a pathology timeline file by image.

    Maps IMAGE_ID -> (patient, timing columns, line, (START_DATE, TIMEPOINT_SOURCE)).

    Every event listing an image must agree on its patient, START_DATE and
    TIMEPOINT_SOURCE. Events are counted per TIMEPOINT_SOURCE whose day zero is
    not first tumor sequencing; with ``day_zero_mismatch`` "error" any such
    event fails the conversion. The file is only read.
    """
    path = Path(path)
    lines = _iter_lines(path, "timeline file")
    header = None
    for _, line in lines:
        if not line.startswith("#") and line.strip():
            header = [column.strip() for column in line.split("\t")]
            break
    if header is None:
        raise ConversionError(f"{path}: timeline file has no header")
    missing = [column for column in TIMELINE_REQUIRED_COLUMNS if column not in header]
    if missing:
        raise ConversionError(f"{path}: timeline file is missing column {', '.join(missing)}")
    patient_index, start_index, source_index, ids_index = (
        header.index(column) for column in TIMELINE_REQUIRED_COLUMNS)
    index = {}
    timings = {}  # (start, source) -> shared timing dict
    mismatched = {}  # TIMEPOINT_SOURCE -> [events, images]
    for line_number, line in lines:
        if not line.strip() or line.startswith("#"):
            continue
        where = f"{path}: line {line_number}"
        fields = line.split("\t")
        if len(fields) != len(header):
            raise ConversionError(f"{where}: expected {len(header)} columns, found {len(fields)}")
        image_ids = _parse_image_ids(fields[ids_index], where)
        if not image_ids:
            continue
        patient_id = fields[patient_index].strip()
        if not patient_id:
            raise ConversionError(f"{where}: PATIENT_ID is required")
        try:
            start_days = int(fields[start_index].strip())
        except ValueError:
            raise ConversionError(f"{where}: START_DATE must be an integer for an event with IMAGE_IDS: "
                                  f"{fields[start_index]!r}") from None
        source = fields[source_index].strip()
        key = (start_days, source)
        if key not in timings:
            timings[key] = timeline_timing(start_days, source, day_zero_mismatch)
        timing = timings[key]
        if timing is None or timing["TIMELINE_DATE_STATUS"] != "AVAILABLE":
            counts = mismatched.setdefault(source, [0, 0])
            counts[0] += 1
            counts[1] += len(image_ids)
        for image_id in image_ids:
            entry = (patient_id, timing, line_number, key)
            previous = index.setdefault(image_id, entry)
            if previous is entry or (previous[0], previous[3]) == (patient_id, key):
                continue
            first_line = previous[2]
            if previous[0] != patient_id:
                raise ConversionError(
                    f"{where}: IMAGE_ID {image_id} is listed for patient {patient_id} here but for "
                    f"{previous[0]} on line {first_line}")
            raise ConversionError(
                f"{where}: IMAGE_ID {image_id} is listed in events with conflicting timing "
                f"(line {first_line} and here); an image can have only one procedure date")
    if mismatched and day_zero_mismatch != "undated":
        summary = "; ".join(f"{events} event(s) with {images} image(s): {source or '(blank)'!r}"
                            for source, (events, images) in sorted(mismatched.items()))
        raise ConversionError(
            f"{path}: TIMEPOINT_SOURCE does not place day zero at first tumor sequencing, the "
            f"only coordinate system the WSI timing contract supports, so START_DATE cannot be "
            f"used as TIMELINE_START_DAYS ({summary}). Re-export the timeline relative to first "
            f"tumor sequencing, or pass --day-zero-mismatch undated to keep these slides without "
            f"an offset (MISSING_REFERENCE_SEQUENCING_DATE)")
    return index


def find_pathology_timeline(study_dir):
    """Return the data file of the study's pathology timeline, or None.

    That is the ``TIMELINE`` clinical file whose header has ``IMAGE_IDS``; more
    than one is ambiguous.
    """
    study_dir = Path(study_dir)
    found = []
    for meta_path in sorted(study_dir.iterdir()):
        if not meta_path.is_file() or "meta" not in meta_path.name.lower():
            continue
        try:
            meta = read_meta(meta_path)
        except ConversionError:
            continue
        if (meta.get("genetic_alteration_type") != "CLINICAL" or meta.get("datatype") != "TIMELINE"
                or not meta.get("data_filename")):
            continue
        data_path = study_dir / meta["data_filename"]
        if "IMAGE_IDS" in _data_header(data_path):
            found.append((meta_path, data_path))
    if len(found) > 1:
        raise ConversionError(
            "the study directory has more than one pathology timeline file (TIMELINE with an "
            f"IMAGE_IDS column: {', '.join(meta.name for meta, _ in found)}); choose one with "
            "--timeline-file")
    return found[0][1] if found else None


def _check_cell(value, file_name):
    text = "" if value is None else str(value)
    if any(character in text for character in "\t\n\r"):
        raise ConversionError(
            f"{file_name}: value contains a tab or line break and cannot be written as TSV: {text!r}")
    return text


def render_tsv(file_name, rows):
    """Render raw tab-separated rows; no quoting, one physical line per record."""
    return "\n".join("\t".join(_check_cell(value, file_name) for value in row) for row in rows) + "\n"


def write_tsv(path, rows):
    path.write_text(render_tsv(path.name, rows), encoding="utf-8")


def render_meta(entries):
    return "".join(f"{key}: {value}\n" for key, value in entries)


def _clinical_header_rows(identifier_columns, attributes):
    names = [name for name, _, _ in identifier_columns] + [attribute[1] for attribute in attributes]
    descriptions = [desc for _, desc, _ in identifier_columns] + [attribute[2] for attribute in attributes]
    datatypes = ["STRING"] * len(identifier_columns) + ["NUMBER"] * len(attributes)
    priorities = ["1"] * (len(identifier_columns) + len(attributes))
    columns = [column for _, _, column in identifier_columns] + [attribute[0] for attribute in attributes]
    return [
        ["#" + names[0]] + names[1:],
        ["#" + descriptions[0]] + descriptions[1:],
        ["#" + datatypes[0]] + datatypes[1:],
        ["#" + priorities[0]] + priorities[1:],
        columns,
    ]


def _data_header(path):
    """Return the column header (first non-comment line) of a tab-delimited file."""
    try:
        with open(path, encoding="utf-8", errors="replace") as stream:
            for line in stream:
                if line.startswith("#") or not line.strip():
                    continue
                return [column.strip() for column in line.rstrip("\r\n").split("\t")]
    except OSError:
        return []
    return []


def _find_clinical_meta(study_dir, datatype):
    """Return (meta path, data path) of the study's clinical file of ``datatype``, or None."""
    found = []
    for meta_path in sorted(Path(study_dir).iterdir()):
        if not meta_path.is_file() or "meta" not in meta_path.name.lower():
            continue
        try:
            meta = read_meta(meta_path)
        except ConversionError:
            continue
        if meta.get("genetic_alteration_type") == "CLINICAL" and meta.get("datatype") == datatype:
            found.append((meta_path, meta))
    if len(found) > 1:
        raise ConversionError(
            f"the study directory has more than one {datatype} clinical meta file "
            f"({', '.join(path.name for path, _ in found)}); a study may contain only one")
    if not found:
        return None
    meta_path, meta = found[0]
    if not meta.get("data_filename"):
        raise ConversionError(f"{meta_path.name}: data_filename is required")
    return meta_path, Path(study_dir) / meta["data_filename"]


def check_study_conflicts(study_dir):
    """Refuse to emit files that would duplicate definitions already in the study."""
    study_dir = Path(study_dir)
    problems = []
    clinical_files = set(study_dir.glob("data_clinical*"))
    resource_meta = []
    for meta_path in sorted(study_dir.iterdir()):
        if not meta_path.is_file() or "meta" not in meta_path.name.lower():
            continue
        try:
            meta = read_meta(meta_path)
        except ConversionError:
            continue
        if meta.get("genetic_alteration_type") == "CLINICAL" and meta.get("data_filename"):
            clinical_files.add(study_dir / meta["data_filename"])
        if meta.get("resource_type") in ("DEFINITION", "SAMPLE", "PATIENT"):
            resource_meta.append((meta_path.name, meta["resource_type"]))
    for path in sorted(clinical_files):
        duplicated = sorted(COUNT_ATTRIBUTE_IDS.intersection(_data_header(path)))
        if duplicated:
            problems.append(f"{path.name} already defines {', '.join(duplicated)}; remove those "
                            f"columns first (the converted counts replace them)")
    for name, resource_type in resource_meta:
        problems.append(
            f"{name} already provides a {resource_type} resource file; a study may contain only one, "
            f"so merge the converted rows into it instead")
    if problems:
        raise ConversionError(
            "the study directory already contains data the converter would duplicate:\n  - "
            + "\n  - ".join(problems))


def _split_lines(text):
    """Split text into (content, line ending) pairs, keeping endings byte for byte."""
    lines = []
    for chunk in re.findall(r"[^\n]*\n|[^\n]+$", text):
        if chunk.endswith("\r\n"):
            lines.append((chunk[:-2], "\r\n"))
        elif chunk.endswith("\n"):
            lines.append((chunk[:-1], "\n"))
        else:
            lines.append((chunk, ""))
    return lines


def merge_clinical_counts(data_path, attributes, key_columns, counts):
    """Return the clinical file text with the count attributes appended.

    ``key_columns`` names the identifier columns that key ``counts`` (a dict from
    identifier tuples to three counts). Rows of entities without slides get NA,
    as the native importer wrote no value for them. Every existing line, value and
    line ending is kept; comment and blank lines after the header are unchanged.
    """
    name = data_path.name
    try:
        with open(data_path, encoding="utf-8", newline="") as stream:
            text = stream.read()
    except (OSError, UnicodeDecodeError) as error:
        raise ConversionError(f"{name}: cannot read clinical file: {error}") from error
    lines = _split_lines(text)
    preamble = 0
    while preamble < len(lines) and lines[preamble][0].startswith("#"):
        preamble += 1
    if preamble != 4:
        raise ConversionError(
            f"{name}: expected the four '#' attribute header rows before the column header, "
            f"found {preamble}; add them so the WSI count attributes can be defined")
    if preamble >= len(lines) or not lines[preamble][0].strip():
        raise ConversionError(f"{name}: column header is missing")
    header = [column.strip() for column in lines[preamble][0].split("\t")]
    missing_columns = [column for column in key_columns if column not in header]
    if missing_columns:
        raise ConversionError(f"{name}: missing column {', '.join(missing_columns)}")
    key_index = [header.index(column) for column in key_columns]
    id_index = key_index[-1]
    header_values = [
        [attribute[1] for attribute in attributes],
        [attribute[2] for attribute in attributes],
        ["NUMBER"] * len(attributes),
        ["1"] * len(attributes),
    ]
    out = []
    for index, values in enumerate(header_values):
        content, ending = lines[index]
        out.append(content + "\t" + "\t".join(values) + ending)
    content, ending = lines[preamble]
    out.append(content + "\t" + "\t".join(attribute[0] for attribute in attributes) + ending)

    counts_by_id = {key[-1]: (key, value) for key, value in counts.items()}
    seen = {}
    for line_number, (content, ending) in enumerate(lines[preamble + 1:], start=preamble + 2):
        if content.startswith("#") or not content.strip():
            out.append(content + ending)
            continue
        fields = content.split("\t")
        if len(fields) != len(header):
            raise ConversionError(f"{name}: line {line_number}: expected {len(header)} columns, "
                                  f"found {len(fields)}")
        identifier = fields[id_index].strip()
        match = counts_by_id.get(identifier)
        if match is None:
            values = ["NA"] * len(attributes)
        else:
            key, value = match
            row_key = tuple(fields[index].strip() for index in key_index)
            if row_key != key:
                raise ConversionError(
                    f"{name}: line {line_number}: {key_columns[-1]} {identifier} belongs to "
                    f"{row_key[0]} here but to {key[0]} in the WSI file")
            values = [str(count) for count in value]
            seen[identifier] = True
        out.append(content + "\t" + "\t".join(values) + ending)
    absent = [identifier for identifier in counts_by_id if identifier not in seen]
    if absent:
        raise ConversionError(
            f"{name}: {key_columns[-1]} with WSI slides not found in the clinical file: "
            + ", ".join(absent))
    return "".join(out)


class _TsvWriter:
    """Write raw TSV rows to a file opened on the first row (see render_tsv)."""

    def __init__(self, path, header):
        self.path = path
        self.header = header
        self.stream = None
        self.rows = 0

    def write(self, row):
        if self.stream is None:
            self.stream = open(self.path, "w", encoding="utf-8", newline="")
            self._write(self.header)
        self._write(row)
        self.rows += 1

    def _write(self, row):
        self.stream.write("\t".join(_check_cell(value, self.path.name) for value in row) + "\n")

    def close(self):
        if self.stream is not None:
            self.stream.close()


def _print_warning(message):
    print(f"convertWsiToResources.py: warning: {message}", file=sys.stderr)


def convert(meta_wsi, output_dir, portal_base_url, study_dir=None, timeline_file=None,
            day_zero_mismatch="error", warn=_print_warning):
    """Convert a legacy WSI pair; return the list of files written.

    Without ``study_dir`` the slide counts are written as standalone clinical file
    pairs. With it, the study's clinical sample and patient files are copied to
    ``output_dir`` under their own names with the count columns appended.

    Format v2 input takes its timing from ``timeline_file`` or, when that is
    omitted, from the pathology timeline found in ``study_dir``; see
    ``timeline_timing`` for the mapping and ``day_zero_mismatch``. Slides in no
    timeline event are undated. ``warn`` receives non-fatal messages.

    Nothing is written to ``output_dir`` unless the whole conversion succeeds.
    """
    meta_wsi = Path(meta_wsi)
    output_dir = Path(output_dir)
    base_url = normalize_base_url(portal_base_url)
    if day_zero_mismatch not in DAY_ZERO_MISMATCH_POLICIES:
        raise ConversionError(f"--day-zero-mismatch must be one of {', '.join(DAY_ZERO_MISMATCH_POLICIES)}")
    meta = read_wsi_meta(meta_wsi)
    format_version = meta["format_version"]
    study_id = meta["cancer_study_identifier"]
    data_path = meta_wsi.parent / meta["data_filename"]
    if study_dir is not None:
        study_dir = Path(study_dir)
        if not study_dir.is_dir():
            raise ConversionError(f"--study-dir is not a directory: {study_dir}")
        if output_dir.resolve() == study_dir.resolve():
            raise ConversionError("--output-dir must differ from --study-dir; the merged clinical "
                                  "files are written under the study's own file names")
        check_study_conflicts(study_dir)

    timeline_index = None
    if format_version == "3":
        if timeline_file is not None:
            raise ConversionError(
                "--timeline-file is only used for format_version 2; format v3 rows carry their own timing")
    else:
        if timeline_file is None and study_dir is not None:
            timeline_file = find_pathology_timeline(study_dir)
        if timeline_file is None:
            raise ConversionError(
                f"{meta_wsi}: format_version 2 has no slide timing; pass --timeline-file with the "
                f"study's pathology timeline file (the TIMELINE file with IMAGE_IDS), or --study-dir "
                f"to find it through its meta file")
        timeline_index = read_timeline_index(timeline_file, day_zero_mismatch)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", suffix=".partial",
                                    dir=output_dir.parent))
    try:
        files = _convert_into(staging, data_path, format_version, timeline_index, timeline_file,
                              study_id, base_url, study_dir, warn)
        output_dir.mkdir(parents=True, exist_ok=True)
        written = []
        for name in files:
            os.replace(staging / name, output_dir / name)
            written.append(output_dir / name)
        return written
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _convert_into(staging, data_path, format_version, timeline_index, timeline_file, study_id,
                  base_url, study_dir, warn):
    """Write every output file into ``staging``; return their names in output order."""
    files = []

    def write_text(name, text):
        with open(staging / name, "w", encoding="utf-8", newline="") as stream:
            stream.write(text)
        files.append(name)

    def meta_entries(entries, data_file):
        return render_meta(entries + [("data_filename", data_file)])

    parser = SlideParser()
    counter = SlideCounter()
    sample_writer = _TsvWriter(staging / SAMPLE_RESOURCE_FILE, RESOURCE_HEADER_SAMPLE)
    patient_writer = _TsvWriter(staging / PATIENT_RESOURCE_FILE, RESOURCE_HEADER_PATIENT)
    try:
        try:
            for line, row in iter_rows(data_path, FORMAT_COLUMNS[format_version]):
                if timeline_index is not None:
                    event = timeline_index.pop(row["IMAGE_ID"], None)
                    if event is None:
                        row.update(UNDATED_TIMING)
                    else:
                        if event[0] != row["PATIENT_ID"]:
                            _fail(line, f"IMAGE_ID {row['IMAGE_ID']} belongs to patient {event[0]} in "
                                        f"{timeline_file} (line {event[2]}) but to {row['PATIENT_ID']} here")
                        row.update(event[1])
                slide = parser.parse(line, row)
                counter.add(slide)
                url = viewer_url(base_url, study_id, slide["patient_id"], slide["image_id"])
                metadata = json.dumps(slide["metadata"], separators=(",", ":"), sort_keys=True,
                                      ensure_ascii=True)
                if slide["sample_id"] is not None:
                    sample_writer.write([slide["patient_id"], slide["sample_id"], SAMPLE_RESOURCE_ID,
                                         url, slide["image_id"], RESOURCE_TYPE, metadata])
                else:
                    patient_writer.write([slide["patient_id"], PATIENT_RESOURCE_ID, url,
                                          slide["image_id"], RESOURCE_TYPE, metadata])
        except ConversionError as error:
            if str(error).startswith(str(data_path)):
                raise
            raise ConversionError(f"{data_path}: {error}") from error
    finally:
        sample_writer.close()
        patient_writer.close()
    if timeline_index:
        examples = ", ".join(sorted(timeline_index)[:5])
        warn(f"{timeline_file}: {len(timeline_index)} image(s) listed in timeline events are not in "
             f"{data_path} (e.g. {examples}); the timeline file is imported unchanged")
    by_sample, by_patient = counter.by_sample, counter.by_patient

    definitions = []
    if sample_writer.rows:
        definitions.append([SAMPLE_RESOURCE_ID, "Pathology slides",
                            "Whole-slide images matched to a sample", "SAMPLE", "FALSE", "1"])
    if patient_writer.rows:
        definitions.append([PATIENT_RESOURCE_ID, "Pathology slides",
                            "Whole-slide images not matched to a sample", "PATIENT", "FALSE", "1"])
    write_text("meta_resource_definition.txt",
               meta_entries([("cancer_study_identifier", study_id), ("resource_type", "DEFINITION")],
                            DEFINITION_FILE))
    write_text(DEFINITION_FILE, render_tsv(DEFINITION_FILE, [
        ["RESOURCE_ID", "DISPLAY_NAME", "DESCRIPTION", "RESOURCE_TYPE", "OPEN_BY_DEFAULT",
         "PRIORITY"]] + definitions))
    for writer, meta_file, resource_type in ((sample_writer, "meta_resource_sample.txt", "SAMPLE"),
                                             (patient_writer, "meta_resource_patient.txt", "PATIENT")):
        if writer.rows:
            write_text(meta_file, meta_entries(
                [("cancer_study_identifier", study_id), ("resource_type", resource_type)],
                writer.path.name))
            files.append(writer.path.name)

    def add_pair(data_file, rows, meta_file, entries):
        write_text(meta_file, meta_entries(entries, data_file))
        write_text(data_file, render_tsv(data_file, rows))

    if study_dir is None:
        if by_sample:
            add_pair(SAMPLE_COUNTS_FILE,
                     _clinical_header_rows(
                         [("Patient Identifier", "Patient identifier", "PATIENT_ID"),
                          ("Sample Identifier", "Sample identifier", "SAMPLE_ID")],
                         SAMPLE_COUNT_ATTRIBUTES)
                     + [[patient, sample] + [str(count) for count in counts]
                        for (patient, sample), counts in by_sample.items()],
                     "meta_clinical_sample_wsi_counts.txt",
                     [("cancer_study_identifier", study_id),
                      ("genetic_alteration_type", "CLINICAL"),
                      ("datatype", "SAMPLE_ATTRIBUTES")])
        add_pair(PATIENT_COUNTS_FILE,
                 _clinical_header_rows(
                     [("Patient Identifier", "Patient identifier", "PATIENT_ID")],
                     PATIENT_COUNT_ATTRIBUTES)
                 + [[patient] + [str(count) for count in counts]
                    for patient, counts in by_patient.items()],
                 "meta_clinical_patient_wsi_counts.txt",
                 [("cancer_study_identifier", study_id),
                  ("genetic_alteration_type", "CLINICAL"),
                  ("datatype", "PATIENT_ATTRIBUTES")])
        return files

    merges = (
        ("SAMPLE_ATTRIBUTES", by_sample, ("PATIENT_ID", "SAMPLE_ID"), SAMPLE_COUNT_ATTRIBUTES),
        ("PATIENT_ATTRIBUTES", {(patient,): counts for patient, counts in by_patient.items()},
         ("PATIENT_ID",), PATIENT_COUNT_ATTRIBUTES),
    )
    for datatype, counts, key_columns, attributes in merges:
        clinical = _find_clinical_meta(study_dir, datatype)
        if clinical is None:
            if counts:
                raise ConversionError(
                    f"the study directory has no {datatype} clinical file to merge the WSI "
                    f"slide counts into; add one that lists "
                    f"{'the samples' if datatype == 'SAMPLE_ATTRIBUTES' else 'the patients'} "
                    f"with slides")
            continue
        meta_path, clinical_data = clinical
        if clinical_data.name in files or meta_path.name in files:
            raise ConversionError(
                f"{meta_path.name}: its file names collide with a converted resource file")
        (staging / meta_path.name).write_bytes(meta_path.read_bytes())
        files.append(meta_path.name)
        write_text(clinical_data.name, merge_clinical_counts(
            clinical_data, attributes, key_columns, counts))
    return files


def interface(args=None):
    parser = argparse.ArgumentParser(
        description="Convert a legacy meta_wsi/data_wsi pair (format_version 2 or 3) into standard "
                    "resource and clinical slide-count files. Runs offline.")
    parser.add_argument("--meta-wsi", type=Path, required=True,
                        help="path to the legacy meta_wsi.txt (its data_filename is read next to it)")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="directory to write the converted meta/data files to")
    parser.add_argument("--portal-base-url", required=True,
                        help="absolute http(s) URL of the portal, including any context path "
                             "(e.g. https://example.org/cbioportal); used for viewer links")
    parser.add_argument("--study-dir", type=Path,
                        help="study directory the output joins: its clinical sample/patient files "
                             "are copied to --output-dir with the WSI count columns appended, and "
                             "it is checked for files the output would duplicate; for format v2 "
                             "its pathology timeline is used unless --timeline-file is given")
    parser.add_argument("--timeline-file", type=Path,
                        help="format v2 only: the pathology timeline data file (a TIMELINE clinical "
                             "file with an IMAGE_IDS column) to take slide timing from; it is only read")
    parser.add_argument("--day-zero-mismatch", choices=DAY_ZERO_MISMATCH_POLICIES, default="error",
                        help="format v2 only: what to do with timeline events whose TIMEPOINT_SOURCE "
                             "does not place day zero at first tumor sequencing: fail (default), or "
                             "keep their slides without an offset as MISSING_REFERENCE_SEQUENCING_DATE")
    return parser.parse_args(args)


def main(args=None):
    parsed = interface(args)
    try:
        written = convert(parsed.meta_wsi, parsed.output_dir, parsed.portal_base_url, parsed.study_dir,
                          parsed.timeline_file, parsed.day_zero_mismatch)
    except ConversionError as error:
        print(f"convertWsiToResources.py: error: {error}", file=sys.stderr)
        return 1
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
