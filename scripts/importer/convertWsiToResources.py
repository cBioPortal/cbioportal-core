#!/usr/bin/env python3
"""Convert a legacy WSI file pair (format v4) into standard cBioPortal study files.

The converter is deliberately offline: it never connects to cBioPortal, a
database, or an artifact store. It reads ``meta_wsi.txt``/``data_wsi.txt``,
parses and normalizes the slide rows, and writes:

* ``data_resource_definition.txt`` with the ``WSI_SAMPLE``/``WSI_PATIENT``
  definitions that have rows;
* ``data_resource_sample.txt`` for slides matched to a sample (``PART`` or
  ``BLOCK``) and ``data_resource_patient.txt`` for unmatched slides; each row
  links to the standalone viewer by its opaque ``slide_key`` and carries the
  slide metadata as JSON;
* six ``WSI_*`` slide-count attributes, counting only viewable (``CAN_SERVE_TILES``) slides: with
  ``--study-dir``, merged into copies of the study's clinical sample and
  patient files (same file names); without it, as standalone
  ``data_clinical_sample_wsi_counts.txt``/``data_clinical_patient_wsi_counts.txt``
  pairs for studies without clinical files of their own or for hand merging;

each with its meta file. A generated data/meta pair is only written when it has rows.
Timeline files are not produced: existing clinical timeline files stay in the
study and are imported unchanged.

Only format v4 is accepted: 30 columns ending with the opaque ``SLIDE_KEY``
(32 lowercase hex characters, unique per study) and ``SEALED_SOURCE``. Files
that also carry the seven slide-timing columns before ``SLIDE_KEY`` (37
columns) are accepted, but those columns are ignored: they are neither
validated nor written. Files with an ``IMAGE_ID``, ``SOURCE_URL`` or
``THUMBNAIL_URL`` column (format v3 and older) are rejected.
De-identification (contract wsi-serving-v6, sealed source): the pathology
image ID and the object URIs that embed it never reach the study files. The
upstream pipeline seals them into ``SEALED_SOURCE``, an opaque value only the
tile server can open; it is copied verbatim into the private ``wsi_serving``
metadata of servable slides. The URL and ``DISPLAY_NAME`` carry only the
slide key and non-identifying labels, and ``BARCODE``/``PART_DESIGNATOR``/
``PATH_DX_TITLE`` are not written. The data provider is responsible for
de-identifying the remaining free-text cells. Error messages name columns,
never values.

Rows are streamed, so large studies convert in bounded memory; output is
staged next to ``--output-dir`` and only moved there when the conversion
succeeds. The output is not validated here beyond the row contract checked
while parsing; run ``validateData.py`` on the study after adding the files.
"""

import argparse
import base64
import binascii
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote, urlparse


# The WSI row contract below (resource IDs, columns, slide key, sealed source, slide
# types, stain rule) is also what validateData.py checks WHOLE_SLIDE_IMAGE rows against.
SAMPLE_RESOURCE_ID = "WSI_SAMPLE"
PATIENT_RESOURCE_ID = "WSI_PATIENT"
RESOURCE_TYPE = "WHOLE_SLIDE_IMAGE"

# Slide-timing columns that some files still carry before SLIDE_KEY.
# They are accepted but ignored: neither validated nor written to the metadata.
IGNORED_TIMING_COLUMNS = (
    "TIMELINE_START_DAYS", "TIMELINE_DATE_STATUS", "TIMELINE_DATE_KIND",
    "TIMELINE_DATE_SOURCE", "TIMELINE_DATE_REASON", "TIMELINE_COORDINATE_SYSTEM",
    "TIMEPOINT_SOURCE",
)
# Format version 4 (30 columns).
COLUMNS = [
    "PATIENT_ID", "REFERENCE_SAMPLE_ID", "SAMPLE_ID",
    "PART_KEY", "PART_NUMBER", "PART_DESIGNATOR", "PART_TYPE",
    "PART_DESCRIPTION", "SUBSPECIALTY", "PATH_DX_TITLE", "BLOCK_KEY",
    "BLOCK_NUMBER", "BLOCK_LABEL", "MATCH_LEVEL", "SPECIMEN_KEY",
    "STAIN_NAME", "STAIN_GROUP", "IS_HNE", "IS_IHC", "MAGNIFICATION",
    "FILE_SIZE_BYTES", "BARCODE", "SLIDE_TYPE", "CAN_SERVE_TILES",
    "TILE_METADATA_JSON", "THUMBNAIL_WIDTH",
    "THUMBNAIL_HEIGHT", "THUMBNAIL_CONTENT_TYPE",
    "SLIDE_KEY", "SEALED_SOURCE",
]
# The same with the ignored timing columns before SLIDE_KEY (37 columns).
COLUMNS_WITH_IGNORED_TIMING = COLUMNS[:-2] + list(IGNORED_TIMING_COLUMNS) + COLUMNS[-2:]
# Format-v3 columns that carry the image ID or an object URI embedding it. A header
# with any of them is rejected outright.
REMOVED_COLUMNS = ("IMAGE_ID", "SOURCE_URL", "THUMBNAIL_URL")
FORMAT_VERSION = "4"

# Public metadata keys, emitted in lower case. Values the backend reads with
# JSONExtractString stay strings (e.g. PART_NUMBER, MAGNIFICATION). BARCODE,
# PART_DESIGNATOR and PATH_DX_TITLE are never written; SEALED_SOURCE is private.
PUBLIC_STRING_FIELDS = [
    "SLIDE_KEY", "PART_KEY", "PART_NUMBER", "PART_TYPE",
    "PART_DESCRIPTION", "SUBSPECIALTY", "BLOCK_KEY",
    "BLOCK_NUMBER", "BLOCK_LABEL", "MATCH_LEVEL", "SPECIMEN_KEY", "STAIN_NAME",
    "STAIN_GROUP", "MAGNIFICATION", "SLIDE_TYPE",
]
SERVING_KEY = "wsi_serving"
# The CUSTOM_METADATA contract of both WSI resource definitions: every public metadata key the
# converter writes, so validateData.py's undeclared-key check holds for converted files. The
# private wsi_serving object is not a column and is never declared. Keys that identify a single
# slide or specimen (nearly every row has its own value) and free text are not filterable;
# otherwise the portal's resource table would list every distinct value as a filter option (over
# a million for slide_key on a large study). Keys, types and labels of the stain, magnification,
# part, block and match-level columns follow the backend's study slide table.
CONTRACT_FIELDS = [
    # (key, type, label, filterable, visibleByDefault)
    ("stain_name", "string", "Stain", True, True),
    ("stain_group", "string", "Stain Group", True, True),
    ("magnification", "string", "Magnification", True, True),
    ("part_number", "number", "Part", True, True),
    ("block_number", "number", "Block", True, True),
    ("match_level", "string", "Matched At", True, False),
    ("slide_type", "string", "Slide Type", True, True),
    ("is_hne", None, "H&E", True, False),
    ("is_ihc", None, "IHC", True, False),
    ("can_serve_tiles", None, "Viewable", True, False),
    ("part_type", "string", "Part Type", True, False),
    ("part_description", "string", "Part Description", False, False),
    ("subspecialty", "string", "Subspecialty", True, False),
    ("block_label", "string", "Block Label", False, False),
    ("file_size_bytes", "number", "File Size (bytes)", False, False),
    ("slide_key", "string", "Slide Key", False, False),
    ("part_key", "string", "Part Key", False, False),
    ("block_key", "string", "Block Key", False, False),
    ("specimen_key", "string", "Specimen Key", False, False),
    ("reference_sample_id", "string", "Reference Sample", False, False),
]
CUSTOM_METADATA = json.dumps(
    {"version": 1, "fields": [
        dict({"key": key}, **({"type": kind} if kind else {}),
             label=label, filterable=filterable, visibleByDefault=visible)
        for key, kind, label, filterable, visible in CONTRACT_FIELDS]},
    separators=(",", ":"))

# Opaque per-slide key computed upstream from a salted hash of image_id.
SLIDE_KEY_PATTERN = re.compile(r"[0-9a-f]{32}")
# SEALED_SOURCE is unpadded base64url of nonce(12) || ciphertext || tag(16), so it
# decodes to at least 29 bytes.
SEALED_SOURCE_PATTERN = re.compile(r"[A-Za-z0-9_-]+")
SEALED_SOURCE_MIN_BYTES = 12 + 1 + 16
SEALED_SOURCE_MAX_LENGTH = 4096

MATCH_LEVELS = ("BLOCK", "PART", "UNMATCHED")
SLIDE_TYPES = ("H&E", "IHC", "Other", "Unknown")

# The count attributes. They count only slides the viewer can open.
SAMPLE_COUNT_ATTRIBUTES = [
    ("WSI_SAMPLE_SLIDE_COUNT", "WSI Viewable Slides per Sample",
     "Pathology slides the slide viewer can open, for the sample."),
    ("WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT", "WSI Viewable Slides per Sample, Part-matched",
     "Pathology slides the slide viewer can open, for the sample, matched to a specimen part."),
    ("WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT", "WSI Viewable Slides per Sample, Block-matched",
     "Pathology slides the slide viewer can open, for the sample, matched to a specimen block."),
]
PATIENT_COUNT_ATTRIBUTES = [
    ("WSI_PATIENT_SLIDE_COUNT", "WSI Viewable Slides per Patient",
     "Pathology slides the slide viewer can open, for the patient."),
    ("WSI_PATIENT_PART_MATCHED_SLIDE_COUNT", "WSI Viewable Slides per Patient, Part-matched",
     "Pathology slides the slide viewer can open, for the patient, matched to a specimen part."),
    ("WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT", "WSI Viewable Slides per Patient, Block-matched",
     "Pathology slides the slide viewer can open, for the patient, matched to a specimen block."),
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
    if meta.get("format_version") != FORMAT_VERSION:
        raise ConversionError(
            f"{path}: unsupported WSI format_version; expected {FORMAT_VERSION} (older formats are "
            f"no longer converted: re-export data_wsi.txt as format v4 with SLIDE_KEY and "
            f"SEALED_SOURCE)")
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


def viewer_url(base_url, study_id, patient_id, slide_key):
    """Absolute link to the standalone viewer route for one slide (by its opaque slide key)."""
    return (f"{base_url}/wsi/patient/{quote(patient_id, safe='')}"
            f"?studyId={quote(study_id, safe='')}&slideKey={quote(slide_key, safe='')}")


def display_name(row):
    """Non-identifying slide label, e.g. ``H&E · Specimen 1 / Block 2``; never the image ID."""
    stain = row["STAIN_NAME"] or row["SLIDE_TYPE"] or "Slide"
    location = []
    if row["PART_NUMBER"]:
        location.append(f"Specimen {row['PART_NUMBER']}")
    if row["BLOCK_NUMBER"]:
        location.append(f"Block {row['BLOCK_NUMBER']}")
    return f"{stain} \u00b7 {' / '.join(location)}" if location else stain


def _iter_lines(path, what):
    """Yield (line number, text) for each line of a UTF-8 file, split on LF only.

    Splitting on LF alone keeps a stray CR inside a value in its row, so it is reported instead of silently starting a new row.
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


def iter_rows(data_path):
    """Stream the legacy data file: leading '#' rows, the exact header, then slide rows.

    The header must be ``COLUMNS`` or ``COLUMNS_WITH_IGNORED_TIMING``. Yields
    (line number, row dict with stripped values) and raises if the file has no
    slide rows. Ignored timing columns stay in the row dict; nothing reads them.
    """
    accepted = [COLUMNS, COLUMNS_WITH_IGNORED_TIMING]
    lines = _iter_lines(data_path, "WSI data file")
    header = None
    for line_number, line in lines:
        if not line.startswith("#"):
            header = line
            break
    removed = [column for column in REMOVED_COLUMNS
               if header is not None and column in header.split("\t")]
    if removed:
        raise ConversionError(
            f"{data_path}: WSI data has {', '.join(removed)} column(s) of format v3 or older; "
            f"format v4 replaces them with SEALED_SOURCE: re-export data_wsi.txt")
    if header is None or header.split("\t") not in accepted:
        raise ConversionError(f"{data_path}: WSI data has an invalid header or column order")
    columns = header.split("\t")
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


def sealed_source_valid(value):
    """Whether a non-empty SEALED_SOURCE has the sealed-source shape (contract wsi-serving-v6)."""
    if len(value) > SEALED_SOURCE_MAX_LENGTH or not SEALED_SOURCE_PATTERN.fullmatch(value):
        return False
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError):
        return False
    return len(decoded) >= SEALED_SOURCE_MIN_BYTES


def stain_flags_valid(slide_type, is_hne, is_ihc):
    """Whether IS_HNE/IS_IHC agree with SLIDE_TYPE (at most one is set, and only for its type)."""
    return (not (is_hne and is_ihc)
            and (slide_type != "H&E" or is_hne)
            and (slide_type != "IHC" or is_ihc)
            and (slide_type not in ("Other", "Unknown") or (not is_hne and not is_ihc)))


def normalize_row(row, line):
    """Parse one row and return its metadata object."""
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

    # SEALED_SOURCE is opaque and may only be opened by the tile server; never echo it.
    sealed_source = row["SEALED_SOURCE"]
    if sealed_source and not sealed_source_valid(sealed_source):
        _fail(line, "SEALED_SOURCE must be unpadded base64url of at least "
                    f"{SEALED_SOURCE_MIN_BYTES} bytes and at most {SEALED_SOURCE_MAX_LENGTH} characters")
    if not can_serve:
        if sealed_source:
            _fail(line, "SEALED_SOURCE must be empty when CAN_SERVE_TILES is FALSE")
        # Non-servable rows have no private serving object.
        return metadata
    for field in ("SEALED_SOURCE", "TILE_METADATA_JSON", "THUMBNAIL_CONTENT_TYPE"):
        _require(row, field, line)
    for value in (thumbnail_width, thumbnail_height):
        if value is None or not 1 <= value <= 8192:
            _fail(line, "servable thumbnail dimensions must be between 1 and 8192")
    metadata[SERVING_KEY] = {
        "sealed_source": sealed_source,
        "tile_metadata_json": tile_metadata,
        "thumbnail_width": thumbnail_width,
        "thumbnail_height": thumbnail_height,
        "thumbnail_content_type": row["THUMBNAIL_CONTENT_TYPE"],
    }
    return metadata


class SlideParser:
    """Normalize rows one at a time, applying the cross-row consistency checks."""

    def __init__(self):
        self.slide_keys = set()
        self.patient_references = {}
        self.parts = {}
        self.blocks = {}

    def parse(self, line, row):
        patient_id = _require(row, "PATIENT_ID", line)
        slide_key = _require(row, "SLIDE_KEY", line)
        if not SLIDE_KEY_PATTERN.fullmatch(slide_key):
            _fail(line, "SLIDE_KEY must be 32 lowercase hex characters")
        if slide_key in self.slide_keys:
            _fail(line, "SLIDE_KEY is not unique")
        self.slide_keys.add(slide_key)
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
            "slide_key": slide_key,
            "display_name": display_name(row),
            "match_level": match_level,
            "metadata": metadata,
        }


class SlideCounter:
    """Per-entity counts of the slides the viewer can open.

    Only viewable slides (CAN_SERVE_TILES=TRUE) are counted, one count per
    slide (SLIDE_KEY, unique per study as SlideParser enforces). Sample counts
    cover matched slides only, so samples without a viewable matched slide get
    no row; patient counts include unmatched slides, so every patient with a
    viewable slide gets a row. Part/block counts follow MATCH_LEVEL and zeros
    are written for entities that have a row.

    ``sample_keys`` and ``patient_keys`` record every entity with any slide,
    viewable or not, since the resource rows reference all of them.
    """

    def __init__(self):
        self.by_sample = {}
        self.by_patient = {}
        self.sample_keys = {}
        self.patient_keys = {}

    def add(self, slide):
        sample_key = None
        self.patient_keys.setdefault((slide["patient_id"],), None)
        if slide["sample_id"] is not None:
            sample_key = (slide["patient_id"], slide["sample_id"])
            self.sample_keys.setdefault(sample_key, None)
        if not slide["metadata"]["can_serve_tiles"]:
            return
        targets = [self.by_patient.setdefault(slide["patient_id"], [0, 0, 0])]
        if sample_key is not None:
            targets.append(self.by_sample.setdefault(sample_key, [0, 0, 0]))
        for counts in targets:
            counts[0] += 1
            if slide["match_level"] == "PART":
                counts[1] += 1
            elif slide["match_level"] == "BLOCK":
                counts[2] += 1


def _check_cell(value, file_name):
    text = "" if value is None else str(value)
    if any(character in text for character in "\t\n\r"):
        # The value may be server-side only (e.g. inside wsi_serving); never echo it.
        raise ConversionError(
            f"{file_name}: value contains a tab or line break and cannot be written as TSV")
    return text


def render_tsv(file_name, rows):
    """Render raw tab-separated rows; no quoting, one physical line per record."""
    return "\n".join("\t".join(_check_cell(value, file_name) for value in row) for row in rows) + "\n"


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


def merge_clinical_counts(data_path, attributes, key_columns, counts, slide_keys=None):
    """Return the clinical file text with the count attributes appended.

    ``key_columns`` names the identifier columns that key ``counts`` (a dict from
    identifier tuples to one count per attribute). ``slide_keys`` lists every entity
    with a slide, viewable or not (default: the keys of ``counts``); each must be in
    the clinical file. Rows of entities without a viewable slide get NA (no value). Every existing line, value and
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

    if slide_keys is None:
        slide_keys = counts
    counts_by_id = {key[-1]: (key, counts.get(key)) for key in slide_keys}
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
            values = (["NA"] * len(attributes) if value is None
                      else [str(count) for count in value])
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


def convert(meta_wsi, output_dir, portal_base_url, study_dir=None):
    """Convert a legacy format-v4 WSI pair; return the list of files written.

    Without ``study_dir`` the slide counts are written as standalone clinical file
    pairs. With it, the study's clinical sample and patient files are copied to
    ``output_dir`` under their own names with the count columns appended.

    Nothing is written to ``output_dir`` unless the whole conversion succeeds.
    """
    meta_wsi = Path(meta_wsi)
    output_dir = Path(output_dir)
    base_url = normalize_base_url(portal_base_url)
    meta = read_wsi_meta(meta_wsi)
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

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", suffix=".partial",
                                    dir=output_dir.parent))
    try:
        files = _convert_into(staging, data_path, study_id, base_url, study_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        written = []
        for name in files:
            os.replace(staging / name, output_dir / name)
            written.append(output_dir / name)
        return written
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _convert_into(staging, data_path, study_id, base_url, study_dir):
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
            for line, row in iter_rows(data_path):
                slide = parser.parse(line, row)
                counter.add(slide)
                url = viewer_url(base_url, study_id, slide["patient_id"], slide["slide_key"])
                name = slide["display_name"]
                metadata = json.dumps(slide["metadata"], separators=(",", ":"), sort_keys=True,
                                      ensure_ascii=True)
                if slide["sample_id"] is not None:
                    sample_writer.write([slide["patient_id"], slide["sample_id"], SAMPLE_RESOURCE_ID,
                                         url, name, RESOURCE_TYPE, metadata])
                else:
                    patient_writer.write([slide["patient_id"], PATIENT_RESOURCE_ID, url,
                                          name, RESOURCE_TYPE, metadata])
        except ConversionError as error:
            if str(error).startswith(str(data_path)):
                raise
            raise ConversionError(f"{data_path}: {error}") from error
    finally:
        sample_writer.close()
        patient_writer.close()
    by_sample, by_patient = counter.by_sample, counter.by_patient

    definitions = []
    if sample_writer.rows:
        definitions.append([SAMPLE_RESOURCE_ID, "Pathology slides",
                            "Whole-slide images matched to a sample", "SAMPLE", "FALSE", "1",
                            CUSTOM_METADATA])
    if patient_writer.rows:
        definitions.append([PATIENT_RESOURCE_ID, "Pathology slides",
                            "Whole-slide images not matched to a sample", "PATIENT", "FALSE", "1",
                            CUSTOM_METADATA])
    write_text("meta_resource_definition.txt",
               meta_entries([("cancer_study_identifier", study_id), ("resource_type", "DEFINITION")],
                            DEFINITION_FILE))
    write_text(DEFINITION_FILE, render_tsv(DEFINITION_FILE, [
        ["RESOURCE_ID", "DISPLAY_NAME", "DESCRIPTION", "RESOURCE_TYPE", "OPEN_BY_DEFAULT",
         "PRIORITY", "CUSTOM_METADATA"]] + definitions))
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

    def count_values(counts):
        # NA (no value) for an entity whose slides are all non-viewable: its row
        # still defines the sample or patient its resource rows refer to.
        return ["NA"] * 3 if counts is None else [str(count) for count in counts]

    if study_dir is None:
        if counter.sample_keys:
            add_pair(SAMPLE_COUNTS_FILE,
                     _clinical_header_rows(
                         [("Patient Identifier", "Patient identifier", "PATIENT_ID"),
                          ("Sample Identifier", "Sample identifier", "SAMPLE_ID")],
                         SAMPLE_COUNT_ATTRIBUTES)
                     + [[patient, sample] + count_values(by_sample.get((patient, sample)))
                        for patient, sample in counter.sample_keys],
                     "meta_clinical_sample_wsi_counts.txt",
                     [("cancer_study_identifier", study_id),
                      ("genetic_alteration_type", "CLINICAL"),
                      ("datatype", "SAMPLE_ATTRIBUTES")])
        if counter.patient_keys:
            add_pair(PATIENT_COUNTS_FILE,
                     _clinical_header_rows(
                         [("Patient Identifier", "Patient identifier", "PATIENT_ID")],
                         PATIENT_COUNT_ATTRIBUTES)
                     + [[patient] + count_values(by_patient.get(patient))
                        for (patient,) in counter.patient_keys],
                     "meta_clinical_patient_wsi_counts.txt",
                     [("cancer_study_identifier", study_id),
                      ("genetic_alteration_type", "CLINICAL"),
                      ("datatype", "PATIENT_ATTRIBUTES")])
        return files

    merges = (
        ("SAMPLE_ATTRIBUTES", by_sample, counter.sample_keys, ("PATIENT_ID", "SAMPLE_ID"),
         SAMPLE_COUNT_ATTRIBUTES),
        ("PATIENT_ATTRIBUTES", {(patient,): counts for patient, counts in by_patient.items()},
         counter.patient_keys, ("PATIENT_ID",), PATIENT_COUNT_ATTRIBUTES),
    )
    for datatype, counts, slide_keys, key_columns, attributes in merges:
        clinical = _find_clinical_meta(study_dir, datatype)
        if clinical is None:
            if slide_keys:
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
            clinical_data, attributes, key_columns, counts, slide_keys))
    return files


def interface(args=None):
    parser = argparse.ArgumentParser(
        description="Convert a legacy meta_wsi/data_wsi pair (format_version 4, with SLIDE_KEY and "
                    "SEALED_SOURCE) into "
                    "standard resource and clinical slide-count files. Runs offline.")
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
                             "it is checked for files the output would duplicate")
    return parser.parse_args(args)


def main(args=None):
    parsed = interface(args)
    try:
        written = convert(parsed.meta_wsi, parsed.output_dir, parsed.portal_base_url, parsed.study_dir)
    except ConversionError as error:
        print(f"convertWsiToResources.py: error: {error}", file=sys.stderr)
        return 1
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
