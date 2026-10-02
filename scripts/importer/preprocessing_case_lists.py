"""Read-only case-list analysis derived from curation-tools PR #76.

Source: d8526de87d4e38e0badf9666cf8e75477aecd87f,
jar-case-list-generator/generate_case_lists_jar.py (AGPL-3.0).
Keep these parsing rules aligned with that generator.
Case membership rules match cmo-pipelines PR #1394; output ordering is irrelevant here.
"""
from functools import lru_cache
import os
import re
from pathlib import Path

NON_CASE_IDS = {"MIRNA", "LOCUS", "ID", "GENE SYMBOL", "ENTREZ_GENE_ID",
                "HUGO_SYMBOL", "LOCUS ID", "CYTOBAND", "COMPOSITE.ELEMENT.REF",
                "HYBRIDIZATION REF"}

SAMPLE_ID_COLUMN_HEADERS = ("Tumor_Sample_Barcode", "SAMPLE_ID", "Sample_ID", "Sample_Id")

TCGA_SAMPLE_BARCODE_REGEX = re.compile(r"^(TCGA-\w\w-\w\w\w\w-\d\d).*$")

def get_sample_id(barcode):
    """StableIdUtil.getSampleId."""
    if not barcode.startswith("TCGA"):
        return barcode
    if "Tumor" in barcode:
        cleaned = barcode.replace("Tumor", "01")
    elif "Normal" in barcode:
        cleaned = barcode.replace("Normal", "11")
    else:
        cleaned = barcode
    parts = cleaned.split("-")
    try:
        sample_id = parts[0] + "-" + parts[1] + "-" + parts[2] + "-" + parts[3]
    except IndexError:
        return barcode + "-01"
    m = TCGA_SAMPLE_BARCODE_REGEX.match(sample_id)
    return m.group(1) if m else sample_id

def read_config(path):
    """Read the four fields used by validation from the generator's TSV."""
    keys = ('case_list_filename', 'staging_filenames', 'meta_stable_id', 'meta_case_list_category')
    with open(path) as stream:
        next(stream, None)
        rows = ([field.strip() for field in line.split("\t")] for line in stream)
        return [dict(zip(keys, fields)) for fields in rows if len(fields) >= 7 and any(fields)]

def case_list_from_sequenced_samples_file(path):
    with open(path) as stream:
        return list(dict.fromkeys(line for line in
                    (raw.rstrip("\r\n") for raw in stream) if line))

def resolve_staging_path(study_dir, staging_filename):
    """Match the configured staging filename case-insensitively against the
    study directory (observed live behavior: a config entry of data_CNA.txt
    matches a study's data_cna.txt). Exact match wins."""
    path = os.path.join(study_dir, staging_filename)
    if os.path.isfile(path):
        return path
    lower = staging_filename.lower()
    try:
        for name in sorted(os.listdir(study_dir)):
            if name.lower() == lower and os.path.isfile(os.path.join(study_dir, name)):
                return os.path.join(study_dir, name)
    except OSError:
        pass
    return None

def case_list_from_staging_file(study_dir, staging_filename):
    """FileUtilsImpl.getCaseListFromStagingFile. Returns [] if file absent."""
    if "data_mutations" in staging_filename.lower():
        seq = os.path.join(study_dir, "sequenced_samples.txt")
        if os.path.exists(seq):
            return case_list_from_sequenced_samples_file(seq)
    path = resolve_staging_path(study_dir, staging_filename)
    if path is None:
        return []
    collector = StagingCaseCollector(staging_filename)
    with open(path) as f:
        for raw in f:
            collector.feed(raw)
            if not collector.active:
                break
    if collector.error:
        raise collector.error
    return collector.members


@lru_cache(maxsize=1)
def configured_staging_filenames():
    return frozenset(name.lower() for spec in read_config(Path(__file__).with_name('case_list_config.tsv'))
                     for name in re.split(r"[|&]", spec['staging_filenames']))


class StagingCaseCollector:
    """Collect generator-compatible IDs while the validator scans UTF-8.

    Raw lines preserve the generator's comment, blank-row and trailing-tab rules.
    Errors are deferred so collection never interrupts normal validation.
    """
    def __init__(self, filename):
        self.filename = filename
        self.members = set()
        self.active = True
        self.error = None
        self.id_column = None

    def feed(self, raw):
        if not self.active:
            return
        line = raw.rstrip("\r\n")
        prefix = "#sequenced_samples:"
        if line.startswith('#'):
            if line.startswith(prefix):
                self.members = set(line[len(prefix):].strip().split())
                self.active = False
            return
        # Split only through the sample column on rows, preserving PR1394's
        # trailing empty values. Blank rows after the header are ignored.
        if self.id_column is None:
            row = line.split('\t')
        else:
            if not line.strip():
                return
            row = line.split('\t', self.id_column + 1)
        if self.id_column is None:
            sample_headers = [column for column in SAMPLE_ID_COLUMN_HEADERS if column in row]
            if sample_headers:
                self.id_column = row.index(sample_headers[0])
            else:
                self.members = {token for token in row if token.upper() not in NON_CASE_IDS}
                self.active = False
        elif self.id_column >= len(row):
            self.error = IndexError(f"{self.filename}: data row has no column {self.id_column}: {line[:80]}")
            self.active = False
        else:
            self.members.add(row[self.id_column])


def missing_generated_case_lists(study_dir, study_id, defined_ids, config_path=None, scanned_members=None):
    """Yield lists that gap-fill preprocessing would create; never change inputs.

    Existing stable IDs (including virtual _all) count regardless of filename.
    A study-local curated list with the same non-generic category also counts;
    ordinary metadata/sample validation remains responsible for its validity.
    Curated membership is not reconstructed from event-only mutation files.
    """
    config_path = config_path or Path(__file__).with_name('case_list_config.tsv')
    cache = {}
    scanned_members = scanned_members or {}
    reported = set(defined_ids)
    categories = set()
    case_dir = Path(study_dir) / "case_lists"
    if case_dir.is_dir():
        for path in case_dir.iterdir():
            if not path.is_file() or path.name.startswith('.') or path.name.endswith('~'):
                continue
            values = {}
            with path.open() as stream:
                for line in stream:
                    if not line.lstrip().startswith('#') and ':' in line:
                        key, value = line.split(':', 1)
                        values[key.strip()] = value.strip()
            if (values.get('stable_id') in reported
                    and values.get('stable_id', '').startswith(study_id + '_')
                    and values.get('cancer_study_identifier') == study_id
                    and values.get('case_list_ids', '').strip()):
                categories.add(values.get('case_list_category'))
    for spec in read_config(config_path):
        stable_id = spec['meta_stable_id'].replace("<CANCER_STUDY>", study_id)
        if stable_id in reported:
            continue
        category = spec['meta_case_list_category']
        if category and category != 'other' and category in categories:
            continue
        patterns = spec['staging_filenames']
        union = '|' in patterns
        intersection = not union and '&' in patterns
        filenames = patterns.split('|' if union else '&')
        if intersection and not all(resolve_staging_path(study_dir, f) for f in filenames):
            continue
        members = None if intersection else set()
        for filename in filenames:
            if filename not in cache:
                path = resolve_staging_path(study_dir, filename)
                # The sidecar overrides mutation rows, including an empty sidecar.
                override = ("data_mutations" in filename.lower() and
                            os.path.exists(os.path.join(study_dir, "sequenced_samples.txt")))
                key = str(Path(path).resolve()) if path else None
                raw_members = (scanned_members[key] if key in scanned_members and not override
                               else case_list_from_staging_file(study_dir, filename))
                cache[filename] = set(map(get_sample_id, raw_members))
            found = cache[filename]
            if intersection:
                members = found.copy() if members is None else members & found
            else:
                members |= found
        if members:
            reported.add(stable_id)
            yield stable_id, spec['case_list_filename'], len(members)
