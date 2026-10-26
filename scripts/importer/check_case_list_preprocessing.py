"""Read-only checks for missing generated case lists.

Checks:
- Determine which configured lists have samples, using the staging-file
  unions/intersections and TCGA sample-ID normalization rules.
- Use sequenced_samples.txt or #sequenced_samples when supplied, so samples
  without mutation records can still count as sequenced.
- Report a non-empty expected list when neither its stable ID nor an equivalent
  curated category is already defined. Generic 'other' categories do not count
  as equivalent; a virtual _all stable ID does count as defined.
- Accept a curated category only from a defined list belonging to this study
  with non-empty case_list_ids.

Study files are never changed. Existing curated membership is not rebuilt.

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
    """Convert a TCGA barcode to the sample ID used by the Java importer.

    Change Tumor/Normal to 01/11, add -01 to patient-only barcodes, and shorten
    longer barcodes to the sample portion. Leave other IDs unchanged.
    """
    if not barcode.startswith("TCGA"):
        return barcode
    if "Tumor" in barcode:
        cleaned = barcode.replace("Tumor", "01")
    elif "Normal" in barcode:
        cleaned = barcode.replace("Normal", "11")
    else:
        cleaned = barcode
    parts = cleaned.split("-")
    if len(parts) < 4:
        return barcode + "-01"
    sample_id = "-".join(parts[:4])
    m = TCGA_SAMPLE_BARCODE_REGEX.match(sample_id)
    return m.group(1) if m else sample_id

def read_config(path):
    """Read the filenames, stable IDs and categories from the case-list config."""
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
    """Find the data file named in the config, or return None if it is missing.

    Prefer an exact filename match. Otherwise allow a difference in letter case,
    such as data_CNA.txt versus data_cna.txt.
    """
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
    """Read the sample IDs that the generator would use from this data file.

    For mutation files, use sequenced_samples.txt when it exists. Otherwise read
    the file's sample column or header. Missing files contribute no samples;
    malformed rows raise an error.
    """
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
    """Collect sample IDs while the validator checks the file's text encoding.

    Keep empty cells when reading rows. Save parsing errors for the caller to
    report so the rest of validation can finish.
    """
    def __init__(self, filename):
        self.filename = filename
        self.members = set()
        self.active = True
        self.error = None
        self.id_column = None

    def feed(self, raw):
        """Read one line and add its sample IDs to the collected set.

        A #sequenced_samples comment or matrix header gives us the full list,
        so we can stop collecting. For files with a sample column, read each
        data row instead. Remember malformed rows for the caller to report.
        """
        if not self.active:
            return
        line = raw.rstrip("\r\n")
        prefix = "#sequenced_samples:"
        if line.startswith('#'):
            if line.startswith(prefix):
                self.members = set(line[len(prefix):].strip().split())
                self.active = False
            return
        if self.id_column is None:
            row = line.split('\t')
            for column in SAMPLE_ID_COLUMN_HEADERS:
                if column in row:
                    self.id_column = row.index(column)
                    return
            # Matrix sample IDs are in the header, so no data rows are needed.
            self.members = {token for token in row if token.upper() not in NON_CASE_IDS}
            self.active = False
            return
        if not line.strip():
            return
        # Read only through the sample column, preserving trailing empty values.
        row = line.split('\t', self.id_column + 1)
        if self.id_column >= len(row):
            self.error = IndexError(f"{self.filename}: data row has no column {self.id_column}: {line[:80]}")
            self.active = False
        else:
            self.members.add(row[self.id_column])


def curated_case_list_categories(study_dir, study_id, defined_ids):
    """Find which sample groups are already covered by this study's case lists.

    Only count lists the validator has recognized, with this study's ID and
    a non-empty sample list. Ignore hidden and backup files.
    """
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
            if (values.get('stable_id') in defined_ids
                    and values.get('stable_id', '').startswith(study_id + '_')
                    and values.get('cancer_study_identifier') == study_id
                    and values.get('case_list_ids', '').strip()):
                categories.add(values.get('case_list_category'))
    return categories


def staging_sample_ids(study_dir, filename, scanned_members):
    """Get sample IDs without reading the same data file again when possible.

    Reuse IDs collected by the validator, unless sequenced_samples.txt supplies
    the mutation sample list. Convert TCGA barcodes before comparing IDs.
    """
    path = resolve_staging_path(study_dir, filename)
    key = str(Path(path).resolve()) if path else None
    sidecar = ("data_mutations" in filename.lower() and
               os.path.exists(os.path.join(study_dir, "sequenced_samples.txt")))
    if key in scanned_members and not sidecar:
        members = scanned_members[key]
    else:
        members = case_list_from_staging_file(study_dir, filename)
    return set(map(get_sample_id, members))


def missing_generated_case_lists(study_dir, study_id, defined_ids, config_path=None, scanned_members=None):
    """Find case lists that the generator would create but the study is missing.

    Skip lists already covered by an existing ID or a curated list of the same
    category. The generic 'other' category cannot stand in for another list.
    Combine samples from the configured files: | means any file, & means every
    file. Report each missing list once, and only when it would contain samples.
    Existing curated lists are never rebuilt or changed.
    """
    config_path = config_path or Path(__file__).with_name('case_list_config.tsv')
    cache = {}
    scanned_members = scanned_members or {}
    reported = set(defined_ids)
    categories = curated_case_list_categories(study_dir, study_id, reported)
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
                cache[filename] = staging_sample_ids(study_dir, filename, scanned_members)
            found = cache[filename]
            if intersection:
                members = found.copy() if members is None else members & found
            else:
                members |= found
        if members:
            reported.add(stable_id)
            yield stable_id, spec['case_list_filename'], len(members)
