"""Read-only checks for missing generated case lists.

Checks:
- Determine which configured lists have samples, using the staging-file
  unions/intersections. Require already normalized TCGA sample IDs.
- Use sequenced_samples.txt or #sequenced_samples when supplied, so samples
  without mutation records can still count as sequenced.
- Report a non-empty expected list when its stable ID is not already defined.
  A virtual _all stable ID counts as defined.

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

# Preprocessing must supply sample-level IDs; validation never repairs them.
TCGA_SAMPLE_ID_REGEX = re.compile(r"TCGA-[A-Za-z0-9]{2}-[A-Za-z0-9]{4}-[0-9]{2}")


def validate_sample_ids(members, filename):
    """Reject noncanonical TCGA IDs without merging or changing sample identities."""
    members = set(members)
    invalid = sorted(sample_id for sample_id in members
                     if sample_id.startswith("TCGA") and
                     not TCGA_SAMPLE_ID_REGEX.fullmatch(sample_id))
    if invalid:
        examples = ', '.join(repr(sample_id) for sample_id in invalid[:5])
        raise ValueError(
            f"{filename}: {len(invalid)} unnormalized or malformed TCGA sample ID(s): "
            f"{examples}. Expected TCGA-XX-XXXX-01 format (two-digit sample type). "
            "Normalize IDs consistently across study files before validation; "
            "patient-only IDs need an explicit sample type.")
    return members


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


def staging_sample_ids(study_dir, filename, scanned_members):
    """Get sample IDs without reading the same data file again when possible.

    Reuse IDs collected by the validator, unless sequenced_samples.txt supplies
    the mutation sample list. Require normalized TCGA IDs before comparing them.
    """
    path = resolve_staging_path(study_dir, filename)
    key = str(Path(path).resolve()) if path else None
    sidecar = ("data_mutations" in filename.lower() and
               os.path.exists(os.path.join(study_dir, "sequenced_samples.txt")))
    if key in scanned_members and not sidecar:
        members = scanned_members[key]
    else:
        members = case_list_from_staging_file(study_dir, filename)
    return validate_sample_ids(members, 'sequenced_samples.txt' if sidecar else filename)


def missing_generated_case_lists(study_dir, study_id, defined_ids, config_path=None, scanned_members=None):
    """Find case lists that the generator would create but the study is missing.

    Skip lists already covered by an existing stable ID.
    Combine samples from the configured files: | means any file, & means every
    file. Report each missing list once, and only when it would contain samples.
    Existing curated lists are never rebuilt or changed.
    """
    config_path = config_path or Path(__file__).with_name('case_list_config.tsv')
    cache = {}
    scanned_members = scanned_members or {}
    reported = set(defined_ids)
    specs = read_config(config_path)
    # Validate sources even when their lists already exist or an intersection's
    # other input is absent. Each source is still read at most once.
    for spec in specs:
        for filename in re.split(r"[|&]", spec['staging_filenames']):
            if filename not in cache:
                cache[filename] = staging_sample_ids(study_dir, filename, scanned_members)
    for spec in specs:
        stable_id = spec['meta_stable_id'].replace("<CANCER_STUDY>", study_id)
        if stable_id in reported:
            continue
        patterns = spec['staging_filenames']
        union = '|' in patterns
        intersection = not union and '&' in patterns
        filenames = patterns.split('|' if union else '&')
        if intersection and not all(resolve_staging_path(study_dir, f) for f in filenames):
            continue
        members = None if intersection else set()
        for filename in filenames:
            found = cache[filename]
            if intersection:
                members = found.copy() if members is None else members & found
            else:
                members |= found
        if members:
            reported.add(stable_id)
            yield stable_id, spec['case_list_filename'], len(members)
