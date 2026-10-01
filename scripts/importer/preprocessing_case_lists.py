"""Read-only case-list analysis derived from curation-tools PR #76.

Source: d8526de87d4e38e0badf9666cf8e75477aecd87f,
jar-case-list-generator/generate_case_lists_jar.py (AGPL-3.0).
Keep these parsing rules aligned with that generator.
Case membership rules match cmo-pipelines PR #1394; output ordering is irrelevant here.
"""
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
    members = set()
    id_column = None
    with open(path) as stream:
        for raw in stream:
            line = raw.rstrip("\r\n")
            if line.startswith('#'):
                if line.startswith('#sequenced_samples:'):
                    return set(line.split(':', 1)[1].strip().split())
                continue
            row = line.split('\t')
            if id_column is None:
                sample_headers = [column for column in SAMPLE_ID_COLUMN_HEADERS if column in row]
                if not sample_headers:
                    return {token for token in row if token.upper() not in NON_CASE_IDS}
                id_column = row.index(sample_headers[0])
            elif line.strip():
                members.add(row[id_column])
    return members


def missing_generated_case_lists(study_dir, study_id, defined_ids, config_path=None):
    """Yield lists that gap-fill preprocessing would create; never change inputs.

    Existing stable IDs (including virtual _all) count regardless of filename.
    A study-local curated list with the same non-generic category also counts;
    ordinary metadata/sample validation remains responsible for its validity.
    Curated membership is not reconstructed from event-only mutation files.
    """
    config_path = config_path or Path(__file__).with_name('case_list_config.tsv')
    cache = {}
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
                cache[filename] = set(map(get_sample_id,
                                          case_list_from_staging_file(study_dir, filename)))
            found = cache[filename]
            if intersection:
                members = found.copy() if members is None else members & found
            else:
                members |= found
        if members:
            reported.add(stable_id)
            yield stable_id, spec['case_list_filename'], len(members)
