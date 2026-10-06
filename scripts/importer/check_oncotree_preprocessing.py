"""Read-only checks for clinical data requiring OncoTree preprocessing.

Checks:
- The reference is a non-empty tumorTypes array with unique, non-empty string
  codes and string/null names and main types (missing labels are allowed).
- Each populated ONCOTREE_CODE exists in the selected reference.
- Each populated CANCER_TYPE matches that code's mainType.
- Each populated CANCER_TYPE_DETAILED matches that code's name.

Blank/NA labels and codes are skipped. Reference failures are reported once;
clinical data is never changed.
"""
import argparse
import hashlib
import json
from pathlib import Path
import requests

UNAVAILABLE_VALUES = {'', 'NA', 'N/A', 'NOT AVAILABLE', '[NOT AVAILABLE]', '[NOT APPLICABLE]'}
ONCOTREE_LABELS = {'CANCER_TYPE': 'mainType', 'CANCER_TYPE_DETAILED': 'name'}


def parse_oncotree_nodes(raw):
    """Read the OncoTree JSON and build a lookup table for its cancer codes.

    Reject empty or malformed data and repeated codes. Turn missing names into
    empty strings so the row checks can handle them the same way.
    """
    records = json.loads(raw)
    if not isinstance(records, list) or not records:
        raise ValueError('expected a non-empty tumorTypes array')
    nodes = {}
    for item in records:
        if not isinstance(item, dict) or not isinstance(item.get('code'), str) or not item['code']:
            raise ValueError('each tumor type requires a code and string/null name and mainType')
        if any(not isinstance(item.get(field), (str, type(None))) for field in ONCOTREE_LABELS.values()):
            raise ValueError('each tumor type requires a code and string/null name and mainType')
        code = item['code']
        if code in nodes:
            raise ValueError('duplicate OncoTree code: ' + code)
        nodes[code] = dict(item, name=item.get('name') or '', mainType=item.get('mainType') or '')
    return nodes


def add_arguments(parser):
    parser.add_argument('--oncotree-cache', help=argparse.SUPPRESS)
    parser.add_argument('--oncotree-file', help='Saved OncoTree tumorTypes JSON for reproducible/offline validation')
    parser.add_argument('--oncotree-version', default='oncotree_latest_stable',
                        help='OncoTree version to fetch when no snapshot is supplied (default: latest stable)')


class OncotreeReference:
    """Load the reference when first needed and reuse the result for later rows."""
    def __init__(self, filename=None, version='oncotree_latest_stable', cache_filename=None):
        self.filename = filename
        self.cache_filename = cache_filename
        self.version = version
        self.nodes = None
        self.error = None
        self.reported_error = False

    def read(self):
        """Read the user's saved file, or reuse the file saved for this batch.

        If neither is available, download the requested OncoTree version.
        Return the JSON together with a description of where it came from.
        """
        if self.filename:
            return Path(self.filename).read_bytes(), str(self.filename)
        source = 'OncoTree version ' + self.version
        if self.cache_filename and Path(self.cache_filename).exists():
            return Path(self.cache_filename).read_bytes(), source + ' (batch snapshot)'
        response = requests.get('https://oncotree.mskcc.org/api/tumorTypes',
                                params={'version': self.version}, timeout=(10, 30))
        response.raise_for_status()
        return response.content, source

    def load(self, logger):
        """Load and check the reference once, then remember the result.

        Save a valid reference for other studies in the batch and log its source
        and checksum. Remember failures too, so each sample does not try again.
        """
        if self.nodes is not None or self.error is not None:
            return self.nodes
        try:
            raw, source = self.read()
            nodes = parse_oncotree_nodes(raw)
            if self.cache_filename and not self.filename:
                Path(self.cache_filename).write_bytes(raw)
            self.nodes = nodes
            logger.info('OncoTree reference: %s; SHA-256 %s', source, hashlib.sha256(raw).hexdigest())
        except (OSError, ValueError, requests.RequestException) as exc:
            self.error = str(exc)
        return self.nodes


def oncotree_findings(row, nodes):
    """Find unknown cancer codes and cancer names that disagree with OncoTree.

    Skip missing values. For each problem, return the column, its current value,
    and the expected value.
    """
    code = row.get('ONCOTREE_CODE', '').strip()
    if code.upper() in UNAVAILABLE_VALUES:
        return
    if code not in nodes:
        yield 'ONCOTREE_CODE', code, 'a code in the selected OncoTree reference; remap retired codes'
        return
    node = nodes[code]
    for column, field in ONCOTREE_LABELS.items():
        expected = node[field] or 'NA'
        actual = row.get(column, '').strip()
        if actual.upper() not in UNAVAILABLE_VALUES and actual != expected:
            yield column, actual, expected


def check_oncotree_row(columns, values, reference, logger, line_number):
    """Check one clinical row and report problems with their line and column.

    Skip rows without an OncoTree column or with the wrong number of cells;
    the normal validator handles those cases. If the reference cannot be read,
    report that failure only once.
    """
    if 'ONCOTREE_CODE' not in columns or len(values) != len(columns):
        return
    nodes = reference.load(logger)
    if nodes is None:
        if not reference.reported_error:
            logger.error('Cannot validate OncoTree preprocessing: %s. Supply a valid '
                         '--oncotree-file snapshot; this check was not completed.', reference.error,
                         extra={'line_number': line_number})
            reference.reported_error = True
        return
    for column, actual, expected in oncotree_findings(dict(zip(columns, values)), nodes):
        extra = {'line_number': line_number}
        if column in columns:
            extra['column_number'] = columns.index(column) + 1
        logger.error('OncoTree preprocessing required: %s is %r; expected %r.',
                     column, actual, expected, extra=extra)
