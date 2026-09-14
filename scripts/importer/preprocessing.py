"""Read-only checks for clinical data requiring OncoTree preprocessing."""
import argparse
import hashlib
import json
from pathlib import Path
import requests


def add_arguments(parser):
    parser.add_argument('--oncotree-cache', help=argparse.SUPPRESS)
    parser.add_argument('--oncotree-file', help='Saved OncoTree tumorTypes JSON for reproducible/offline validation')
    parser.add_argument('--oncotree-version', default='oncotree_latest_stable',
                        help='OncoTree version to fetch when no snapshot is supplied (default: latest stable)')


class OncotreeReference:
    """Fetch lazily once, including failures; never retry for each sample."""
    def __init__(self, filename=None, version='oncotree_latest_stable', cache_filename=None):
        self.filename = filename
        self.cache_filename = cache_filename
        self.version = version
        self.nodes = None
        self.error = None
        self.reported_error = False

    def load(self, logger):
        if self.nodes is not None or self.error is not None:
            return self.nodes
        try:
            if self.filename:
                raw = Path(self.filename).read_bytes()
                source = str(self.filename)
            elif self.cache_filename and Path(self.cache_filename).exists():
                raw = Path(self.cache_filename).read_bytes()
                source = 'OncoTree version ' + self.version + ' (batch snapshot)'
            else:
                response = requests.get('https://oncotree.mskcc.org/api/tumorTypes',
                                        params={'version': self.version}, timeout=(10, 30))
                response.raise_for_status()
                raw = response.content
                source = 'OncoTree version ' + self.version
            records = json.loads(raw)
            if not isinstance(records, list) or not records:
                raise ValueError('expected a non-empty tumorTypes array')
            nodes = {}
            for item in records:
                if (not isinstance(item, dict) or not isinstance(item.get('code'), str)
                        or not item['code'] or not isinstance(item.get('name', ''), (str, type(None)))
                        or not isinstance(item.get('mainType', ''), (str, type(None)))):
                    raise ValueError('each tumor type requires a code and string/null name and mainType')
                if item['code'] in nodes:
                    raise ValueError('duplicate OncoTree code: ' + item['code'])
                nodes[item['code']] = dict(item, name=item.get('name') or '',
                                          mainType=item.get('mainType') or '')
            if self.cache_filename and not self.filename:
                Path(self.cache_filename).write_bytes(raw)
            self.nodes = nodes
            logger.info('OncoTree reference: %s; SHA-256 %s', source, hashlib.sha256(raw).hexdigest())
        except (OSError, ValueError, requests.RequestException) as exc:
            self.error = str(exc)
        return self.nodes


def oncotree_findings(row, nodes):
    """Yield (column, actual, expected); follow strict audit/--force label policy."""
    code = row.get('ONCOTREE_CODE', '')
    if code.strip().lower() in ('', 'na', '[not available]', '[not applicable]'):
        for column in ('CANCER_TYPE', 'CANCER_TYPE_DETAILED'):
            if column not in row:
                yield column, '<missing column>', 'NA'
        return
    if code not in nodes:
        yield 'ONCOTREE_CODE', code, 'a code in the selected OncoTree reference; remap retired codes'
        return
    node = nodes[code]
    for column, expected in (('CANCER_TYPE', node['mainType'] or 'NA'),
                             ('CANCER_TYPE_DETAILED', node['name'])):
        if row.get(column) != expected:
            yield column, row.get(column, '<missing column>'), expected


def check_oncotree_row(columns, values, reference, logger, line_number):
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
