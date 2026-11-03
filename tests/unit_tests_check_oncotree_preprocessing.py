"""Check OncoTree validation using saved data and mocked downloads."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

from importer import check_oncotree_preprocessing as checks


TUMOR_TYPES = [{'code': 'LUAD', 'mainType': 'Lung', 'name': 'Lung Adenocarcinoma'}]


class OncotreePreprocessingTests(unittest.TestCase):
    def test_reference_rejects_bad_data_and_accepts_missing_labels(self):
        """Bad or repeated codes must fail; missing names are allowed."""
        invalid = [[], {}, [None], [{'code': ''}], [{'code': 1}],
                   [{'code': 'LUAD', 'name': 1}], [{'code': 'LUAD', 'mainType': []}],
                   TUMOR_TYPES * 2]
        for records in invalid:
            with self.subTest(records=records), self.assertRaises(ValueError):
                checks.parse_oncotree_nodes(json.dumps(records))
        with self.assertRaises(ValueError):
            checks.parse_oncotree_nodes('not JSON')
        nodes = checks.parse_oncotree_nodes('[{"code": "LUAD", "name": null}]')
        self.assertEqual(('', ''), (nodes['LUAD']['name'], nodes['LUAD']['mainType']))

    def test_findings_report_unknown_codes_and_wrong_labels(self):
        """Return the bad values and expected names without changing the clinical row."""
        nodes = checks.parse_oncotree_nodes(json.dumps(TUMOR_TYPES))
        unknown = list(checks.oncotree_findings({'ONCOTREE_CODE': 'RETIRED'}, nodes))
        self.assertEqual(('ONCOTREE_CODE', 'RETIRED'), unknown[0][:2])
        row = {'ONCOTREE_CODE': ' LUAD ', 'CANCER_TYPE': 'Wrong',
               'CANCER_TYPE_DETAILED': 'Old name'}
        before = row.copy()
        self.assertEqual([('CANCER_TYPE', 'Wrong', 'Lung'),
                          ('CANCER_TYPE_DETAILED', 'Old name', 'Lung Adenocarcinoma')],
                         list(checks.oncotree_findings(row, nodes)))
        self.assertEqual(before, row)

    def test_matching_or_missing_values_have_no_findings(self):
        """Correct names and allowed missing values do not require preprocessing."""
        nodes = checks.parse_oncotree_nodes(json.dumps(TUMOR_TYPES))
        for missing in ('', 'NA', 'n/a', 'Not Available', '[Not Available]', '[Not Applicable]'):
            with self.subTest(missing=missing):
                self.assertEqual([], list(checks.oncotree_findings({'ONCOTREE_CODE': missing}, nodes)))
                row = {'ONCOTREE_CODE': 'LUAD', 'CANCER_TYPE': missing,
                       'CANCER_TYPE_DETAILED': missing}
                self.assertEqual([], list(checks.oncotree_findings(row, nodes)))
        self.assertEqual([], list(checks.oncotree_findings(
            {'ONCOTREE_CODE': 'LUAD', 'CANCER_TYPE': 'Lung',
             'CANCER_TYPE_DETAILED': 'Lung Adenocarcinoma'}, nodes)))

    @patch.object(checks.requests, 'get')
    def test_saved_reference_is_read_without_downloading(self, get):
        """A portal-info oncotree.json is used as-is, without downloading."""
        with tempfile.TemporaryDirectory() as directory:
            saved = Path(directory) / 'oncotree.json'
            raw = json.dumps(TUMOR_TYPES).encode()
            saved.write_bytes(raw)
            reference = checks.OncotreeReference(saved)
            self.assertIn('LUAD', reference.load(Mock()))
            self.assertEqual(raw, saved.read_bytes())
        get.assert_not_called()

    @patch.object(checks.requests, 'get')
    def test_download_is_reused_for_later_rows(self, get):
        """Download the requested version once, then reuse the result."""
        raw = json.dumps(TUMOR_TYPES).encode()
        get.return_value = Mock(content=raw)
        reference = checks.OncotreeReference(version='pinned')
        nodes = reference.load(Mock())
        self.assertIs(nodes, reference.load(Mock()))
        get.assert_called_once_with('https://oncotree.mskcc.org/api/tumorTypes',
                                    params={'version': 'pinned'}, timeout=(10, 30))
        get.return_value.raise_for_status.assert_called_once()

    @patch.object(checks.requests, 'get', side_effect=requests.RequestException('unavailable'))
    def test_reference_failure_is_reported_once(self, get):
        reference, logger = checks.OncotreeReference(), Mock()
        for line in (5, 6):
            checks.check_oncotree_row(['ONCOTREE_CODE'], ['LUAD'], reference, logger, line)
        get.assert_called_once()
        logger.error.assert_called_once()
        self.assertIn('unavailable', logger.error.call_args.args)

    def test_row_errors_include_location_and_malformed_rows_are_skipped(self):
        """Report where the bad label is; leave unrelated row errors to the validator."""
        reference, logger = Mock(), Mock()
        reference.load.return_value = checks.parse_oncotree_nodes(json.dumps(TUMOR_TYPES))
        checks.check_oncotree_row(['SAMPLE_ID'], ['S1'], reference, logger, 1)
        checks.check_oncotree_row(['ONCOTREE_CODE'], [], reference, logger, 2)
        reference.load.assert_not_called()
        checks.check_oncotree_row(['ONCOTREE_CODE', 'CANCER_TYPE'],
                                 ['LUAD', 'Wrong'], reference, logger, 8)
        logger.error.assert_called_once()
        self.assertEqual({'line_number': 8, 'column_number': 2},
                         logger.error.call_args.kwargs['extra'])
