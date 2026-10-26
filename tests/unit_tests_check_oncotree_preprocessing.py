"""Check OncoTree validation against the target portal's cancer types."""
import unittest
from unittest.mock import Mock

from importer import check_oncotree_preprocessing as checks


# Shaped like validateData's cancer_type_dict: lower-case IDs from the portal API.
PORTAL_CANCER_TYPES = {'luad': {'cancerTypeId': 'luad', 'name': 'Lung Adenocarcinoma',
                                'color': 'Gainsboro'}}


class OncotreePreprocessingTests(unittest.TestCase):
    def test_reference_combines_portal_and_study_cancer_types(self):
        """Codes are upper-cased; the portal's name wins over a study's redefinition."""
        reference = checks.CancerTypeReference(
            PORTAL_CANCER_TYPES, {'luad': 'Study name', 'newtype': 'New Type'})
        self.assertEqual({'LUAD': 'Lung Adenocarcinoma', 'NEWTYPE': 'New Type'}, reference.names)
        self.assertEqual({'X': ''}, checks.CancerTypeReference({'x': {'name': None}}).names)
        self.assertIsNone(checks.CancerTypeReference(None, {'newtype': 'New Type'}).names)

    def test_findings_report_unknown_codes_and_wrong_detailed_names(self):
        """Return the bad values and expected names without changing the clinical row."""
        names = checks.CancerTypeReference(PORTAL_CANCER_TYPES).names
        unknown = list(checks.oncotree_findings({'ONCOTREE_CODE': 'RETIRED'}, names))
        self.assertEqual(('ONCOTREE_CODE', 'RETIRED'), unknown[0][:2])
        row = {'ONCOTREE_CODE': ' LUAD ', 'CANCER_TYPE': 'Anything',
               'CANCER_TYPE_DETAILED': 'Old name'}
        before = row.copy()
        self.assertEqual([('CANCER_TYPE_DETAILED', 'Old name', 'Lung Adenocarcinoma')],
                         list(checks.oncotree_findings(row, names)))
        self.assertEqual(before, row)

    def test_codes_match_ignoring_case_but_names_do_not(self):
        """The portal stores lower-case IDs; detailed names stay case-sensitive."""
        names = checks.CancerTypeReference(PORTAL_CANCER_TYPES).names
        for code in ('LUAD', 'luad', 'Luad'):
            with self.subTest(code=code):
                self.assertEqual([], list(checks.oncotree_findings({'ONCOTREE_CODE': code}, names)))
        self.assertEqual(
            [('CANCER_TYPE_DETAILED', 'lung adenocarcinoma', 'Lung Adenocarcinoma')],
            list(checks.oncotree_findings({'ONCOTREE_CODE': 'luad',
                                           'CANCER_TYPE_DETAILED': 'lung adenocarcinoma'}, names)))

    def test_cancer_type_is_not_checked(self):
        """The portal has no OncoTree main type, so any CANCER_TYPE is accepted."""
        names = checks.CancerTypeReference(PORTAL_CANCER_TYPES).names
        self.assertEqual([], list(checks.oncotree_findings(
            {'ONCOTREE_CODE': 'LUAD', 'CANCER_TYPE': 'Not Lung'}, names)))

    def test_matching_or_missing_values_have_no_findings(self):
        """Correct names and allowed missing values do not require preprocessing."""
        names = checks.CancerTypeReference(PORTAL_CANCER_TYPES).names
        for missing in ('', 'NA', 'n/a', 'Not Available', '[Not Available]', '[Not Applicable]'):
            with self.subTest(missing=missing):
                self.assertEqual([], list(checks.oncotree_findings({'ONCOTREE_CODE': missing}, names)))
                row = {'ONCOTREE_CODE': 'LUAD', 'CANCER_TYPE_DETAILED': missing}
                self.assertEqual([], list(checks.oncotree_findings(row, names)))
        self.assertEqual([], list(checks.oncotree_findings(
            {'ONCOTREE_CODE': 'LUAD', 'CANCER_TYPE_DETAILED': 'Lung Adenocarcinoma'}, names)))

    def test_study_defined_cancer_types_are_accepted(self):
        """A code the study's cancer type file adds will exist once the study is imported."""
        names = checks.CancerTypeReference(PORTAL_CANCER_TYPES, {'newtype': 'New Type'}).names
        self.assertEqual([], list(checks.oncotree_findings(
            {'ONCOTREE_CODE': 'NEWTYPE', 'CANCER_TYPE_DETAILED': 'New Type'}, names)))

    def test_missing_portal_cancer_types_are_reported_once(self):
        reference, logger = checks.CancerTypeReference(None), Mock()
        for line in (5, 6):
            checks.check_oncotree_row(['ONCOTREE_CODE'], ['LUAD'], reference, logger, line)
        logger.error.assert_called_once()
        self.assertIn('cancer types are not available', logger.error.call_args.args[0])

    def test_row_errors_include_location_and_malformed_rows_are_skipped(self):
        """Report where the bad label is; leave unrelated row errors to the validator."""
        reference, logger = checks.CancerTypeReference(PORTAL_CANCER_TYPES), Mock()
        checks.check_oncotree_row(['SAMPLE_ID'], ['S1'], reference, logger, 1)
        checks.check_oncotree_row(['ONCOTREE_CODE'], [], reference, logger, 2)
        logger.error.assert_not_called()
        checks.check_oncotree_row(['ONCOTREE_CODE', 'CANCER_TYPE_DETAILED'],
                                 ['LUAD', 'Wrong'], reference, logger, 8)
        logger.error.assert_called_once()
        self.assertEqual({'line_number': 8, 'column_number': 2},
                         logger.error.call_args.kwargs['extra'])
