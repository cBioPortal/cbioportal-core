"""Check expected case lists using small, temporary study files."""
from pathlib import Path
import tempfile
import unittest

from importer import check_case_list_preprocessing as checks


class CaseListPreprocessingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = self.root / 'config.tsv'

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def configure(self, *rules):
        """Write the seven-column config format with the rules needed by this test."""
        lines = ['CASE_LIST_FILENAME\tSTAGING_FILENAME\tMETA_STABLE_ID\t'
                 'META_CASE_LIST_CATEGORY\tMETA_CANCER_STUDY_ID\tMETA_CASE_LIST_NAME\t'
                 'META_CASE_LIST_DESCRIPTION']
        lines.extend('\t'.join([name + '.txt', sources, '<CANCER_STUDY>_' + name,
                                category, '<CANCER_STUDY>', name, name])
                     for name, sources, category in rules)
        self.config.write_text('\n'.join(lines) + '\n')

    def missing(self, defined_ids=(), scanned_members=None):
        return list(checks.missing_generated_case_lists(
            self.root, 'study', defined_ids, self.config, scanned_members))

    def test_union_intersection_and_missing_inputs(self):
        """Count samples in either file or both files, and skip empty results."""
        self.write('data_CNA.txt', 'Hugo_Symbol\tS1\tS2\nGENE\t0\t1\n')
        self.write('data_mutations.txt', 'Tumor_Sample_Barcode\nS2\nS3\nS3\n')
        self.configure(('union', 'data_CNA.txt|data_mutations.txt', 'other'),
                       ('both', 'data_CNA.txt&data_mutations.txt', 'other'),
                       ('absent', 'missing.txt', 'other'),
                       ('incomplete', 'data_CNA.txt&missing.txt', 'other'))
        self.assertEqual([('study_union', 'union.txt', 3), ('study_both', 'both.txt', 1)],
                         self.missing())

    def test_existing_ids_and_curated_lists_are_preserved(self):
        """Only the expected stable ID satisfies the rule; curated files are not rewritten."""
        self.write('data_CNA.txt', 'Hugo_Symbol\tS1\nGENE\t1\n')
        self.configure(('cna', 'data_CNA.txt', 'all_cases_with_cna_data'))
        curated = self.write('case_lists/custom.txt', 'cancer_study_identifier: study\n'
                             'stable_id: study_custom\ncase_list_category: all_cases_with_cna_data\n'
                             'case_list_ids: S1\n')
        original = curated.read_bytes()
        self.assertEqual([], self.missing(['study_cna']))
        self.assertEqual([('study_cna', 'cna.txt', 1)], self.missing(['study_custom']))
        self.assertEqual(original, curated.read_bytes())
        # A foreign, empty, generic, or unrecognized list does not satisfy the rule.
        self.assertEqual([('study_cna', 'cna.txt', 1)], self.missing())
        for changed in (original.decode().replace('identifier: study', 'identifier: foreign'),
                        original.decode().replace('case_list_ids: S1', 'case_list_ids:'),
                        original.decode().replace('all_cases_with_cna_data', 'other')):
            with self.subTest(metadata=changed):
                curated.write_text(changed)
                self.assertEqual([('study_cna', 'cna.txt', 1)], self.missing(['study_custom']))

    def test_duplicate_rules_and_virtual_all_list(self):
        self.write('data_CNA.txt', 'Hugo_Symbol\tS1\n')
        self.configure(('all', 'data_CNA.txt', 'all_cases_in_study'),
                       ('all', 'data_CNA.txt', 'all_cases_in_study'))
        self.assertEqual([('study_all', 'all.txt', 1)], self.missing())
        self.assertEqual([], self.missing(['study_all']))

    def test_sequenced_samples_override_mutation_rows_and_cached_scan(self):
        """Include samples with no mutations; even an empty override takes precedence."""
        maf = self.write('data_mutations.txt', 'Tumor_Sample_Barcode\nS1\n')
        self.configure(('sequenced', 'data_mutations.txt', 'all_cases_with_mutation_data'))
        scanned = {str(maf.resolve()): {'S2', 'S3'}}
        self.assertEqual([('study_sequenced', 'sequenced.txt', 2)], self.missing(scanned_members=scanned))
        sidecar = self.write('sequenced_samples.txt', 'S4\nS5\nS6\nS4\n')
        self.assertEqual([('study_sequenced', 'sequenced.txt', 3)], self.missing(scanned_members=scanned))
        sidecar.write_text('')
        self.assertEqual([], self.missing(scanned_members=scanned))

    def test_file_parsing_and_bad_rows(self):
        """Read matrix headers, sample columns and sequenced comments; reject short rows."""
        examples = [('matrix.txt', '#comment\nHugo_Symbol\tEntrez_Gene_Id\tS1\tS2\n', {'S1', 'S2'}),
                    ('samples.txt', 'PATIENT_ID\tSample_Id\nP1\tS1\n\nP2\tS2\n', {'S1', 'S2'}),
                    ('data_mutations.txt', '#sequenced_samples: S1 S2\n'
                     'Tumor_Sample_Barcode\nS1\n', {'S1', 'S2'})]
        for name, text, expected in examples:
            with self.subTest(name=name):
                self.write(name, text)
                self.assertEqual(expected, set(checks.case_list_from_staging_file(self.root, name)))
        self.write('broken.txt', 'PATIENT_ID\tSAMPLE_ID\nP1\n')
        with self.assertRaises(IndexError):
            checks.case_list_from_staging_file(self.root, 'broken.txt')

    def test_normalized_tcga_ids_and_filename_casing(self):
        # GIVEN normalized tumor/normal IDs and an ordinary non-TCGA ID.
        path = self.write(
            'data_cna.txt',
            'Hugo_Symbol\tTCGA-AA-1234-01\tTCGA-AA-1234-11\tS1\n',
        )
        original = path.read_bytes()
        self.configure(('cna', 'data_CNA.txt', 'all_cases_with_cna_data'))

        # WHEN the configured filename differs only in capitalization.
        missing_lists = self.missing()

        # THEN all three samples count separately and the source stays unchanged.
        self.assertEqual([('study_cna', 'cna.txt', 3)], missing_lists)
        self.assertEqual(original, path.read_bytes())

    def test_unnormalized_ids_are_rejected_even_if_case_list_exists(self):
        # GIVEN IDs that previously were silently shortened or assigned a type.
        self.configure(('cna', 'data_CNA.txt', 'all_cases_with_cna_data'))
        unnormalized_ids = (
            'TCGA-AA-1234',
            'TCGA-AA-1234-Tumor',
            'TCGA-AA-1234-Normal',
            'TCGA-AA-1234-01A',
            'TCGA-AA-1234-03A-01D',
            'TCGA---',
        )

        for sample_id in unnormalized_ids:
            for defined in ((), ('study_cna',)):
                with self.subTest(sample_id=sample_id, defined=defined):
                    path = self.write('data_CNA.txt', 'Hugo_Symbol\t' + sample_id + '\n')
                    original = path.read_bytes()

                    # WHEN / THEN checking reports the bad ID instead of repairing it.
                    with self.assertRaisesRegex(ValueError, 'unnormalized or malformed TCGA'):
                        self.missing(defined)

                    self.assertEqual(original, path.read_bytes())

    def test_cached_scan_does_not_bypass_barcode_validation(self):
        # GIVEN the existing scan has collected an unnormalized mutation ID.
        path = self.write('data_mutations.txt', 'Tumor_Sample_Barcode\nTCGA-AA-1234-Tumor\n')
        scanned = {str(path.resolve()): {'TCGA-AA-1234-Tumor'}}
        self.configure(('sequenced', 'data_mutations.txt', 'all_cases_with_mutation_data'))

        # WHEN / THEN reusing the scan still reports the offending source file.
        with self.assertRaisesRegex(ValueError, 'data_mutations.txt.*unnormalized'):
            self.missing(scanned_members=scanned)

    def test_sequenced_comment_requires_normalized_ids(self):
        # GIVEN the mutation sample list comes from a comment.
        self.write('data_mutations.txt', '#sequenced_samples: TCGA-AA-1234-01A\n')
        self.configure(('sequenced', 'data_mutations.txt', 'all_cases_with_mutation_data'))

        # WHEN / THEN the comment's unnormalized ID is rejected.
        with self.assertRaisesRegex(ValueError, 'unnormalized'):
            self.missing()

    def test_sequenced_sidecar_requires_normalized_ids(self):
        # GIVEN a sidecar overrides an otherwise valid mutation-file scan.
        path = self.write('data_mutations.txt', 'Tumor_Sample_Barcode\nS1\n')
        self.write('sequenced_samples.txt', 'TCGA-AA-1234-Normal\n')
        scanned = {str(path.resolve()): {'S1'}}
        self.configure(('sequenced', 'data_mutations.txt', 'all_cases_with_mutation_data'))

        # WHEN / THEN the error identifies the sidecar, not the mutation file.
        with self.assertRaisesRegex(ValueError, 'sequenced_samples.txt.*unnormalized'):
            self.missing(scanned_members=scanned)

    def test_intersection_rejects_ids_that_normalization_would_merge(self):
        # GIVEN distinct raw IDs that the old normalization mapped to one sample.
        self.write('data_CNA.txt', 'Hugo_Symbol\tTCGA-AA-1234-01\n')
        self.write('data_mutations.txt', 'Tumor_Sample_Barcode\nTCGA-AA-1234-01A\n')
        self.configure(('both', 'data_CNA.txt&data_mutations.txt', 'other'))

        # WHEN / THEN comparison fails instead of reporting a fabricated match.
        with self.assertRaisesRegex(ValueError, 'unnormalized'):
            self.missing()

    def test_normalized_tumor_and_normal_ids_do_not_intersect(self):
        # GIVEN two valid IDs with different sample types.
        self.write('data_CNA.txt', 'Hugo_Symbol\tTCGA-AA-1234-01\n')
        self.write('data_mutations.txt', 'Tumor_Sample_Barcode\nTCGA-AA-1234-11\n')
        self.configure(('both', 'data_CNA.txt&data_mutations.txt', 'other'))

        # WHEN finding lists for samples shared by both files.
        missing_lists = self.missing()

        # THEN the intersection is empty, so no list is required.
        self.assertEqual([], missing_lists)

    def test_missing_intersection_input_does_not_hide_bad_ids(self):
        # GIVEN one input is absent and the other has a patient-only barcode.
        self.write('data_mutations.txt', 'Tumor_Sample_Barcode\nTCGA-AA-1234\n')
        self.configure(('both', 'data_mutations.txt&missing.txt', 'other'))

        # WHEN / THEN the absent file does not bypass validation of the present one.
        with self.assertRaisesRegex(ValueError, 'unnormalized'):
            self.missing()
