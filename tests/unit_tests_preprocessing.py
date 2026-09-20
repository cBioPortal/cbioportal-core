"""Regression tests for validation before/after required preprocessing."""
import json
import logging
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

from importer import preprocessing, preprocessing_case_lists as cases, validateData, validateStudies


NODES = [{'code': 'LUAD', 'mainType': 'Non-Small Cell Lung Cancer',
          'name': 'Lung Adenocarcinoma'}]


class PreprocessingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.logger = logging.getLogger(self.id())
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        self.handler = validateData.MaxLevelTrackingHandler()
        self.logger.addHandler(self.handler)
        self.addCleanup(self.logger.removeHandler, self.handler)
        self.snapshot = self.root / 'oncotree.json'
        self.snapshot.write_text(json.dumps(NODES))
        self.portal = validateData.PortalInstance(None, {}, {'GENE': [1]}, {'ALIAS': [1]}, [], [], None)
        self.portal.oncotree = preprocessing.OncotreeReference(self.snapshot)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def test_curated_case_category_satisfies_generated_role(self):
        self.write('data_cna.txt', 'Hugo_Symbol\tS1\nGENE\t1\n')
        path = self.write('case_lists/curated.txt', 'cancer_study_identifier: study\n'
                          'stable_id: study_custom\ncase_list_category: all_cases_with_cna_data\n'
                          'case_list_ids: S1\n')
        defined = ['study_all', 'study_custom']
        self.assertEqual([], list(cases.missing_generated_case_lists(self.root, 'study', defined)))
        for text in (path.read_text().replace('all_cases_with_cna_data', 'other'),
                     path.read_text().replace('cancer_study_identifier: study', 'cancer_study_identifier: foreign'),
                     path.read_text().replace('case_list_ids: S1', 'case_list_ids:')):
            with self.subTest(metadata=text):
                path.write_text(text)
                self.assertTrue(list(cases.missing_generated_case_lists(self.root, 'study', defined)))

    def test_oncotree_stale_labels_fail_then_corrected_labels_pass(self):
        columns = ['SAMPLE_ID', 'PATIENT_ID', 'ONCOTREE_CODE', 'CANCER_TYPE', 'CANCER_TYPE_DETAILED']
        header = '\n'.join(['#' + '\t'.join(columns), '#' + '\t'.join(columns),
                            '#STRING\tSTRING\tSTRING\tSTRING\tSTRING', '#1\t1\t1\t1\t1', '\t'.join(columns)])
        path = self.write('samples.txt', header + '\nS1\tP1\tLUAD\tOld type\tOld name\n')
        validator = validateData.SampleClinicalValidator(str(self.root), {'data_filename': path.name},
                                                         self.portal, self.logger, True, False)
        with self.assertLogs(self.logger, logging.ERROR) as logs:
            validator.validate()
        self.assertTrue(any('CANCER_TYPE is' in msg for msg in logs.output))
        self.assertTrue(any('CANCER_TYPE_DETAILED is' in msg for msg in logs.output))
        path.write_text(header + '\nS1\tP1\tLUAD\tNon-Small Cell Lung Cancer\tLung Adenocarcinoma\n')
        self.handler.max_level = logging.NOTSET
        validateData.SampleClinicalValidator(str(self.root), {'data_filename': path.name},
                                             self.portal, self.logger, True, False).validate()
        self.assertLess(self.handler.max_level, logging.ERROR)

    def test_unknown_code_and_missing_labels(self):
        nodes = {n['code']: n for n in NODES}
        self.assertEqual('ONCOTREE_CODE', list(preprocessing.oncotree_findings({'ONCOTREE_CODE': 'RETIRED'}, nodes))[0][0])
        self.assertEqual([], list(preprocessing.oncotree_findings({'ONCOTREE_CODE': 'LUAD'}, nodes)))
        self.assertEqual([], list(preprocessing.oncotree_findings({'ONCOTREE_CODE': 'NA'}, nodes)))
        self.assertEqual([], list(preprocessing.oncotree_findings(
            {'ONCOTREE_CODE': 'NA', 'CANCER_TYPE': 'Custom', 'CANCER_TYPE_DETAILED': 'Custom'}, nodes)))

    def test_snapshot_failure_fails_once_without_network(self):
        self.snapshot.write_text('[]')
        with patch('importer.preprocessing.requests.get') as get:
            with self.assertLogs(self.logger, logging.ERROR) as logs:
                for line in (5, 6):
                    preprocessing.check_oncotree_row(['ONCOTREE_CODE'], ['LUAD'],
                        self.portal.oncotree, self.logger, line)
            self.assertEqual(1, len(logs.output))
            get.assert_not_called()

    def test_online_reference_cached_and_bounded(self):
        reference = preprocessing.OncotreeReference(version='pinned-version')
        response = Mock(content=json.dumps(NODES).encode())
        with patch('importer.preprocessing.requests.get', return_value=response) as get:
            self.assertEqual(reference.load(self.logger), reference.load(self.logger))
            get.assert_called_once_with('https://oncotree.mskcc.org/api/tumorTypes',
                                       params={'version': 'pinned-version'}, timeout=(10, 30))
        self.assertEqual(['LUAD'], list(reference.nodes))

    def test_batch_wrapper_uses_one_temporary_snapshot(self):
        args = validateStudies.interface(['-l', 'study-a,study-b', '-n',
                                         '-html', str(self.root / 'reports')])
        # Avoid unrelated meta_study lookups in this subprocess orchestration test.
        args.html_folder = None
        paths = []
        def run(command, **kwargs):
            path = Path(command[command.index('--oncotree-cache') + 1])
            paths.append(path)
            if len(paths) == 1:
                path.write_text(json.dumps(NODES))
            else:
                self.assertEqual(NODES, json.loads(path.read_text()))
            return 0
        with patch.object(validateStudies.subprocess, 'call', side_effect=run):
            self.assertEqual(0, validateStudies.main(args))
        self.assertEqual(paths[0], paths[1])
        self.assertFalse(paths[0].exists())

    def test_matrix_scan_supplies_case_members(self):
        self.write('data_CNA.txt', 'Hugo_Symbol\tEntrez_Gene_Id\tS1\nGENE\t1\t0\n')
        with patch.object(validateData, 'DEFINED_SAMPLE_IDS', {'S1'}):
            validator = validateData.CNADiscreteValidator(
                str(self.root), {'data_filename': 'data_CNA.txt'}, self.portal,
                self.logger, False, False)
            validator.validate()
        self.assertEqual({'S1'}, validator.generated_case_members)

    def test_batch_snapshot_shared_across_reference_instances(self):
        cache = self.root / 'batch.json'
        response = Mock(content=json.dumps(NODES).encode())
        with patch('importer.preprocessing.requests.get', return_value=response) as get:
            first = preprocessing.OncotreeReference(cache_filename=cache)
            second = preprocessing.OncotreeReference(cache_filename=cache)
            self.assertEqual(first.load(self.logger), second.load(self.logger))
            get.assert_called_once()
        # Explicit snapshots override any batch cache.
        cache.write_text('invalid')
        self.assertEqual(['LUAD'], list(preprocessing.OncotreeReference(
            self.snapshot, cache_filename=cache).load(self.logger)))

    def test_scanned_case_members_avoid_reopen_and_keep_overrides(self):
        path = self.write('data_mutations_extended.txt',
                          'Tumor_Sample_Barcode\tOther\nS1\tx\nS2\ty\n')
        collector = cases.StagingCaseCollector(path.name)
        for line in path.read_text().splitlines(keepends=True):
            collector.feed(line)
        scanned = {str(path.resolve()): collector.members}
        with patch.object(cases, 'case_list_from_staging_file', wraps=cases.case_list_from_staging_file) as read:
            found = list(cases.missing_generated_case_lists(
                self.root, 'study', ['study_all'], scanned_members=scanned))
            self.assertIn(('study_sequenced', 'cases_sequenced.txt', 2), found)
            self.assertNotIn('data_mutations_extended.txt', [call.args[1] for call in read.call_args_list])
        self.write('sequenced_samples.txt', '')
        self.assertEqual([], list(cases.missing_generated_case_lists(
            self.root, 'study', ['study_all'], scanned_members=scanned)))

    def test_scan_preserves_inline_and_malformed_row_rules(self):
        for content, expected in (
                ('Tumor_Sample_Barcode\nS1\n#sequenced_samples: S2 S3\nS4\n', {'S2', 'S3'}),
                ('SAMPLE_ID\tOTHER\nS1\tx\nS1\ty\n', {'S1'}),
                ('Hugo_Symbol\tTCGA-AA-1234-01A\nGENE\t2\n', {'TCGA-AA-1234-01A'})):
            collector = cases.StagingCaseCollector('data.txt')
            for line in content.splitlines(keepends=True):
                collector.feed(line)
            self.assertEqual(expected, collector.members)
        collector = cases.StagingCaseCollector('data.txt')
        for line in ('Other\tSAMPLE_ID\n', 'x\n'):
            collector.feed(line)
        self.assertIsInstance(collector.error, IndexError)

    def test_cna_alias_collision_fails_and_merged_copy_passes(self):
        with patch.object(validateData, 'DEFINED_SAMPLE_IDS', ['S1']):
            path = self.write('cna.txt', 'Hugo_Symbol\tEntrez_Gene_Id\tS1\nGENE\t1\t0\nALIAS\t\t2\n')
            validator = validateData.CNADiscreteValidator(str(self.root), {'data_filename': path.name},
                                                         self.portal, self.logger, False, False)
            with self.assertLogs(self.logger, logging.ERROR) as logs:
                validator.validate()
            self.assertIn('Duplicate CNA gene', '\n'.join(logs.output))
            path.write_text('Hugo_Symbol\tEntrez_Gene_Id\tS1\nGENE\t1\t2\n')
            self.handler.max_level = logging.NOTSET
            validateData.CNADiscreteValidator(str(self.root), {'data_filename': path.name},
                                             self.portal, self.logger, False, False).validate()
            self.assertEqual(0, self.handler.get_exit_status())

    def test_continuous_cna_duplicates_fail_without_changing_expression_policy(self):
        with patch.object(validateData, 'DEFINED_SAMPLE_IDS', ['S1']):
            self.write('values.txt', 'Hugo_Symbol\tEntrez_Gene_Id\tS1\nGENE\t1\t2\nGENE\t1\t-2\n')
            for cls, status in ((validateData.CNAContinuousValuesValidator, 1),
                                (validateData.ContinuousValuesValidator, 3)):
                self.handler.max_level = logging.NOTSET
                cls(str(self.root), {'data_filename': 'values.txt'}, self.portal,
                    self.logger, False, False).validate()
                self.assertEqual(status, self.handler.get_exit_status())

    def test_missing_case_lists_and_existing_curated_lists(self):
        self.write('data_CNA.txt', 'Hugo_Symbol\tS1\tS2\nGENE\t0\t2\n')
        missing = list(cases.missing_generated_case_lists(self.root, 'study', ['study_all']))
        self.assertEqual([('study_cna', 'cases_cna.txt', 2)], missing)
        # Stable ID, not filename/order/event-derived membership, establishes coverage.
        self.assertEqual([], list(cases.missing_generated_case_lists(self.root, 'study', ['study_all', 'study_cna', 'study_custom'])))

    def test_sequenced_override_includes_samples_without_mutations(self):
        self.write('data_mutations_extended.txt', 'Tumor_Sample_Barcode\nS1\n')
        self.write('sequenced_samples.txt', 'S1\nS2\n')
        missing = list(cases.missing_generated_case_lists(self.root, 'study', ['study_all']))
        self.assertEqual([('study_sequenced', 'cases_sequenced.txt', 2)], missing)

    def test_intersection_requires_all_files_and_nonempty_overlap(self):
        self.write('data_CNA.txt', 'Hugo_Symbol\tS1\nGENE\t2\n')
        self.write('data_mutations_extended.txt', 'Tumor_Sample_Barcode\nS2\n')
        found = list(cases.missing_generated_case_lists(self.root, 'study', ['study_all']))
        self.assertNotIn('study_cnaseq', [x[0] for x in found])
        self.write('data_mutations_extended.txt', '#sequenced_samples: S1 S2\nTumor_Sample_Barcode\nS2\n')
        found = list(cases.missing_generated_case_lists(self.root, 'study', ['study_all']))
        self.assertIn(('study_cnaseq', 'cases_cnaseq.txt', 1), found)

    def test_case_checks_leave_input_bytes_unchanged(self):
        self.write('data_cna.txt', 'Hugo_Symbol\tTCGA-AA-1234-01A\nGENE\t2\n')
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        found = list(cases.missing_generated_case_lists(self.root, 'study', ['study_all']))
        self.assertEqual([('study_cna', 'cases_cna.txt', 1)], found)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})

    def test_profile_presence_alone_does_not_prove_nonempty_intersection(self):
        with self.assertLogs(self.logger, logging.WARNING) as logs:
            validateData.validate_defined_caselists('study', ['study_all', 'study_cna', 'study_sequenced'],
                                                   ['meta_mutations_extended', 'meta_CNA'], self.logger)
        self.assertIn('study_cnaseq', '\n'.join(logs.output))

    def test_three_way_intersection_does_not_restart_after_disjoint_pair(self):
        self.write('data_RNA_Seq_v2_mRNA_median_Zscores.txt', 'Hugo_Symbol\tS1\nGENE\t1\n')
        self.write('data_CNA.txt', 'Hugo_Symbol\tS2\nGENE\t2\n')
        self.write('data_mutations_extended.txt', 'Tumor_Sample_Barcode\nS2\n')
        found = list(cases.missing_generated_case_lists(self.root, 'study', ['study_all']))
        self.assertNotIn('study_3way_complete', [x[0] for x in found])

    def test_timeline_requires_reference_not_matching_meta_name(self):
        self.write('meta_study.txt', 'cancer_study_identifier: test\ntype_of_cancer: lung\n'
                   'name: Test\ndescription: Test\nadd_global_case_list: true\n')
        data = self.write('DATA_TIMELINE_treatment.tsv',
                          'PATIENT_ID\tSTART_DATE\tSTOP_DATE\tEVENT_TYPE\nP1\t0\t1\tTREATMENT\n')
        # A similarly named file with the wrong data reference does not cover it.
        meta = self.write('meta_timeline_treatment.txt',
                         'cancer_study_identifier: test\ngenetic_alteration_type: CLINICAL\n'
                         'datatype: TIMELINE\ndata_filename: other.txt\n')
        with self.assertLogs(self.logger, logging.ERROR) as logs:
            validateData.process_metadata_files(str(self.root), self.portal, self.logger, False, False)
        self.assertIn('no referencing timeline meta file', '\n'.join(logs.output))
        meta.unlink()
        self.write('meta_arbitrary_name.txt',
                   'cancer_study_identifier: test\ngenetic_alteration_type: CLINICAL\n'
                   'datatype: TIMELINE\ndata_filename: ./DATA_TIMELINE_treatment.tsv\n')
        self.handler.max_level = logging.NOTSET
        validateData.process_metadata_files(str(self.root), self.portal, self.logger, False, False)
        self.assertLess(self.handler.max_level, logging.ERROR)
        self.assertTrue(data.exists())

    def test_timeline_cli_failure_then_reference_added(self):
        self.write('meta_study.txt', 'cancer_study_identifier: test\ntype_of_cancer: lung\n'
                   'name: Test\ndescription: Test\nadd_global_case_list: true\n')
        self.write('meta_clinical_sample.txt', 'cancer_study_identifier: test\n'
                   'genetic_alteration_type: CLINICAL\ndatatype: SAMPLE_ATTRIBUTES\n'
                   'data_filename: data_clinical_sample.txt\n')
        self.write('data_clinical_sample.txt',
                   '#Sample\tPatient\n#Sample\tPatient\n#STRING\tSTRING\n#1\t1\n'
                   'SAMPLE_ID\tPATIENT_ID\nS1\tP1\n')
        self.write('data_timeline.txt', 'PATIENT_ID\tSTART_DATE\tSTOP_DATE\tEVENT_TYPE\n'
                   'P1\t0\t1\tTREATMENT\n')
        # Archived files, editor backups, and unrelated data are outside this rule.
        for name in ('archived_files/data_timeline_old.txt', 'data_timeline_old.txt~',
                     '.data_timeline_hidden.txt', 'data_expression.txt'):
            self.write(name, 'unused\n')
        command = [sys.executable, str(Path(validateData.__file__)), '-s', str(self.root), '-n']
        before = {str(p): p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn('no referencing timeline meta file', result.stdout + result.stderr)
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.root.rglob('*') if p.is_file()})
        self.write('meta_events.txt', 'cancer_study_identifier: test\n'
                   'genetic_alteration_type: CLINICAL\ndatatype: TIMELINE\n'
                   'data_filename: data_timeline.txt\n')
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertIn(result.returncode, (0, 3), result.stdout + result.stderr)
        self.assertNotIn('no referencing timeline meta file', result.stdout + result.stderr)

    def test_cli_oncotree_failure_then_corrected_study(self):
        self.write('meta_study.txt', 'cancer_study_identifier: test\ntype_of_cancer: lung\n'
                   'name: Test\ndescription: Test\nadd_global_case_list: true\n')
        self.write('meta_clinical_sample.txt', 'cancer_study_identifier: test\n'
                   'genetic_alteration_type: CLINICAL\ndatatype: SAMPLE_ATTRIBUTES\n'
                   'data_filename: data_clinical_sample.txt\n')
        header = ('#Sample\tPatient\tCode\tType\tDetailed\n'
                  '#Sample\tPatient\tCode\tType\tDetailed\n'
                  '#STRING\tSTRING\tSTRING\tSTRING\tSTRING\n#1\t1\t1\t1\t1\n'
                  'SAMPLE_ID\tPATIENT_ID\tONCOTREE_CODE\tCANCER_TYPE\tCANCER_TYPE_DETAILED\n')
        path = self.write('data_clinical_sample.txt', header + 'S1\tP1\tLUAD\tStale\tStale\n')
        script = Path(validateData.__file__)
        command = [sys.executable, str(script), '-s', str(self.root), '-n',
                   '--oncotree-file', str(self.snapshot)]
        before = path.read_bytes()
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertIn('OncoTree preprocessing required', result.stdout + result.stderr)
        self.assertEqual(before, path.read_bytes())
        path.write_text(header + 'S1\tP1\tLUAD\tNon-Small Cell Lung Cancer\tLung Adenocarcinoma\n')
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertIn(result.returncode, (0, 3), result.stderr)
        self.assertNotIn('OncoTree preprocessing required', result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
