#!/usr/bin/env python3

#
# SPDX-License-Identifier: Apache-2.0
#

import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock
from unittest.mock import call
from importer import cbioportalImporter
from importer.cbioportal_common import JavaRunException, MetaFileTypes

common_part = ('-Dspring.profiles.active=dbcp', '-cp', 'test.jar')
STEP_RUNNER_CLASS = 'org.mskcc.cbio.portal.scripts.ImportStepRunner'
VERSION_UTIL_CLASS = 'org.mskcc.cbio.portal.util.VersionUtil'


class StepRunnerRecorder:
    '''
    run_java stand-in for directory imports, which run their steps in one JVM
    through ImportStepRunner. call_args_list holds one call per manifest step,
    shaped like the java call that would run that step on its own.
    '''

    def __init__(self, fail_step_class=None, fail_status=1):
        self.runs = 0
        self.call_args_list = []
        self.fail_step_class = fail_step_class
        self.fail_status = fail_status

    def __call__(self, *args):
        self.runs += 1
        runner = args.index(STEP_RUNNER_CLASS)
        options = args[runner + 1:]
        with open(options[options.index('--manifest') + 1]) as manifest_file:
            steps = json.load(manifest_file)['steps']
        result_filename = options[options.index('--result') + 1]
        for index, step in enumerate(steps):
            self.call_args_list.append(call(*args[:runner], step['class'], *step['args']))
            if step['class'] == self.fail_step_class:
                with open(result_filename, 'w') as result_file:
                    json.dump({'status': self.fail_status, 'failed_step': index}, result_file)
                raise JavaRunException(self.fail_status, 'Aborting due to error while executing step.')
        with open(result_filename, 'w') as result_file:
            json.dump({'status': 0, 'failed_step': None}, result_file)

class DataImporterTests(unittest.TestCase):
    '''
    Tests of commands produced by scripts
    '''

    def setUp(self):
        self.maxDiff = None

    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java', new_callable=StepRunnerRecorder)
    def test_full_study_load(self, run_java, locate_jar):
        '''
        Tests java commands full study load produces
        '''
        locate_jar.return_value = "test.jar"

        study_directory = 'test_data/study_es_0'
        args = ['--study_directory', study_directory]
        parsed_args = cbioportalImporter.interface(args)
        study_id = cbioportalImporter.main(parsed_args)

        remove_study_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.RemoveCancerStudy',
            'study_es_0', '--noprogress')
        create_study_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportCancerStudy',
            f'{study_directory}/meta_study.txt', '--noprogress')
        clinical_sample_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData',
            '--meta', f'{study_directory}/meta_clinical_samples.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_clinical_samples.txt', '--noprogress')
        mol_profile_calls = [
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_cna_log2.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_cna_log2.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_expression_median.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_expression_median.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_generic_assay_patient_test.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_generic_assay_patient_test.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_generic_assay_binary.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_generic_assay_binary.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_generic_assay_categorical.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_generic_assay_categorical.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_methylation_hm27.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_methylation_hm27.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_mutational_signature.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_mutational_signature.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_mutations_extended.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_mutations_extended.maf', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceData', '--meta', f'{study_directory}/meta_resource_patient.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_patient.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceData', '--meta', f'{study_directory}/meta_resource_study.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_study.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_treatment_ec50.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_treatment_ec50.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_treatment_ic50.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_treatment_ic50.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_structural_variants.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_structural_variants.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_cna_discrete.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_cna_discrete.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_expression_median_Zscores.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_expression_median_Zscores.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_gsva_scores.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_gsva_scores.txt', '--noprogress'),
                    call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--meta', f'{study_directory}/meta_gsva_pvalues.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{study_directory}/data_gsva_pvalues.txt', '--noprogress'),

        ]
        self.assertCountEqual(run_java.call_args_list, [
            call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportTypesOfCancers', f'{study_directory}/data_cancer_type.txt', 'false', '--noprogress'),
            remove_study_call,
            create_study_call,
            clinical_sample_call,
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceDefinition', '--meta', f'{study_directory}/meta_resource_definition.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_definition.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportResourceData', '--meta', f'{study_directory}/meta_resource_sample.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_resource_sample.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData', '--meta', f'{study_directory}/meta_clinical_patients.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_clinical_patients.txt', '--noprogress'),
            *mol_profile_calls,
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportCopyNumberSegmentData', '--meta', f'{study_directory}/meta_cna_hg19_seg.txt', '--loadMode', 'bulkload', '--data', f'{study_directory}/data_cna_hg19.seg', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportGisticData', '--data', f'{study_directory}/data_gistic_genes_amp.txt', '--study', 'study_es_0', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportGenePanelProfileMap', '--meta', f'{study_directory}/meta_gene_panel_matrix.txt', '--data', f'{study_directory}/data_gene_panel_matrix.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_cna.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_cnaseq.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_custom.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_sequenced.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportSampleList', f'{study_directory}/case_lists/cases_test.txt', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.AddCaseList', 'study_es_0', 'all', '--noprogress'),
        ])

        self.assertTrue(run_java.call_args_list.index(remove_study_call) < run_java.call_args_list.index(create_study_call))
        self.assertTrue(run_java.call_args_list.index(create_study_call) < run_java.call_args_list.index(clinical_sample_call))
        self.assertTrue(all(run_java.call_args_list.index(clinical_sample_call) <  run_java.call_args_list.index(mol_profile_call)
            for mol_profile_call in mol_profile_calls))
        # The study is left UNAVAILABLE until the derived tables are rebuilt
        self.assertNotIn('AVAILABLE', [c.args[-2] for c in run_java.call_args_list])
        self.assertEqual(run_java.runs, 1)
        self.assertEqual(study_id, 'study_es_0')


    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java', new_callable=StepRunnerRecorder)
    def test_incremental_load(self, run_java, locate_jar):
        '''
        Tests java commands incremental load produces
        '''
        locate_jar.return_value = "test.jar"

        data_directory = 'test_data/study_es_0_inc'
        args = ['--data_directory', data_directory]
        parsed_args = cbioportalImporter.interface(args)
        study_id = cbioportalImporter.main(parsed_args)

        clinical_patient_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_clinical_patients.txt', '--loadMode', 'bulkload', '--data', f'{data_directory}/data_clinical_patients.txt', '--noprogress')
        clinical_sample_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportClinicalData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_clinical_samples.txt', '--loadMode', 'bulkload', '--data', f'{data_directory}/data_clinical_samples.txt', '--noprogress')
        mutation_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_mutations_extended.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_mutations_extended.maf', '--noprogress')
        cna_discrete_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_cna_discrete.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_cna_discrete.txt', '--noprogress')
        cna_log2_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_cna_log2.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_cna_log2.txt', '--noprogress')
        expression_median_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_expression_median.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_expression_median.txt', '--noprogress')
        methylation_hm27_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_methylation_hm27.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_methylation_hm27.txt', '--noprogress')
        treatment_ic50_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_treatment_ic50.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_treatment_ic50.txt', '--noprogress')
        sv_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_structural_variants.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_structural_variants.txt', '--noprogress')
        timeline_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportTimelineData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_timeline.txt', '--loadMode', 'bulkload', '--data', f'{data_directory}/data_timeline.txt', '--noprogress')
        case_list_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.UpdateCaseListsSampleIds',
            '--meta', f'{data_directory}/meta_clinical_samples.txt', '--case-lists', f'{data_directory}/case_lists')
        gene_panel_matrix_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportGenePanelProfileMap', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_gene_panel_matrix.txt', '--data', f'{data_directory}/data_gene_panel_matrix.txt', '--noprogress')
        seg_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportCopyNumberSegmentData', '--overwrite-existing',
            '--meta', f'{data_directory}/meta_cna_hg19_seg.txt', '--loadMode', 'bulkload', '--data', f'{data_directory}/data_cna_hg19.seg', '--noprogress')

        make_study_unavailable_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.UpdateCancerStudy',
            'study_es_0', 'UNAVAILABLE', '--noprogress')

        self.assertCountEqual(run_java.call_args_list, [
            call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
            make_study_unavailable_call,
            clinical_patient_call,
            clinical_sample_call,
            mutation_call,
            cna_discrete_call,
            cna_log2_call,
            expression_median_call,
            methylation_hm27_call,
            treatment_ic50_call,
            sv_call,
            timeline_call,
            gene_panel_matrix_call,
            seg_call,
            case_list_call,
        ])

        self.assertTrue(run_java.call_args_list.index(clinical_sample_call) < run_java.call_args_list.index(mutation_call))
        self.assertTrue(run_java.call_args_list.index(clinical_sample_call) < run_java.call_args_list.index(case_list_call))
        self.assertEqual(run_java.call_args_list[1], make_study_unavailable_call)
        self.assertEqual(study_id, 'study_es_0')


    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java', new_callable=StepRunnerRecorder)
    def test_incremental_load_cna_discrete_long(self, run_java, locate_jar):
        '''
        Tests java commands incremental load produces
        '''
        locate_jar.return_value = "test.jar"
        data_directory = 'test_data/study_es_0_inc/cna_long'
        args = ['--data_directory', data_directory]
        parsed_args = cbioportalImporter.interface(args)
        cbioportalImporter.main(parsed_args)
        cna_discrete_long_call = call(*common_part, 'org.mskcc.cbio.portal.scripts.ImportProfileData', '--overwrite-existing',
                '--meta', f'{data_directory}/meta_cna_discrete_long.txt', '--loadMode', 'bulkload', '--update-info', 'False', '--data', f'{data_directory}/data_cna_discrete_long.txt', '--noprogress')

        self.assertEqual(run_java.call_args_list, [
            call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.UpdateCancerStudy', 'study_es_0', 'UNAVAILABLE', '--noprogress'),
            cna_discrete_long_call,
            ])

    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java')
    def test_make_studies_available(self, run_java, locate_jar):
        '''
        Tests java commands that make-studies-available produces
        '''
        locate_jar.return_value = "test.jar"

        args = ['make-studies-available', '--study_ids', 'STUDY1,STUDY2']
        parsed_args = cbioportalImporter.interface(args)
        cbioportalImporter.main(parsed_args)

        self.assertEqual(run_java.call_args_list, [
            call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.UpdateCancerStudy', 'STUDY1', 'AVAILABLE', '--noprogress'),
            call(*common_part, 'org.mskcc.cbio.portal.scripts.UpdateCancerStudy', 'STUDY2', 'AVAILABLE', '--noprogress'),
        ])

    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java')
    def test_remove_samples(self, run_java, locate_jar):
            '''
            Tests java commands removal of sample in study produces
            '''
            locate_jar.return_value = "test.jar"

            args = ['remove-samples', '--study_ids', 'STUDY1,STUDY2', '--sample_ids', 'SAMPLE1,SAMPLE2']
            parsed_args = cbioportalImporter.interface(args)
            cbioportalImporter.main(parsed_args)

            self.assertCountEqual(run_java.call_args_list, [
                call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
                call(*common_part, 'org.mskcc.cbio.portal.scripts.RemoveSamples', '--study_ids', 'STUDY1,STUDY2', '--sample_ids', 'SAMPLE1,SAMPLE2'),
            ])

    @mock.patch('importer.cbioportalImporter.locate_jar')
    @mock.patch('importer.cbioportalImporter.run_java')
    def test_remove_patients(self, run_java, locate_jar):
            '''
            Tests java commands removal of patient in study produces
            '''
            locate_jar.return_value = "test.jar"

            args = ['remove-patients', '--study_ids', 'STUDY1,STUDY2', '--patient_ids', 'PATIENT1,PATIENT2']
            parsed_args = cbioportalImporter.interface(args)
            cbioportalImporter.main(parsed_args)

            self.assertCountEqual(run_java.call_args_list, [
                call(*common_part, 'org.mskcc.cbio.portal.util.VersionUtil',),
                call(*common_part, 'org.mskcc.cbio.portal.scripts.RemovePatients', '--study_ids', 'STUDY1,STUDY2', '--patient_ids', 'PATIENT1,PATIENT2'),
            ])

    @mock.patch('importer.cbioportalImporter.locate_jar', return_value='test.jar')
    def test_version_mismatch_prints_migration_help(self, locate_jar):
        recorder = StepRunnerRecorder(fail_step_class=VERSION_UTIL_CLASS)
        output = io.StringIO()
        with mock.patch('importer.cbioportalImporter.run_java', recorder), \
                mock.patch('importer.cbioportalImporter.OUTPUT_FILE', output):
            with self.assertRaises(JavaRunException) as raised:
                cbioportalImporter.main(cbioportalImporter.interface(['--study_directory', 'test_data/study_es_0']))
        self.assertEqual(raised.exception.failed_step.class_name, VERSION_UTIL_CLASS)
        self.assertEqual(raised.exception.process_return_status, 1)
        self.assertIn('migrate_db.py', output.getvalue())

    @mock.patch('importer.cbioportalImporter.locate_jar', return_value='test.jar')
    def test_incremental_cna_long_failure_is_reported(self, locate_jar):
        recorder = StepRunnerRecorder(fail_step_class='org.mskcc.cbio.portal.scripts.ImportProfileData', fail_status=65)
        with mock.patch('importer.cbioportalImporter.run_java', recorder):
            with self.assertLogs('importer.cbioportalImporter', level='ERROR') as logs:
                with self.assertRaises(JavaRunException) as raised:
                    cbioportalImporter.main(cbioportalImporter.interface(['--data_directory', 'test_data/study_es_0_inc/cna_long']))
        self.assertEqual(raised.exception.failed_step.meta_file_type, MetaFileTypes.CNA_DISCRETE_LONG)
        self.assertIn('the exit status returned by the java process was 65', '\n'.join(logs.output))

    @mock.patch('importer.cbioportalImporter.locate_jar', return_value='test.jar')
    def test_missing_sample_file_runs_nothing(self, locate_jar):
        study_directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, study_directory)
        for filename in ('meta_study.txt', 'meta_cancer_type.txt', 'data_cancer_type.txt'):
            shutil.copy(os.path.join('test_data/study_es_0', filename), study_directory)
        recorder = StepRunnerRecorder()
        with mock.patch('importer.cbioportalImporter.run_java', recorder):
            with self.assertRaisesRegex(RuntimeError, 'No sample attribute file found'):
                cbioportalImporter.main(cbioportalImporter.interface(['--study_directory', study_directory]))
        self.assertEqual(recorder.runs, 0)
        self.assertEqual(recorder.call_args_list, [])

    @mock.patch('importer.cbioportalImporter.locate_jar', return_value='test.jar')
    def test_incremental_preparation_failure_runs_nothing(self, locate_jar):
        recorder = StepRunnerRecorder()
        with mock.patch('importer.cbioportalImporter.run_java', recorder), \
                mock.patch('importer.cbioportalImporter.case_list_update_steps',
                           side_effect=RuntimeError('Invalid case list')):
            with self.assertRaisesRegex(RuntimeError, 'Invalid case list'):
                cbioportalImporter.main(cbioportalImporter.interface(
                    ['--data_directory', 'test_data/study_es_0_inc/cna_long']))
        self.assertEqual(recorder.runs, 0)
        self.assertEqual(recorder.call_args_list, [])

if __name__ == '__main__':
    unittest.main(buffer=True)
