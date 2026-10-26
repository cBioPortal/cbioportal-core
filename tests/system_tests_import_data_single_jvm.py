#!/usr/bin/env python3

'''
This code is licensed under the GNU Affero General Public License (AGPL),
version 3, or (at your option) any later version.
'''

import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock
from importer import cbioportalImporter
from importer.cbioportal_common import JavaRunException, MetaFileTypes, SINGLE_JVM_ENV_VAR

common_part = ('-Dspring.profiles.active=dbcp', '-cp', 'test.jar')
STEP_RUNNER_CLASS = 'org.mskcc.cbio.portal.scripts.ImportStepRunner'
VERSION_UTIL_CLASS = 'org.mskcc.cbio.portal.util.VersionUtil'
REMOVE_STUDY_CLASS = 'org.mskcc.cbio.portal.scripts.RemoveCancerStudy'
IMPORT_STUDY_CLASS = 'org.mskcc.cbio.portal.scripts.ImportCancerStudy'
IMPORT_CANCER_TYPE_CLASS = 'org.mskcc.cbio.portal.scripts.ImportTypesOfCancers'

single_jvm_on = mock.patch.dict(os.environ, {SINGLE_JVM_ENV_VAR: '1'})
single_jvm_off = mock.patch.dict(os.environ, {SINGLE_JVM_ENV_VAR: '0'})


class ManifestRecorder:
    '''run_java stand-in that records each ImportStepRunner manifest, and can fail a step.'''

    def __init__(self, fail_step_class=None, fail_status=1):
        self.manifests = []
        self.fail_step_class = fail_step_class
        self.fail_status = fail_status

    def __call__(self, *args):
        self.call_args = args
        assert args[:len(common_part)] == common_part
        assert args[len(common_part)] == STEP_RUNNER_CLASS
        options = args[len(common_part) + 1:]
        manifest_filename = options[options.index('--manifest') + 1]
        result_filename = options[options.index('--result') + 1]
        with open(manifest_filename) as manifest_file:
            manifest = json.load(manifest_file)
        self.manifests.append(manifest)
        steps = [(step['class'], *step['args']) for step in manifest['steps']]
        for index, step in enumerate(steps):
            if step[0] == self.fail_step_class:
                with open(result_filename, 'w') as result_file:
                    json.dump({'status': self.fail_status, 'failed_step': index}, result_file)
                raise JavaRunException(self.fail_status, 'Aborting due to error while executing step.')
        with open(result_filename, 'w') as result_file:
            json.dump({'status': 0, 'failed_step': None}, result_file)

    def steps(self):
        return [(step['class'], *step['args']) for manifest in self.manifests for step in manifest['steps']]


def per_step_calls(run_java):
    '''The class and arguments of each separate java call, without the JVM options.'''
    calls = []
    for java_call in run_java.call_args_list:
        assert java_call.args[:len(common_part)] == common_part
        calls.append(java_call.args[len(common_part):])
    return calls


class SingleJvmImporterTests(unittest.TestCase):
    '''
    Tests that running a directory import in one JVM runs the same steps, in the
    same order, as running each step in its own JVM
    '''

    def setUp(self):
        self.maxDiff = None
        locate_jar = mock.patch('importer.cbioportalImporter.locate_jar', return_value='test.jar')
        locate_jar.start()
        self.addCleanup(locate_jar.stop)

    def import_per_step(self, args):
        with single_jvm_off, mock.patch('importer.cbioportalImporter.run_java') as run_java:
            result = cbioportalImporter.main(cbioportalImporter.interface(args))
        return result, per_step_calls(run_java)

    def import_single_jvm(self, args, recorder=None):
        recorder = recorder or ManifestRecorder()
        with single_jvm_on, mock.patch('importer.cbioportalImporter.run_java', side_effect=recorder) as run_java:
            result = cbioportalImporter.main(cbioportalImporter.interface(args))
        self.assertEqual(run_java.call_count, 1)
        return result, recorder.steps()

    def test_full_study_load_runs_same_steps(self):
        args = ['--study_directory', 'test_data/study_es_0']
        per_step_result, per_step = self.import_per_step(args)
        single_jvm_result, single_jvm = self.import_single_jvm(args)

        self.assertEqual(single_jvm, per_step)
        self.assertEqual(single_jvm[0], (VERSION_UTIL_CLASS,))
        self.assertGreater(len(single_jvm), 20)
        self.assertEqual(single_jvm_result, per_step_result)

    def test_incremental_load_runs_same_steps(self):
        for data_directory in ('test_data/study_es_0_inc', 'test_data/study_es_0_inc/cna_long'):
            with self.subTest(data_directory=data_directory):
                args = ['--data_directory', data_directory]
                per_step_result, per_step = self.import_per_step(args)
                single_jvm_result, single_jvm = self.import_single_jvm(args)

                self.assertEqual(single_jvm, per_step)
                self.assertEqual(single_jvm_result, per_step_result)

    def test_single_jvm_is_the_default(self):
        args = ['--study_directory', 'test_data/study_es_0']
        _, per_step = self.import_per_step(args)
        for value in (None, '', '1', 'true'):
            with self.subTest(value=value):
                environ = {k: v for k, v in os.environ.items() if k != SINGLE_JVM_ENV_VAR}
                if value is not None:
                    environ[SINGLE_JVM_ENV_VAR] = value
                recorder = ManifestRecorder()
                with mock.patch.dict(os.environ, environ, clear=True), \
                        mock.patch('importer.cbioportalImporter.run_java', side_effect=recorder) as run_java:
                    cbioportalImporter.main(cbioportalImporter.interface(args))
                self.assertEqual(run_java.call_count, 1)
                self.assertEqual(recorder.steps(), per_step)

    def test_opt_out_runs_one_jvm_per_step(self):
        args = ['--study_directory', 'test_data/study_es_0']
        for value in ('0', 'false', 'no', 'NO'):
            with self.subTest(value=value):
                with mock.patch.dict(os.environ, {SINGLE_JVM_ENV_VAR: value}), \
                        mock.patch('importer.cbioportalImporter.run_java') as run_java:
                    cbioportalImporter.main(cbioportalImporter.interface(args))
                classes = [step[0] for step in per_step_calls(run_java)]
                self.assertNotIn(STEP_RUNNER_CLASS, classes)
                self.assertGreater(len(classes), 20)

    def test_subcommands_still_run_one_jvm_per_step(self):
        args = ['make-studies-available', '--study_ids', 'STUDY1,STUDY2']
        _, per_step = self.import_per_step(args)
        with single_jvm_on, mock.patch('importer.cbioportalImporter.run_java') as run_java:
            cbioportalImporter.main(cbioportalImporter.interface(args))
        self.assertEqual(per_step_calls(run_java), per_step)

    def test_version_mismatch_prints_migration_help(self):
        recorder = ManifestRecorder(fail_step_class=VERSION_UTIL_CLASS)
        output = io.StringIO()
        with mock.patch('importer.cbioportalImporter.OUTPUT_FILE', output):
            with self.assertRaises(JavaRunException) as raised:
                self.import_single_jvm(['--study_directory', 'test_data/study_es_0'], recorder)
        self.assertEqual(raised.exception.failed_step.class_name, VERSION_UTIL_CLASS)
        self.assertEqual(raised.exception.java_process_status, 1)
        self.assertIn('migrate_db.py', output.getvalue())

    def test_incremental_cna_long_failure_is_reported(self):
        cna_long_directory = 'test_data/study_es_0_inc/cna_long'
        args = ['--data_directory', cna_long_directory]
        recorder = ManifestRecorder(fail_step_class='org.mskcc.cbio.portal.scripts.ImportProfileData', fail_status=65)
        with self.assertLogs('importer.cbioportalImporter', level='ERROR') as logs:
            with self.assertRaises(JavaRunException) as raised:
                self.import_single_jvm(args, recorder)
        self.assertEqual(raised.exception.failed_step.meta_file_type, MetaFileTypes.CNA_DISCRETE_LONG)
        self.assertIn('the exit status returned by the java process was 65', '\n'.join(logs.output))

        def fail_on_cna_long(*java_args):
            if 'org.mskcc.cbio.portal.scripts.ImportProfileData' in java_args:
                raise JavaRunException(65, 'Aborting due to error while executing step.')
        with single_jvm_off, mock.patch('importer.cbioportalImporter.run_java', side_effect=fail_on_cna_long):
            with self.assertLogs('importer.cbioportalImporter', level='ERROR') as logs:
                with self.assertRaises(JavaRunException) as raised:
                    cbioportalImporter.main(cbioportalImporter.interface(args))
        self.assertEqual(raised.exception.failed_step.meta_file_type, MetaFileTypes.CNA_DISCRETE_LONG)
        self.assertIn('the exit status returned by the java process was 65', '\n'.join(logs.output))

    def test_steps_before_a_missing_sample_file_still_run(self):
        study_directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, study_directory)
        for filename in ('meta_study.txt', 'meta_cancer_type.txt', 'data_cancer_type.txt'):
            shutil.copy(os.path.join('test_data/study_es_0', filename), study_directory)
        args = ['--study_directory', study_directory]
        expected = [
            (VERSION_UTIL_CLASS,),
            (IMPORT_CANCER_TYPE_CLASS, os.path.join(study_directory, 'data_cancer_type.txt'), 'false', '--noprogress'),
            (REMOVE_STUDY_CLASS, 'study_es_0', '--noprogress'),
            (IMPORT_STUDY_CLASS, os.path.join(study_directory, 'meta_study.txt'), '--noprogress'),
        ]

        with single_jvm_off, mock.patch('importer.cbioportalImporter.run_java') as run_java:
            with self.assertRaisesRegex(RuntimeError, 'No sample attribute file found'):
                cbioportalImporter.main(cbioportalImporter.interface(args))
        self.assertEqual(per_step_calls(run_java), expected)

        recorder = ManifestRecorder()
        with single_jvm_on, mock.patch('importer.cbioportalImporter.run_java', side_effect=recorder):
            with self.assertRaisesRegex(RuntimeError, 'No sample attribute file found'):
                cbioportalImporter.main(cbioportalImporter.interface(args))
        self.assertEqual(recorder.steps(), expected)


if __name__ == '__main__':
    unittest.main(buffer=True)
