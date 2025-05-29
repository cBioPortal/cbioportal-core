#!/usr/bin/env python3

'''
Copyright (c) 2016 The Hyve B.V.
This code is licensed under the GNU Affero General Public License (AGPL),
version 3, or (at your option) any later version.
'''

import unittest
import logging
import tempfile
import os
import shutil
import time
import difflib

from importer import validateData

try:
    WindowsError
except NameError:
    WindowsError = None

# globals:
PORTAL_INFO_DIR = 'test_data/api_json_system_tests'

class ValidateDataSystemTester(unittest.TestCase):
    '''Test cases around running the complete validateData script

    (such as "does it return the correct exit status?" or "does it generate
    the html report when requested?", etc)
    '''

    def setUp(self):
        _resetClassVars()

        # Prepare global variables related to sample profiled for mutations and gene panels
        self.mutation_sample_ids = None
        self.mutation_file_sample_ids = set()
        self.fusion_file_sample_ids = set()

    def tearDown(self):
        """Close logging handlers after running validator and remove tmpdir."""
        # restore original function
        validateData.mutation_sample_ids = None
        validateData.mutation_file_sample_ids = set()
        validateData.fusion_file_sample_ids = set()

        # get the logger used in validateData.main_validate()
        validator_logger = logging.getLogger(validateData.__name__)
        # flush and close all handlers of this logger
        for logging_handler in validator_logger.handlers:
            logging_handler.close()
        # remove the handlers from the logger to reset it
        validator_logger.handlers = []
        super(ValidateDataSystemTester, self).tearDown()

    def assertFileGenerated(self, tmp_file_name, expected_file_name):
        """Assert that a file has been generated with the expected contents."""
        self.assertTrue(os.path.exists(tmp_file_name))
        with open(tmp_file_name, 'r') as out_file, \
             open(expected_file_name, 'r') as ref_file:
            base_filename = os.path.basename(tmp_file_name)
            diff_result = difflib.context_diff(
                    ref_file.readlines(),
                    out_file.readlines(),
                    fromfile='Expected {}'.format(base_filename),
                    tofile='Generated {}'.format(base_filename))
        diff_line_list = list(diff_result)
        self.assertEqual(diff_line_list, [],
                         msg='\n' + ''.join(diff_line_list))
        # remove temp file if all is fine:
        try:
            os.remove(tmp_file_name)
        except WindowsError:
            # ignore this Windows specific error...probably happens because of virus scanners scanning the temp file...
            pass        

    def test_exit_status_success(self):
        '''study 0 : no errors, expected exit_status = 0.

        If there are errors, the script should return
                0: 'succeeded',
                1: 'failed',
                2: 'not performed as problems occurred',
                3: 'succeeded with warnings'
        '''

        # build up the argument list
        print("===study 0")
        args = ['--study_directory', 'test_data/study_es_0/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v']
        # execute main function with arguments provided as if from sys.argv
        args = validateData.interface(args)
        exit_status = validateData.main_validate(args)
        self.assertEqual(0, exit_status)

    def test_exit_status_failure(self):
        '''study 1 : errors, expected exit_status = 1.'''
        #Build up arguments and run
        print("===study 1")
        args = ['--study_directory', 'test_data/study_es_1/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v']
        args = validateData.interface(args)
        # Execute main function with arguments provided through sys.argv
        exit_status = validateData.main_validate(args)
        self.assertEqual(1, exit_status)

    @unittest.SkipTest
    # FIXME Study test_data/study_es_invalid does not exist
    def test_exit_status_invalid(self):
        '''test to fail: give wrong hugo file, or let a meta file point to a non-existing data file, expected exit_status = 2.'''
        #Build up arguments and run
        print("===study invalid")
        args = ['--study_directory', 'test_data/study_es_invalid/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v']
        args = validateData.interface(args)
        # Execute main function with arguments provided through sys.argv
        exit_status = validateData.main_validate(args)
        self.assertEqual(2, exit_status)

    def test_exit_status_warnings(self):
        '''study 3 : warnings only, expected exit_status = 3.'''
        # data_filename: test
        #Build up arguments and run
        print("===study 3")
        args = ['--study_directory', 'test_data/study_es_3/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v']
        args = validateData.interface(args)
        # Execute main function with arguments provided through sys.argv
        exit_status = validateData.main_validate(args)
        self.assertEqual(3, exit_status)

    def test_html_output(self):
        '''
        Test if html file is correctly generated when 'html_table' is given
        '''
        #Build up arguments and run
        out_file_name = 'test_data/study_es_0/result_report.html~'
        args = ['--study_directory', 'test_data/study_es_0/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v',
                '--html_table', out_file_name]
        args = validateData.interface(args)
        # Execute main function with arguments provided through sys.argv
        exit_status = validateData.main_validate(args)
        self.assertEqual(0, exit_status)
        self.assertFileGenerated(out_file_name,
                                 'test_data/study_es_0/result_report.html')

    def test_portal_mismatch(self):
        '''Test if validation fails when data contradicts the portal.'''
        # build up arguments and run
        argv = ['--study_directory', 'test_data/study_portal_mismatch',
                '--portal_info_dir', PORTAL_INFO_DIR, '--verbose']
        parsed_args = validateData.interface(argv)
        exit_status = validateData.main_validate(parsed_args)
        # flush logging handlers used in validateData
        validator_logger = logging.getLogger(validateData.__name__)
        for logging_handler in validator_logger.handlers:
            logging_handler.flush()
        # expecting only warnings (about the skipped checks), no errors
        self.assertEqual(exit_status, 1)

    def test_no_portal_checks(self):
        '''Test if validation skips portal-specific checks when instructed.'''
        # build up arguments and run
        argv = ['--study_directory', 'test_data/study_portal_mismatch',
                '--verbose',
                '--no_portal_checks']
        parsed_args = validateData.interface(argv)
        exit_status = validateData.main_validate(parsed_args)
        # flush logging handlers used in validateData
        validator_logger = logging.getLogger(validateData.__name__)
        for logging_handler in validator_logger.handlers:
            logging_handler.flush()
        # expecting only warnings (about the skipped checks), no errors
        self.assertEqual(exit_status, 3)

    def test_problem_in_clinical(self):
        '''Test whether the script aborts if the sample file cannot be parsed.

        Further files cannot be validated in this case, as all sample IDs will
        be undefined. Validate if the script is giving the proper error.
        '''
        # build the argument list
        out_file_name = 'test_data/study_wr_clin/result_report.html~'
        print('==test_problem_in_clinical==')
        args = ['--study_directory', 'test_data/study_wr_clin/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v',
                '--html_table', out_file_name]
        # execute main function with arguments provided as if from sys.argv
        args = validateData.interface(args)
        exit_status = validateData.main_validate(args)
        self.assertEqual(1, exit_status)
        # TODO - set logger in main_validate and read out buffer here to assert on nr of errors
        self.assertFileGenerated(out_file_name,
                                 'test_data/study_wr_clin/result_report.html')

    def test_various_issues(self):
        '''Test if output is generated for a mix of errors and warnings.

        This includes HTML ouput, the error line file and the exit status.
        '''
        # build the argument list
        html_file_name = 'test_data/study_various_issues/result_report.html~'
        error_file_name = 'test_data/study_various_issues/error_file.txt~'
        args = ['--study_directory', 'test_data/study_various_issues/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v',
                '--html_table', html_file_name,
                '--error_file', error_file_name]
        args = validateData.interface(args)
        # execute main function with arguments provided through sys.argv
        exit_status = validateData.main_validate(args)
        # flush logging handlers used in validateData
        validator_logger = logging.getLogger(validateData.__name__)
        for logging_handler in validator_logger.handlers:
            logging_handler.flush()
        # should fail because of various errors in addition to warnings
        self.assertEqual(1, exit_status)
        # In MAF files (mutation data) there is a column called
        # "Matched_Norm_Sample_Barcode". The respective metadata file supports
        # giving a list of sample codes against which this column is validated.
        # This and other errors are expected in these output files.
        self.assertFileGenerated(
                html_file_name,
                'test_data/study_various_issues/result_report.html')
        self.assertFileGenerated(
                error_file_name,
                'test_data/study_various_issues/error_file.txt')

    def test_files_with_quotes(self):
        '''
        Tests the scenario where data files contain quotes. This should give errors.
        '''
        #Build up arguments and run
        out_file_name = 'test_data/study_quotes/result_report.html~'
        print('==test_files_with_quotes==')
        args = ['--study_directory', 'test_data/study_quotes/',
                '--portal_info_dir', PORTAL_INFO_DIR, '-v',
                '--html_table', out_file_name]
        args = validateData.interface(args)
        # Execute main function with arguments provided through sys.argv
        exit_status = validateData.main_validate(args)
        # should fail because of errors with quotes
        self.assertEqual(1, exit_status)
        self.assertFileGenerated(out_file_name,
                                 'test_data/study_quotes/result_report.html')

    def test_resource_definition_custom_metadata(self):
        '''Test validation of CUSTOM_METADATA in resource definition files.'''
        study_dir = 'tests/test_data/study_resource_custom_metadata/'
        
        # Test case 1: File with valid and invalid JSON metadata
        # Expecting errors due to invalid JSON, so exit_status = 1
        print("=== test_resource_definition_custom_metadata")
        args = ['--study_directory', study_dir,
                '--portal_info_dir', PORTAL_INFO_DIR, '-v'] # PORTAL_INFO_DIR is a global in the test file
        parsed_args = validateData.interface(args)
        exit_status = validateData.main_validate(parsed_args)
        
        # Assert that there's an error (exit_status 1) because of the invalid JSON entries
        self.assertEqual(exit_status, 1, 
                         "Expected exit_status 1 for resource definition with invalid JSON, but got " + str(exit_status))

        # Optional: Add more specific assertions if the validator logs errors to a file that can be checked.
        # For instance, if errors are logged to an error file:
        error_file_path = os.path.join(study_dir, 'errors.txt') # Assuming errors are logged here by the validator
        args_with_error_file = args + ['--error_file', error_file_path]
        parsed_args_with_error_file = validateData.interface(args_with_error_file)
        validateData.main_validate(parsed_args_with_error_file)
        self.assertTrue(os.path.exists(error_file_path))
        with open(error_file_path, 'r') as f:
            errors = f.read()
            self.assertIn("RES_INVALID_JSON", errors) # Check if specific error messages are present
            self.assertIn("Invalid JSON in CUSTOM_METADATA column", errors)
            self.assertIn("RES_INVALID_JSON_UNQUOTED_KEY", errors)
            self.assertIn("RES_INVALID_JSON_TRAILING_COMMA_IN_ARRAY", errors)
        if os.path.exists(error_file_path):
           os.remove(error_file_path)

def _resetClassVars():
    """Reset the state of classes that check mulitple files of the same type.
    
    GsvaWiseFileValidator classes check
    consistency between multiple data files by collecting information in class variables.
    This implementation is not consistent with the unit test environment that simulates
    different studies to be loaded. To ensure real-world fucntionality the class variables 
    should be reset before each unit test that tests multi file consistency."""

    for c in [ validateData.GsvaWiseFileValidator ]:
        c.prior_validated_sample_ids = None
        c.prior_validated_feature_ids = None
        c.prior_validated_header = None

if __name__ == '__main__':
    unittest.main(buffer=True)
