"""The preprocessing policy is opt-in; existing validation still runs by default."""
import json
import io
from contextlib import redirect_stderr
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from importer import metaImport, validateData
import unit_tests_validate_data as fixtures


def setUpModule():
    fixtures.setUpModule()


class StrictModeTests(fixtures.PostClinicalDataFileTestCase):
    def setUp(self):
        super().setUp()
        self.portal = validateData.load_portal_info(
            'test_data/api_json_unit_tests', self.logger, offline=True)
        self.logger.setLevel(logging.WARNING)
        self.get_log_records()

    def test_both_clis_keep_maf_checks_independent(self):
        for module in (validateData, metaImport):
            for flags, strict, maf in (([], False, False), (['--strict'], True, False),
                                       (['--strict-mode'], True, False),
                                       (['-m'], False, True), (['--strict', '-m'], True, True)):
                with self.subTest(module=module.__name__, flags=flags), patch.object(
                        sys, 'argv', ['validator', '-s', 'test_data'] + flags):
                    args = module.interface()
                    self.assertEqual(strict, args.strict)
                    self.assertEqual(maf, args.strict_maf_checks)

    def test_strict_offline_combination_exits_with_actionable_error(self):
        # GIVEN either CLI and either spelling of strict mode with offline data.
        for module in (validateData, metaImport):
            for strict_flag in ('--strict', '--strict-mode'):
                for offline_flag in ('-p', '--portal_info_dir'):
                    arguments = ['validator', '-s', 'study', strict_flag,
                                 offline_flag, 'saved-portal-info']
                    with self.subTest(cli=module.__name__, strict=strict_flag,
                                      offline=offline_flag):
                        stderr = io.StringIO()

                        # WHEN the incompatible command is parsed.
                        with patch.object(sys, 'argv', arguments), redirect_stderr(stderr):
                            with self.assertRaises(SystemExit) as result:
                                module.interface()

                        # THEN it fails before validation with a corrective message.
                        self.assertEqual(2, result.exception.code)
                        self.assertIn('requires online OncoTree access', stderr.getvalue())
                        self.assertIn('Remove -p or --strict', stderr.getvalue())

    def test_offline_validation_and_strict_online_modes_remain_available(self):
        # GIVEN supported offline and online combinations; -m is independent.
        cases = (
            (['-p', 'saved-portal-info'], False),
            (['-p', 'saved-portal-info', '-m'], False),
            (['--strict', '-u', 'https://portal.example'], True),
            (['--strict', '-n'], True),
        )
        for module in (validateData, metaImport):
            for flags, expected_strict in cases:
                with self.subTest(cli=module.__name__, flags=flags):
                    # WHEN parsing a supported command.
                    with patch.object(sys, 'argv', ['validator', '-s', 'study'] + flags):
                        args = module.interface()

                    # THEN parsing succeeds and preserves the requested mode.
                    self.assertEqual(expected_strict, args.strict)

    def test_cli_passes_mode_into_study_validation(self):
        # GIVEN an online CLI invocation and a stubbed portal response.
        for strict in (False, True):
            args = validateData.interface(['-s', 'test_data', '-u',
                'https://portal.example'] + (['--strict'] if strict else []))
            logger = logging.getLogger(validateData.__name__)
            original_handlers = list(logger.handlers)
            try:
                with patch.object(validateData, 'load_portal_info', return_value=self.portal), \
                        patch.object(validateData, 'validate_study') as validate:
                    # WHEN the validation entry point runs.
                    validateData.main_validate(args)

                    # THEN study validation receives the requested strict setting.
                    self.assertEqual(strict, validate.call_args.args[1].strict_mode)
            finally:
                for handler in logger.handlers[:]:
                    if handler not in original_handlers:
                        logger.removeHandler(handler)
                        handler.close()

    def test_invalid_cna_identifier_policy_is_opt_in(self):
        for strict in (False, True):
            for identifier in ('-1', '2147483648'):
                with self.subTest(strict=strict, identifier=identifier):
                    self.portal.strict_mode = strict
                    validator = validateData.CNADiscreteValidator('test_data',
                        {'data_filename': 'data_cna_duplicate_gene.txt'}, self.portal,
                        self.logger, False, False)
                    validator.checkGeneIdentification(entrez_id=identifier)
                    records = self.get_log_records()
                    self.assertEqual(identifier == '-1',
                        any(record.levelno >= logging.ERROR for record in records))
                    self.assertEqual(strict and identifier == '-1', any('Invalid CNA Entrez gene id' in record.getMessage()
                        for record in records))

    def test_duplicate_cna_is_warning_by_default_and_error_in_strict_mode(self):
        for strict, level in ((False, logging.WARNING), (True, logging.ERROR)):
            with self.subTest(strict=strict):
                self.portal.strict_mode = strict
                records = self.validate('data_cna_duplicate_gene.txt',
                    validateData.CNADiscreteValidator, portal_instance=self.portal)
                self.assertEqual([level], [record.levelno for record in records])

    def test_existing_cna_errors_still_apply_in_both_modes(self):
        self.logger.setLevel(logging.ERROR)
        for strict in (False, True):
            with self.subTest(strict=strict):
                self.portal.strict_mode = strict
                records = self.validate('data_cna_invalid_values.txt',
                    validateData.CNADiscreteValidator, portal_instance=self.portal)
                self.assertEqual(6, len(records))

    def test_unnormalized_tcga_source_is_an_error_only_in_strict_mode(self):
        # GIVEN an existing RPPA list and a source with an unnormalized barcode.
        files = {
            'meta_study.txt': 'cancer_study_identifier: spam\ntype_of_cancer: brca\n'
                'name: Spam\ndescription: Test\nadd_global_case_list: true\n',
            'meta_samples.txt': (
                'cancer_study_identifier: spam\n'
                'genetic_alteration_type: CLINICAL\n'
                'datatype: SAMPLE_ATTRIBUTES\n'
                'data_filename: data_samples.txt\n'
            ),
            'data_samples.txt': (
                '#Patient\tSample\n'
                '#Patient\tSample\n'
                '#STRING\tSTRING\n'
                '#1\t1\n'
                'PATIENT_ID\tSAMPLE_ID\n'
                'TCGA-AA-1234\tTCGA-AA-1234-01\n'
            ),
            'data_rppa.txt': 'Composite.Element.REF\tTCGA-AA-1234-01A\nTP53|p53\t1\n',
            'case_lists/rppa.txt': 'cancer_study_identifier: spam\nstable_id: spam_rppa\n'
                'case_list_name: RPPA\ncase_list_description: RPPA samples\n'
                'case_list_category: all_cases_with_rppa_data\ncase_list_ids: TCGA-AA-1234-01\n',
        }

        for strict in (False, True):
            with self.subTest(strict=strict), fixtures.temp_inputfolder(files) as directory:
                self.portal.strict_mode = strict

                # WHEN the complete study validator runs in either mode.
                validateData.validate_study(directory, self.portal, self.logger, False, False)

                # THEN only strict mode reports this barcode preprocessing error.
                errors = [record.getMessage() for record in self.get_log_records()
                          if record.levelno >= logging.ERROR]
                self.assertEqual(strict, any('unnormalized or malformed TCGA' in m for m in errors))
                if strict:
                    self.assertTrue(any('data_rppa.txt' in m and 'TCGA-AA-1234-01A' in m
                                        and 'Normalize IDs consistently' in m for m in errors))

    def test_study_preprocessing_checks_and_reference_access_are_opt_in(self):
        files = {
            'meta_study.txt': 'cancer_study_identifier: spam\ntype_of_cancer: brca\n'
                'name: Spam\ndescription: Test\nadd_global_case_list: true\n',
            'meta_samples.txt': 'cancer_study_identifier: spam\ngenetic_alteration_type: CLINICAL\n'
                'datatype: SAMPLE_ATTRIBUTES\ndata_filename: data_samples.txt\n',
            'data_samples.txt': '#Patient\tSample\tOncotree\n#Patient\tSample\tOncotree\n'
                '#STRING\tSTRING\tSTRING\n#1\t1\t1\nPATIENT_ID\tSAMPLE_ID\tONCOTREE_CODE\n'
                'Patient1\tS1\tRETIRED\nPatient2\tS2\tNA\n',
            'data_clinical_supp.txt': 'SAMPLE_ID\tEXTRA\nS1\tvalue\n',
            'data_rppa.txt': 'Composite.Element.REF\tS1\nTP53|p53\t1\n',
        }
        maf_lines = Path('test_data/mutations/data_mutations_name_swissprot.maf').read_text().splitlines()
        row = maf_lines[1].split('\t')
        row[maf_lines[0].split('\t').index('Tumor_Sample_Barcode')] = 'S1'
        files['data_mutations.txt'] = maf_lines[0] + '\n' + '\t'.join(row) + '\n'
        files['meta_mutations.txt'] = ('cancer_study_identifier: spam\n'
            'genetic_alteration_type: MUTATION_EXTENDED\ndatatype: MAF\n'
            'stable_id: mutations\nshow_profile_in_analysis_tab: true\n'
            'profile_name: Mutations\nprofile_description: Test mutations\n'
            'data_filename: data_mutations.txt\n')
        files['case_lists/custom.txt'] = ('cancer_study_identifier: spam\nstable_id: spam_sequenced\n'
            'case_list_name: Sequenced\ncase_list_description: Sequenced samples\n'
            'case_list_category: all_cases_with_mutation_data\ncase_list_ids: S2\n')
        reference = json.dumps([{'code': 'LUAD', 'mainType': 'Lung',
                                 'name': 'Lung Adenocarcinoma'}]).encode()
        for strict in (False, True):
            with self.subTest(strict=strict), fixtures.temp_inputfolder(files) as directory, patch.object(
                    validateData.OncotreeReference, 'read', return_value=(reference, 'test')) as read:
                self.portal.strict_mode = strict
                validateData.validate_study(directory, self.portal, self.logger, False, False)
                messages = '\n'.join(record.getMessage() for record in self.get_log_records())
                self.assertEqual(strict, 'Clinical data file has no referencing' in messages)
                self.assertEqual(strict, 'Missing generated case list' in messages)
                self.assertEqual(strict, 'RETIRED' in messages)
                self.assertEqual(strict, "are missing from the '_sequenced' case list" in messages)
                self.assertEqual(int(strict), read.call_count)


if __name__ == '__main__':
    unittest.main()
