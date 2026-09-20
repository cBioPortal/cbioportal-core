"""End-to-end regressions for skipped preprocessing and clean/no-op controls."""
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from importer import preprocessing, preprocessing_case_lists as cases, validateData


class OmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.study = self.root / 'study'
        self.study.mkdir()
        self.write('meta_study.txt', 'cancer_study_identifier: test\ntype_of_cancer: lung\n'
                   'name: Test\ndescription: Test\nadd_global_case_list: true\n')
        self.metadata('meta_samples.txt', 'data_clinical_sample.txt', 'CLINICAL', 'SAMPLE_ATTRIBUTES')
        self.clinical('data_clinical_sample.txt', ['SAMPLE_ID', 'PATIENT_ID'], [['S1', 'P1'], ['S2', 'P2']])

    def write(self, name, text):
        path = self.study / name
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(text)
        return path

    def metadata(self, name, data, alteration, datatype, stable=None):
        text = (f'cancer_study_identifier: test\ngenetic_alteration_type: {alteration}\n'
                f'datatype: {datatype}\ndata_filename: {data}\n')
        if stable:
            text += (f'stable_id: {stable}\nshow_profile_in_analysis_tab: true\n'
                     'profile_name: Test\nprofile_description: Test\n')
        return self.write(name, text)

    def clinical(self, name, columns, rows):
        return self.write(name, '\n'.join(['#' + '\t'.join(columns)] * 2 +
            ['#' + '\t'.join(['STRING'] * len(columns)), '#' + '\t'.join(['1'] * len(columns)),
             '\t'.join(columns)] + ['\t'.join(row) for row in rows]) + '\n')

    def maf(self, name='data_mutations.txt', samples=('S1',)):
        columns = ['Hugo_Symbol', 'Entrez_Gene_Id', 'Chromosome', 'Start_Position', 'End_Position',
                   'Variant_Classification', 'Variant_Type', 'Reference_Allele', 'Tumor_Seq_Allele1',
                   'Tumor_Seq_Allele2', 'HGVSp_Short', 'Tumor_Sample_Barcode']
        rows = [['GENE', '1', '1', '100', '100', 'Missense_Mutation', 'SNP', 'A', 'A', 'T', 'p.A1V', s]
                for s in samples]
        return self.write(name, '\t'.join(columns) + '\n' + ''.join('\t'.join(r) + '\n' for r in rows))

    def case_list(self, members='S1\tS2'):
        return self.write('case_lists/cases_sequenced.txt', 'cancer_study_identifier: test\n'
                          'stable_id: test_sequenced\ncase_list_name: Sequenced\n'
                          'case_list_description: Sequenced\ncase_list_ids: ' + members + '\n')

    def cli(self, study=None, extra=()):
        study = study or self.study
        before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in study.rglob('*') if p.is_file()}
        result = subprocess.run([sys.executable, validateData.__file__, '-s', str(study), '-n', *extra],
                                capture_output=True, text=True, timeout=60)
        after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in study.rglob('*') if p.is_file()}
        self.assertEqual(before, after, 'Validator changed study data')
        return result.returncode, result.stdout + result.stderr

    def assert_passes(self):
        code, output = self.cli()
        self.assertIn(code, (0, 3), output)

    def test_clean_study_no_marker_needed(self):
        self.assert_passes()

    def test_unmerged_unreferenced_clinical_fails_then_merged_passes(self):
        supp = self.clinical('data_clinical_supp.txt', ['SAMPLE_ID', 'EXTRA'], [['S1', 'yes']])
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        self.assertIn('no referencing clinical meta file', output)
        self.clinical('data_clinical_sample.txt', ['SAMPLE_ID', 'PATIENT_ID', 'EXTRA'],
                      [['S1', 'P1', 'yes'], ['S2', 'P2', 'NA']])
        supp.unlink()
        self.assert_passes()

    def test_referenced_supplemental_still_fails_multiple_clinical_profiles(self):
        self.clinical('data_clinical_supp.txt', ['SAMPLE_ID', 'PATIENT_ID'], [['S1', 'P1']])
        self.metadata('meta_supp.txt', 'data_clinical_supp.txt', 'CLINICAL', 'SAMPLE_ATTRIBUTES')
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        self.assertIn('Multiple sample attribute files', output)

    def test_unreferenced_maf_fails_then_fused_passes(self):
        primary = self.maf()
        self.metadata('meta_mutations.txt', primary.name, 'MUTATION_EXTENDED', 'MAF', 'mutations')
        self.case_list()
        secondary = self.maf('data_mutations_extra.txt', ['S2'])
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        self.assertIn('no referencing mutation meta file', output)
        self.maf(samples=['S1', 'S2'])
        secondary.unlink()
        self.assert_passes()

    def test_same_profile_metadata_collision_fails(self):
        for name in ('data_mutations.txt', 'data_mutations_extra.txt'):
            self.maf(name)
            self.metadata('meta_' + name[5:], name, 'MUTATION_EXTENDED', 'MAF', 'mutations')
        self.case_list()
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        self.assertIn('stable_id repeated', output)

    def test_distinct_called_and_uncalled_profiles_remain_separate(self):
        self.maf()
        self.maf('data_mutations_uncalled.txt', ['S2'])
        self.metadata('meta_called.txt', 'data_mutations.txt', 'MUTATION_EXTENDED', 'MAF', 'mutations')
        self.metadata('meta_uncalled.txt', 'data_mutations_uncalled.txt', 'MUTATION_UNCALLED', 'MAF', 'mutations_uncalled')
        self.case_list('S1')
        self.assert_passes()

    def test_duplicate_maf_fails_then_dedup_passes(self):
        self.maf(samples=['S1', 'S1'])
        self.metadata('meta_mutations.txt', 'data_mutations.txt', 'MUTATION_EXTENDED', 'MAF', 'mutations')
        self.case_list()
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        self.assertIn('Duplicate mutation found', output)
        self.maf()
        self.assert_passes()

    def test_incomplete_sequenced_list_checked_after_reading_mutations(self):
        self.maf(samples=['S1', 'S2'])
        self.metadata('meta_mutations.txt', 'data_mutations.txt', 'MUTATION_EXTENDED', 'MAF', 'mutations')
        self.case_list('S1')
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        self.assertIn("missing from the '_sequenced'", output)
        self.case_list()
        self.assert_passes()

    def test_mutation_membership_does_not_leak_between_studies(self):
        portal = validateData.PortalInstance(None, None, None, None, None, None, None)
        logger = logging.getLogger(self.id())
        with patch.multiple(validateData, mutation_file_sample_ids={'PREVIOUS'},
                            mutation_sample_ids={'PREVIOUS'}, DEFINED_SAMPLE_IDS=None,
                            DEFINED_SAMPLE_ATTRIBUTES=None, PATIENTS_WITH_SAMPLES=None,
                            study_meta_dictionary={}):
            validateData.validate_study(str(self.study), portal, logger, False, False)
            self.assertEqual(set(), validateData.mutation_file_sample_ids)
            self.assertIsNone(validateData.mutation_sample_ids)

    def test_wrong_metadata_type_does_not_cover_file(self):
        self.write('data_timeline_events.txt', 'PATIENT_ID\tSTART_DATE\tEVENT_TYPE\nP1\t0\tTEST\n')
        self.metadata('meta_events.txt', 'data_timeline_events.txt', 'CLINICAL', 'PATIENT_ATTRIBUTES')
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        self.assertIn('no referencing timeline meta file', output)

    def test_archives_and_backup_files_not_misclassified(self):
        for name in ('archive/data_clinical_supp.txt', 'data_mutations_old.txt~', '.data_mutations.txt',
                     'data_clinical_notes.md', 'data_expression.txt'):
            self.write(name, 'unused\n')
        self.assert_passes()

    def test_case_parser_supports_pr1394_headers_and_blank_rows(self):
        for sample_header in ('SAMPLE_ID', 'Sample_ID', 'Sample_Id', 'Tumor_Sample_Barcode'):
            with self.subTest(header=sample_header):
                path = self.write('data_sv.txt', sample_header + '\tOther\nS1\tx\n\nS2\ty\n')
                collector = cases.StagingCaseCollector(path.name)
                for line in path.read_text().splitlines(keepends=True):
                    collector.feed(line)
                self.assertIsNone(collector.error)
                self.assertEqual({'S1', 'S2'}, collector.members)
                self.assertEqual({'S1', 'S2'}, cases.case_list_from_staging_file(self.study, path.name))

    def test_canonical_mutation_file_in_gap_fill_config(self):
        self.maf()
        self.assertIn(('test_sequenced', 'cases_sequenced.txt', 1),
                      list(cases.missing_generated_case_lists(self.study, 'test', ['test_all'])))

    def test_oncotree_optional_values_follow_audit_policy(self):
        nodes = {'LUAD': {'mainType': 'Lung', 'name': 'Lung Adenocarcinoma'}}
        for code in ('NA', 'N/A', '', ' NOT AVAILABLE '):
            self.assertEqual([], list(preprocessing.oncotree_findings({'ONCOTREE_CODE': code}, nodes)))
        self.assertEqual([], list(preprocessing.oncotree_findings(
            {'ONCOTREE_CODE': ' LUAD ', 'CANCER_TYPE': 'NA'}, nodes)))
        self.assertEqual([('CANCER_TYPE', 'Wrong', 'Lung')], list(preprocessing.oncotree_findings(
            {'ONCOTREE_CODE': 'LUAD', 'CANCER_TYPE': 'Wrong'}, nodes)))

    def test_cna_invalid_identifiers_are_errors_and_integer_spellings_collide(self):
        logger = logging.getLogger(self.id())
        portal = validateData.PortalInstance(None, None, {'GENE': [1]}, {'ALIAS': [1]}, None, None, None)
        for value in ('NA', '999', '0', '-1', '1_0', '2147483648', '9' * 5000):
            validator = validateData.CNADiscreteValidator(str(self.study), {'data_filename': 'unused'},
                                                         portal, logger, False, False)
            with self.subTest(value=value[:12]), self.assertLogs(logger, logging.ERROR):
                self.assertIsNone(validator.checkGeneIdentification('GENE', value))
        self.write('data_cna.txt', 'Hugo_Symbol\tEntrez_Gene_Id\tS1\nGENE\t001\t0\nGENE\t1\t2\n')
        with patch.object(validateData, 'DEFINED_SAMPLE_IDS', {'S1'}), self.assertLogs(logger, logging.ERROR) as logs:
            validateData.CNADiscreteValidator(str(self.study), {'data_filename': 'data_cna.txt'},
                                             portal, logger, False, False).validate()
        self.assertIn('Duplicate CNA gene', '\n'.join(logs.output))

    def test_ambiguous_numeric_alias_ids_report_error_without_crashing(self):
        logger = logging.getLogger(self.id())
        portal = validateData.PortalInstance(None, None, {'ONE': [1], 'TWO': [2]},
                                             {'AMBIGUOUS': [1, 2]}, None, None, None)
        validator = validateData.CNADiscreteValidator(str(self.study), {'data_filename': 'unused'},
                                                     portal, logger, False, False)
        with self.assertLogs(logger, logging.WARNING) as logs:
            self.assertIsNone(validator.checkGeneIdentification('AMBIGUOUS', None))
        self.assertIn('(1/2)', '\n'.join(logs.output))
        self.assertTrue(any(record.levelno == logging.ERROR for record in logs.records))

    @unittest.skipUnless(os.environ.get('PREPROCESS_TOOLS_DIR'), 'Pinned preprocessing tools not supplied')
    def test_actual_pinned_tools_clear_all_applicable_omission_checks(self):
        tools = Path(os.environ['PREPROCESS_TOOLS_DIR'])
        # This fixture needs a generated physical _all list, not the virtual
        # list (the upstream generator does not inspect add_global_case_list).
        study_meta = self.study / 'meta_study.txt'
        study_meta.write_text(study_meta.read_text().replace('add_global_case_list: true', 'add_global_case_list: false'))
        def run(name, *args, allowed=(0,)):
            result = subprocess.run([sys.executable, str(tools / name), *map(str, args)],
                                    capture_output=True, text=True, timeout=60)
            self.assertIn(result.returncode, allowed, result.stdout + result.stderr)
        self.maf(samples=['S1', 'S1'])
        self.maf('data_mutations_extra.txt', ['S2'])
        self.metadata('meta_mutations.txt', 'data_mutations.txt', 'MUTATION_EXTENDED', 'MAF', 'mutations')
        self.clinical('data_clinical_supp.txt', ['SAMPLE_ID', 'EXTRA'], [['S1', 'yes'], ['S2', 'no']])
        self.write('data_timeline_events.txt', 'PATIENT_ID\tSTART_DATE\tSTOP_DATE\tEVENT_TYPE\nP1\t0\t1\tSTATUS\n')
        self.write('data_cna.txt', 'Hugo_Symbol\tEntrez_Gene_Id\tS1\nGENE\t1\t0\nGENE\t1\t2\n')
        self.metadata('meta_cna.txt', 'data_cna.txt', 'COPY_NUMBER_ALTERATION', 'DISCRETE', 'cna')
        code, output = self.cli()
        self.assertEqual(code, 1, output)
        for diagnostic in ('Duplicate CNA gene', 'Duplicate mutation', 'no referencing clinical meta',
                           'no referencing mutation meta', 'no referencing timeline meta', 'test_sequenced'):
            self.assertIn(diagnostic, output)
        genes = self.root / 'genes.tsv'
        aliases = self.root / 'aliases.tsv'
        genes.write_text('1\tGENE\n')
        aliases.write_text('1\tALIAS\n')
        run('cna_merge.py', '--gene-table', genes, '--gene-alias', aliases, self.study / 'data_cna.txt')
        run('merge_clinical_supp.py', self.study)
        run('fuse_mafs.py', self.study)
        run('remove_duplicate_maf_variants.py', '--input-maf-file', self.study / 'data_mutations.txt',
            '--strategy', 'first', '--in-place')
        run('add_missing_clinical_meta.py', self.root)
        (self.study / 'case_lists').mkdir()
        run('generate_case_lists.py', '--study-dir', self.study, '--case-list-dir', self.study / 'case_lists',
            '--case-list-config-file', Path(cases.__file__).with_name('case_list_config.tsv'), '--normalize-tcga-barcodes')
        self.assert_passes()
        # OncoTree is an audit, not a transform: it must not clear a finding by
        # changing the input. The same snapshot/rows give matching findings.
        import importlib.util
        spec = importlib.util.spec_from_file_location('pinned_oncotree', tools / 'oncotree_code_converter.py')
        audit = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(audit)
        clinical = self.clinical('data_clinical_sample.txt', ['SAMPLE_ID', 'PATIENT_ID', 'ONCOTREE_CODE', 'CANCER_TYPE'],
                                 [['S1', 'P1', 'LUAD', 'Wrong'], ['S2', 'P2', 'LUAD', 'NA']])
        snapshot = self.root / 'oncotree.json'
        snapshot.write_text('[{"code":"LUAD","mainType":"Lung","name":"Lung"}]')
        before = clinical.read_bytes()
        findings = audit.audit_clinical_file(audit.extract_oncotree_code_mappings_from_oncotree_json(snapshot.read_text()), str(clinical))
        self.assertEqual(1, sum(findings['cancer_type_mismatches'].values()))
        self.assertEqual(before, clinical.read_bytes())
        code, output = self.cli(extra=['--oncotree-file', str(snapshot)])
        self.assertEqual(code, 1, output)
        self.assertIn('OncoTree preprocessing required', output)
        clinical.write_text(clinical.read_text().replace('Wrong', 'Lung'))
        code, output = self.cli(extra=['--oncotree-file', str(snapshot)])
        self.assertIn(code, (0, 3), output)
