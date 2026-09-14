"""Behavioral regressions for validator hot-path optimizations."""
import logging
import unittest
from unittest.mock import Mock
from importer import validateData


class ValidatorPerformanceTests(unittest.TestCase):
    def test_limit_values_keep_diagnostics(self):
        logger = logging.getLogger(self.id())
        validator = validateData.GenericAssayContinuousValidator(
            '.', {'data_filename': 'unused', 'generic_entity_meta_properties': 'NAME'}, None, logger, False, False)
        validator.logger = Mock()
        for value in ('0.25', '> 1', '<\t2', '>\u20031', '', 'NA', '>NA'):
            validator.checkValue(value, 2)
        validator.logger.error.assert_not_called()
        validator.logger.warning.assert_not_called()
        for value in ('0', '1.0'):
            validator.checkValue(value, 2)
        self.assertEqual(2, validator.logger.warning.call_count)
        for value in ('nan', 'inf', '>>1', 'text'):
            validator.checkValue(value, 2)
        self.assertEqual(4, validator.logger.error.call_count)
        self.assertEqual(3, validator.logger.error.call_args.kwargs['extra']['column_number'])

    def test_mutation_duplicate_key_header_order_and_whitespace(self):
        portal = validateData.PortalInstance(None, None, None, None, None, None, None)
        logger = logging.getLogger(self.id())
        validator = validateData.MutationsExtendedValidator(
            '.', {'data_filename': 'unused'}, portal, logger, False, False)
        columns = list(reversed(list(dict.fromkeys(validator.REQUIRED_HEADERS + [
            'Entrez_Gene_Id', 'Chromosome', 'Start_Position', 'End_Position',
            'Variant_Classification', 'Tumor_Seq_Allele2', 'HGVSp_Short', 'Tumor_Sample_Barcode']))))
        # Header ordering is not part of the duplicate identity.
        validator.checkHeader(columns)
        validator.logger = Mock()
        row = ['1' for _ in columns]
        validator.checkDuplicateMutation(row)
        validator.checkDuplicateMutation([' 1 ' for _ in columns])
        duplicate = [call for call in validator.logger.error.call_args_list
                     if 'Duplicate mutation found' in call.args[0]]
        self.assertEqual(1, len(duplicate))
        changed = list(row)
        changed[columns.index('Tumor_Sample_Barcode')] = 'another-sample'
        validator.checkDuplicateMutation(changed)
        self.assertEqual(2, len(validator.seen_mutations))
