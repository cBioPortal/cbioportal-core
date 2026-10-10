#!/usr/bin/env python3

"""Tests for the offline legacy-WSI to resource file converter.

The emitted files are run through the real validateData validators, and the
committed Java integration-test fixture is checked to be current converter
output.
"""

import base64
import json
import logging.handlers
import shutil
import textwrap
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from importer import cbioportal_common
from importer import convertWsiToResources as converter
from importer import validateData


FIXTURE_DIR = Path('test_data/wsi_convert')
STUDY_FIXTURE_DIR = FIXTURE_DIR / 'study'
JAVA_FIXTURE_DIR = Path('../src/test/resources/wsi_resources')
BASE_URL = 'https://portal.example.org/cbioportal'
STUDY_ID = 'wsi_convert_test'
SAMPLE_TO_PATIENT = {
    'WSI-P1-S1': 'WSI-P1',
    'WSI-P1-S2': 'WSI-P1',
    'WSI-P2-S1': 'WSI-P2',
    'WSI+P3-S1': 'WSI+P3',
}
EXPECTED_FILES = [
    'data_resource_definition.txt',
    'data_resource_patient.txt',
    'data_resource_sample.txt',
    'meta_resource_definition.txt',
    'meta_resource_patient.txt',
    'meta_resource_sample.txt',
]
# Fixture slides by their opaque slide key.
SLIDE_LABELS = {
    '2c96f13783250ad2c6bcfcd5b7c3ef22': 'slide-1',
    '90f033ed369247b19bd7a252e7ae86c8': 'slide-2',
    'f9bce50b1498c94fa0fa6cce809f64f2': 'slide-3',
    'c66ee336f70e8b59e48bd7afbf0e606d': 'slide-4',
    'a72487fd68ef59b99fcee054e738f893': 'slide-5',
    '3474b861eb683902b420f4ee95dfe0fa': 'slide-6',
}
# SEALED_SOURCE of slide-1: the contract wsi-serving-v6 test vector.
SEALED_SOURCE_VECTOR = (
    'AAECAwQFBgcICQoLNpOTYnY8HVb-KcPzu1F5HPykr7D0YY_UhVbbyjOFRlxC63fxCt09YO1aYC-phb85wDhN5PPPpC0X46RS'
    'D0K0bRRgSptRb9wDiqMLtftFQ6VpBGfGILaddEV_s-Zsmpp28fG5Z3XTNWnyoBRa9qfr9t209wS7V-LhGl8i')


def data_rows(path):
    """Return the non-comment rows of a TSV file as lists of raw cells."""
    return [line.split('\t') for line in Path(path).read_text(encoding='utf-8').splitlines()
            if not line.startswith('#')]


def rows_by_slide(path):
    """Return the resource rows of a file keyed by the fixture label of their slide key."""
    header, *rows = data_rows(path)
    records = [dict(zip(header, row)) for row in rows]
    return {SLIDE_LABELS[json.loads(record['METADATA'])['slide_key']]: record for record in records}


class ConverterTestCase(unittest.TestCase):

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.out = Path(self.tmp.name) / 'out'

    def tearDown(self):
        self.tmp.cleanup()

    def convert(self, meta=FIXTURE_DIR / 'meta_wsi.txt', base_url=BASE_URL + '/', study_dir=None):
        return converter.convert(meta, self.out, base_url, study_dir)

    def write_legacy(self, rows, header=None):
        """Write a legacy pair made of the fixture's comment rows and the given data rows."""
        source = (FIXTURE_DIR / 'data_wsi.txt').read_text(encoding='utf-8').splitlines()
        legacy = Path(self.tmp.name) / 'legacy'
        legacy.mkdir(exist_ok=True)
        shutil.copy(FIXTURE_DIR / 'meta_wsi.txt', legacy / 'meta_wsi.txt')
        lines = source[:4] + [header if header is not None else source[4]] + rows
        (legacy / 'data_wsi.txt').write_text('\n'.join(lines) + '\n', encoding='utf-8')
        return legacy / 'meta_wsi.txt'

    def copy_study(self):
        """Copy the study-like fixture (clinical sample and patient files) to a scratch dir."""
        study = Path(self.tmp.name) / 'study'
        shutil.copytree(STUDY_FIXTURE_DIR, study)
        return study

    def fixture_rows(self):
        return (FIXTURE_DIR / 'data_wsi.txt').read_text(encoding='utf-8').splitlines()[5:]


class ConvertedOutputTestCase(ConverterTestCase):

    def test_writes_expected_file_pairs(self):
        written = self.convert()
        self.assertEqual(EXPECTED_FILES, sorted(path.name for path in written))
        self.assertEqual(EXPECTED_FILES, sorted(path.name for path in self.out.iterdir()))
        meta = (self.out / 'meta_resource_sample.txt').read_text()
        self.assertEqual(
            'cancer_study_identifier: wsi_convert_test\nresource_type: SAMPLE\n'
            'data_filename: data_resource_sample.txt\n', meta)

    def test_sample_and_patient_rows(self):
        self.convert()
        sample_header = data_rows(self.out / 'data_resource_sample.txt')[0]
        self.assertEqual(['PATIENT_ID', 'SAMPLE_ID', 'RESOURCE_ID', 'URL', 'DISPLAY_NAME', 'TYPE',
                          'METADATA'], sample_header)
        samples = rows_by_slide(self.out / 'data_resource_sample.txt')
        patients = rows_by_slide(self.out / 'data_resource_patient.txt')
        self.assertEqual(['slide-1', 'slide-2', 'slide-3', 'slide-5'], list(samples))
        self.assertEqual(['slide-4', 'slide-6'], list(patients))
        self.assertEqual({(r['PATIENT_ID'], r['SAMPLE_ID'], r['RESOURCE_ID'], r['TYPE'])
                          for r in samples.values()},
                         {('WSI-P1', 'WSI-P1-S1', 'WSI_SAMPLE', 'WHOLE_SLIDE_IMAGE'),
                          ('WSI-P1', 'WSI-P1-S2', 'WSI_SAMPLE', 'WHOLE_SLIDE_IMAGE'),
                          ('WSI-P2', 'WSI-P2-S1', 'WSI_SAMPLE', 'WHOLE_SLIDE_IMAGE')})
        self.assertEqual({(r['PATIENT_ID'], r['RESOURCE_ID']) for r in patients.values()},
                         {('WSI-P1', 'WSI_PATIENT'), ('WSI+P3', 'WSI_PATIENT')})
        self.assertNotIn('SAMPLE_ID', data_rows(self.out / 'data_resource_patient.txt')[0])

    def test_viewer_urls_are_absolute_and_encoded(self):
        self.convert()
        samples = rows_by_slide(self.out / 'data_resource_sample.txt')
        patients = rows_by_slide(self.out / 'data_resource_patient.txt')
        self.assertEqual(
            'https://portal.example.org/cbioportal/wsi/patient/WSI-P2'
            '?studyId=wsi_convert_test&slideKey=a72487fd68ef59b99fcee054e738f893',
            samples['slide-5']['URL'])
        self.assertEqual(
            'https://portal.example.org/cbioportal/wsi/patient/WSI%2BP3'
            '?studyId=wsi_convert_test&slideKey=3474b861eb683902b420f4ee95dfe0fa',
            patients['slide-6']['URL'])

    def test_raw_tsv_metadata_round_trips(self):
        self.convert()
        text = (self.out / 'data_resource_sample.txt').read_text(encoding='utf-8')
        self.assertNotIn('""', text, 'metadata must not be CSV-quoted')
        for line in text.splitlines()[1:]:
            self.assertTrue(line.split('\t')[-1].startswith('{'))
        samples = rows_by_slide(self.out / 'data_resource_sample.txt')
        first = json.loads(samples['slide-1']['METADATA'])
        self.assertIs(True, first['is_hne'])
        self.assertIs(False, first['is_ihc'])
        self.assertIs(True, first['can_serve_tiles'])
        self.assertEqual(716956681, first['file_size_bytes'])
        self.assertEqual('1', first['part_number'])
        self.assertEqual('Left "upper" lobe \\ wedge', first['part_description'])
        self.assertEqual('2c96f13783250ad2c6bcfcd5b7c3ef22', first['slide_key'])
        for removed in ('image_id', 'barcode', 'part_designator', 'path_dx_title'):
            self.assertNotIn(removed, first)
        self.assertEqual('WSI-P1-S1', first['reference_sample_id'])
        serving = first['wsi_serving']
        self.assertEqual(256, serving['thumbnail_width'])
        self.assertEqual(192, serving['thumbnail_height'])
        self.assertEqual({'height': 768, 'width': 1024}, serving['tile_metadata_json']['dimensions'])
        self.assertEqual({'model': 'Scan "Q" \\ 40', 'objective_power': 40, 'calibrated': True},
                         serving['tile_metadata_json']['vendor']['scanner'])
        # UNMATCHED reference samples are dropped (stored as no reference sample)
        self.assertNotIn('reference_sample_id', json.loads(samples['slide-5']['METADATA']))
        self.assertEqual(0, json.loads(samples['slide-5']['METADATA'])['file_size_bytes'])

    def test_unservable_slides_have_no_serving_object(self):
        self.convert()
        samples = rows_by_slide(self.out / 'data_resource_sample.txt')
        patients = rows_by_slide(self.out / 'data_resource_patient.txt')
        # slide-2 has a thumbnail width in the legacy row but CAN_SERVE_TILES=FALSE
        second = json.loads(samples['slide-2']['METADATA'])
        self.assertIs(False, second['can_serve_tiles'])
        self.assertNotIn('wsi_serving', second)
        self.assertNotIn('wsi_serving', json.loads(patients['slide-6']['METADATA']))

    def test_only_pairs_with_rows_are_written(self):
        match_level = converter.COLUMNS.index('MATCH_LEVEL')
        unmatched = [row for row in self.fixture_rows() if row.split('\t')[match_level] == 'UNMATCHED']
        written = self.convert(meta=self.write_legacy(unmatched))
        self.assertEqual(
            ['data_resource_definition.txt', 'data_resource_patient.txt',
             'meta_resource_definition.txt', 'meta_resource_patient.txt'],
            sorted(path.name for path in written))
        definitions = data_rows(self.out / 'data_resource_definition.txt')
        self.assertEqual(['WSI_PATIENT'], [row[0] for row in definitions[1:]])

    def test_definitions_declare_the_full_public_contract(self):
        self.convert()
        header, *definitions = data_rows(self.out / 'data_resource_definition.txt')
        self.assertEqual('CUSTOM_METADATA', header[-1])
        for row in definitions:
            contract = json.loads(row[-1])
            self.assertEqual(1, contract['version'])
            fields = {field['key']: field for field in contract['fields']}
            # every public key the converter writes, and never the private serving object
            self.assertEqual(PUBLIC_KEYS, set(fields))
            unfilterable = {key for key, field in fields.items() if not field['filterable']}
            self.assertTrue({'slide_key', 'part_key', 'block_key', 'specimen_key',
                             'reference_sample_id'} <= unfilterable, unfilterable)

    def test_java_fixture_is_current_converter_output(self):
        self.convert(base_url=BASE_URL)
        for name in EXPECTED_FILES:
            self.assertEqual((self.out / name).read_text(encoding='utf-8'),
                             (JAVA_FIXTURE_DIR / name).read_text(encoding='utf-8'),
                             '%s is stale; regenerate it with convertWsiToResources.py' % name)


class ConverterInputTestCase(ConverterTestCase):

    def assertConversionError(self, text, **kwargs):
        with self.assertRaises(converter.ConversionError) as context:
            self.convert(**kwargs)
        self.assertIn(text, str(context.exception))

    def test_portal_base_url_must_be_absolute_http(self):
        for value in ('/cbioportal', 'portal.example.org', 'ftp://portal.example.org',
                      'https://portal.example.org/?x=1'):
            self.assertConversionError('--portal-base-url', base_url=value)

    def test_header_must_match_format_v4(self):
        header = (FIXTURE_DIR / 'data_wsi.txt').read_text().splitlines()[4]
        swapped = header.replace('PATIENT_ID\tREFERENCE_SAMPLE_ID', 'REFERENCE_SAMPLE_ID\tPATIENT_ID')
        self.assertConversionError('invalid header', meta=self.write_legacy(self.fixture_rows(), swapped))

    def test_match_level_must_agree_with_sample(self):
        rows = self.fixture_rows()
        rows[0] = rows[0].replace('\tBLOCK\t', '\tUNMATCHED\t', 1)
        self.assertConversionError('UNMATCHED rows cannot have SAMPLE_ID', meta=self.write_legacy(rows))
        rows = self.fixture_rows()
        rows[3] = rows[3].replace('\tUNMATCHED\t', '\tPART\t', 1)
        self.assertConversionError('matched rows require SAMPLE_ID', meta=self.write_legacy(rows))

    def test_slide_type_and_stain_flags(self):
        slide_type = converter.COLUMNS.index('SLIDE_TYPE')
        is_hne = converter.COLUMNS.index('IS_HNE')
        for column, value, message in ((slide_type, 'Frozen', 'SLIDE_TYPE must be one of'),
                                       (slide_type, '', 'SLIDE_TYPE must be one of'),
                                       (is_hne, 'FALSE', 'inconsistent with SLIDE_TYPE H&E')):
            rows = self.fixture_rows()
            fields = rows[0].split('\t')
            fields[column] = value
            rows[0] = '\t'.join(fields)
            self.assertConversionError(message, meta=self.write_legacy(rows))

    def test_timing_columns_are_ignored(self):
        # Some files carry seven slide-timing columns before SLIDE_KEY. They are accepted
        # without being required or validated, and never reach the metadata.
        reference = Path(self.tmp.name) / 'reference'
        written = converter.convert(FIXTURE_DIR / 'meta_wsi.txt', reference, BASE_URL)
        timing_values = [
            ['0', 'AVAILABLE', 'RECORDED', 'PATHOLOGY_REPORT', '',
             'patient_first_tumor_sequencing_day_zero', 'surgery 20210314'],
            # values that would not pass as timing data
            ['not-a-day', 'BOGUS', '', '', 'reason', 'other_coordinates', ''],
        ]
        source = (FIXTURE_DIR / 'data_wsi.txt').read_text(encoding='utf-8').splitlines()

        def with_timing(line, values):
            fields = line.split('\t')
            return '\t'.join(fields[:-2] + values + fields[-2:])

        header = with_timing(source[4], list(converter.IGNORED_TIMING_COLUMNS))
        self.assertEqual(converter.COLUMNS_WITH_IGNORED_TIMING, header.split('\t'))
        rows = [with_timing(row, timing_values[index % 2])
                for index, row in enumerate(self.fixture_rows())]
        self.convert(meta=self.write_legacy(rows, header), base_url=BASE_URL)
        for path in written:
            self.assertEqual(path.read_bytes(), (self.out / path.name).read_bytes(), path.name)
        for name in ('data_resource_sample.txt', 'data_resource_patient.txt'):
            for record in rows_by_slide(self.out / name).values():
                metadata = json.loads(record['METADATA'])
                self.assertFalse([key for key in metadata
                                  if key.startswith(('timeline_', 'timepoint_'))], metadata)

    def test_line_break_in_output_cell_is_rejected(self):
        rows = self.fixture_rows()
        # STAIN_NAME reaches DISPLAY_NAME verbatim
        rows[0] = rows[0].replace('\tH&E\tH&E\tTRUE\t', '\tH\r&E\tH&E\tTRUE\t', 1)
        self.assertConversionError('tab or line break', meta=self.write_legacy(rows))
        with self.assertRaises(converter.ConversionError):
            converter.render_tsv('x.txt', [['a\tb']])

    def test_existing_resource_files_conflict(self):
        study = Path(self.tmp.name) / 'study'
        study.mkdir()
        (study / 'meta_resource_definition.txt').write_text(
            'cancer_study_identifier: wsi_convert_test\nresource_type: DEFINITION\n'
            'data_filename: data_resource_definition.txt\n')
        self.assertConversionError('meta_resource_definition.txt already provides a DEFINITION',
                                   study_dir=study)

    def test_output_dir_must_differ_from_study_dir(self):
        study = self.copy_study()
        self.out = study
        self.assertConversionError('--output-dir must differ from --study-dir', study_dir=study)


class V4OnlyTestCase(ConverterTestCase):

    """Only format v4 (30 columns ending with SLIDE_KEY and SEALED_SOURCE; 37 with ignored
    timing columns) is converted."""

    def assertConversionError(self, text, meta):
        with self.assertRaises(converter.ConversionError) as context:
            converter.convert(meta, self.out, BASE_URL)
        self.assertIn(text, str(context.exception))
        self.assertFalse(self.out.exists())

    def test_columns(self):
        self.assertEqual(30, len(converter.COLUMNS))
        self.assertEqual(['THUMBNAIL_CONTENT_TYPE', 'SLIDE_KEY', 'SEALED_SOURCE'], converter.COLUMNS[-3:])
        for name in ('IMAGE_ID', 'SOURCE_URL', 'THUMBNAIL_URL'):
            self.assertNotIn(name, converter.COLUMNS)
        self.assertEqual(37, len(converter.COLUMNS_WITH_IGNORED_TIMING))
        self.assertEqual(list(converter.IGNORED_TIMING_COLUMNS),
                         converter.COLUMNS_WITH_IGNORED_TIMING[28:35])
        self.assertEqual(['SLIDE_KEY', 'SEALED_SOURCE'], converter.COLUMNS_WITH_IGNORED_TIMING[-2:])

    def test_older_format_versions_are_rejected(self):
        for version in ('2', '3'):
            meta = self.write_legacy(self.fixture_rows())
            meta.write_text(meta.read_text().replace('format_version: 4', 'format_version: ' + version))
            self.assertConversionError('unsupported WSI format_version; expected 4', meta)

    def test_format_v3_columns_are_rejected_without_echoing_values(self):
        source = (FIXTURE_DIR / 'data_wsi.txt').read_text(encoding='utf-8').splitlines()
        for names in (['IMAGE_ID'], ['SOURCE_URL'], ['THUMBNAIL_URL'],
                      ['IMAGE_ID', 'SOURCE_URL', 'THUMBNAIL_URL']):
            header = '\t'.join(source[4].split('\t') + names)
            rows = [row + '\tS-SECRET-1' * len(names) for row in source[5:]]
            with self.assertRaises(converter.ConversionError) as context:
                converter.convert(self.write_legacy(rows, header), self.out, BASE_URL)
            message = str(context.exception)
            self.assertIn('%s column(s) of format v3 or older' % ', '.join(names), message)
            self.assertIn('SEALED_SOURCE', message)
            self.assertNotIn('S-SECRET', message)
            self.assertFalse(self.out.exists())

    def test_file_without_slide_key_or_sealed_source_is_rejected(self):
        source = (FIXTURE_DIR / 'data_wsi.txt').read_text(encoding='utf-8').splitlines()
        for drop in (-1, -2):
            rows = ['\t'.join(cell for index, cell in enumerate(row.split('\t'))
                              if index != len(converter.COLUMNS) + drop) for row in source[5:]]
            header = '\t'.join(name for name in converter.COLUMNS if name != converter.COLUMNS[drop])
            self.assertConversionError('invalid header', self.write_legacy(rows, header))
        # nor with the ignored timing columns and neither key column
        timing = ['' for _ in converter.IGNORED_TIMING_COLUMNS]
        rows = ['\t'.join(row.split('\t')[:-2] + timing) for row in source[5:]]
        header = '\t'.join(source[4].split('\t')[:-2] + list(converter.IGNORED_TIMING_COLUMNS))
        self.assertConversionError('invalid header', self.write_legacy(rows, header))

    def test_failure_late_in_the_file_leaves_no_output(self):
        rows = self.fixture_rows()
        rows[-1] = rows[-1].replace('\tUNMATCHED\t', '\tSOMETIMES\t', 1)
        self.assertConversionError('invalid MATCH_LEVEL', self.write_legacy(rows))
        self.assertEqual([], [path.name for path in Path(self.tmp.name).iterdir()
                              if path.name.endswith('.partial')])

    def test_command_line(self):
        args = ['--meta-wsi', str(FIXTURE_DIR / 'meta_wsi.txt'), '--output-dir', str(self.out),
                '--portal-base-url', BASE_URL]
        with patch('sys.stdout'):
            self.assertEqual(0, converter.main(args))
        self.assertEqual(EXPECTED_FILES, sorted(path.name for path in self.out.iterdir()))
        with patch('sys.stderr'):
            self.assertEqual(1, converter.main(args[:-1] + ['not-a-url']))


SLIDE_KEY = converter.COLUMNS.index('SLIDE_KEY')
SEALED_SOURCE = converter.COLUMNS.index('SEALED_SOURCE')


class SlideKeyAndDeidTestCase(ConverterTestCase):

    """Slide keys, sealed sources and the de-identification rules of contract wsi-serving-v6."""

    def with_cell(self, column, value, row=0):
        rows = self.fixture_rows()
        fields = rows[row].split('\t')
        fields[column] = value
        rows[row] = '\t'.join(fields)
        return self.write_legacy(rows)

    def conversion_error(self, meta):
        with self.assertRaises(converter.ConversionError) as context:
            converter.convert(meta, self.out, BASE_URL)
        self.assertFalse(self.out.exists())
        return str(context.exception)

    def test_slide_key_is_required_and_must_be_lowercase_hex(self):
        for value in ('', '2C96F13783250AD2C6BCFCD5B7C3EF22', '2c96f13783250ad2', 'g' * 32,
                      '2c96f13783250ad2c6bcfcd5b7c3ef22a'):
            message = self.conversion_error(self.with_cell(SLIDE_KEY, value))
            self.assertIn('SLIDE_KEY', message, value)

    def test_slide_key_must_be_unique(self):
        rows = self.fixture_rows()
        duplicate = rows[1].split('\t')
        duplicate[SLIDE_KEY] = rows[0].split('\t')[SLIDE_KEY]
        rows[1] = '\t'.join(duplicate)
        self.assertIn('line 7: SLIDE_KEY is not unique', self.conversion_error(self.write_legacy(rows)))

    def test_sealed_source_shape(self):
        def sealed(size):
            return base64.urlsafe_b64encode(bytes(index % 256 for index in range(size))).decode().rstrip('=')
        self.assertEqual(29, len(base64.urlsafe_b64decode(sealed(29) + '===')))
        for value in (sealed(29), 'A' * 4096):
            self.convert(meta=self.with_cell(SEALED_SOURCE, value))
            shutil.rmtree(self.out)
        for value in (sealed(28),                              # shorter than nonce, one byte and tag
                      SEALED_SOURCE_VECTOR + '==',             # padded
                      SEALED_SOURCE_VECTOR.replace('-', '+'),  # standard alphabet
                      SEALED_SOURCE_VECTOR + '.x',             # not base64url
                      'A' * 41,                                # not a whole number of bytes
                      'A' * 4097):                             # too long
            message = self.conversion_error(self.with_cell(SEALED_SOURCE, value))
            self.assertIn('line 6: SEALED_SOURCE must be unpadded base64url', message)
            self.assertNotIn(value[:20], message)

    def test_sealed_source_is_required_iff_servable(self):
        message = self.conversion_error(self.with_cell(SEALED_SOURCE, ''))
        self.assertIn('line 6: SEALED_SOURCE is required', message)
        # slide-2 (second row) cannot serve tiles
        message = self.conversion_error(self.with_cell(SEALED_SOURCE, SEALED_SOURCE_VECTOR, row=1))
        self.assertIn('line 7: SEALED_SOURCE must be empty when CAN_SERVE_TILES is FALSE', message)
        self.assertNotIn(SEALED_SOURCE_VECTOR[:20], message)

    def test_url_display_name_and_metadata_shape(self):
        self.convert()
        for name in ('data_resource_sample.txt', 'data_resource_patient.txt'):
            header, *records = data_rows(self.out / name)
            for record in records:
                row = dict(zip(header, record))
                metadata = json.loads(row['METADATA'])
                self.assertRegex(metadata['slide_key'], '^[0-9a-f]{32}$')
                self.assertTrue(row['URL'].endswith('&slideKey=' + metadata['slide_key']))
                for field in ('URL', 'DISPLAY_NAME'):
                    for value in ('imageId', 'BC-0001', 'sealed'):
                        self.assertNotIn(value, row[field])
                public = {key for key in metadata if key != 'wsi_serving'}
                self.assertFalse(public & {'image_id', 'barcode', 'part_designator', 'path_dx_title',
                                           'source_url', 'thumbnail_url', 'sealed_source'}, public)
                self.assertLessEqual(public, PUBLIC_KEYS)
                public_text = json.dumps({key: metadata[key] for key in public})
                for value in ('BC-0001', 'Adenocarcinoma'):
                    self.assertNotIn(value, public_text)
                if 'wsi_serving' in metadata:
                    self.assertNotIn(metadata['wsi_serving']['sealed_source'], public_text)
                    self.assertNotIn(metadata['wsi_serving']['sealed_source'], row['URL'])
            # no image ID, object URI or barcode is written anywhere
            text = (self.out / name).read_text(encoding='utf-8')
            for value in ('IMG', 'image_id', 'source_url', 'thumbnail_url', '.svs', '.jpg', 'BC-0001'):
                self.assertNotIn(value, text)
        samples = rows_by_slide(self.out / 'data_resource_sample.txt')
        self.assertEqual('H&E \u00b7 Specimen 1 / Block 1', samples['slide-1']['DISPLAY_NAME'])
        self.assertEqual('PD-L1 (22C3) \u00b7 Specimen 2 / Block 1', samples['slide-3']['DISPLAY_NAME'])
        patients = rows_by_slide(self.out / 'data_resource_patient.txt')
        self.assertEqual('H&E \u00b7 Specimen 1', patients['slide-6']['DISPLAY_NAME'])

    def test_display_name_fallbacks(self):
        row = {name: '' for name in converter.COLUMNS}
        self.assertEqual('Slide', converter.display_name(row))
        row.update(SLIDE_TYPE='IHC', BLOCK_NUMBER='3')
        self.assertEqual('IHC \u00b7 Block 3', converter.display_name(row))

    def test_wsi_serving_carries_sealed_source(self):
        self.convert()
        samples = rows_by_slide(self.out / 'data_resource_sample.txt')
        serving = json.loads(samples['slide-1']['METADATA'])['wsi_serving']
        self.assertEqual(
            {'sealed_source', 'tile_metadata_json', 'thumbnail_width', 'thumbnail_height',
             'thumbnail_content_type'}, set(serving))
        self.assertEqual(SEALED_SOURCE_VECTOR, serving['sealed_source'])
        self.assertEqual('image/jpeg', serving['thumbnail_content_type'])
        # non-servable rows have no serving object
        self.assertNotIn('wsi_serving', json.loads(samples['slide-2']['METADATA']))

    def test_tab_in_output_is_not_echoed(self):
        with self.assertRaises(converter.ConversionError) as context:
            converter.render_tsv('x.txt', [['SECRET\t1']])
        self.assertNotIn('SECRET', str(context.exception))

PUBLIC_KEYS = {
    'slide_key', 'reference_sample_id', 'part_key', 'part_number', 'part_type',
    'part_description', 'subspecialty', 'block_key', 'block_number', 'block_label', 'match_level',
    'specimen_key', 'stain_name', 'stain_group', 'magnification', 'slide_type', 'is_hne', 'is_ihc',
    'can_serve_tiles', 'file_size_bytes',
}


class ConvertedFilesValidationTestCase(ConverterTestCase):

    """Run the emitted files through the real validateData validators."""

    @classmethod
    def setUpClass(cls):
        cls.portal = validateData.load_portal_info(
            'test_data/api_json_unit_tests', logging.getLogger(__name__), offline=True)

    def setUp(self):
        super().setUp()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.buffer = logging.handlers.BufferingHandler(capacity=1e6)
        self.logger.addHandler(self.buffer)
        self.saved = {name: getattr(validateData, name) for name in (
            'DEFINED_SAMPLE_IDS', 'PATIENTS_WITH_SAMPLES', 'SAMPLE_TO_PATIENT',
            'RESOURCE_DEFINITION_DICTIONARY', 'RESOURCE_CONTRACT_KEYS', 'WSI_RESOURCE_STATE',
            'DEFINED_SAMPLE_ATTRIBUTES')}
        validateData.DEFINED_SAMPLE_IDS = set(SAMPLE_TO_PATIENT)
        validateData.PATIENTS_WITH_SAMPLES = set(SAMPLE_TO_PATIENT.values())
        validateData.SAMPLE_TO_PATIENT = dict(SAMPLE_TO_PATIENT)
        validateData.DEFINED_SAMPLE_ATTRIBUTES = {'PATIENT_ID', 'SAMPLE_ID'}
        validateData.reset_wsi_resource_state()

    def tearDown(self):
        for name, value in self.saved.items():
            setattr(validateData, name, value)
        self.logger.removeHandler(self.buffer)
        super().tearDown()

    def run_validator(self, validator_class, data_file):
        validator = validator_class(str(self.out), {'data_filename': data_file}, self.portal,
                                    self.logger, False, False)
        validator.validate()
        records = list(self.buffer.buffer)
        self.buffer.flush()
        return validator, [record for record in records if record.levelno >= logging.WARNING]

    def test_resource_files_pass_validation(self):
        self.convert()
        validator, problems = self.run_validator(validateData.ResourceDefinitionValidator,
                                                 'data_resource_definition.txt')
        self.assertEqual([], [r.getMessage() for r in problems])
        validateData.RESOURCE_DEFINITION_DICTIONARY = validator.resource_definition_dictionary
        # the contract applies: no undeclared keys (wsi_serving is private) and no unused ones
        validateData.RESOURCE_CONTRACT_KEYS = validator.resource_contract_keys
        self.assertEqual({'WSI_SAMPLE', 'WSI_PATIENT'}, set(validator.resource_contract_keys))
        _, problems = self.run_validator(validateData.SampleResourceValidator, 'data_resource_sample.txt')
        self.assertEqual([], [(r.getMessage(), getattr(r, 'cause', None)) for r in problems])
        _, problems = self.run_validator(validateData.PatientResourceValidator, 'data_resource_patient.txt')
        # the fixture's unmatched slides leave these optional cells blank, which the contract
        # check reports as a warning, as it would for any resource
        self.assertEqual([(logging.WARNING, 'file_size_bytes, part_description, subspecialty')],
                         [(r.levelno, getattr(r, 'cause', None)) for r in problems])

    def test_converted_study_passes_and_meta_wsi_is_rejected(self):
        study = self.copy_study()
        self.convert(study_dir=study)
        full = Path(self.tmp.name) / 'full'
        shutil.copytree(study, full)
        for path in self.out.iterdir():
            shutil.copy(path, full / path.name)
        validateData.validate_study(str(full), self.portal, self.logger, False, False)
        errors = [r for r in self.buffer.buffer if r.levelno >= logging.ERROR]
        self.buffer.flush()
        self.assertEqual([], [(r.getMessage(), getattr(r, 'cause', None)) for r in errors])

        shutil.copy(FIXTURE_DIR / 'meta_wsi.txt', full / 'meta_wsi.txt')
        shutil.copy(FIXTURE_DIR / 'data_wsi.txt', full / 'data_wsi.txt')
        validateData.validate_study(str(full), self.portal, self.logger, False, False)
        errors = [r.getMessage() for r in self.buffer.buffer if r.levelno >= logging.ERROR]
        self.buffer.flush()
        self.assertEqual([cbioportal_common.LEGACY_WSI_IMPORT_MESSAGE], errors)

if __name__ == '__main__':
    unittest.main(buffer=True)
