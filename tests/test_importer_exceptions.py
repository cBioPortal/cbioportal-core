#!/usr/bin/env python3

import unittest
from unittest import mock

from importer import cbioportalImporter
from importer.cbioportal_common import JavaRunException, MetaFileTypes


class ImporterExceptionTests(unittest.TestCase):
    def test_remove_samples_preserves_java_failure_status(self):
        failure = JavaRunException(7, "remove failed")
        with mock.patch.object(cbioportalImporter, "LOGGER", mock.Mock()):
            with mock.patch.object(cbioportalImporter, "run_java", side_effect=failure):
                with self.assertRaises(JavaRunException) as raised:
                    cbioportalImporter.remove_samples("", "study", "sample")
        self.assertIs(raised.exception, failure)
        self.assertEqual(raised.exception.process_return_status, 7)

    def test_incremental_cna_failure_preserves_java_failure_status(self):
        failure = JavaRunException(9, "incremental import failed")
        metadata = {
            MetaFileTypes.CNA_DISCRETE_LONG: [
                ("/tmp/meta_cna.txt", {"data_filename": "data_cna.txt"})
            ]
        }
        with mock.patch.object(cbioportalImporter, "LOGGER", mock.Mock()):
            with mock.patch.object(cbioportalImporter, "import_data", side_effect=failure):
                with self.assertRaises(JavaRunException) as raised:
                    cbioportalImporter.import_incremental_data("", "/tmp", False, metadata)
        self.assertIs(raised.exception, failure)
        self.assertEqual(raised.exception.process_return_status, 9)


if __name__ == "__main__":
    unittest.main()
