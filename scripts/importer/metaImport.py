#!/usr/bin/env python3

__author__ = 'priti'

# ----------------------------------------------------------------------------
# Import
# ----------------------------------------------------------------------------


import sys
import os
import importlib
import argparse
import logging
from pathlib import Path

# configure relative imports if running as a script; see PEP 366
# it might passed as empty string by certain tooling to mark a top level module
if __name__ == "__main__" and (__package__ is None or __package__ == ''):
    # replace the script's location in the Python search path by the main
    # scripts/ folder, above it, so that the importer package folder is in
    # scope and *not* directly in sys.path; see PEP 395
    sys.path[0] = str(Path(sys.path[0]).resolve().parent)
    __package__ = 'importer'
    # explicitly import the package, which is needed on CPython 3.4 because it
    # doesn't include https://github.com/python/cpython/pull/2639
    importlib.import_module(__package__)

from . import validateData
from . import cbioportalImporter
from . import importOncokbMutation
from . import importOncokbDiscreteCNA
from . import libImportOncokb
from . import derive_tables


# ----------------------------------------------------------------------------
# Global variables
# ----------------------------------------------------------------------------

class Color(object):
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


# ----------------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------------

def interface():
    parser = argparse.ArgumentParser(description='cBioPortal meta Importer')
    data_source_group = parser.add_mutually_exclusive_group()
    data_source_group.add_argument('-s', '--study_directory',
                                   type=str, help='path to study directory.')
    data_source_group.add_argument('-d', '--data_directory',
                                   type=str, help='path to data directory for incremental upload.')
    portal_mode_group = parser.add_mutually_exclusive_group()
    portal_mode_group.add_argument('-u', '--url_server',
                                   type=str,
                                   default='http://localhost/cbioportal',
                                   help='URL to cBioPortal server. You can '
                                        'set this if your URL is not '
                                        'http://localhost/cbioportal')
    portal_mode_group.add_argument('-p', '--portal_info_dir',
                                   type=str,
                                   help='Path to a directory of cBioPortal '
                                        'info files to be used instead of '
                                        'contacting the web API')
    #  temporary workaround to simplify import process when no web-server is running. TODO: replace by solution for #1466
    portal_mode_group.add_argument('-n', '--no_portal_checks', default=False,
                                       action='store_true',
                                       help='Skip tests requiring information '
                                            'from the cBioPortal installation')
    parser.add_argument('-jvo', '--java_opts', type=str, default=os.environ.get('JAVA_OPTS'),
                        help='Path to specify JAVA_OPTS for the importer. \
                        (default: gets the JAVA_OPTS from the environment)')
    parser.add_argument('-jar', '--jar_path', type=str, required=False,
                        help='Path to scripts JAR file (default: locate it '
                             'relative to the import script)')
    parser.add_argument('-html', '--html_table', type=str,
                        help='path to html report')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='report status info messages while validating')
    parser.add_argument('-o', '--override_warning', action='store_true',
                        help='override warnings and continue importing')
    parser.add_argument('-r', '--relaxed_clinical_definitions', required=False,
                        action='store_true', default=False,
                        help='Option to enable relaxed mode for validator when '
                             'validating clinical data without header definitions')
    parser.add_argument('-m', '--strict_maf_checks', required=False,
                        action='store_true', default=False,
                        help='Option to enable strict mode for validator when '
                             'validating mutation data')
    parser.add_argument('-update', '--update_generic_assay_entity', type=str, required=False, default="False",
                        help='Set as True to update the existing generic assay entities, set as False to keep the existing generic assay entities for generic assay')
    parser.add_argument('-oncokb', '--import_oncokb', action='store_true',
                        help='Set as True to download OncoKB annotations for Mutations and CNA and load as custom driver annotations')
    parser.add_argument('-skipimport', '--skip_db_import', action='store_true',
                        help='Perform validation and OncoKB download but do not import study into database.')
    parser.add_argument('--no-derive-tables', action='store_true',
                        help='Skip derived table construction after import.')
    parser.add_argument('--ch-props', type=str,
                        help='Path to ClickHouse application.properties '
                             '(for derived table construction). '
                             'Default: $IMPORTER_PROPERTIES_FILEPATH or '
                             '/cbioportal/application.properties')
    parser.add_argument('--sql-dir', type=str,
                        help='Local directory with derived table SQL files '
                             '(default: download from GitHub).')
    parser.add_argument('--github-branch', type=str,
                        help='GitHub branch for derived table SQL '
                             '(default: master).')
    parser = parser.parse_args()
    return parser


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def _resolve_ch_props_path(args):
    """Resolve the path to ClickHouse application.properties.

    Priority:
    1. --ch-props CLI argument
    2. $IMPORTER_PROPERTIES_FILEPATH environment variable
    3. /cbioportal/application.properties (Docker convention)
    """
    if hasattr(args, 'ch_props') and args.ch_props:
        return args.ch_props
    env_path = os.environ.get('IMPORTER_PROPERTIES_FILEPATH')
    if env_path:
        return env_path
    return '/cbioportal/application.properties'


def _rebuild_derived_tables(args):
    """Rebuild ClickHouse derived tables after a database-mutating operation.

    Returns True on success, False on failure.
    """
    ch_props_path = _resolve_ch_props_path(args)
    sql_dir = getattr(args, 'sql_dir', None)
    github_branch = getattr(args, 'github_branch', None)
    try:
        return derive_tables.main_derive(
            ch_props_path, github_branch=github_branch, sql_dir=sql_dir)
    except Exception as e:
        print(Color.RED +
              f"Derived table construction failed: {e}" +
              Color.END, file=sys.stderr)
        return False


if __name__ == '__main__':
    # Check for derive-tables subcommand before normal parsing
    derive_tables_only = False
    if len(sys.argv) > 1 and sys.argv[1] == 'derive-tables':
        derive_tables_only = True
        sys.argv.pop(1)  # remove 'derive-tables' from args

    # Parse user input
    args = interface()

    # Handle derive-tables subcommand (no validation, no import)
    if derive_tables_only:
        success = _rebuild_derived_tables(args)
        sys.exit(0 if success else 1)
    # supply parameters that the validation script expects to have parsed
    args.error_file = False

    data_dir = args.data_directory if args.data_directory is not None else args.study_directory

    # Validate the study directory.
    print("Starting validation...\n", file=sys.stderr)
    try:
        exitcode = validateData.main_validate(args)
    except KeyboardInterrupt:
        print(Color.BOLD + "\nProcess interrupted. " + Color.END, file=sys.stderr)
        print("#" * 71 + "\n", file=sys.stderr)
        raise
    except:
        print("!" * 71, file=sys.stderr)
        print(Color.RED + "Error occurred during validation step:" + Color.END, file=sys.stderr)
        raise
    finally:
        # make sure all log messages are flushed
        validator_logger = logging.getLogger(validateData.__name__)
        for log_handler in validator_logger.handlers:
            log_handler.close()
        validator_logger.handlers = []

    # Import OncoKB annotations when asked, and there are no validation warnings or warnings are overruled
    study_is_valid = exitcode == 0 or (exitcode == 3 and args.override_warning)
    if study_is_valid and args.import_oncokb:
        mutation_meta_file_path = libImportOncokb.find_meta_file_by_fields(data_dir, {'genetic_alteration_type': 'MUTATION_EXTENDED'})
        mutation_data_file_name = libImportOncokb.find_data_file_from_meta_file(mutation_meta_file_path)
        mutation_data_file_path = os.path.join(data_dir, mutation_data_file_name)
        study_is_modified = False
        print("\n")
        if os.path.exists(mutation_data_file_path):
            print("Starting import of OncoKB annotations for mutations file ...\n", file=sys.stderr)
            try:
                exitcode = importOncokbMutation.main_import(args)
                study_is_modified = True
            except KeyboardInterrupt:
                print(Color.BOLD + "\nProcess interrupted. " + Color.END, file=sys.stderr)
                print("#" * 71 + "\n", file=sys.stderr)
                raise
            except:
                print("!" * 71, file=sys.stderr)
                print(Color.RED + "Error occurred during import of OncoKB for mutations file:" + Color.END, file=sys.stderr)
                raise
            finally:
                # make sure all log messages are flushed
                validator_logger = logging.getLogger(importOncokbMutation.__name__)
                for log_handler in validator_logger.handlers:
                    log_handler.close()
                validator_logger.handlers = []
        cna_meta_file_path = libImportOncokb.find_meta_file_by_fields(data_dir, {'genetic_alteration_type': 'COPY_NUMBER_ALTERATION', 'datatype': 'DISCRETE'})
        cna_data_file_name = libImportOncokb.find_data_file_from_meta_file(cna_meta_file_path)
        cna_data_file_path = os.path.join(data_dir, cna_data_file_name)
        if os.path.exists(cna_data_file_path):
            print("Starting import of OncoKB annotations for discrete CNA file ...\n", file=sys.stderr)
            try:
                exitcode = importOncokbDiscreteCNA.main_import(args)
                study_is_modified = True
            except KeyboardInterrupt:
                print(Color.BOLD + "\nProcess interrupted. " + Color.END, file=sys.stderr)
                print("#" * 71 + "\n", file=sys.stderr)
                raise
            except:
                print("!" * 71, file=sys.stderr)
                print(Color.RED + "Error occurred during import of OncoKB for discrete CNA file:" + Color.END, file=sys.stderr)
                raise
            finally:
                # make sure all log messages are flushed
                validator_logger = logging.getLogger(importOncokbDiscreteCNA.__name__)
                for log_handler in validator_logger.handlers:
                    log_handler.close()
                validator_logger.handlers = []
        # Revalidate when custom annotations were added
        if study_is_modified:
            print("Starting re-validation of study with OncoKB annotations ...\n", file=sys.stderr)
            exitcode = validateData.main_validate(args)

    # Depending on validation results, load the study or notify the user
    try:
        print("\n")
        print("#" * 71, file=sys.stderr)
        if exitcode == 1:
            print(Color.RED + "One or more errors reported above. Please fix your files accordingly" + Color.END, file=sys.stderr)
            print("!" * 71, file=sys.stderr)
        elif exitcode == 3:
            if args.override_warning and not args.skip_db_import:
                print(Color.BOLD + "Overriding Warnings. Importing study now" + Color.END, file=sys.stderr)
                print("#" * 71 + "\n", file=sys.stderr)
                cbioportalImporter.main(args)
                exitcode = 0
                # Rebuild derived tables after database-mutating operation
                if not getattr(args, 'no_derive_tables', False):
                    print("\n")
                    print("#" * 71, file=sys.stderr)
                    print(Color.BOLD +
                          "Rebuilding ClickHouse derived tables..." +
                          Color.END, file=sys.stderr)
                    if not _rebuild_derived_tables(args):
                        print(Color.RED +
                              "Derived table construction failed. "
                              "The database may be in an inconsistent state." +
                              Color.END, file=sys.stderr)
                        exitcode = 1
            else:
                print(Color.BOLD + "Warnings. Please fix your files or import with override warning option" + Color.END, file=sys.stderr)
                print("#" * 71, file=sys.stderr)
        elif exitcode == 0 and not args.skip_db_import:
            print(Color.BOLD + "Everything looks good. Importing study now" + Color.END, file=sys.stderr)
            print("#" * 71 + "\n", file=sys.stderr)
            cbioportalImporter.main(args)
            # Rebuild derived tables after database-mutating operation
            if not getattr(args, 'no_derive_tables', False):
                print("\n")
                print("#" * 71, file=sys.stderr)
                print(Color.BOLD +
                      "Rebuilding ClickHouse derived tables..." +
                      Color.END, file=sys.stderr)
                if not _rebuild_derived_tables(args):
                    print(Color.RED +
                          "Derived table construction failed. "
                          "The database may be in an inconsistent state." +
                          Color.END, file=sys.stderr)
                    exitcode = 1
    except KeyboardInterrupt:
        print(Color.BOLD + "\nProcess interrupted. You will have to run this again to make sure study is completely loaded." + Color.END, file=sys.stderr)
        print("#" * 71, file=sys.stderr)
        raise
    except:
        print("!" * 71, file=sys.stderr)
        print(Color.RED + "Error occurred during data loading step. Please fix the problem and run this again to make sure study is completely loaded." + Color.END, file=sys.stderr)
        raise
    sys.exit(exitcode)
