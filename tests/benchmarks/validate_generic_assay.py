#!/usr/bin/env python3
"""Benchmark generic-assay matrix validation without importing a study.

Run from the repository root, using the same Python environment for both refs:
    PYTHONPATH=scripts python tests/benchmarks/validate_generic_assay.py DATA_FILE

Sample columns are treated as clinically defined to isolate matrix validation.
All matrix checks and WARNING/ERROR diagnostics remain enabled. Diagnostics are
hashed in order rather than retained in memory. Use --profile PATH for cProfile;
compare unprofiled runs for elapsed time because profiling adds overhead.
"""

import argparse
import cProfile
import hashlib
import json
import logging
from pathlib import Path
import platform
import resource
import sys
import time

from importer import validateData


class DiagnosticDigest(logging.Handler):
    def __init__(self):
        super().__init__()
        self.counts = {}
        self.digest = hashlib.sha256()

    def emit(self, record):
        self.counts[record.levelname] = self.counts.get(record.levelname, 0) + 1
        diagnostic = [record.levelname, record.getMessage(),
                      getattr(record, 'line_number', None),
                      getattr(record, 'column_number', None),
                      getattr(record, 'cause', None)]
        self.digest.update(json.dumps(diagnostic).encode('utf-8'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('data_file', type=Path)
    parser.add_argument('--meta-properties', default='NAME,DESCRIPTION,TRANSCRIPT_ID')
    parser.add_argument('--profile', help='Write cProfile stats to this path')
    args = parser.parse_args()
    logger = logging.getLogger('generic-assay-benchmark')
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    diagnostics = DiagnosticDigest()
    logger.addHandler(diagnostics)
    with args.data_file.open() as data_file:
        header = data_file.readline().rstrip('\r\n').split('\t')
    non_sample_columns = 1 + len(args.meta_properties.split(','))
    validateData.DEFINED_SAMPLE_IDS = set(header[non_sample_columns:])
    validator = validateData.GenericAssayContinuousValidator(
        str(args.data_file.parent),
        {'data_filename': args.data_file.name,
         'generic_entity_meta_properties': args.meta_properties},
        None, logger, False, False)
    profile = cProfile.Profile() if args.profile else None
    started = time.perf_counter()
    cpu_started = time.process_time()
    if profile:
        profile.enable()
    validator.validate()
    if profile:
        profile.disable()
    elapsed = time.perf_counter() - started
    cpu_elapsed = time.process_time() - cpu_started
    if profile:
        profile.dump_stats(args.profile)
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux reports KiB.
    max_rss_bytes = max_rss if sys.platform == 'darwin' else max_rss * 1024
    print(json.dumps({
        'python': platform.python_version(), 'platform': platform.platform(),
        'seconds': elapsed, 'cpu_seconds': cpu_elapsed,
        'rows': validator.line_number - 1, 'samples': len(validator.sampleIds),
        'parsed': validator.fileCouldBeParsed,
        'diagnostics': diagnostics.counts,
        'diagnostic_sha256': diagnostics.digest.hexdigest(),
        'max_rss_bytes': max_rss_bytes,
    }))


if __name__ == '__main__':
    main()
