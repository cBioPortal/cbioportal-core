"""Read-only checks for clinical data requiring OncoTree preprocessing.

The reference is the target portal's own cancer types: the type_of_cancer
table as reported by the portal's web API, or by the cancer-types.json file in
a saved portal info directory. Cancer types the study defines in its own cancer
type file are accepted too, because importing the study adds them.

Checks:
- Each populated ONCOTREE_CODE is a known cancer type ID, ignoring case.
- Each populated CANCER_TYPE_DETAILED matches that cancer type's name.

CANCER_TYPE is not checked: the portal does not store OncoTree main types.
Blank/NA labels and codes are skipped. A missing reference is reported once;
clinical data is never changed.
"""

UNAVAILABLE_VALUES = {'', 'NA', 'N/A', 'NOT AVAILABLE', '[NOT AVAILABLE]', '[NOT APPLICABLE]'}


class CancerTypeReference:
    """Map each cancer type code, upper-cased, to its name.

    Build the map from the portal's cancer types plus the study's own cancer
    types. If the portal's cancer types are unavailable, leave the map unset so
    the row check can report that once.
    """
    def __init__(self, portal_cancer_types, study_cancer_types=None):
        self.names = None
        self.reported_error = False
        if portal_cancer_types is None:
            return
        self.names = {}
        for code, name in (study_cancer_types or {}).items():
            self.names[code.upper()] = name or ''
        # The portal's definition wins; the cancer type validator already
        # reports study definitions that disagree with it.
        for code, cancer_type in portal_cancer_types.items():
            self.names[code.upper()] = (cancer_type or {}).get('name') or ''


def oncotree_findings(row, names):
    """Find unknown cancer codes and detailed cancer type names that disagree with the portal.

    Skip missing values. For each problem, return the column, its current value,
    and the expected value.
    """
    code = row.get('ONCOTREE_CODE', '').strip()
    if code.upper() in UNAVAILABLE_VALUES:
        return
    if code.upper() not in names:
        yield ('ONCOTREE_CODE', code,
               "a cancer type in the target portal's type_of_cancer table; remap retired codes")
        return
    expected = names[code.upper()] or 'NA'
    actual = row.get('CANCER_TYPE_DETAILED', '').strip()
    if actual.upper() not in UNAVAILABLE_VALUES and actual != expected:
        yield 'CANCER_TYPE_DETAILED', actual, expected


def check_oncotree_row(columns, values, reference, logger, line_number):
    """Check one clinical row and report problems with their line and column.

    Skip rows without an OncoTree column or with the wrong number of cells;
    the normal validator handles those cases. If the portal's cancer types are
    unavailable, report that only once.
    """
    if 'ONCOTREE_CODE' not in columns or len(values) != len(columns):
        return
    if reference.names is None:
        if not reference.reported_error:
            logger.error("Cannot validate ONCOTREE_CODE values: the target portal's cancer "
                         'types are not available. Validate against a portal with -u, or '
                         'with a portal info directory (-p) that includes cancer-types.json; '
                         'this check was not completed.',
                         extra={'line_number': line_number})
            reference.reported_error = True
        return
    for column, actual, expected in oncotree_findings(dict(zip(columns, values)), reference.names):
        extra = {'line_number': line_number}
        if column in columns:
            extra['column_number'] = columns.index(column) + 1
        logger.error('OncoTree preprocessing required: %s is %r; expected %r.',
                     column, actual, expected, extra=extra)
