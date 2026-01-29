package org.mskcc.cbio.portal.scripts;

import org.mskcc.cbio.portal.dao.ClickHouseReferentialIntegrityChecker;
import org.mskcc.cbio.portal.dao.ClickHouseReferentialIntegrityChecker.ReferenceViolation;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Checks ClickHouse referential integrity for the portal schema.
 */
public class CheckReferentialIntegrity extends ConsoleRunnable {

    private static final int SAMPLE_VALUE_LIMIT = 5;

    private record ViolationGroupKey(
            String refereeTable,
            String refereeColumns,
            String referredTable,
            String referredColumns
    ) {
    }

    private static final class ViolationGroup {
        private int totalCount;
        private final List<String> sampleValues = new ArrayList<>();

        private void add(String refereeValue) {
            totalCount++;
            if (sampleValues.size() < SAMPLE_VALUE_LIMIT && !sampleValues.contains(refereeValue)) {
                sampleValues.add(refereeValue);
            }
        }
    }

    @Override
    public void run() {
        ProgressMonitor.setCurrentMessage("Checking ClickHouse referential integrity...");
        try {
            List<ReferenceViolation> violations = ClickHouseReferentialIntegrityChecker.findReferenceViolations();
            if (violations.isEmpty()) {
                ProgressMonitor.setCurrentMessage("No referential integrity violations found.");
            } else {
                var groupedViolations = groupViolations(violations);
                ProgressMonitor.setCurrentMessage(String.format(
                        Locale.ROOT,
                        "Referential integrity violations found: %d rows across %d relationships.",
                        violations.size(),
                        groupedViolations.size()
                ));
                for (var entry : groupedViolations.entrySet()) {
                    ViolationGroupKey key = entry.getKey();
                    ViolationGroup group = entry.getValue();
                    ProgressMonitor.logWarning(formatViolationGroup(key, group));
                }
                ConsoleUtil.showWarnings();
                System.exit(1);
            }
        } catch (DaoException e) {
            throw new RuntimeException("Failed to check ClickHouse referential integrity.", e);
        }
        ProgressMonitor.setCurrentMessage("Done.");
    }

    private static Map<ViolationGroupKey, ViolationGroup> groupViolations(List<ReferenceViolation> violations) {
        Map<ViolationGroupKey, ViolationGroup> grouped = new LinkedHashMap<>();
        for (ReferenceViolation violation : violations) {
            ViolationGroupKey key = new ViolationGroupKey(
                    violation.refereeTable(),
                    violation.refereeColumns(),
                    violation.referredTable(),
                    violation.referredColumns()
            );
            grouped.computeIfAbsent(key, unused -> new ViolationGroup()).add(violation.refereeValues());
        }
        return grouped;
    }

    private static String formatViolationGroup(ViolationGroupKey key, ViolationGroup group) {
        String sampleText = group.sampleValues.isEmpty()
                ? "No sample values captured."
                : String.join(", ", group.sampleValues);
        int remainingCount = Math.max(0, group.totalCount - group.sampleValues.size());
        String remainingText = remainingCount > 0
                ? " (+" + remainingCount + " more)"
                : "";
        return String.format(
                Locale.ROOT,
                "Table %s (columns: %s) has %d values that do not exist in table %s (columns: %s). Examples: %s%s.",
                key.refereeTable(),
                key.refereeColumns(),
                group.totalCount,
                key.referredTable(),
                key.referredColumns(),
                sampleText,
                remainingText
        );
    }

    public CheckReferentialIntegrity(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        ConsoleRunnable runner = new CheckReferentialIntegrity(args);
        runner.runInConsole();
    }
}
