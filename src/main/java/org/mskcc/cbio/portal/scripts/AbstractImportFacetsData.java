/*
 * Copyright (c) 2026 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.scripts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import joptsimple.OptionSet;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.FileUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.mskcc.cbio.portal.util.StableIdUtil;

/**
 * Shared logic for importing FACETS allele-specific copy number data
 * ({@code facets_cncf} and {@code facets_genes}). Both data types carry the
 * same core columns (sample, genomic coordinates, tcn/lcn/cellular fraction,
 * purity); this class centralizes CLI parsing, coordinate/sample validation,
 * numeric parsing, and the bulk-load / incremental-update lifecycle, mirroring
 * {@link ImportCopyNumberSegmentData}.
 *
 * Subclasses only need to resolve any data-type-specific columns (e.g. the
 * HUGO gene symbol for gene-level data) and persist the row through the
 * appropriate DAO.
 */
public abstract class AbstractImportFacetsData extends ConsoleRunnable {

    protected static final String SAMPLE_ID_COLUMN = "SAMPLE_ID";
    protected static final String CHROMOSOME_COLUMN = "CHROMOSOME";
    protected static final String START_POSITION_COLUMN = "START_POSITION";
    protected static final String END_POSITION_COLUMN = "END_POSITION";
    protected static final String TCN_COLUMN = "TCN";
    protected static final String LCN_COLUMN = "LCN";
    protected static final String CELLULAR_FRACTION_COLUMN = "CELLULAR_FRACTION";
    protected static final String PURITY_COLUMN = "PURITY";

    protected boolean isIncrementalUpdateMode;
    protected final Set<Integer> processedSampleIds = new HashSet<>();
    private int entriesSkipped;

    public AbstractImportFacetsData(String[] args) {
        super(args);
    }

    /** A human-readable label used in log/error messages (e.g. "FACETS CNCF"). */
    protected abstract String dataTypeLabel();

    /** @return true if data for this data type already exists for the given study. */
    protected abstract boolean dataExistsForCancerStudy(int cancerStudyId) throws DaoException;

    /** Deletes previously imported rows for the given samples (used for --overwrite-existing). */
    protected abstract void deleteDataForSamples(int cancerStudyId, Set<Integer> sampleIds) throws DaoException;

    /** @return the additional (data-type-specific) column names required in the data file, e.g. HUGO_SYMBOL. */
    protected String[] getAdditionalRequiredColumns() {
        return new String[0];
    }

    /**
     * Persists a single parsed row via the appropriate DAO. Common fields have
     * already been parsed and validated; the subclass is responsible for
     * resolving any additional columns (via colIndex/parts) and issuing the
     * DAO insert call.
     */
    protected abstract boolean storeRow(CommonFacetsFields common, String[] parts, Map<String, Integer> colIndex,
            CancerStudy study, Sample sample) throws DaoException;

    @Override
    public void run() {
        try {
            String description = "Import '" + dataTypeLabel() + "' data files";
            OptionSet options = ConsoleUtil.parseStandardDataAndMetaOptions(args, description, true);
            if (options.has("loadMode") && !"bulkLoad".equalsIgnoreCase((String) options.valueOf("loadMode"))) {
                throw new UnsupportedOperationException("This loader supports bulkLoad load mode only, but "
                        + options.valueOf("loadMode") + " has been supplied.");
            }
            String dataFile = (String) options.valueOf("data");
            File metaFile = new File((String) options.valueOf("meta"));
            isIncrementalUpdateMode = options.has("overwrite-existing");

            Properties properties = new Properties();
            properties.load(new java.io.FileInputStream(metaFile));

            CancerStudy cancerStudy = getCancerStudy(properties);

            if (!isIncrementalUpdateMode && dataExistsForCancerStudy(cancerStudy.getInternalId())) {
                throw new IllegalArgumentException(dataTypeLabel() + " data for cancer study "
                        + cancerStudy.getCancerStudyStableId() + " has already been imported: " + dataFile);
            }

            ClickHouseBulkLoader.bulkLoadOn();
            importData(new File(dataFile), cancerStudy);
            if (isIncrementalUpdateMode) {
                deleteDataForSamples(cancerStudy.getInternalId(), processedSampleIds);
            }
            ClickHouseBulkLoader.flushAll();
            ClickHouseBulkLoader.bulkLoadOff();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CancerStudy getCancerStudy(Properties properties) throws DaoException {
        String stableId = properties.getProperty("cancer_study_identifier").trim();
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId(stableId);
        if (cancerStudy == null) {
            throw new RuntimeException("Unknown cancer study: " + stableId);
        }
        return cancerStudy;
    }

    private void importData(File file, CancerStudy cancerStudy) throws IOException, DaoException {
        int numLines = FileUtil.getNumLines(file);
        ProgressMonitor.setCurrentMessage("Importing " + dataTypeLabel() + " data from file: " + file.getCanonicalPath());
        ProgressMonitor.setMaxValue(numLines);
        entriesSkipped = 0;

        try (FileReader reader = new FileReader(file);
             BufferedReader buf = new BufferedReader(reader)) {
            String headerLine = buf.readLine();
            if (headerLine == null) {
                throw new RuntimeException("Empty data file: " + file.getCanonicalPath());
            }
            Map<String, Integer> colIndex = buildColumnIndex(headerLine.split("\t"));

            String line;
            while ((line = buf.readLine()) != null) {
                ProgressMonitor.incrementCurValue();
                ConsoleUtil.showProgress();
                if (line.trim().isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (!processRow(line, colIndex, cancerStudy)) {
                    entriesSkipped++;
                }
            }
        }
        if (entriesSkipped > 0) {
            ProgressMonitor.setCurrentMessage(" --> total number of data entries skipped: " + entriesSkipped);
        }
    }

    private Map<String, Integer> buildColumnIndex(String[] headers) {
        Map<String, Integer> colIndex = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            colIndex.put(headers[i].trim().toUpperCase(), i);
        }
        requireColumn(colIndex, SAMPLE_ID_COLUMN);
        requireColumn(colIndex, CHROMOSOME_COLUMN);
        requireColumn(colIndex, START_POSITION_COLUMN);
        requireColumn(colIndex, END_POSITION_COLUMN);
        requireColumn(colIndex, TCN_COLUMN);
        requireColumn(colIndex, LCN_COLUMN);
        requireColumn(colIndex, CELLULAR_FRACTION_COLUMN);
        requireColumn(colIndex, PURITY_COLUMN);
        for (String column : getAdditionalRequiredColumns()) {
            requireColumn(colIndex, column);
        }
        return colIndex;
    }

    private void requireColumn(Map<String, Integer> colIndex, String column) {
        if (!colIndex.containsKey(column)) {
            throw new RuntimeException("Error: the following column should be present in the " + dataTypeLabel()
                    + " data file: " + column);
        }
    }

    private boolean processRow(String line, Map<String, Integer> colIndex, CancerStudy cancerStudy) throws DaoException {
        String[] parts = line.split("\t", -1);

        String stableSampleId = StableIdUtil.getSampleId(getValue(parts, colIndex, SAMPLE_ID_COLUMN));
        Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), stableSampleId);
        if (sample == null) {
            if (StableIdUtil.isNormal(stableSampleId)) {
                return false;
            }
            throw new RuntimeException("Unknown sample id '" + stableSampleId + "' found in " + dataTypeLabel() + " data file.");
        }

        String chrom = getValue(parts, colIndex, CHROMOSOME_COLUMN);
        ValidationUtils.validateChromosome(chrom);

        long start = Long.parseLong(getValue(parts, colIndex, START_POSITION_COLUMN).trim());
        long end = Long.parseLong(getValue(parts, colIndex, END_POSITION_COLUMN).trim());
        if (start >= end) {
            ProgressMonitor.logWarning("Start position is not lower than end position. Skipping this entry.");
            return false;
        }

        Double tcn = parseNullableDouble(getValue(parts, colIndex, TCN_COLUMN));
        Double lcn = parseNullableDouble(getValue(parts, colIndex, LCN_COLUMN));
        Double cellularFraction = parseNullableDouble(getValue(parts, colIndex, CELLULAR_FRACTION_COLUMN));
        Double purity = parseNullableDouble(getValue(parts, colIndex, PURITY_COLUMN));

        CommonFacetsFields common = new CommonFacetsFields(chrom, start, end, tcn, lcn, cellularFraction, purity);
        boolean stored = storeRow(common, parts, colIndex, cancerStudy, sample);
        if (stored) {
            processedSampleIds.add(sample.getInternalId());
        }
        return stored;
    }

    protected static String getValue(String[] parts, Map<String, Integer> colIndex, String column) {
        Integer index = colIndex.get(column);
        if (index == null || index >= parts.length) {
            throw new RuntimeException("Missing value for column '" + column + "' in data row.");
        }
        return parts[index];
    }

    private static Double parseNullableDouble(String value) {
        if (value == null || value.trim().isEmpty() || "NA".equalsIgnoreCase(value.trim())) {
            return null;
        }
        return Double.parseDouble(value.trim());
    }

    /** Common, already-validated fields for a single FACETS data row. */
    protected static final class CommonFacetsFields {
        final String chr;
        final long start;
        final long end;
        final Double tcn;
        final Double lcn;
        final Double cellularFraction;
        final Double purity;

        CommonFacetsFields(String chr, long start, long end, Double tcn, Double lcn, Double cellularFraction, Double purity) {
            this.chr = chr;
            this.start = start;
            this.end = end;
            this.tcn = tcn;
            this.lcn = lcn;
            this.cellularFraction = cellularFraction;
            this.purity = purity;
        }
    }
}
