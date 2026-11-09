/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.mskcc.cbio.portal.scripts;

import java.io.*;
import java.util.*;
import joptsimple.OptionSet;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.ClickHouseOptimizer;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoCopyNumberSegment;
import org.mskcc.cbio.portal.dao.DaoCopyNumberSegmentFile;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.CopyNumberSegment;
import org.mskcc.cbio.portal.model.CopyNumberSegmentFile;
import org.mskcc.cbio.portal.model.ReferenceGenome;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.FileUtil;
import org.mskcc.cbio.portal.util.FractionGenomeAlteredCalculator;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.mskcc.cbio.portal.util.StableIdUtil;

/**
 * Imports Segment data into {@code copy_number_seg}. The 6 core columns (ID,
 * chrom, loc.start, loc.end, num.mark, seg.mean) are always required; TCN,
 * LCN, CELLULAR_FRACTION, PURITY, PLOIDY and DIP_LOG_R are optional
 * supplemental ASCN (allele-specific copy number) columns, present only for
 * ALLELE_SPECIFIC_COPY_NUMBER studies and otherwise left
 * null. Columns are resolved by header name rather than position, so the
 * optional ASCN columns may appear anywhere in the file and plain SEG files
 * (with no ASCN columns at all) are handled by the same code path.
 *
 * TCN/LCN are stored as whole-number allele counts. A value that parses as a
 * number but isn't a whole number (e.g. "2.5") is neither silently accepted
 * nor rejected: it is rounded to the nearest integer and a warning is logged
 * via {@link #parseNullableInteger}, so the discrepancy is visible without
 * failing the import.
 *
 * A blank cell or the exact (case-sensitive) string "NA" in an optional ASCN
 * column is parsed as a real Java {@code null} (see {@link #isNullMarker}),
 * not the literal string "NA" — {@link ClickHouseBulkLoader} serializes that
 * {@code null} as ClickHouse's native TSV null marker ({@code \N}), so the
 * column is stored as a genuine SQL {@code NULL}. The Python validator
 * (validateData.py's {@code SegValidator}) recognizes the same exact-case
 * "NA" marker, matching the convention already used for other NA-bearing
 * numeric matrix columns in this codebase.
 *
 * @author jj
 */
public class ImportCopyNumberSegmentData extends ConsoleRunnable {

    private static final String ID_COLUMN = "ID";
    private static final String CHROM_COLUMN = "chrom";
    private static final String LOC_START_COLUMN = "loc.start";
    private static final String LOC_END_COLUMN = "loc.end";
    private static final String NUM_MARK_COLUMN = "num.mark";
    private static final String SEG_MEAN_COLUMN = "seg.mean";
    private static final String TCN_COLUMN = "TCN";
    private static final String LCN_COLUMN = "LCN";
    private static final String CELLULAR_FRACTION_COLUMN = "CELLULAR_FRACTION";
    private static final String PURITY_COLUMN = "PURITY";
    private static final String PLOIDY_COLUMN = "PLOIDY";
    private static final String DIP_LOG_R_COLUMN = "DIP_LOG_R";

    private int entriesSkipped;
    private boolean isIncrementalUpdateMode;
    private Set<Integer> processedSampleIds;
    private FractionGenomeAlteredCalculator fractionGenomeAlteredCalculator;

    private void importData(File file, int cancerStudyId) throws IOException, DaoException {
        FileReader reader = new FileReader(file);
        BufferedReader buf = new BufferedReader(reader);
        try {
            String headerLine = buf.readLine();
            if (headerLine == null) {
                throw new RuntimeException("Empty data file: " + file.getCanonicalPath());
            }
            Map<String, Integer> colIndex = buildColumnIndex(headerLine.split("\t"));

            long segId = DaoCopyNumberSegment.getLargestId();
            processedSampleIds = new HashSet<>();
            fractionGenomeAlteredCalculator = new FractionGenomeAlteredCalculator();
            String line;
            while ((line=buf.readLine()) != null) {
                ProgressMonitor.incrementCurValue();
                ConsoleUtil.showProgress();

                if (line.trim().isEmpty() || line.startsWith("#")) {
                    continue;
                }

                String[] parts = line.split("\t", -1);

                String chrom = getValue(parts, colIndex, CHROM_COLUMN).trim();
                //validate in same way as GistitReader:
                ValidationUtils.validateChromosome(chrom);

                long start = Double.valueOf(getValue(parts, colIndex, LOC_START_COLUMN)).longValue();
                long end = Double.valueOf(getValue(parts, colIndex, LOC_END_COLUMN)).longValue();
                if (start >= end) {
                    //workaround to skip with warning, according to https://github.com/cBioPortal/cbioportal/issues/839#issuecomment-203452415
                    ProgressMonitor.logWarning("Start position of segment is not lower than end position. Skipping this entry.");
                    entriesSkipped++;
                    continue;
                }
                int numProbes = (int) Double.parseDouble(getValue(parts, colIndex, NUM_MARK_COLUMN));
                double segMean = Double.parseDouble(getValue(parts, colIndex, SEG_MEAN_COLUMN));

                String sampleId = StableIdUtil.getSampleId(getValue(parts, colIndex, ID_COLUMN));
                Sample s = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudyId, sampleId);
                if (s == null) {
                    if (StableIdUtil.isNormal(sampleId)) {
                        entriesSkipped++;
                        continue;
                    }
                    else {
                        //this likely will not be reached since samples are added on the fly above if not known to database
                        throw new RuntimeException("Unknown sample id '" + sampleId + "' found in seg file: " + file.getCanonicalPath());
                    }
                }
                CopyNumberSegment cns = new CopyNumberSegment(cancerStudyId, s.getInternalId(), chrom, start, end, numProbes, segMean);
                cns.setSegId(++segId);
                cns.setTcn(parseNullableInteger(getOptionalValue(parts, colIndex, TCN_COLUMN), TCN_COLUMN));
                cns.setLcn(parseNullableInteger(getOptionalValue(parts, colIndex, LCN_COLUMN), LCN_COLUMN));
                cns.setCellularFraction(parseNullableDouble(getOptionalValue(parts, colIndex, CELLULAR_FRACTION_COLUMN)));
                cns.setPurity(parseNullableDouble(getOptionalValue(parts, colIndex, PURITY_COLUMN)));
                cns.setPloidy(parseNullableDouble(getOptionalValue(parts, colIndex, PLOIDY_COLUMN)));
                cns.setDipLogR(parseNullableDouble(getOptionalValue(parts, colIndex, DIP_LOG_R_COLUMN)));
                DaoCopyNumberSegment.addCopyNumberSegment(cns);
                processedSampleIds.add(s.getInternalId());
                fractionGenomeAlteredCalculator.addSegment(s.getInternalId(), start, end, segMean);
            }
            if (isIncrementalUpdateMode) {
                DaoCopyNumberSegment.deleteSegmentDataForSamples(cancerStudyId, processedSampleIds);
            }
        }
        finally {
            buf.close();
        }
    }

    private static Map<String, Integer> buildColumnIndex(String[] headers) {
        Map<String, Integer> colIndex = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            colIndex.put(headers[i].trim(), i);
        }
        requireColumn(colIndex, ID_COLUMN);
        requireColumn(colIndex, CHROM_COLUMN);
        requireColumn(colIndex, LOC_START_COLUMN);
        requireColumn(colIndex, LOC_END_COLUMN);
        requireColumn(colIndex, NUM_MARK_COLUMN);
        requireColumn(colIndex, SEG_MEAN_COLUMN);
        return colIndex;
    }

    private static void requireColumn(Map<String, Integer> colIndex, String column) {
        if (!colIndex.containsKey(column)) {
            throw new RuntimeException("Error: the following column should be present in the segment data file: " + column);
        }
    }

    private static String getValue(String[] parts, Map<String, Integer> colIndex, String column) {
        Integer index = colIndex.get(column);
        if (index == null || index >= parts.length) {
            throw new RuntimeException("Missing value for column '" + column + "' in data row.");
        }
        return parts[index];
    }

    /**
     * Returns the raw value for an optional (ASCN-only) column, or null if the
     * column isn't present in this file at all (a plain, non-ASCN SEG file).
     */
    private static String getOptionalValue(String[] parts, Map<String, Integer> colIndex, String column) {
        Integer index = colIndex.get(column);
        if (index == null || index >= parts.length) {
            return null;
        }
        return parts[index];
    }

    private static Double parseNullableDouble(String value) {
        if (isNullMarker(value)) {
            return null;
        }
        return Double.parseDouble(value.trim());
    }

    /**
     * Parses a nullable integer-constrained column (TCN/LCN). A value that is
     * numeric but not whole (e.g. "2.5") is rounded to the nearest integer and a
     * warning is logged, rather than being silently accepted as-is or rejected
     * outright. A value that isn't numeric at all still fails fast via
     * {@link NumberFormatException}.
     */
    private static Integer parseNullableInteger(String value, String columnName) {
        if (isNullMarker(value)) {
            return null;
        }
        String trimmed = value.trim();
        double parsed = Double.parseDouble(trimmed);
        int rounded = (int) Math.round(parsed);
        if (parsed != rounded) {
            ProgressMonitor.logWarning("Non-integer value '" + trimmed + "' found for column '" + columnName
                    + "'. Rounding to nearest integer: " + rounded + ".");
        }
        return rounded;
    }

    /**
     * A blank cell or the literal (case-sensitive) string "NA" marks an
     * optional ASCN column as absent. "NA" is matched exactly, not
     * case-insensitively, to agree with the Python validator (SegValidator),
     * which only recognizes exact-case "NA" — matching the convention already
     * used for other NA-bearing numeric matrix columns in this codebase (see
     * ContinuousValuesValidator.NULL_VALUES in validateData.py). A value that
     * doesn't match this exactly (e.g. "na", "N/A") is treated as a real,
     * unparseable value and fails fast via NumberFormatException, rather than
     * being silently nulled out.
     */
    private static boolean isNullMarker(String value) {
        return value == null || value.trim().isEmpty() || "NA".equals(value.trim());
    }

    public void run() {
        try {
            String description = "Import 'segment data' files";

            OptionSet options = ConsoleUtil.parseStandardDataAndMetaOptions(args, description, true);
            if (options.has("loadMode") && !"bulkLoad".equalsIgnoreCase((String) options.valueOf("loadMode"))) {
                throw new UnsupportedOperationException("This loader supports bulkLoad load mode only, but "
                        + options.valueOf("loadMode")
                        + " has been supplied.");
            }
            String dataFile = (String) options.valueOf("data");
            File descriptorFile = new File((String) options.valueOf("meta"));
            isIncrementalUpdateMode = options.has("overwrite-existing");

            Properties properties = new Properties();
            properties.load(new FileInputStream(descriptorFile));

            ProgressMonitor.setCurrentMessage("Reading data from:  " + dataFile);

            CancerStudy cancerStudy = getCancerStudy(properties);

            if (!isIncrementalUpdateMode && segmentDataExistsForCancerStudy(cancerStudy)) {
                 throw new IllegalArgumentException("Seg data for cancer study " + cancerStudy.getCancerStudyStableId() + " has already been imported: " + dataFile);
            }
            ClickHouseBulkLoader.bulkLoadOn();
            importCopyNumberSegmentFileMetadata(cancerStudy, properties, isIncrementalUpdateMode);
            importCopyNumberSegmentFileData(cancerStudy, dataFile);
            DaoCopyNumberSegment.createFractionGenomeAlteredClinicalData(cancerStudy.getInternalId(),
                    fractionGenomeAlteredCalculator.getFractionGenomeAltered(), isIncrementalUpdateMode);
            ClickHouseOptimizer.optimizeTables("clinical_sample", "clinical_patient");
            ClickHouseBulkLoader.bulkLoadOff();
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException|DaoException e) {
            throw new RuntimeException(e);
        }
    }

    private static CancerStudy getCancerStudy(Properties properties) throws DaoException {
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId(properties.getProperty("cancer_study_identifier").trim());
        if (cancerStudy == null) {
            throw new RuntimeException("Unknown cancer study: " + properties.getProperty("cancer_study_identifier").trim());
        }
        return cancerStudy;
    }

    private static boolean segmentDataExistsForCancerStudy(CancerStudy cancerStudy) throws DaoException {
        return (DaoCopyNumberSegment.segmentDataExistForCancerStudy(cancerStudy.getInternalId()));
    }

    /**
     * Registers (or, for an incremental update, validates against) the study's
     * segment-file metadata.
     */
    static void importCopyNumberSegmentFileMetadata(CancerStudy cancerStudy, Properties properties,
            boolean isIncrementalUpdateMode) throws DaoException {
        CopyNumberSegmentFile copyNumSegFile = new CopyNumberSegmentFile();
        copyNumSegFile.cancerStudyId = cancerStudy.getInternalId();
        String referenceGenomeId = properties.getProperty("reference_genome_id").trim();
        String referenceGenome = cancerStudy.getReferenceGenome();
        if (referenceGenome == null) {
            referenceGenome = ReferenceGenome.HOMO_SAPIENS_DEFAULT_GENOME_NAME;
        }
        if (!referenceGenomeId.equalsIgnoreCase(referenceGenome)) {
            ProgressMonitor.setCurrentMessage(" Genome Build Name does not match, expecting "
                    + cancerStudy.getReferenceGenome());
        }
        copyNumSegFile.referenceGenomeId = getRefGenId(referenceGenomeId);
        copyNumSegFile.description = properties.getProperty("description").trim();
        copyNumSegFile.filename = properties.getProperty("data_filename").trim();
        CopyNumberSegmentFile storedCopyNumSegFile = DaoCopyNumberSegmentFile.getCopyNumberSegmentFile(cancerStudy.getInternalId());
        if (isIncrementalUpdateMode && storedCopyNumSegFile != null) {
            if (storedCopyNumSegFile.referenceGenomeId != copyNumSegFile.referenceGenomeId) {
                throw new IllegalStateException("You are trying to upload "
                        + copyNumSegFile.referenceGenomeId
                        + " reference genome data into "
                        + storedCopyNumSegFile.referenceGenomeId
                        + " reference genome data.");
            }
        } else {
            DaoCopyNumberSegmentFile.addCopyNumberSegmentFile(copyNumSegFile);
        }
    }

    private void importCopyNumberSegmentFileData(CancerStudy cancerStudy, String dataFilename) throws IOException, DaoException {
        File file = new File(dataFilename);
        int numLines = FileUtil.getNumLines(file);
        ProgressMonitor.setCurrentMessage(" --> total number of data lines:  " + (numLines-1));
        ProgressMonitor.setMaxValue(numLines);
        entriesSkipped = 0;
        importData(file, cancerStudy.getInternalId());
        ProgressMonitor.setCurrentMessage(" --> total number of entries skipped:  " + entriesSkipped);
    }

    private static CopyNumberSegmentFile.ReferenceGenomeId getRefGenId(String potentialRefGenId) {
        if (CopyNumberSegmentFile.ReferenceGenomeId.has(potentialRefGenId)) {
            return CopyNumberSegmentFile.ReferenceGenomeId.valueOf(potentialRefGenId);
        }
        else {
            throw new RuntimeException ("Unknown reference genome id: " + potentialRefGenId);
        }
    }

    /**
     * Makes an instance to run with the given command line arguments.
     *
     * @param args  the command line arguments to be used
     */
    public ImportCopyNumberSegmentData(String[] args) {
        super(args);
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args  the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new ImportCopyNumberSegmentData(args);
        runner.runInConsole();
    }
}
