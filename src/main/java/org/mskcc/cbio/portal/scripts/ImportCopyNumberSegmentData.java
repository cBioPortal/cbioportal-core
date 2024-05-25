/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
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

import joptsimple.OptionSet;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoCopyNumberSegment;
import org.mskcc.cbio.portal.dao.DaoCopyNumberSegmentFile;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.MySQLbulkLoader;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.CopyNumberSegment;
import org.mskcc.cbio.portal.model.CopyNumberSegmentFile;
import org.mskcc.cbio.portal.model.ReferenceGenome;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.FileUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.mskcc.cbio.portal.util.StableIdUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Import Segment data into database.
 * @author jj
 */
public class ImportCopyNumberSegmentData extends ConsoleRunnable {

    private int entriesSkipped;
    private boolean updateMode;
    private Set<Integer> processedSampleIds;

    private void importData(File file, int cancerStudyId) throws IOException, DaoException {
        MySQLbulkLoader.bulkLoadOn();
        FileReader reader = new FileReader(file);
        BufferedReader buf = new BufferedReader(reader);
        try {
            String line = buf.readLine(); // skip header line
            long segId = DaoCopyNumberSegment.getLargestId();
            processedSampleIds = new HashSet<>();
            while ((line=buf.readLine()) != null) {
                ProgressMonitor.incrementCurValue();
                ConsoleUtil.showProgress();
                
                String[] strs = line.split("\t");
                if (strs.length<6) {
                    System.err.println("wrong format: "+line);
                }

                String chrom = strs[1].trim();
                //validate in same way as GistitReader:
                ValidationUtils.validateChromosome(chrom);
                
                long start = Double.valueOf(strs[2]).longValue();
                long end = Double.valueOf(strs[3]).longValue();
                if (start >= end) {
                    //workaround to skip with warning, according to https://github.com/cBioPortal/cbioportal/issues/839#issuecomment-203452415
                    ProgressMonitor.logWarning("Start position of segment is not lower than end position. Skipping this entry.");
                    entriesSkipped++;
                    continue;
                }            
                int numProbes = new BigDecimal((strs[4])).intValue();
                double segMean = Double.parseDouble(strs[5]);

                String sampleId = StableIdUtil.getSampleId(strs[0]);
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
                DaoCopyNumberSegment.addCopyNumberSegment(cns);
                processedSampleIds.add(s.getInternalId());
            }
            if (updateMode) {
                DaoCopyNumberSegment.deleteSegmentDataForSamples(cancerStudyId, processedSampleIds);
            }
            MySQLbulkLoader.flushAll();
        }
        finally {
            buf.close();
        }
    }
    
    public void run() {
        try {
            String description = "Import 'segment data' files";
            
            OptionSet options = ConsoleUtil.parseStandardDataAndMetaOptions(args, description, true);
            String dataFile = (String) options.valueOf("data");
            File descriptorFile = new File((String) options.valueOf("meta"));
            updateMode = options.has("overwrite-existing");
        
            Properties properties = new Properties();
            properties.load(new FileInputStream(descriptorFile));
            
            ProgressMonitor.setCurrentMessage("Reading data from:  " + dataFile);
            
            CancerStudy cancerStudy = getCancerStudy(properties);
            
            if (!updateMode && segmentDataExistsForCancerStudy(cancerStudy)) {
                 throw new IllegalArgumentException("Seg data for cancer study " + cancerStudy.getCancerStudyStableId() + " has already been imported: " + dataFile);
            }
        
            importCopyNumberSegmentFileMetadata(cancerStudy, properties);
            importCopyNumberSegmentFileData(cancerStudy, dataFile);
            DaoCopyNumberSegment.createFractionGenomeAlteredClinicalData(cancerStudy.getInternalId(), processedSampleIds, updateMode);
            if( MySQLbulkLoader.isBulkLoad()) {
                MySQLbulkLoader.flushAll();
            }
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

    private void importCopyNumberSegmentFileMetadata(CancerStudy cancerStudy, Properties properties) throws DaoException {
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
        if (updateMode && storedCopyNumSegFile != null) {
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
