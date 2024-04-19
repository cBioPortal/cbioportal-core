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

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoSampleList;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.SampleList;
import org.mskcc.cbio.portal.util.CaseList;
import org.mskcc.cbio.portal.util.CaseListReader;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.mskcc.cbio.portal.validate.CaseListValidator;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

//TODO Make this loader to accept sample_ids_only
public class UpdateCaseListsSampleIds extends ConsoleRunnable {

    private File metaFile;
    private File dataFile;
    private List<File> caseListFiles = List.of();
    private String cancerStudyStableId;
    private Map<String, Set<String>> caseListSampleIdToSampleIds = new LinkedHashMap<>();
    private DaoSampleList daoSampleList = new DaoSampleList();
    private LinkedHashSet<String> allSampleIds;

    public UpdateCaseListsSampleIds(String[] args) {
        super(args);
    }

    /**
     * Updates case list sample ids from clinical sample and case list files
     */
    public void run() {
        parseArguments();
        readStudyIdAndDataFileFromMetaFile();
        this.allSampleIds = readSampleIdsFromDataFile(this.dataFile);
        // TODO has the all case list always to exist?
        this.caseListSampleIdToSampleIds.put(cancerStudyStableId + "_all", this.allSampleIds);
        Map<String, Set<String>> readCaseListSampleIds = readCaseListFiles();
        this.caseListSampleIdToSampleIds.putAll(readCaseListSampleIds);
        updateCaseLists(this.caseListSampleIdToSampleIds);
    }

    private Map<String, Set<String>> readCaseListFiles() {
        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>();
        for (File caseListFile: this.caseListFiles) {
            CaseList caseList = CaseListReader.readFile(caseListFile);
            CaseListValidator.validateIdFields(caseList);
            String cancerStudyIdentifier = caseList.getCancerStudyIdentifier();
            if (!cancerStudyIdentifier.equals(this.cancerStudyStableId)) {
                ProgressMonitor.logWarning(
                        String.format(
                                "Skipping %s case list file as it belongs to %s study and we uploading %s study.",
                                caseListFile, cancerStudyIdentifier, this.cancerStudyStableId));
                continue;
            }
            LinkedHashSet<String> extraSampleIds = new LinkedHashSet<>(caseList.getSampleIds());
            extraSampleIds.removeAll(this.allSampleIds);
            if (!extraSampleIds.isEmpty()) {
                throw new RuntimeException(caseListFile.getAbsolutePath() + ": The following sample ids present in the case list file, but not specified in the clinical sample file: " + String.join(", ", extraSampleIds));
            }
            result.put(caseList.getStableId(), new LinkedHashSet<>(caseList.getSampleIds()));
        }
        return result;
    }

    private void updateCaseLists(Map<String, Set<String>> caseListSampleIdToSampleIds) {
        // TODO Do we really have to do this? Is there a better way?
        DaoCancerStudy.reCacheAll();
        try {
            for (Map.Entry<String, Set<String>> caseListStableIdToSampleIds: caseListSampleIdToSampleIds.entrySet()) {
                String caseListStableId = caseListStableIdToSampleIds.getKey();
                Set<String> sampleIds = caseListStableIdToSampleIds.getValue();
                SampleList sampleList = daoSampleList.getSampleListByStableId(caseListStableId);
                if (sampleList == null) {
                    throw new RuntimeException("No case list with " + caseListStableId + " stable id is found");
                }
                LinkedHashSet<String> newCaseListSampleIds = new LinkedHashSet<>(sampleIds);
                newCaseListSampleIds.addAll(sampleList.getSampleList());
                ArrayList<String> newSampleArrayList = new ArrayList<>(newCaseListSampleIds);
                sampleList.setSampleList(newSampleArrayList);
                //TODO no need to run expensive db update if sampleList hasn't effectively changed
                daoSampleList.updateSampleListList(sampleList);
            }
            CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId(this.cancerStudyStableId);
            List<SampleList> sampleLists = daoSampleList.getAllSampleLists(cancerStudy.getInternalId());
            List<SampleList> remainingLists = sampleLists.stream().filter(sl ->
                    !caseListSampleIdToSampleIds.containsKey(sl.getStableId()) && sl.getSampleList().stream().anyMatch(this.allSampleIds::contains)
            ).collect(Collectors.toList());
            for (SampleList remainingList: remainingLists) {
                ArrayList<String> newSampleList = new ArrayList<>(remainingList.getSampleList());
                newSampleList.removeAll(this.allSampleIds);
                remainingList.setSampleList(newSampleList);
                //TODO for optimization purpose we could supply to the update method 2 set of samples: samples that have to be added and samples that have to be removed
                daoSampleList.updateSampleListList(remainingList);
            }
        } catch (DaoException e) {
            throw new RuntimeException(e);
        }
    }

    private LinkedHashSet<String> readSampleIdsFromDataFile(File dataFile) {
        LinkedHashSet<String> allSampleIds = new LinkedHashSet<>();
        FileReader reader = null;
        try {
            reader = new FileReader(dataFile);
            try (BufferedReader buff = new BufferedReader(reader)) {
                String line;
                int sampleIdPosition = -1;
                while ((line = buff.readLine()) != null) {
                    String trimmedLine = line.trim();
                    if (trimmedLine.isEmpty() || trimmedLine.startsWith("#")) {
                        continue;
                    }

                    String[] fieldValues = line.split("\t");
                    if (sampleIdPosition == -1) {
                        sampleIdPosition = List.of(fieldValues).indexOf("SAMPLE_ID");
                        if (sampleIdPosition == -1) {
                            throw new RuntimeException("No SAMPLE_ID header is found");
                        }
                    } else {
                        allSampleIds.add(fieldValues[sampleIdPosition].trim());
                    }
                }
                return allSampleIds;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void readStudyIdAndDataFileFromMetaFile() {
        TrimmedProperties properties = new TrimmedProperties();
        try {
            FileInputStream inStream = new FileInputStream(this.metaFile);
            properties.load(inStream);
            this.cancerStudyStableId = properties.getProperty("cancer_study_identifier");
            String dataFilename = properties.getProperty("data_filename");
            this.dataFile = new File(metaFile.getParent(), dataFilename);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void parseArguments() {
        String progName = getClass().getName();
        String description = "Updates (adds/removes) sample ids in specified case lists.";

        OptionParser parser = new OptionParser();
        OptionSpec<String> metaOpt = parser.accepts( "meta",
               "clinical sample (genetic_alteration_type=CLINICAL and datatype=SAMPLE_ATTRIBUTES or datatype=MIXED_ATTRIBUTES) meta data file. All sample ids found in the file will be added to the _all case list." ).withRequiredArg().required().describedAs( "meta_clinical_sample.txt" ).ofType( String.class );
        OptionSpec<String> caseListDirOrFileOpt = parser.accepts( "case-lists",
                "case list file or a directory with case list files" ).withRequiredArg().describedAs( "case_lists/" ).ofType( String.class );

        try {
            OptionSet options = parser.parse( args );
            this.metaFile = new File(options.valueOf(metaOpt));
            if(options.has(caseListDirOrFileOpt)){
                File caseListDirOrFile = new File(options.valueOf(caseListDirOrFileOpt));
                if (caseListDirOrFile.isDirectory()) {
                    this.caseListFiles = Arrays.stream(Objects.requireNonNull(caseListDirOrFile.listFiles()))
                            .filter(file -> !file.getName().startsWith(".") && !file.getName().endsWith("~")).collect(Collectors.toList());
                } else if (caseListDirOrFile.isFile()) {
                    this.caseListFiles = List.of(caseListDirOrFile);
                } else {
                    throw new RuntimeException("No file " + caseListDirOrFile.getAbsolutePath() + " exists");
                }
            }
        } catch (OptionException e) {
            throw new UsageException(
                    progName, description, parser,
                    e.getMessage());
        }
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args  the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new UpdateCaseListsSampleIds(args);
        runner.runInConsole();
    }
}
