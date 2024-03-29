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

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoSampleList;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.SampleList;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class UpdateCaseListsSampleIds extends ConsoleRunnable {

    private File metaFile;
    private File dataFile;
    private Set<String> addToCaseListsStableIds = Set.of();
    private String cancerStudyStableId;
    private LinkedHashSet<String> sampleIds;
    private DaoSampleList daoSampleList = new DaoSampleList();

    public UpdateCaseListsSampleIds(String[] args) {
        super(args);
    }

    /**
     * Imports clinical data and clinical attributes (from the worksheet)
     */
    public void run() {
        parseArguments();
        readStudyIdAndDataFileFromMetaFile();
        readSampleIdsFromDataFile();
        updateCaseLists();
    }

    private void updateCaseLists() {
        // TODO Do we really have to do this? Is there a better way?
        DaoCancerStudy.reCacheAll();
        try {
            Set<String> addSamplesToTheCaseListsStableIds = new LinkedHashSet<>(this.addToCaseListsStableIds);
            // TODO has the all case list always to exist?
            String allCaseListStableId = this.cancerStudyStableId + "_all";
            // we always add sample to the all case list
            addSamplesToTheCaseListsStableIds.add(allCaseListStableId);
            for (String caseListStableId: addSamplesToTheCaseListsStableIds) {
                SampleList sampleList = daoSampleList.getSampleListByStableId(caseListStableId);
                if (sampleList == null) {
                    throw new RuntimeException("No case list with " + caseListStableId + " stable id is found");
                }
                LinkedHashSet<String> newCaseListSampleIds = new LinkedHashSet<>(this.sampleIds);
                newCaseListSampleIds.addAll(sampleList.getSampleList());
                ArrayList<String> newSampleArrayList = new ArrayList<>(newCaseListSampleIds);
                sampleList.setSampleList(newSampleArrayList);
                //TODO no need to run expensive db update if sampleList hasn't effectively changed
                daoSampleList.updateSampleListList(sampleList);
            }
            CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId(this.cancerStudyStableId);
            List<SampleList> sampleLists = daoSampleList.getAllSampleLists(cancerStudy.getInternalId());
            List<SampleList> remainingLists = sampleLists.stream().filter(sl ->
                    !addSamplesToTheCaseListsStableIds.contains(sl.getStableId()) && sl.getSampleList().stream().anyMatch(this.sampleIds::contains)
            ).collect(Collectors.toList());
            for (SampleList remainingList: remainingLists) {
                ArrayList<String> newSampleList = new ArrayList<>(remainingList.getSampleList());
                newSampleList.removeAll(this.sampleIds);
                remainingList.setSampleList(newSampleList);
                //TODO for optimization purpose we could supply to the update method 2 set of samples: samples that have to be added and samples that have to be removed
                daoSampleList.updateSampleListList(remainingList);
            }
        } catch (DaoException e) {
            throw new RuntimeException(e);
        }
    }

    private void readSampleIdsFromDataFile() {
        this.sampleIds = new LinkedHashSet<>();
        FileReader reader = null;
        try {
            reader = new FileReader(this.dataFile);
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
                        sampleIds.add(fieldValues[sampleIdPosition].trim());
                    }
                }
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
        //TODO Do we want to have --sample-ids option instead to make command more flexible which samples we want to add to a given profile?
        OptionSpec<String> meta = parser.accepts( "meta",
               "clinical sample (genetic_alteration_type=CLINICAL and datatype=SAMPLE_ATTRIBUTES) meta data file" ).withRequiredArg().required().describedAs( "meta_clinical_sample.txt" ).ofType( String.class );
        OptionSpec<String> addToCaseLists = parser.accepts( "add-to-case-lists",
                "comma-separated list of case list stable ids to add sample ids found in the data file" ).withRequiredArg().describedAs( "study_id_mrna,study_id_sequenced" ).ofType( String.class );

        try {
            OptionSet options = parser.parse( args );
            this.metaFile = new File(options.valueOf(meta));
            if(options.has(addToCaseLists)){
                this.addToCaseListsStableIds = new LinkedHashSet<>(List.of(options.valueOf(addToCaseLists).split(",")));
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
