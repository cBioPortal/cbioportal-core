/*
 * Copyright (c) 2018 - 2022 The Hyve B.V.
 * This code is licensed under the GNU Affero General Public License (AGPL),
 * version 3, or (at your option) any later version.
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

import com.google.common.base.*;
import com.google.common.collect.*;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.util.*;

import java.io.*;
import java.util.Optional;
import java.util.*;
import java.util.stream.*;

import static com.google.common.collect.Lists.*;
import static java.lang.String.*;
import static org.cbioportal.model.MolecularProfile.DataType.DISCRETE;
import static org.cbioportal.model.MolecularProfile.ImportType.DISCRETE_LONG;

public class ImportCnaDiscreteLongData {

    private final File cnaFile;
    private final int geneticProfileId;
    private final GeneticAlterationImporter geneticAlterationGeneImporter;
    private String genePanel;
    private final DaoGeneOptimized daoGene;
    private CnaUtil cnaUtil;
    private Set<CnaEvent.Event> existingCnaEvents = new HashSet<>();
    private int samplesSkipped = 0;
    private Set<String> namespaces;

    private boolean updateMode;

    private GeneticProfile geneticProfile;

    private final ArrayList<SampleIdGeneticProfileId> sampleIdGeneticProfileIds = new ArrayList<>();
    private ArrayList<Integer> orderedImportedSampleList;
    private ArrayList<Integer> orderedSampleList;

    private HashMap<Integer, HashMap<Integer, String>> geneticAlterationMap;

    public ImportCnaDiscreteLongData(
            File cnaFile,
            int geneticProfileId,
            String genePanel,
            DaoGeneOptimized daoGene,
            DaoGeneticAlteration daoGeneticAlteration,
            Set<String> namespaces,
            boolean updateMode
    ) {
        this.namespaces = namespaces;
        this.cnaFile = cnaFile;
        this.geneticProfileId = geneticProfileId;
        this.geneticProfile = DaoGeneticProfile.getGeneticProfileById(geneticProfileId);
        if (!Set.of(DISCRETE.name(), DISCRETE_LONG.name()).contains(geneticProfile.getDatatype())) {
            throw new IllegalStateException("Platform "
                    + geneticProfileId
                    + " has not supported datatype: "
                    + geneticProfile.getDatatype());
        }
        this.genePanel = genePanel;
        this.daoGene = daoGene;
        this.geneticAlterationGeneImporter = new GeneticAlterationImporter(geneticProfileId, daoGeneticAlteration);
        this.updateMode = updateMode;
    }

    public ImportCnaDiscreteLongData(
        File cnaFile,
        int geneticProfileId,
        String genePanel,
        DaoGeneOptimized daoGene,
        DaoGeneticAlteration daoGeneticAlteration,
        Set<String> namespaces
    ) {
       this(cnaFile, geneticProfileId, genePanel, daoGene, daoGeneticAlteration, namespaces, false);
    }

    public void importData() throws Exception {
        FileReader reader = new FileReader(this.cnaFile);
        BufferedReader buf = new BufferedReader(reader);

        // Pass first line with headers to util:
        String line = buf.readLine();
        int lineIndex = 1;
        String[] headerParts = line.split("\t", -1);
        this.cnaUtil = new CnaUtil(headerParts, this.namespaces);
        if (updateMode) {
            geneticAlterationMap = DaoGeneticAlteration.getInstance().getGeneticAlterationMapForEntityIds(geneticProfile.getGeneticProfileId(), null);
        }

        boolean isDiscretizedCnaProfile = geneticProfile != null
            && geneticProfile.getGeneticAlterationType() == GeneticAlterationType.COPY_NUMBER_ALTERATION
            && geneticProfile.showProfileInAnalysisTab();

        if (isDiscretizedCnaProfile) {
            existingCnaEvents.addAll(DaoCnaEvent.getAllCnaEvents());
            MySQLbulkLoader.bulkLoadOn();
        }

        CnaImportData toImport = new CnaImportData();

        while ((line = buf.readLine()) != null) {
            lineIndex++;
            ProgressMonitor.incrementCurValue();
            ConsoleUtil.showProgress();
            this.extractDataToImport(geneticProfile, line, lineIndex, toImport);
        }

        orderedSampleList = newArrayList(toImport.eventsTable.columnKeySet());
        if (updateMode) {
            ArrayList <Integer> savedOrderedSampleList = DaoGeneticProfileSamples.getOrderedSampleList(geneticProfileId);
            int initialOrderSampleListSize = savedOrderedSampleList.size();
            checkSamplesInDataEqualTo(initialOrderSampleListSize);
            // add all new sample ids at the end
            ArrayList<Integer> extendedSampleList = new ArrayList<>(savedOrderedSampleList);
            List<Integer> newSampleIds = orderedSampleList.stream().filter(sampleId -> !savedOrderedSampleList.contains(sampleId)).toList();
            extendedSampleList.addAll(newSampleIds);
            orderedImportedSampleList = orderedSampleList;
            orderedSampleList = extendedSampleList;


            DaoGeneticProfileSamples.deleteAllSamplesInGeneticProfile(geneticProfileId);
        }
        DaoGeneticProfileSamples.addGeneticProfileSamples(geneticProfileId, orderedSampleList);

        for (Long entrezId : toImport.eventsTable.rowKeySet()) {
            boolean added = storeGeneticAlterations(toImport, entrezId);
            if (added) {
                storeCnaEvents(toImport, entrezId);
            } else {
                ProgressMonitor.logWarning("Values not added to gene with entrezId: " + entrezId + ". Skip creation of cna events.");
            }
        }

        // Once the CNA import is done, update DISCRETE_LONG input datatype into resulting DISCRETE datatype:
        geneticProfile.setDatatype(DISCRETE.name());
        DaoGeneticProfile.updateDatatype(geneticProfile.getGeneticProfileId(), geneticProfile.getDatatype());

        ProgressMonitor.setCurrentMessage(" --> total number of samples skipped (normal samples): " + getSamplesSkipped());
        buf.close();
        expandRemainingGeneticEntityTabDelimitedRowsWithBlankValue();
        MySQLbulkLoader.flushAll();
    }

    /**
     * First we collect all the events
     * to import all events related to a single gene in one query:
     */
    public void extractDataToImport(
        GeneticProfile geneticProfile,
        String line,
        int lineIndex,
        CnaImportData importContainer
    ) throws Exception {
        if (!FileUtil.isInfoLine(line)) {
            return;
        }
        String[] lineParts = line.split("\t", -1);
        CanonicalGene gene = this.getGene(cnaUtil.getEntrezSymbol(lineParts), lineParts, cnaUtil);
        importContainer.genes.add(gene);

        if (gene == null) {
            ProgressMonitor.logWarning("Ignoring line with no Hugo_Symbol and no Entrez_Id");
            return;
        }

        int cancerStudyId = geneticProfile.getCancerStudyId();

        String sampleIdStr = cnaUtil.getSampleIdStr(lineParts);
        Sample sample = findSample(sampleIdStr, cancerStudyId);
        if (sample == null) {
            if (StableIdUtil.isNormal(sampleIdStr)) {
                return;
            }
            throw new RuntimeException("Sample with stable id " + sampleIdStr + " is not found in the database.");
        }
        createSampleProfile(sample);

        long entrezId = gene.getEntrezGeneId();
        int sampleId = sample.getInternalId();
        CnaEventImportData eventContainer = new CnaEventImportData();
        eventContainer.cnaEvent = cnaUtil.createEvent(geneticProfile, sampleId, entrezId, lineParts);

        Table<Long, Integer, CnaEventImportData> geneBySampleEventTable = importContainer.eventsTable;

        if (!geneBySampleEventTable.contains(entrezId, sample.getInternalId())) {
            geneBySampleEventTable.put(entrezId, sampleId, eventContainer);
        } else {
            ProgressMonitor.logWarning(format("Skipping line %d with duplicate gene %d and sample %d", lineIndex, entrezId, sampleId));
        }

    }

    /**
     * Store all cna events related to a single gene
     */
    private void storeCnaEvents(CnaImportData toImport, Long entrezId) throws DaoException {
        List<CnaEvent> events = toImport.eventsTable
            .row(entrezId)
            .values()
            .stream()
            .filter(v -> v.cnaEvent != null)
            .map(v -> v.cnaEvent)
            .collect(Collectors.toList());
        if (updateMode) {
            DaoCnaEvent.removeSampleCnaEvents(geneticProfileId, orderedImportedSampleList);
        }
        CnaUtil.storeCnaEvents(existingCnaEvents, events);
    }

    /**
     * Store all events related to a single gene
     */
    private boolean storeGeneticAlterations(CnaImportData toImport, Long entrezId) throws DaoException {
        String[] values = toImport.eventsTable
            .columnKeySet()
            .stream()
            .map(sample -> {
                CnaEventImportData event = toImport
                    .eventsTable
                    .get(entrezId, sample);
                if (event == null) {
                    return "";
                }
                return "" + event
                    .cnaEvent
                    .getAlteration()
                    .getCode();
            })
            .toArray(String[]::new);

        Optional<CanonicalGene> gene = toImport.genes
            .stream()
            .filter(g -> g != null && g.getEntrezGeneId() == entrezId)
            .findFirst();

        if (!gene.isPresent()) {
            ProgressMonitor.logWarning("No gene found for entrezId: " + entrezId);
            return false;
        }

        String geneSymbol = !Strings.isNullOrEmpty(gene.get().getHugoGeneSymbolAllCaps())
            ? gene.get().getHugoGeneSymbolAllCaps()
            : "" + entrezId;

        return saveValues(gene.get(), values, geneSymbol);
    }

    //TODO duplicate
    private void checkSamplesInDataEqualTo(int initialOrderSampleListSize) {
        geneticAlterationMap.forEach((geneticEntityId, sampleToValue) -> {
            if (sampleToValue.size() != initialOrderSampleListSize) {
                throw new IllegalStateException("Number of samples ("
                        + sampleToValue.size() + ") for genetic entity with id "
                        + geneticEntityId + " does not match with the number in the inital sample list ("
                        + initialOrderSampleListSize + ").");
            }
        });
    }

    //TODO duplicate
    private boolean saveValues(CanonicalGene canonicalGene, String[] values, String geneSymbol) throws DaoException {
        if (updateMode) {
            values = updateValues(canonicalGene.getGeneticEntityId(), values);
            if (!geneticAlterationGeneImporter.isImportedAlready(canonicalGene)) {
                DaoGeneticAlteration.getInstance().deleteAllRecordsInGeneticProfile(geneticProfile.getGeneticProfileId(), canonicalGene.getGeneticEntityId());
            }
        }
        return geneticAlterationGeneImporter.store(values, canonicalGene, geneSymbol);
    }
    //TODO duplicate
    private boolean saveValues(int geneticEntityId, String[] values) throws DaoException {
        if (updateMode) {
            DaoGeneticAlteration.getInstance().deleteAllRecordsInGeneticProfile(geneticProfile.getGeneticProfileId(), geneticEntityId);
            values = updateValues(geneticEntityId, values);
        }
        return geneticAlterationGeneImporter.store(geneticEntityId, values);
    }

    //TODO duplicate
    private String[] updateValues(int geneticEntityId, String[] values) {
        Map<Integer, String> sampleIdToValue = ArrayUtil.zip(orderedImportedSampleList.toArray(new Integer[0]), values);
        String[] updatedSampleValues = new String[orderedSampleList.size()];
        for (int i = 0; i < orderedSampleList.size(); i++) {
            updatedSampleValues[i] = "";
            int sampleId = orderedSampleList.get(i);
            if (geneticAlterationMap.containsKey(geneticEntityId)) {
                HashMap<Integer, String> savedSampleIdToValue = geneticAlterationMap.get(geneticEntityId);
                updatedSampleValues[i] = savedSampleIdToValue.containsKey(sampleId) ? savedSampleIdToValue.remove(sampleId): "";
                if (savedSampleIdToValue.isEmpty()) {
                    geneticAlterationMap.remove(geneticEntityId);
                }
            }
            if (sampleIdToValue.containsKey(sampleId)) {
                updatedSampleValues[i] = sampleIdToValue.get(sampleId);
            }
        }
        return updatedSampleValues;
    }

    //TODO duplicate
    private void expandRemainingGeneticEntityTabDelimitedRowsWithBlankValue() {
        if (updateMode) {
            // Expand remaining genetic entity id rows that were not mentioned in the file
            new HashSet<>(geneticAlterationMap.keySet()).forEach(geneticEntityId -> {
                try {
                    String[] values = new String[orderedImportedSampleList.size()];
                    Arrays.fill(values, "");
                    saveValues(geneticEntityId, values);
                } catch (DaoException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /**
     * Try to find gene by entrez ID, or else by hugo ID
     * @return null when no gene could be found
     */
    private CanonicalGene getGene(
        long entrez,
        String[] parts,
        CnaUtil util
    ) {

        String hugoSymbol = util.getHugoSymbol(parts);

        if (Strings.isNullOrEmpty(hugoSymbol) && entrez == 0) {
            return null;
        }

        // 1. try entrez:
        if (entrez != 0) {
            CanonicalGene foundByEntrez = this.daoGene.getGene(entrez);
            if (foundByEntrez != null) {
                return foundByEntrez;
            }
        }

        // 2. try hugo:
        if (!Strings.isNullOrEmpty(hugoSymbol)) {
            if (hugoSymbol.contains("///") || hugoSymbol.contains("---")) {
                //  Ignore gene IDs separated by ///.  This indicates that
                //  the line contains information regarding multiple genes, and
                //  we cannot currently handle this.
                //  Also, ignore gene IDs that are specified as ---.  This indicates
                //  the line contains information regarding an unknown gene, and
                //  we cannot currently handle this.
                ProgressMonitor.logWarning("Ignoring gene ID:  " + hugoSymbol);
                return null;
            }
            int ix = hugoSymbol.indexOf("|");
            if (ix > 0) {
                hugoSymbol = hugoSymbol.substring(0, ix);
            }
            List<CanonicalGene> genes = daoGene.getGene(hugoSymbol, true);
            if (genes.size() > 1) {
                throw new IllegalStateException("Found multiple genes for Hugo symbol " + hugoSymbol + " while importing cna");
            }
            // Ignore gene if it cannot be found in the database
            if (genes.size() < 1) {
                ProgressMonitor.logWarning("Ignoring Hugo symbol " + hugoSymbol + "not found in the database");
                return null;
            }
            return genes.get(0);
        }
        ProgressMonitor.logWarning("Entrez_Id " + entrez + " not found. Record will be skipped for this gene.");
        return null;
    }

    /**
     * Find sample and create sample profile when needed
     *
     * @return boolean created or not
     */
    public boolean createSampleProfile(
        Sample sample
    ) throws Exception {
        boolean inDatabase = DaoSampleProfile.sampleExistsInGeneticProfile(sample.getInternalId(), geneticProfileId);
        Integer genePanelID = (genePanel == null) ? null : GeneticProfileUtil.getGenePanelId(genePanel);
        SampleIdGeneticProfileId toCreate = new SampleIdGeneticProfileId(sample.getInternalId(), geneticProfileId);
        boolean isQueued = this.sampleIdGeneticProfileIds.contains(toCreate);
        if (!inDatabase && !isQueued) {
            DaoSampleProfile.addSampleProfile(sample.getInternalId(), geneticProfileId, genePanelID);
            this.sampleIdGeneticProfileIds.add(toCreate);
            return true;
        }
        return false;
    }


    private static class SampleIdGeneticProfileId {
        public int sampleId;
        public int geneticProfileId;

        public SampleIdGeneticProfileId(int sampleId, int geneticProfileId) {
            this.sampleId = sampleId;
            this.geneticProfileId = geneticProfileId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SampleIdGeneticProfileId that = (SampleIdGeneticProfileId) o;
            return sampleId == that.sampleId
                && geneticProfileId == that.geneticProfileId;
        }
    }

    /**
     * Find sample and create sample profile when needed
     */
    public Sample findSample(
        String sampleId,
        int cancerStudyId
    ) throws Exception {
        Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(
            cancerStudyId,
            StableIdUtil.getSampleId(sampleId)
        );
        // can be null in case of 'normal' sample, throw exception if not 'normal' and sample not found in db
        if (sample == null) {
            if (StableIdUtil.isNormal(sampleId)) {
                samplesSkipped++;
                return null;
            } else {
                throw new RuntimeException("Unknown sample id '" + StableIdUtil.getSampleId(sampleId));
            }
        }
        return sample;
    }

    private class CnaImportData {
        // Entrez ID x Sample ID table:
        public Table<Long, Integer, CnaEventImportData> eventsTable = HashBasedTable.create();
        public Set<CanonicalGene> genes = new HashSet<>();
    }

    private class CnaEventImportData {
        public int line;
        public CnaEvent cnaEvent;
        public String geneSymbol;
    }

    public int getSamplesSkipped() {
        return samplesSkipped;
    }
}

