package org.mskcc.cbio.portal.scripts;

import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfileSamples;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.util.ArrayUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class GeneticAlterationIncrementalImporter extends GeneticAlterationImporter {

    private final List<Integer> fileOrderedSampleList;
    private final DaoGeneticAlteration daoGeneticAlteration = DaoGeneticAlteration.getInstance();
    private final HashMap<Integer, HashMap<Integer, String>> geneticAlterationMap;

    public GeneticAlterationIncrementalImporter(
            int geneticProfileId,
            List<Integer> fileOrderedSampleList
    ) throws DaoException {

        this.geneticProfileId = geneticProfileId;
        this.geneticAlterationMap = daoGeneticAlteration.getGeneticAlterationMapForEntityIds(geneticProfileId, null);
        this.fileOrderedSampleList = fileOrderedSampleList;

        ArrayList <Integer> savedOrderedSampleList = DaoGeneticProfileSamples.getOrderedSampleList(this.geneticProfileId);
        int initialOrderSampleListSize = savedOrderedSampleList.size();
        geneticAlterationMap.forEach((geneticEntityId, sampleToValue) -> {
            if (sampleToValue.size() != initialOrderSampleListSize) {
                throw new IllegalStateException("Number of samples ("
                        + sampleToValue.size() + ") for genetic entity with id "
                        + geneticEntityId + " does not match with the number in the inital sample list ("
                        + initialOrderSampleListSize + ").");
            }
        });
        // add all new sample ids at the end
        this.orderedSampleList = new ArrayList<>(savedOrderedSampleList);
        List<Integer> newSampleIds = this.fileOrderedSampleList.stream().filter(sampleId -> !savedOrderedSampleList.contains(sampleId)).toList();
        this.orderedSampleList.addAll(newSampleIds);
        DaoGeneticProfileSamples.deleteAllSamplesInGeneticProfile(this.geneticProfileId);
        this.storeOrderedSampleList();
        daoGeneticAlteration.deleteAllRecordsInGeneticProfile(this.geneticProfileId);
    }

    @Override
    public boolean store(String[] values, CanonicalGene gene, String geneSymbol) throws DaoException {
        int geneticEntityId = gene.getGeneticEntityId();
        String[] expandedValues = extendValues(geneticEntityId, values);
        return super.store(expandedValues, gene, geneSymbol);
    }

    @Override
    public boolean store(int geneticEntityId, String[] values) throws DaoException {
        String[] expandedValues = extendValues(geneticEntityId, values);
        return super.store(geneticEntityId, expandedValues);
    }

    @Override
    public void finalise() {
        expandRemainingGeneticEntityTabDelimitedRowsWithBlankValue();
        super.finalise();
    }

    private String[] extendValues(int geneticEntityId, String[] values) {
        Map<Integer, String> sampleIdToValue = mapWithFileOrderedSampleList(values);
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

    private Map<Integer, String> mapWithFileOrderedSampleList(String[] values) {
        return ArrayUtil.zip(fileOrderedSampleList.toArray(Integer[]::new), values);
    }

    private void expandRemainingGeneticEntityTabDelimitedRowsWithBlankValue() {
        // Expand remaining genetic entity id rows that were not mentioned in the file
        new HashSet<>(geneticAlterationMap.keySet()).forEach(geneticEntityId -> {
            try {
                String[] values = new String[fileOrderedSampleList.size()];
                Arrays.fill(values, "");
                this.store(geneticEntityId, values);
            } catch (DaoException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
