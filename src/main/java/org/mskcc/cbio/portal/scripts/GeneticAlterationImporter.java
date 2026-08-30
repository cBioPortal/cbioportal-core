package org.mskcc.cbio.portal.scripts;

import java.util.*;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticEntity;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoGeneticProfileSamples;
import org.mskcc.cbio.portal.dao.DaoPatient;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.model.shared.GeneticEntity;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import static java.lang.String.format;

public class GeneticAlterationImporter {

    protected int geneticProfileId;
    protected List<Integer> orderedSampleList;
    private final Set<Long> importSetOfGenes = new HashSet<>();
    private final Set<Integer> importSetOfGeneticEntityIds = new HashSet<>();

    private final DaoGeneticAlteration daoGeneticAlteration = DaoGeneticAlteration.getInstance();

    // --- no-explode mode: write exploded rows straight into genetic_alteration_derived at import
    // time instead of storing packed `values` and rebuilding the derived table with the ARRAY JOIN
    // derive step. Toggled by ClickHouseBulkLoader.isNoExplode(). Pair with metaImport.py
    // --no-derive-tables so the SQL derive does not clobber the directly-written rows.
    private static final String DERIVED_TABLE = "genetic_alteration_derived";
    private static final String[] DERIVED_COLUMNS =
        {"sample_unique_id", "cancer_study_identifier", "hugo_gene_symbol", "profile_type", "alteration_value"};
    private static final int DERIVED_FLUSH_THRESHOLD = 500_000;
    private final boolean noExplode = ClickHouseBulkLoader.isNoExplode();
    private String cancerStudyIdentifier;
    private String profileType;
    private String[] sampleUniqueIds;
    private ClickHouseBulkLoader derivedLoader;
    private int pendingDerivedRows = 0;

    // --- no-explode for generic assay (entity-based) -> generic_assay_data_derived (wider schema) ---
    private static final String GA_DERIVED_TABLE = "generic_assay_data_derived";
    private static final String[] GA_DERIVED_COLUMNS = {"sample_unique_id", "patient_unique_id",
        "genetic_entity_id", "value", "generic_assay_type", "profile_stable_id", "entity_stable_id",
        "datatype", "patient_level", "profile_type"};
    private Boolean genericAssayProfile;     // null = not yet resolved
    private String profileStableId;
    private String genericAssayType;
    private String datatype;
    private String patientLevel;
    private String[] patientUniqueIds;
    private ClickHouseBulkLoader gaDerivedLoader;
    private int pendingGaRows = 0;

    protected GeneticAlterationImporter() {}
    public GeneticAlterationImporter(
        int geneticProfileId,
        List<Integer> orderedSampleList
    ) {
        this.geneticProfileId = geneticProfileId;
        this.orderedSampleList = orderedSampleList;
    }

    protected void storeOrderedSampleList() throws DaoException {
        int rowCount = DaoGeneticProfileSamples.addGeneticProfileSamples(geneticProfileId, orderedSampleList);
        if (rowCount < 1) {
            throw new IllegalStateException("Failed to store the ordered sample list.");
        }
    }

    /**
     * Check that we have not already imported information regarding this gene.
     * This is an important check, because a GISTIC or RAE file may contain
     * multiple rows for the same gene, and we only want to import the first row.
     */
    public boolean store(
            String[] values,
            CanonicalGene gene,
            String geneSymbol
    ) throws DaoException {
        ensureNumberOfValuesIsCorrect(values.length);
        if (importSetOfGenes.add(gene.getEntrezGeneId())) {
            if (noExplode) {
                writeExplodedRows(gene.getHugoGeneSymbolAllCaps(), values);
            } else {
                daoGeneticAlteration.addGeneticAlterations(geneticProfileId, gene.getEntrezGeneId(), values);
            }
            return true;
        }
        String geneSymbolMessage = "";
        if (geneSymbol != null && !geneSymbol.equalsIgnoreCase(gene.getHugoGeneSymbolAllCaps())) {
            geneSymbolMessage = " (given as alias in your file as: " + geneSymbol + ")";
        }
        ProgressMonitor.logWarning(format(
            "Gene %s (%d)%s found to be duplicated in your file. Duplicated row will be ignored!",
            gene.getHugoGeneSymbolAllCaps(),
            gene.getEntrezGeneId(),
            geneSymbolMessage)
        );
        return false;
    }


    /**
     * Universal method that stores values for different genetic entities
     * @param geneticEntityId
     * @param values
     * @return true if entity has been stored, false - if entity already existed
     * @throws DaoException
     */
    public boolean store(
            int geneticEntityId,
            String[] values
    ) throws DaoException {
        ensureNumberOfValuesIsCorrect(values.length);
        if (importSetOfGeneticEntityIds.add(geneticEntityId)) {
            // no-explode handles generic assay (-> generic_assay_data_derived). Other entity-based
            // profiles (e.g. GSVA geneset scores, which have no derived table and are read from the
            // packed table directly) always keep the packed path.
            if (noExplode && isGenericAssayProfile()) {
                writeGenericAssayExplodedRows(geneticEntityId, values);
            } else {
                daoGeneticAlteration.addGeneticAlterationsForGeneticEntity(geneticProfileId, geneticEntityId, values);
            }
            return true;
        }
        ProgressMonitor.logWarning("Data for genetic entity with id " + geneticEntityId + " already imported from file. Record will be skipped.");
        return false;
    }

    /**
     * Resolve the constants needed to denormalize a row to the derived table (study, profile_type,
     * and the per-column sample_unique_id) once per profile. These are exactly the values the SQL
     * derive recovers by joining gene / sample_derived / genetic_profile — known here at import time.
     */
    private void initNoExplodeContext() throws DaoException {
        GeneticProfile geneticProfile = DaoGeneticProfile.getGeneticProfileById(geneticProfileId);
        CancerStudy study = DaoCancerStudy.getCancerStudyByInternalId(geneticProfile.getCancerStudyId());
        this.cancerStudyIdentifier = study.getCancerStudyStableId();
        String prefix = cancerStudyIdentifier + "_";
        String stableId = geneticProfile.getStableId();
        this.profileType = stableId.startsWith(prefix) ? stableId.substring(prefix.length()) : stableId;
        this.sampleUniqueIds = new String[orderedSampleList.size()];
        for (int i = 0; i < orderedSampleList.size(); i++) {
            Sample sample = DaoSample.getSampleById(orderedSampleList.get(i));
            this.sampleUniqueIds[i] = cancerStudyIdentifier + "_" + sample.getStableId();
        }
        this.derivedLoader = ClickHouseBulkLoader.getClickHouseBulkLoader(DERIVED_TABLE);
        this.derivedLoader.setFieldNames(DERIVED_COLUMNS);
    }

    /**
     * Emit one exploded (sample_unique_id, study, gene, profile_type, value) row per cell straight
     * into genetic_alteration_derived. Mirrors the SQL derive exactly: a cell is dropped when its
     * value is "NA" or empty (the derive maps '' -> NULL and then filters with "!= 'NA'", which
     * also excludes the NULLs). Flushes periodically so memory stays bounded for large matrices.
     */
    private void writeExplodedRows(String hugoGeneSymbol, String[] values) throws DaoException {
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            if (value.isEmpty() || "NA".equals(value)) {
                continue;
            }
            derivedLoader.insertRecord(
                sampleUniqueIds[i], cancerStudyIdentifier, hugoGeneSymbol, profileType, value);
            pendingDerivedRows++;
        }
        if (pendingDerivedRows >= DERIVED_FLUSH_THRESHOLD) {
            derivedLoader.flush();
            pendingDerivedRows = 0;
        }
    }

    /** True iff this profile is a generic assay profile; resolves and caches the GA context once. */
    private boolean isGenericAssayProfile() throws DaoException {
        if (genericAssayProfile == null) {
            GeneticProfile geneticProfile = DaoGeneticProfile.getGeneticProfileById(geneticProfileId);
            genericAssayProfile = geneticProfile.getGenericAssayType() != null;
            if (genericAssayProfile) {
                initGenericAssayContext(geneticProfile);
            }
        }
        return genericAssayProfile;
    }

    /**
     * Resolve the constants the generic_assay_data_derived row needs (study, profile fields, and the
     * per-column sample_unique_id / patient_unique_id) once per profile — the same values the SQL
     * derive recovers by joining genetic_profile / genetic_entity / sample_derived.
     */
    private void initGenericAssayContext(GeneticProfile geneticProfile) throws DaoException {
        CancerStudy study = DaoCancerStudy.getCancerStudyByInternalId(geneticProfile.getCancerStudyId());
        this.cancerStudyIdentifier = study.getCancerStudyStableId();
        this.profileStableId = geneticProfile.getStableId();
        String prefix = cancerStudyIdentifier + "_";
        this.profileType = profileStableId.startsWith(prefix) ? profileStableId.substring(prefix.length()) : profileStableId;
        this.genericAssayType = geneticProfile.getGenericAssayType();
        this.datatype = geneticProfile.getDatatype();
        this.patientLevel = geneticProfile.getPatientLevel() ? "1" : "0";
        this.sampleUniqueIds = new String[orderedSampleList.size()];
        this.patientUniqueIds = new String[orderedSampleList.size()];
        for (int i = 0; i < orderedSampleList.size(); i++) {
            Sample sample = DaoSample.getSampleById(orderedSampleList.get(i));
            this.sampleUniqueIds[i] = cancerStudyIdentifier + "_" + sample.getStableId();
            Patient patient = DaoPatient.getPatientById(sample.getInternalPatientId());
            this.patientUniqueIds[i] = cancerStudyIdentifier + "_" + patient.getStableId();
        }
        this.gaDerivedLoader = ClickHouseBulkLoader.getClickHouseBulkLoader(GA_DERIVED_TABLE);
        this.gaDerivedLoader.setFieldNames(GA_DERIVED_COLUMNS);
    }

    /**
     * Emit one generic_assay_data_derived row per cell. Mirrors that derive, which (unlike the gene
     * one) does NOT filter "NA": it maps '' -> NULL and keeps every other value, so no cell is dropped.
     */
    private void writeGenericAssayExplodedRows(int geneticEntityId, String[] values) throws DaoException {
        GeneticEntity entity = DaoGeneticEntity.getGeneticEntityById(geneticEntityId);
        String entityStableId = entity.getStableId();
        String entityIdString = Integer.toString(geneticEntityId);
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            gaDerivedLoader.insertRecord(sampleUniqueIds[i], patientUniqueIds[i], entityIdString,
                value.isEmpty() ? "\\N" : value, genericAssayType, profileStableId, entityStableId,
                datatype, patientLevel, profileType);
            pendingGaRows++;
        }
        if (pendingGaRows >= DERIVED_FLUSH_THRESHOLD) {
            gaDerivedLoader.flush();
            pendingGaRows = 0;
        }
    }

    private void ensureNumberOfValuesIsCorrect(int valuesNumber) {
        if (valuesNumber != orderedSampleList.size()) {
            throw new IllegalArgumentException("There has to be " + orderedSampleList.size() + " values, but only " + valuesNumber+ " has passed.");
        }
    }


    public void initialize() {
        try {
            storeOrderedSampleList();
            // Gene-based profiles set up the genetic_alteration_derived context now; generic assay
            // sets up its own (generic_assay_data_derived) context lazily via isGenericAssayProfile().
            if (noExplode && !isGenericAssayProfile()) {
                initNoExplodeContext();
            }
        } catch (DaoException e) {
            throw new RuntimeException(e);
        }
    }

    public void complete() throws DaoException {
        if (noExplode && derivedLoader != null) {
            derivedLoader.flush();
            pendingDerivedRows = 0;
        }
        if (noExplode && gaDerivedLoader != null) {
            gaDerivedLoader.flush();
            pendingGaRows = 0;
        }
    }
}
