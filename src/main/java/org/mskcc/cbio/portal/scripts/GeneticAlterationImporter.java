package org.mskcc.cbio.portal.scripts;

import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

public class GeneticAlterationImporter {

    private final int geneticProfileId;
    private Set<Long> importSetOfGenes = new HashSet<>();
    private Set<Integer> importSetOfGeneticEntityIds = new HashSet<>();
    private DaoGeneticAlteration daoGeneticAlteration;

    public GeneticAlterationImporter(
        int geneticProfileId, 
        DaoGeneticAlteration daoGeneticAlteration
    ) {
        this.geneticProfileId = geneticProfileId;
        this.daoGeneticAlteration = daoGeneticAlteration;
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
        try {
            if (importSetOfGenes.add(gene.getEntrezGeneId())) {
                daoGeneticAlteration.addGeneticAlterations(geneticProfileId, gene.getEntrezGeneId(), values);
                return true;
            } else {
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
        } catch (Exception e) {
            throw new RuntimeException("Aborted: Error found for row starting with " + geneSymbol + ": " + e.getMessage());
        }
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
        if (importSetOfGeneticEntityIds.add(geneticEntityId)) {
            daoGeneticAlteration.addGeneticAlterationsForGeneticEntity(geneticProfileId, geneticEntityId, values);
            return true;
        }
        else {
            ProgressMonitor.logWarning("Data for genetic entity with id " + geneticEntityId + " already imported from file. Record will be skipped.");
            return false;
        }
    }

    public boolean isImportedAlready(CanonicalGene gene) {
        return importSetOfGenes.contains(gene.getEntrezGeneId());
    }


}
