package org.mskcc.cbio.portal.scripts;

import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.model.CanonicalGene;

public interface GeneticAlterationImporter {

    boolean store(
            String[] values,
            CanonicalGene gene,
            String geneSymbol
    ) throws DaoException;

    boolean store(
            int geneticEntityId,
            String[] values
    ) throws DaoException;

    void finalise();
}
