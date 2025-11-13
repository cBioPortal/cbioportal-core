package org.mskcc.cbio.portal.scripts;

import org.cbioportal.legacy.model.EntityType;
import org.cbioportal.legacy.model.GeneticEntity;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GenericAssayMetaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(GenericAssayMetaUtils.class);

    public static Map<String, Integer> buildGenericAssayStableIdToEntityIdMap() {
        Map<String, Integer> genericAssayStableIdToEntityIdMap = new HashMap<>();
        try {
            List<GeneticEntity> allEntities = DaoGeneticEntity.getAllGeneticEntities();
            genericAssayStableIdToEntityIdMap = allEntities.stream()
                .filter(entityType -> EntityType.GENERIC_ASSAY.toString().equals(entityType.getEntityType()))
                .collect(Collectors.toMap(
                    GeneticEntity::getStableId,
                    GeneticEntity::getId,
                    (existing, replacement) -> {
                        if (!existing.equals(replacement)) {
                            LOG.warn("Duplicate generic assay stable id {} detected. Keeping entity id {} and ignoring {}.",
                                    existing, existing, replacement);
                        }
                        return existing;
                    },
                    LinkedHashMap::new));
        } catch (DaoException e) {
            e.printStackTrace();
        }
        return genericAssayStableIdToEntityIdMap;
    }
}
