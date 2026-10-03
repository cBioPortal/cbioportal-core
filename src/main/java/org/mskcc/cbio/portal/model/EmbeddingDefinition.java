package org.mskcc.cbio.portal.model;

import java.util.Objects;

public class EmbeddingDefinition {
    private Integer internalId;
    private String embeddingId;
    private String shortName;
    private String name;
    private String description;
    private String entityType;
    private String reductionTechnique;


    public EmbeddingDefinition(String embeddingId, String shortName, String name,
                               String description, String entityType, String reductionTechnique) {
        this.embeddingId = embeddingId;
        this.shortName = shortName;
        this.name = name;
        this.description = description;
        this.entityType = entityType;
        this.reductionTechnique = reductionTechnique;
    }

    public void setEmbeddingId(String embeddingId) {
        this.embeddingId = embeddingId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public void setReductionTechnique(String reductionTechnique) {
        this.reductionTechnique = reductionTechnique;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getName() {
        return name;
    }

    public String getShortName() {
        return shortName;
    }

    public String getEmbeddingId() {
        return embeddingId;
    }

    public String getReductionTechnique() {
        return reductionTechnique;
    }

    public Integer getInternalId() {
        return internalId;
    }

    public void setInternalId(Integer internalId) {
        this.internalId = internalId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddingId, reductionTechnique, entityType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EmbeddingDefinition other = (EmbeddingDefinition) obj;
        return Objects.equals(embeddingId, other.embeddingId) &&
                Objects.equals(reductionTechnique, other.reductionTechnique) &&
                Objects.equals(entityType, other.entityType);
    }
}
