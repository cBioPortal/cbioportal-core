package org.mskcc.cbio.portal.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Gene panel representation for portal info dumps.
 */
public class PortalGenePanel implements Serializable {

    @JsonIgnore
    private Integer internalId;
    @JsonProperty("genePanelId")
    private String stableId;
    private String description;
    private List<PortalGenePanelGene> genes;

    public Integer getInternalId() {
        return internalId;
    }

    public void setInternalId(Integer internalId) {
        this.internalId = internalId;
    }

    public String getGenePanelId() {
        return stableId;
    }

    public void setGenePanelId(String stableId) {
        this.stableId = stableId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<PortalGenePanelGene> getGenes() {
        return genes;
    }

    public void setGenes(List<PortalGenePanelGene> genes) {
        this.genes = genes;
    }
}
