package org.mskcc.cbio.portal.model;

/**
 * Encapsulates Resource Base Data.
 */
public class ResourceBaseData {
    private int cancerStudyId;
    private String stableId;
    private String resourceId;
    private String url;
    private String metadata;
    private String groupPath;

    /**
     * Constructor
     */
    public ResourceBaseData() {
        this(-1, "", "", "", null, null);
    }

    /**
     * Constructor
     *
     * @param cancerStudyId database id of cancer study
     * @param stableId stable id of the patient or sample or study
     * @param resourceId resource id
     * @param url        url of the resource
     */
    public ResourceBaseData(int cancerStudyId, String stableId, String resourceId, String url) {
        this(cancerStudyId, stableId, resourceId, url, null, null);
    }

    public ResourceBaseData(int cancerStudyId, String stableId, String resourceId, String url, String metadata, String groupPath) {
        this.setCancerStudyId(cancerStudyId);
        this.setStableId(stableId);
        this.setResourceId(resourceId);
        this.setUrl(url);
        this.setMetadata(metadata);
        this.setGroupPath(groupPath);
    }

    public ResourceBaseData(ResourceBaseData other) {
        this(other.getCancerStudyId(), other.getStableId(), other.getResourceId(), other.getUrl(), other.getMetadata(), other.getGroupPath());
    }

    public int getCancerStudyId() {
        return cancerStudyId;
    }

    public void setCancerStudyId(int cancerStudyId) {
        this.cancerStudyId = cancerStudyId;
    }

    public String getStableId() {
        return stableId;
    }

    public void setStableId(String stableId) {
        this.stableId = stableId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public String getGroupPath() {
        return groupPath;
    }

    public void setGroupPath(String groupPath) {
        this.groupPath = groupPath;
    }
}
