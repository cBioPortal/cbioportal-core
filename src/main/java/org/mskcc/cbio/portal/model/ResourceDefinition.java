package org.mskcc.cbio.portal.model;

import org.mskcc.cbio.portal.model.shared.ResourceType;

public class ResourceDefinition {

    private String resourceId;
    private String displayName;
    private String description;
    private ResourceType resourceType;
    private boolean openByDefault;
    private Integer priority;
    private Integer cancerStudyId;
    private String customMetadata;
    private String urlTemplate;

    public ResourceDefinition(String resourceId, String displayName,
            String description, ResourceType resourceType, boolean openByDefault, Integer priority, Integer cancerStudyId, String customMetadata) {
        this(resourceId, displayName, description, resourceType, openByDefault, priority, cancerStudyId, customMetadata, null);
    }

    public ResourceDefinition(String resourceId, String displayName,
            String description, ResourceType resourceType, boolean openByDefault, Integer priority, Integer cancerStudyId, String customMetadata,
            String urlTemplate) {
        this.resourceId = resourceId;
        this.displayName = displayName;
        this.description = description;
        this.resourceType = resourceType;
        this.openByDefault = openByDefault;
        this.priority = priority;
        this.cancerStudyId = cancerStudyId;
        this.customMetadata = customMetadata;
        this.urlTemplate = urlTemplate;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public void setResourceType(ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    public boolean isOpenByDefault() {
        return openByDefault;
    }

    public void setOpenByDefault(boolean openByDefault) {
        this.openByDefault = openByDefault;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }
    public Integer getCancerStudyId() {
        return cancerStudyId;
    }
    public void setCancerStudyId(Integer cancerStudyId) {
        this.cancerStudyId = cancerStudyId;
    }

    public String getCustomMetadata() {
        return customMetadata;
    }

    public void setCustomMetadata(String customMetadata) {
        this.customMetadata = customMetadata;
    }

    /**
     * The URL the portal builds for this resource's rows that carry no URL of their own, or null.
     * See {@link org.mskcc.cbio.portal.util.ResourceUrlTemplate} for the placeholder syntax.
     */
    public String getUrlTemplate() {
        return urlTemplate;
    }

    public void setUrlTemplate(String urlTemplate) {
        this.urlTemplate = urlTemplate;
    }
}
