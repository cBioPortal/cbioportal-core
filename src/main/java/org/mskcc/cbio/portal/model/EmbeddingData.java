package org.mskcc.cbio.portal.model;

public class EmbeddingData {
    private int embeddingId;
    private String patientId;
    private String sampleId;
    private double x;
    private double y;
    private int cancerStudyId; // storing the id for the cancer study identifierand
    private String customAttribute;
    public EmbeddingData(int embeddingId, String sampleId, String patientId,
                         float x, float y, String customAttribute, Integer cancerStudyId) {
        this.embeddingId = embeddingId;
        this.sampleId = sampleId;
        this.patientId = patientId;
        this.x = x;
        this.y = y;
        this.customAttribute =  customAttribute;
        this.cancerStudyId = cancerStudyId;
    }

    public int getEmbeddingId() {
        return embeddingId;
    }

    public void setEmbeddingId(int embeddingId) {
        this.embeddingId = embeddingId;
    }

    public String getPatientId() {
        return patientId;
    }

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public String getSampleId() {
        return sampleId;
    }

    public void setSampleId(String sampleId) {
        this.sampleId = sampleId;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public Integer getCancerStudyId() {
        return cancerStudyId;
    }

    public void setCancerStudyId(Integer cancerStudyId) {
        this.cancerStudyId = cancerStudyId;
    }

    public String getCustomAttribute() {
        return customAttribute;
    }

    public void setCustomAttribute(String customAttribute) {
        this.customAttribute = customAttribute;
    }

    //TODO need to add the equals/hascode method
}
