/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.mskcc.cbio.portal.model;

/**
 *
 * @author jgao
 */
public class CopyNumberSegment {
    private long segId;
    private int cancerStudyId;
    private int sampleId;
    private String chr; // 1-22,X/Y,M
    private long start;
    private long end;
    private int numProbes;
    private double segMean;
    // Supplemental ASCN (allele-specific copy number) fields, populated only
    // for segments from an ALLELE_SPECIFIC_COPY_NUMBER study;
    // null for plain COPY_NUMBER_ALTERATION SEG data. See ImportCopyNumberSegmentData.
    private Integer tcn;
    private Integer lcn;
    private Double cellularFraction;
    private Double purity;
    private Double ploidy;
    private Double dipLogR;

    public CopyNumberSegment(int cancerStudyId, int sampleId, String chr,
            long start, long end, int numProbes, double segMean) {
        this.cancerStudyId = cancerStudyId;
        this.sampleId = sampleId;
        this.chr = chr;
        this.start = start;
        this.end = end;
        this.numProbes = numProbes;
        this.segMean = segMean;
    }

    public int getCancerStudyId() {
        return cancerStudyId;
    }

    public void setCancerStudyId(int cancerStudyId) {
        this.cancerStudyId = cancerStudyId;
    }

    public String getChr() {
        return chr;
    }

    public void setChr(String chr) {
        this.chr = chr;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public int getNumProbes() {
        return numProbes;
    }

    public void setNumProbes(int numProbes) {
        this.numProbes = numProbes;
    }

    public int getSampleId() {
        return sampleId;
    }

    public void setSampleId(int sampleId) {
        this.sampleId = sampleId;
    }

    public long getSegId() {
        return segId;
    }

    public void setSegId(long segId) {
        this.segId = segId;
    }

    public double getSegMean() {
        return segMean;
    }

    public void setSegMean(double segMean) {
        this.segMean = segMean;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public Integer getTcn() {
        return tcn;
    }

    public void setTcn(Integer tcn) {
        this.tcn = tcn;
    }

    public Integer getLcn() {
        return lcn;
    }

    public void setLcn(Integer lcn) {
        this.lcn = lcn;
    }

    public Double getCellularFraction() {
        return cellularFraction;
    }

    public void setCellularFraction(Double cellularFraction) {
        this.cellularFraction = cellularFraction;
    }

    public Double getPurity() {
        return purity;
    }

    public void setPurity(Double purity) {
        this.purity = purity;
    }

    public Double getPloidy() {
        return ploidy;
    }

    public void setPloidy(Double ploidy) {
        this.ploidy = ploidy;
    }

    public Double getDipLogR() {
        return dipLogR;
    }

    public void setDipLogR(Double dipLogR) {
        this.dipLogR = dipLogR;
    }
}
