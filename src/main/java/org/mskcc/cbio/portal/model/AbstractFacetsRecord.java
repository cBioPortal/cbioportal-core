/*
 * Copyright (c) 2026 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.model;

/**
 * Common fields shared by FACETS allele-specific copy number records, whether
 * at the raw segment level ({@link FacetsCncfSegment}) or the derived
 * gene level ({@link FacetsGeneLevelRecord}).
 */
public abstract class AbstractFacetsRecord {

    private long id;
    private int cancerStudyId;
    private int sampleId;
    private String chr; // 1-22,X/Y,M
    private long start;
    private long end;
    private Double tcn;
    private Double lcn;
    private Double cellularFraction;
    private Double purity;

    protected AbstractFacetsRecord() {}

    protected AbstractFacetsRecord(int cancerStudyId, int sampleId, String chr, long start, long end,
            Double tcn, Double lcn, Double cellularFraction, Double purity) {
        this.cancerStudyId = cancerStudyId;
        this.sampleId = sampleId;
        this.chr = chr;
        this.start = start;
        this.end = end;
        this.tcn = tcn;
        this.lcn = lcn;
        this.cellularFraction = cellularFraction;
        this.purity = purity;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getCancerStudyId() {
        return cancerStudyId;
    }

    public void setCancerStudyId(int cancerStudyId) {
        this.cancerStudyId = cancerStudyId;
    }

    public int getSampleId() {
        return sampleId;
    }

    public void setSampleId(int sampleId) {
        this.sampleId = sampleId;
    }

    public String getChr() {
        return chr;
    }

    public void setChr(String chr) {
        this.chr = chr;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public Double getTcn() {
        return tcn;
    }

    public void setTcn(Double tcn) {
        this.tcn = tcn;
    }

    public Double getLcn() {
        return lcn;
    }

    public void setLcn(Double lcn) {
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
}
