/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
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

package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.model.*;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

/**
 *
 * @author jgao
 */
public final class DaoCopyNumberSegment {

    private static final double FRACTION_GENOME_ALTERED_CUTOFF = 0.2;
    private static final String FRACTION_GENOME_ALTERED_ATTR_ID = "FRACTION_GENOME_ALTERED";

    private DaoCopyNumberSegment() {}
    
    public static int addCopyNumberSegment(CopyNumberSegment seg) throws DaoException {
        if (!ClickHouseBulkLoader.isBulkLoad()) {
            throw new DaoException("You have to turn on ClickHouseBulkLoader in order to insert mutations");
        } else {
            ClickHouseBulkLoader.getClickHouseBulkLoader("copy_number_seg").insertRecord(
                    Long.toString(seg.getSegId()),
                    Integer.toString(seg.getCancerStudyId()),
                    Integer.toString(seg.getSampleId()),
                    seg.getChr(),
                    Long.toString(seg.getStart()),
                    Long.toString(seg.getEnd()),
                    Integer.toString(seg.getNumProbes()),
                    Double.toString(seg.getSegMean())
            );
            return 1;
        }
    }

    /**
     * Ensures FRACTION_GENOME_ALTERED clinical sample attribute is created and up to date.
     * @param cancerStudyId - id of the study to create the clinical attribute for
     * @param sampleIds - specifies for which samples to calculate this attribute.
     *                  if sampleIds=null, the calculation is done for all samples in the study
     * @param updateMode -  if true, updates the attribute if it exists
     * @throws DaoException
     */

    public static void createFractionGenomeAlteredClinicalData(int cancerStudyId, Set<Integer> sampleIds, boolean updateMode) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegment.class);
            pstmt = con.prepareStatement(
                    "SELECT c1.`sample_id`, " +
                            "CASE WHEN SUM(c1.`end` - c1.`start`) > 0 THEN " +
                            "ROUND(SUM(CASE WHEN ABS(c1.`segment_mean`) >= ? THEN (c1.`end` - c1.`start`) ELSE 0 END) * 1.0 / " +
                            "SUM(c1.`end` - c1.`start`), 4) " +
                            "ELSE 0 END AS `value` " +
                            "FROM `copy_number_seg` AS c1 " +
                            "INNER JOIN `cancer_study` ON c1.`cancer_study_id` = cancer_study.`cancer_study_id` " +
                            "WHERE cancer_study.`cancer_study_id`=? " +
                            (sampleIds == null ? "" : ("AND c1.`sample_id` IN (" + String.join(",", Collections.nCopies(sampleIds.size(), "?")) + ") ")) +
                            "GROUP BY cancer_study.`cancer_study_id`, c1.`sample_id` " +
                            "HAVING SUM(c1.`end` - c1.`start`) > 0");
            int parameterIndex = 1;
            pstmt.setDouble(parameterIndex++, FRACTION_GENOME_ALTERED_CUTOFF);
            pstmt.setInt(parameterIndex++, cancerStudyId);
            if (sampleIds != null) {
                for (Integer sampleId : sampleIds) {
                    pstmt.setInt(parameterIndex++, sampleId);
                }
            }
            Map<Integer, String> fractionGenomeAltereds = new HashMap<Integer, String>();
            rs = pstmt.executeQuery();
            while (rs.next()) {
                fractionGenomeAltereds.put(rs.getInt(1), rs.getString(2));
            }

            ClinicalAttribute clinicalAttribute = DaoClinicalAttributeMeta.getDatum(FRACTION_GENOME_ALTERED_ATTR_ID, cancerStudyId);
            if (clinicalAttribute == null) {
                ClinicalAttribute attr = new ClinicalAttribute(FRACTION_GENOME_ALTERED_ATTR_ID, "Fraction Genome Altered", "Fraction Genome Altered", "NUMBER",
                    false, "20", cancerStudyId);
                DaoClinicalAttributeMeta.addDatum(attr);
            }

            if (updateMode) {
                DaoClinicalData.removeSampleAttributesData(fractionGenomeAltereds.keySet(), FRACTION_GENOME_ALTERED_ATTR_ID);
            }
            for (Map.Entry<Integer, String> fractionGenomeAltered : fractionGenomeAltereds.entrySet()) {
                DaoClinicalData.addSampleDatum(fractionGenomeAltered.getKey(), FRACTION_GENOME_ALTERED_ATTR_ID, fractionGenomeAltered.getValue());
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegment.class, con, pstmt, rs);
        }
    }
    
    public static long getLargestId() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoMutation.class);
            pstmt = con.prepareStatement
                    ("SELECT max(`seg_id`) FROM `copy_number_seg`");
            rs = pstmt.executeQuery();
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoMutation.class, con, pstmt, rs);
        }
    }
    
    public static List<CopyNumberSegment> getSegmentForASample(
            int sampleId, int cancerStudyId) throws DaoException {
        return getSegmentForSamples(Collections.singleton(sampleId),cancerStudyId);
    }
    
    public static List<CopyNumberSegment> getSegmentForSamples(
            Collection<Integer> sampleIds, int cancerStudyId) throws DaoException {
        if (sampleIds.isEmpty()) {
            return Collections.emptyList();
        }
        String concatSampleIds = "('"+StringUtils.join(sampleIds, "','")+"')";
        
        List<CopyNumberSegment> segs = new ArrayList<CopyNumberSegment>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegment.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM copy_number_seg"
                    + " WHERE `sample_id` IN "+ concatSampleIds
                    + " AND `cancer_study_id`="+cancerStudyId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                CopyNumberSegment seg = new CopyNumberSegment(
                        rs.getInt("cancer_study_id"),
                        rs.getInt("sample_id"),
                        rs.getString("chr"),
                        rs.getLong("start"),
                        rs.getLong("end"),
                        rs.getInt("num_probes"),
                        rs.getDouble("segment_mean"));
                seg.setSegId(rs.getLong("seg_id"));
                segs.add(seg);
            }
            return segs;
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegment.class, con, pstmt, rs);
        }
    }
    
    public static double getCopyNumberActeredFraction(int sampleId,
            int cancerStudyId, double cutoff) throws DaoException {
        Double d = getCopyNumberActeredFraction(Collections.singleton(sampleId), cancerStudyId, cutoff)
                .get(sampleId);
        return d==null ? Double.NaN : d;
    }
    
    public static Map<Integer,Double> getCopyNumberActeredFraction(Collection<Integer> sampleIds,
            int cancerStudyId, double cutoff) throws DaoException {
        Map<Integer,Long> alteredLength = getCopyNumberAlteredLength(sampleIds, cancerStudyId, cutoff);
        Map<Integer,Long> measuredLength = getCopyNumberAlteredLength(sampleIds, cancerStudyId, 0);
        Map<Integer,Double> fraction = new HashMap<Integer,Double>(alteredLength.size());
        for (Integer sampleId : sampleIds) {
            Long ml = measuredLength.get(sampleId);
            if (ml==null || ml==0) {
                continue;
            }
            Long al = alteredLength.get(sampleId);
            if (al==null) {
                al = (long) 0;
            }
            fraction.put(sampleId, 1.0*al/ml);
        }
        return fraction;
    }
    
    private static Map<Integer,Long> getCopyNumberAlteredLength(Collection<Integer> sampleIds,
            int cancerStudyId, double cutoff) throws DaoException {
        Map<Integer,Long> map = new HashMap<Integer,Long>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegment.class);
            if (cutoff>0) {
                sql = "SELECT  `sample_id`, sum(`end`-`start`)"
                    + " FROM `copy_number_seg`"
                    + " WHERE `cancer_study_id`="+cancerStudyId
                    + " AND abs(`segment_mean`)>=" + cutoff
                    + " AND `sample_id` IN ('" + StringUtils.join(sampleIds,"','") +"')"
                    + " GROUP BY `sample_id`";
            } else {
                sql = "SELECT  `sample_id`, sum(`end`-`start`)"
                    + " FROM `copy_number_seg`"
                    + " WHERE `cancer_study_id`="+cancerStudyId
                    + " AND `sample_id` IN ('" + StringUtils.join(sampleIds,"','") +"')"
                    + " GROUP BY `sample_id`";
            }
            
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                map.put(rs.getInt(1), rs.getLong(2));
            }
            
            return map;
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegment.class, con, pstmt, rs);
        }
    }
    
    /**
     * 
     * @param cancerStudyId
     * @return true if segment data exist for the cancer study
     * @throws DaoException 
     */
    public static boolean segmentDataExistForCancerStudy(int cancerStudyId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegment.class);
            pstmt = con.prepareStatement("SELECT EXISTS (SELECT 1 FROM `copy_number_seg` WHERE `cancer_study_id`=?)");
            pstmt.setInt(1, cancerStudyId);
            rs = pstmt.executeQuery();
            return rs.next() && rs.getInt(1)==1;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegment.class, con, pstmt, rs);
        }
    }
    
    /**
     * 
     * @param cancerStudyId
     * @param sampleId
     * @return true if segment data exist for the case
     * @throws DaoException 
     */
    public static boolean segmentDataExistForSample(int cancerStudyId, int sampleId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegment.class);
            pstmt = con.prepareStatement("SELECT EXISTS(SELECT 1 FROM `copy_number_seg`"
                + " WHERE `cancer_study_id`=? AND `sample_id`=?");
            pstmt.setInt(1, cancerStudyId);
            pstmt.setInt(2, sampleId);
            rs = pstmt.executeQuery();
            return rs.next() && rs.getInt(1)==1;
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegment.class, con, pstmt, rs);
        }
    }

    public static void  deleteSegmentDataForSamples(int cancerStudyId, Set<Integer> sampleIds) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegment.class);
            pstmt = con.prepareStatement("DELETE FROM `copy_number_seg`" +
                    " WHERE `cancer_study_id`= ?" +
                    " AND `sample_id` IN (" + String.join(",", Collections.nCopies(sampleIds.size(), "?"))
                    + ")");
            int parameterIndex = 1;
            pstmt.setInt(parameterIndex++, cancerStudyId);
            for (Integer sampleId : sampleIds) {
                pstmt.setInt(parameterIndex++, sampleId);
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegment.class, con, pstmt, rs);
        }
    }
}
