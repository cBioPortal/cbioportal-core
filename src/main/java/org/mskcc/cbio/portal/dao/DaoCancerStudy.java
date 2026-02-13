/*
 * Copyright (c) 2015 - 2017 Memorial Sloan-Kettering Cancer Center.
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

import org.apache.commons.lang3.StringUtils;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.CancerStudyTags;
import org.mskcc.cbio.portal.model.ReferenceGenome;
import org.mskcc.cbio.portal.model.TypeOfCancer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analogous to and replaces the old DaoCancerType. A CancerStudy has a NAME and
 * DESCRIPTION. If PUBLIC is true a CancerStudy can be accessed by anyone,
 * otherwise can only be accessed through access control.
 *
 * @author Ethan Cerami
 * @author Arthur Goldberg goldberg@cbio.mskcc.org
 * @author Ersin Ciftci
 */
public final class DaoCancerStudy {

    private static final int DELETE_BATCH_SIZE = 500;

    public static enum Status {
        UNAVAILABLE,
        AVAILABLE
    }

    private static final Map<String, java.util.Date> cacheDateByStableId = new HashMap<String, java.util.Date>();
    private static final Map<Integer, java.util.Date> cacheDateByInternalId = new HashMap<Integer, java.util.Date>();
    private static final Map<String,CancerStudy> byStableId = new HashMap<String,CancerStudy>();
    private static final Map<Integer,CancerStudy> byInternalId = new HashMap<Integer,CancerStudy>();
    private static final String CANCER_STUDY_SEQUENCE = "seq_cancer_study";

    static {
        reCacheAll();
    }

    private DaoCancerStudy() {}

    public static synchronized void reCacheAll() {
        System.out.println("Recaching... ");
        DaoCancerStudy.reCache();
        DaoGeneticProfile.reCache();
        DaoPatient.reCache();
        DaoSample.reCache();
        DaoClinicalData.reCache();
        DaoInfo.setVersion();
        System.out.println("Finished recaching... ");
    }

    private static synchronized void reCache() {
        cacheDateByStableId.clear();
        cacheDateByInternalId.clear();
        byStableId.clear();
        byInternalId.clear();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            pstmt = con.prepareStatement("SELECT * FROM cancer_study");
            rs = pstmt.executeQuery();
            while (rs.next()) {
                CancerStudy cancerStudy = extractCancerStudy(rs);
                cacheCancerStudy(cancerStudy, new java.util.Date());
            }
        } catch (SQLException | DaoException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    private static void cacheCancerStudy(CancerStudy study, java.util.Date importDate) {
        cacheDateByStableId.put(study.getCancerStudyStableId(), importDate);
        cacheDateByInternalId.put(study.getInternalId(), importDate);
        byStableId.put(study.getCancerStudyStableId(), study);
        byInternalId.put(study.getInternalId(), study);
    }

    /**
     * Removes the cancer study from cache
     * @param internalCancerStudyId Internal cancer study ID
     */
    private static void removeCancerStudyFromCache(int internalCancerStudyId) {
        String stableId = byInternalId.get(internalCancerStudyId).getCancerStudyStableId();
        cacheDateByStableId.remove(stableId);
        cacheDateByInternalId.remove(internalCancerStudyId);
        byStableId.remove(stableId);
        byInternalId.remove(internalCancerStudyId);
    }

    public static void setStatus(Status status, String stableCancerStudyId, Integer ... internalId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            if (internalId.length > 0) {
                pstmt = con.prepareStatement("UPDATE cancer_study set status = ? where cancer_study_id = ?");
                pstmt.setInt(1, status.ordinal());
                pstmt.setInt(2, internalId[0]);
            } else {
                pstmt = con.prepareStatement("UPDATE cancer_study set status = ? where cancer_study_identifier = ?");
                pstmt.setInt(1, status.ordinal());
                pstmt.setString(2, stableCancerStudyId);
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            if (!e.getMessage().toLowerCase().contains("unknown column")) {
                throw new DaoException(e);
            }
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    public static Status getStatus(String stableCancerStudyId, Integer ... internalId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            if (internalId.length > 0) {
                pstmt = con.prepareStatement("SELECT status FROM cancer_study where cancer_study_id = ?");
                pstmt.setInt(1, internalId[0]);
            } else {
                pstmt = con.prepareStatement("SELECT status FROM cancer_study where cancer_study_identifier = ?");
                pstmt.setString(1, stableCancerStudyId);
            }
            rs = pstmt.executeQuery();
            if (rs.next()) {
                Integer status = rs.getInt(1);
                if (rs.wasNull()) {
                    return Status.AVAILABLE;
                }
                if (status>=Status.values().length) {
                    return Status.AVAILABLE;
                }
                return Status.values()[status];
            } else {
                return Status.AVAILABLE;
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    private static Integer getStudyCount() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            pstmt = con.prepareStatement("SELECT count(*) from cancer_study");
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return 0;
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    public static void setImportDate(Integer internalId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            pstmt = con.prepareStatement("UPDATE cancer_study set import_date = now() where cancer_study_id = ?");
            pstmt.setInt(1, internalId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            if (e.getMessage().toLowerCase().contains("unknown column")) {
                return;
            }
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    public static java.util.Date getImportDate(String stableCancerStudyId, Integer ... internalId) throws DaoException, ParseException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            if (internalId.length > 0) {
                pstmt = con.prepareStatement("SELECT import_date FROM cancer_study where cancer_study_id = ?");
                pstmt.setInt(1, internalId[0]);
            } else {
                pstmt = con.prepareStatement("SELECT import_date FROM cancer_study where cancer_study_identifier = ?");
                pstmt.setString(1, stableCancerStudyId);
            }
            rs = pstmt.executeQuery();
            if (rs.next()) {
                java.sql.Timestamp importDate = rs.getTimestamp(1);
                if (rs.wasNull()) {
                    return new SimpleDateFormat("yyyyMMdd").parse("19180511");
                } else {
                    return importDate;
                }
            } else {
                return new SimpleDateFormat("yyyyMMdd").parse("19180511");
            }
        } catch (SQLException e) {
            if (e.getMessage().toLowerCase().contains("unknown column")) {
                return new SimpleDateFormat("yyyyMMdd").parse("19180511");
            }
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    /**
     * Adds a cancer study to the Database.
     * Updates cancerStudy with its auto incremented uid, in studyID.
     *
     * @param cancerStudy   Cancer Study Object.
     * @throws DaoException Database Error.
     */
    public static void addCancerStudy(CancerStudy cancerStudy) throws DaoException {
        addCancerStudy(cancerStudy, false);
    }

    /**
     * Adds a cancer study to the Database.
     * @param cancerStudy
     * @param overwrite if true, overwrite if exist.
     * @throws DaoException
     */
    public static void addCancerStudy(CancerStudy cancerStudy, boolean overwrite) throws DaoException {
        // make sure that cancerStudy refers to a valid TypeOfCancerId
        // TODO: have a foreign key constraint do this; why not?
        TypeOfCancer aTypeOfCancer = DaoTypeOfCancer.getTypeOfCancerById(cancerStudy.getTypeOfCancerId());
        if (null == aTypeOfCancer) {
            throw new DaoException("cancerStudy.getTypeOfCancerId() '" + cancerStudy.getTypeOfCancerId() + "' does not refer to a TypeOfCancer.");
        }
        // CANCER_STUDY_IDENTIFIER cannot be null
        String stableId = cancerStudy.getCancerStudyStableId();
        if (stableId == null) {
            throw new DaoException("Cancer study stable ID cannot be null.");
        }
        CancerStudy existing = getCancerStudyByStableId(stableId);
        if (existing!=null) {
            if (overwrite) {
                //setStatus(Status.UNAVAILABLE, stableId);
                deleteCancerStudy(existing.getInternalId());
            } else {
                throw new DaoException("Cancer study " + stableId + "is already imported.");
            }
        }
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            long cancerStudyId = ClickHouseAutoIncrement.nextId(CANCER_STUDY_SEQUENCE);
            pstmt = con.prepareStatement("INSERT INTO cancer_study " +
                    "( `cancer_study_id`, `cancer_study_identifier`, `name`, "
                    + "`description`, `public`, `type_of_cancer_id`, "
                    + "`pmid`, `citation`, `groups`, `status`,`reference_genome_id`, `import_date` ) VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW())");
            pstmt.setLong(1, cancerStudyId);
            pstmt.setString(2, stableId);
            pstmt.setString(3, cancerStudy.getName());
            pstmt.setString(4, cancerStudy.getDescription());
            pstmt.setBoolean(5, cancerStudy.isPublicStudy());
            pstmt.setString(6, cancerStudy.getTypeOfCancerId());
            pstmt.setString(7, cancerStudy.getPmid());
            pstmt.setString(8, cancerStudy.getCitation());
            Set<String> groups = cancerStudy.getGroups();
            if (groups==null) {
                pstmt.setString(9, null);
            } else {
                pstmt.setString(9, StringUtils.join(groups, ";"));
            }
            //status is UNAVAILABLE until other data is loaded for this study. Once all is loaded, the
            //data loading process can set this to AVAILABLE:
            //TODO - use this field in parts of the system that build up the list of studies to display in home page:
            pstmt.setInt(10, Status.UNAVAILABLE.ordinal());
            try {
                ReferenceGenome referenceGenome = DaoReferenceGenome.getReferenceGenomeByGenomeName(cancerStudy.getReferenceGenome());
                pstmt.setInt(11, referenceGenome.getReferenceGenomeId());
            }
            catch (NullPointerException e) {
                throw new DaoException("Unsupported reference genome");
            }
            pstmt.executeUpdate();
            cancerStudy.setInternalId((int)cancerStudyId);
            cacheCancerStudy(cancerStudy, new java.util.Date());
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
        reCacheAll();
    }

    public static void addCancerStudyTags(CancerStudyTags cancerStudyTags) throws DaoException {
    
            Connection con = null;
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
                pstmt = con.prepareStatement("INSERT INTO cancer_study_tags " +
                        "( `cancer_study_id`,`tags` ) VALUES (?,?)");
                pstmt.setInt(1, cancerStudyTags.getCancerStudyId());
                pstmt.setString(2, cancerStudyTags.getTags());
                pstmt.executeUpdate();
            } catch (SQLException e) {
                throw new DaoException(e);
            } finally {
                JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
            }
        } 

    /**
     * Return the cancerStudy identified by the internal cancer study ID, if it exists.
     *
     * @param internalId     Internal (int) Cancer Study ID.
     * @return Cancer Study Object, or null if there's no such study.
     */
    public static CancerStudy getCancerStudyByInternalId(int internalId) throws DaoException {
        return byInternalId.get(internalId);
    }

    /**
     * Returns the cancerStudy identified by the stable identifier, if it exists.
     *
     * @param stableId Cancer Study Stable ID.
     * @return the CancerStudy, or null if there's no such study.
     */
    public static CancerStudy getCancerStudyByStableId(String stableId) throws DaoException {
        return byStableId.get(stableId);
    }

    /**
     * Indicates whether the cancerStudy identified by the stable ID exists.
     *
     * @param cancerStudyStableId Cancer Study Stable ID.
     * @return true if the CancerStudy exists, otherwise false
     */
    public static boolean doesCancerStudyExistByStableId(String cancerStudyStableId) {
        return byStableId.containsKey(cancerStudyStableId);
    }

    /**
     * Indicates whether the cancerStudy identified by internal study ID exist.
     * does no access control, so only returns a boolean.
     *
     * @param internalCancerStudyId Internal Cancer Study ID.
     * @return true if the CancerStudy exists, otherwise false
     */
    public static boolean doesCancerStudyExistByInternalId(int internalCancerStudyId) {
        return byInternalId.containsKey(internalCancerStudyId);
    }

    /**
     * Returns all the cancerStudies.
     *
     * @return ArrayList of all CancerStudy Objects.
     */
    public static ArrayList<CancerStudy> getAllCancerStudies() {
        return new ArrayList<CancerStudy>(byStableId.values());
    }

    /**
     * Gets Number of Cancer Studies.
     * @return number of cancer studies.
     */
    public static int getCount() {
        return byStableId.size();
    }

    /**
     * Deletes all Cancer Studies.
     * @throws DaoException Database Error.
     *
     * @deprecated this should not be used. Use deleteCancerStudy(cancerStudyStableId) instead
     */
    public static void deleteAllRecords() throws DaoException {
        cacheDateByStableId.clear();
        cacheDateByInternalId.clear();
        byStableId.clear();
        byInternalId.clear();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE cancer_study");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    /**
     *
     * @param cancerStudyStableId
     * @throws DaoException
     * @deprecated
     */
    public static void deleteCancerStudy(String cancerStudyStableId) throws DaoException {
        CancerStudy study = getCancerStudyByStableId(cancerStudyStableId);
        if (study != null){
            //setStatus(Status.UNAVAILABLE, cancerStudyStableId);
            deleteCancerStudy(study.getInternalId());
        }
    }

    public static Set<String> getFreshGroups(int internalCancerStudyId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            pstmt = con.prepareStatement("SELECT * FROM cancer_study where cancer_study_id = ?");
            pstmt.setInt(1, internalCancerStudyId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                CancerStudy cancerStudy = extractCancerStudy(rs);
                return cancerStudy.getGroups();
            } else {
                return Collections.emptySet();
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    /**
     * Deletes the Specified Cancer Study.
     * This method uses a large set of database operations to progressively delete all associated
     * data for a cancer study (including the study itself). A cleaner alternative based on
     * foreign key constraints and the cascade of delete operations is also available
     * (see deleteCancerStudyByCascade(int internalCancerStudyId)).
     *
     * Note to maintainers: Although deprecated, this method must be maintained for use in
     * environments where foreign key constraints are not in use, so remember to update the
     * implementation of this method if new tables are added or existing tables are changed.
     *
     * @param internalCancerStudyId Internal Cancer Study ID.
     * @throws DaoException Database Error.
     * @deprecated
     */
    public static void deleteCancerStudy(int internalCancerStudyId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            // check whether should delete generic assay meta profile by profile
            DaoGenericAssay.checkAndDeleteGenericAssayMetaInStudy(internalCancerStudyId);
            
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            List<Integer> geneticProfileIds = collectIds(con,
                "SELECT genetic_profile_id FROM genetic_profile WHERE cancer_study_id=?", internalCancerStudyId);
            List<Integer> patientIds = collectIds(con,
                "SELECT internal_id FROM patient WHERE cancer_study_id=?", internalCancerStudyId);
            List<Integer> sampleIds = collectIds(con,
                "SELECT internal_id FROM sample WHERE patient_id IN (SELECT internal_id FROM patient WHERE cancer_study_id=?)",
                internalCancerStudyId);
            List<Integer> sampleListIds = collectIds(con,
                "SELECT list_id FROM sample_list WHERE cancer_study_id=?", internalCancerStudyId);
            List<Integer> clinicalEventIds = collectIds(con,
                "SELECT clinical_event_id FROM clinical_event WHERE patient_id IN (SELECT internal_id FROM patient WHERE cancer_study_id=?)",
                internalCancerStudyId);
            List<Integer> gisticIds = collectIds(con,
                "SELECT gistic_roi_id FROM gistic WHERE cancer_study_id=?", internalCancerStudyId);

            deleteByIds(con, "DELETE FROM sample_cna_event WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM genetic_alteration WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM genetic_profile_samples WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM sample_profile WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM mutation WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM alteration_driver_annotation WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM mutation_count_by_keyword WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM structural_variant WHERE genetic_profile_id IN ", geneticProfileIds);
            deleteByIds(con, "DELETE FROM genetic_profile_link WHERE referred_genetic_profile_id IN ", geneticProfileIds);

            deleteByIds(con, "DELETE FROM gistic_to_gene WHERE gistic_roi_id IN ", gisticIds);

            deleteByIds(con, "DELETE FROM clinical_event_data WHERE clinical_event_id IN ", clinicalEventIds);
            deleteByIds(con, "DELETE FROM clinical_event WHERE clinical_event_id IN ", clinicalEventIds);

            deleteByIds(con, "DELETE FROM sample_list_list WHERE list_id IN ", sampleListIds);

            deleteByIds(con, "DELETE FROM clinical_sample WHERE internal_id IN ", sampleIds);
            deleteByIds(con, "DELETE FROM resource_sample WHERE internal_id IN ", sampleIds);

            deleteByIds(con, "DELETE FROM sample WHERE internal_id IN ", sampleIds);
            deleteByIds(con, "DELETE FROM clinical_patient WHERE internal_id IN ", patientIds);
            deleteByIds(con, "DELETE FROM resource_patient WHERE internal_id IN ", patientIds);

            deleteByStudyId(con, "DELETE FROM clinical_attribute_meta WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM resource_definition WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM resource_study WHERE internal_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM cancer_study_tags WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM copy_number_seg WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM copy_number_seg_file WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM patient WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM sample_list WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM genetic_profile WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM gistic WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM mut_sig WHERE cancer_study_id=?", internalCancerStudyId);
            deleteByStudyId(con, "DELETE FROM cancer_study WHERE cancer_study_id=?", internalCancerStudyId);

            removeCancerStudyFromCache(internalCancerStudyId);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
        purgeUnreferencedRecordsAfterDeletionOfStudy();
        reCacheAll();
        System.out.println("deleted study:\nID: "+internalCancerStudyId);
    }

    /**
     * Cleans up unreferenced records after cancer study deletion
     * @throws DaoException
     */
    public static void purgeUnreferencedRecordsAfterDeletionOfStudy() throws DaoException {
        String[] deleteStudyStatements = {
            "DELETE FROM cna_event WHERE cna_event_id NOT IN (SELECT DISTINCT cna_event_id FROM sample_cna_event)",
            "DELETE FROM mutation_event WHERE mutation_event_id NOT IN (SELECT DISTINCT mutation_event_id FROM mutation)"
        };
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCancerStudy.class);
            for (String statementString : deleteStudyStatements) {
                pstmt = con.prepareStatement(statementString);
                pstmt.executeUpdate();
                pstmt.close();
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, con, pstmt, rs);
        }
    }

    private static List<Integer> collectIds(Connection con, String sql, int cancerStudyId) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Integer> ids = new ArrayList<>();
        try {
            pstmt = con.prepareStatement(sql);
            pstmt.setInt(1, cancerStudyId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                ids.add(rs.getInt(1));
            }
        } finally {
            JdbcUtil.closeAll(DaoCancerStudy.class, null, pstmt, rs);
        }
        return ids;
    }

    private static void deleteByStudyId(Connection con, String sql, int cancerStudyId) throws SQLException {
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setInt(1, cancerStudyId);
            pstmt.executeUpdate();
        }
    }

    private static void deleteByIds(Connection con, String sqlPrefix, List<Integer> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        for (int start = 0; start < ids.size(); start += DELETE_BATCH_SIZE) {
            int end = Math.min(start + DELETE_BATCH_SIZE, ids.size());
            List<Integer> chunk = ids.subList(start, end);
            String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
            try (PreparedStatement pstmt = con.prepareStatement(sqlPrefix + "(" + placeholders + ")")) {
                int idx = 1;
                for (Integer id : chunk) {
                    pstmt.setInt(idx++, id);
                }
                pstmt.executeUpdate();
            }
        }
    }

    /**
     * Extracts Cancer Study JDBC Results.
     */
    private static CancerStudy extractCancerStudy(ResultSet rs) throws DaoException {
        try {
            CancerStudy cancerStudy = new CancerStudy(rs.getString("name"),
                    rs.getString("description"),
                    rs.getString("cancer_study_identifier"),
                    rs.getString("type_of_cancer_id"),
                    rs.getBoolean("public"));
            cancerStudy.setPmid(rs.getString("pmid"));
            cancerStudy.setCitation(rs.getString("citation"));
            cancerStudy.setGroupsInUpperCase(rs.getString("groups"));
            cancerStudy.setInternalId(rs.getInt("cancer_study_id"));
            cancerStudy.setImportDate(rs.getDate("import_date"));
            cancerStudy.setReferenceGenome(DaoReferenceGenome.getReferenceGenomeByInternalId(
                rs.getInt("reference_genome_id")).getGenomeName());
            return cancerStudy;
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    private static boolean studyNeedsRecaching(String stableId, Integer ... internalId) {
        if (cacheOutOfSyncWithDb()) {
            return true;
        }
        try {
            java.util.Date importDate = null;
            java.util.Date cacheDate = null;
            if (internalId.length > 0) {
                importDate = getImportDate(null, internalId[0]);
                cacheDate = cacheDateByInternalId.get(internalId[0]);
            } else {
                if (stableId.equals(org.mskcc.cbio.portal.util.AccessControl.ALL_CANCER_STUDIES_ID)) {
                    return false;
                }
                importDate = getImportDate(stableId);
                cacheDate = cacheDateByStableId.get(stableId);
            }
            return (importDate == null || cacheDate == null) ? false : cacheDate.before(importDate);
        } catch (ParseException e) {
            return false;
        } catch (DaoException e) {
            return false;
        }
    }

    private static boolean cacheOutOfSyncWithDb() {
        try {
            return getStudyCount() != byStableId.size();
        } catch (DaoException e) {
        }
        return false;
    }
}
