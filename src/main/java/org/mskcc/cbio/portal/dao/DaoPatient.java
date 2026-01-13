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

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.ClinicalAttribute;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.model.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DAO to `patient`.
 * 
 * @author Benjamin Gross
 */
public class DaoPatient {

    private static final Logger log = LoggerFactory.getLogger(DaoPatient.class);

    private static final String SAMPLE_COUNT_ATTR_ID = "SAMPLE_COUNT";

    private static final Map<Integer, Patient> byInternalId = new HashMap<Integer, Patient>();
    private static final Map<Integer, Set<Patient>> byInternalCancerStudyId = new HashMap<Integer, Set<Patient>>();
    private static final MultiKeyMap byCancerIdAndStablePatientId = new MultiKeyMap();

    private static void clearCache()
    {
        byInternalId.clear();
        byInternalCancerStudyId.clear();
        byCancerIdAndStablePatientId.clear();
    }

    public static synchronized void reCache()
    {
        clearCache();

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoPatient.class);
            pstmt = con.prepareStatement("SELECT * FROM patient");
            rs = pstmt.executeQuery();
            ArrayList<Patient> list = new ArrayList<Patient>();
            while (rs.next()) {
                Patient p = extractPatient(rs);
                if (p != null) {
                    cachePatient(p, p.getCancerStudy().getInternalId());
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            JdbcUtil.closeAll(DaoPatient.class, con, pstmt, rs);
        }
    }

    public static void cachePatient(Patient patient, int cancerStudyId)
    {
        if (!byInternalId.containsKey(patient.getInternalId())) {
            byInternalId.put(patient.getInternalId(), patient);
        } 
        if (byInternalCancerStudyId.containsKey(patient.getCancerStudy().getInternalId())) {
            byInternalCancerStudyId.get(patient.getCancerStudy().getInternalId()).add(patient);
        }
        else {
            Set<Patient> patientList = new HashSet<Patient>();
            patientList.add(patient);
            byInternalCancerStudyId.put(patient.getCancerStudy().getInternalId(), patientList);
        }

        if (!byCancerIdAndStablePatientId.containsKey(cancerStudyId, patient.getStableId())) {
            byCancerIdAndStablePatientId.put(cancerStudyId, patient.getStableId(), patient);
        }
    }

    public static int addPatient(Patient patient) throws DaoException
    {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoPatient.class);
            pstmt = con.prepareStatement("INSERT INTO patient (`STABLE_ID`, `CANCER_STUDY_ID`) VALUES (?,?)",
                                         Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, patient.getStableId());
            pstmt.setInt(2, patient.getCancerStudy().getInternalId());
            pstmt.executeUpdate();
            rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                cachePatient(new Patient(patient.getCancerStudy(), patient.getStableId(), rs.getInt(1)), patient.getCancerStudy().getInternalId());
                return rs.getInt(1);
            }
            return -1;
        }
        catch (SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoPatient.class, con, pstmt, rs);
        }
    }

    public static Patient getPatientById(int internalId)
    {
        return byInternalId.get(internalId);
    }

    public static Patient getPatientByCancerStudyAndPatientId(int cancerStudyId, String stablePatientId) 
    {
        return (Patient)byCancerIdAndStablePatientId.get(cancerStudyId, stablePatientId);
    }

    public static Set<Patient> getPatientsByCancerStudyId(int cancerStudyId)
    {
        return byInternalCancerStudyId.get(cancerStudyId);
    }

    public static List<Patient> getAllPatients()
    {
        return (byInternalId.isEmpty()) ? Collections.<Patient>emptyList() :
            new ArrayList<Patient>(byInternalId.values());
    }

    public static void deleteAllRecords() throws DaoException
    {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoPatient.class);
            JdbcUtil.disableForeignKeyCheck(con);
            pstmt = con.prepareStatement("TRUNCATE TABLE patient");
            pstmt.executeUpdate();
            JdbcUtil.enableForeignKeyCheck(con);
        }
        catch (SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoPatient.class, con, pstmt, rs);
        }

        clearCache();
    }

    public static void createSampleCountClinicalData(int cancerStudyId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegment.class);
            pstmt = con.prepareStatement(
                    "REPLACE `clinical_patient`" +
                    "SELECT patient.`INTERNAL_ID`, 'SAMPLE_COUNT', COUNT(*) FROM sample " + 
                    "INNER JOIN patient ON sample.`PATIENT_ID` = patient.`INTERNAL_ID`" +
                    "WHERE patient.`CANCER_STUDY_ID`=? " +
                    "GROUP BY patient.`INTERNAL_ID`;");
            pstmt.setInt(1, cancerStudyId);
            ClinicalAttribute clinicalAttribute = DaoClinicalAttributeMeta.getDatum(SAMPLE_COUNT_ATTR_ID, cancerStudyId);
            if (clinicalAttribute == null) {
                ClinicalAttribute attr = new ClinicalAttribute(SAMPLE_COUNT_ATTR_ID, "Number of Samples Per Patient", 
                    "Number of Samples Per Patient", "STRING", true, "1", cancerStudyId);
                DaoClinicalAttributeMeta.addDatum(attr);
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegment.class, con, pstmt, rs);
        }
    }

    private static Patient extractPatient(ResultSet rs) throws SQLException
    {
		try {
			CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByInternalId(rs.getInt("CANCER_STUDY_ID"));
			if (cancerStudy == null) return null;
			return new Patient(cancerStudy,
							   rs.getString("STABLE_ID"),
							   rs.getInt("INTERNAL_ID"));
		}
		catch (DaoException e) {
			throw new SQLException(e);
		}
    }

    /**
     * Removes patients information from the study
     * @param internalStudyId - id of the study that contains the patients
     * @param patientStableIds - patient stable ids to remove
     * @throws DaoException
     */
    public static void deletePatients(int internalStudyId, Set<String> patientStableIds) throws DaoException
    {
        if (patientStableIds == null || patientStableIds.isEmpty()) {
            log.info("No patients specified to remove for study with internal id={}. Skipping.", internalStudyId);
            return;
        }
        log.info("Removing {} patients from study with internal id={} ...", patientStableIds, internalStudyId);

        Set<Integer> internalPatientIds = findInternalPatientIdsInStudy(internalStudyId, patientStableIds);
        Set<String> patientsSampleStableIds = internalPatientIds.stream().flatMap(internalPatientId ->
                DaoSample.getSamplesByPatientId(internalPatientId).stream().map(Sample::getStableId))
                .collect(Collectors.toSet());
        DaoSample.deleteSamples(internalStudyId, patientsSampleStableIds);

        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = JdbcUtil.getDbConnection(DaoPatient.class);
            pstmt = con.prepareStatement("DELETE FROM `patient` WHERE `INTERNAL_ID` IN ("
                    + String.join(",", Collections.nCopies(internalPatientIds.size(), "?"))
                    + ")");
            int parameterIndex = 1;
            for (Integer internalPatientId : internalPatientIds) {
                pstmt.setInt(parameterIndex++, internalPatientId);
            };
            pstmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoPatient.class, con, pstmt, null);
        }
        log.info("Removing {} patients from study with internal id={} done.", patientStableIds, internalStudyId);
    }

    public static Set<Integer> findInternalPatientIdsInStudy(Integer internalStudyId, Set<String> patientStableIds) {
        HashSet<Integer> internalPatientIds = new HashSet<>();
        for (String patientId : patientStableIds) {
            Patient patientByCancerStudyAndPatientId = DaoPatient.getPatientByCancerStudyAndPatientId(internalStudyId, patientId);
            if (patientByCancerStudyAndPatientId == null) {
                throw new NoSuchElementException("Patient with stable id=" + patientId + " not found in study with internal id=" + internalStudyId + ".");
            }
            internalPatientIds.add(patientByCancerStudyAndPatientId.getInternalId());
        }
        return internalPatientIds;
    }
}
