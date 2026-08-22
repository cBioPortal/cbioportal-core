package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.model.EmbeddingData;

import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class DaoEmbeddingData {

    public static final String TABLE = "embedding_data";

    private DaoEmbeddingData(){}

    public static void addEmbeddingData( String InternalId, String  patientId,
                                       String sampleId, String x, String y, String customAttribute, int CancerStudyId)
    throws DaoException{

        if(!ClickHouseBulkLoader.isBulkLoad()){
            throw new DaoException("You have to turn on ClickHouseBulkLoader in order to insert embedding data");
        }else{
            ClickHouseBulkLoader.getClickHouseBulkLoader(TABLE).insertRecord(
                    InternalId, patientId, sampleId, x, y, customAttribute, Integer.toString(CancerStudyId)
            );
        }
    }

    public static List<EmbeddingData> getEmbeddingDataByCancerStudy(int cancerStudyId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<EmbeddingData> result = new ArrayList<>();
        try {
            con = JdbcUtil.getDbConnection(DaoEmbeddingData.class);
            pstmt = con.prepareStatement("SELECT * FROM " + TABLE +
                    " WHERE study_id = ?");
            pstmt.setInt(1, cancerStudyId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                EmbeddingData datum = new EmbeddingData(
                        rs.getInt("embedding_definition_id"),
                        rs.getString("sample_id"),
                        rs.getString("patient_id"),
                        rs.getFloat("x"),
                        rs.getFloat("y"),
                        rs.getString("custom_attribute"),
                        rs.getInt("study_id")
                );
                result.add(datum);
            }
            return result;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoEmbeddingData.class, con, pstmt, rs);
        }
    }
}
