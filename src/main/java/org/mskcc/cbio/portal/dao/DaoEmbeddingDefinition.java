package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.model.EmbeddingDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class DaoEmbeddingDefinition {

    private static final Logger log = LoggerFactory.getLogger(DaoEmbeddingDefinition.class);

    private static final String EMBEDDING_DEFINITION_SEQUENCE = "seq_embedding_definition";
    public static final String TABLE = "embedding_definition";

    public static void addDatum(EmbeddingDefinition embeddingDefinition) throws DaoException
    {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        //Check if it exists
        try{
            long internalId = ClickHouseAutoIncrement.nextId(EMBEDDING_DEFINITION_SEQUENCE); // ClickHouseAutoIncrement.nextId(EMBEDDINGDEFINION_SEQUENCE)
            con = JdbcUtil.getDbConnection(DaoEmbeddingDefinition.class);
            pstmt = con.prepareStatement("INSERT INTO " +TABLE +
                    " ( `internal_id`, `embedding_id`, `short_name`, `name`, `description`,`entity_type`,`reduction_technique`) " +
                    "VALUES (?,?,?,?,?,?,?)");
            pstmt.setLong(1,internalId);
            pstmt.setString(2,embeddingDefinition.getEmbeddingId());
            pstmt.setString(3,embeddingDefinition.getShortName());
            pstmt.setString(4,embeddingDefinition.getName());
            pstmt.setString(5,embeddingDefinition.getDescription());
            pstmt.setString(6,embeddingDefinition.getEntityType());
            pstmt.setString(7,embeddingDefinition.getReductionTechnique());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoEmbeddingDefinition.class, con, pstmt, null);
        }
    }

    public static boolean checkDefinitionExists(String embeddingID) throws DaoException{
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try{
            con = JdbcUtil.getDbConnection(DaoEmbeddingDefinition.class);
            pstmt = con.prepareStatement("SELECT 1 FROM "+ TABLE +
                    " WHERE embedding_id=?");
            pstmt.setString(1,embeddingID);
            rs = pstmt.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoEmbeddingDefinition.class, con, pstmt, rs);
        }
    }

    public static int getDefinitionId(String embeddingID) throws DaoException{
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try{
            con = JdbcUtil.getDbConnection(DaoEmbeddingDefinition.class);
            pstmt = con.prepareStatement("SELECT internal_id FROM "+ TABLE +
                    " WHERE embedding_id=?");
            pstmt.setString(1,embeddingID);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("internal_id");
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoEmbeddingDefinition.class, con, pstmt, rs);
        }
        return -1;
    }
}
