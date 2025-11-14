/*
 * Copyright (c) 2017 The Hyve B.V.
 * This code is licensed under the GNU Affero General Public License (AGPL),
 * version 3, or (at your option) any later version.
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

/*
 * @author Sander Tan
*/

package org.mskcc.cbio.portal.dao;

import java.sql.*;

import org.mskcc.cbio.portal.model.GenesetHierarchy;

public class DaoGenesetHierarchyNode {

    private static final String GENESET_HIERARCHY_SEQUENCE = "seq_geneset_hierarchy_node";
	
	private DaoGenesetHierarchyNode() {
	}
    
	/**
     * Add gene set hierarchy object to geneset_hierarchy_node table in database.
     * @throws DaoException 
     */
	public static void addGenesetHierarchy(GenesetHierarchy genesetHierarchy) throws DaoException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        
        try {
        	// Open connection to database
            connection = JdbcUtil.getDbConnection(DaoGenesetHierarchyNode.class);
	        
	        // Prepare SQL statement
            preparedStatement = connection.prepareStatement("INSERT INTO geneset_hierarchy_node "
	                + "(`node_id`, `node_name`, `parent_id`) VALUES(?,?,?)");
            long nodeId = ClickHouseAutoIncrement.nextId(GENESET_HIERARCHY_SEQUENCE);
            preparedStatement.setLong(1, nodeId);
	        
            // Fill in statement
            preparedStatement.setString(2, genesetHierarchy.getNodeName());
            
            if (genesetHierarchy.getParentId() == 0) {
                preparedStatement.setNull(3, java.sql.Types.INTEGER);
            } else {
                preparedStatement.setInt(3, genesetHierarchy.getParentId());
            }
            
            // Execute statement
            preparedStatement.executeUpdate();

            // Get the auto generated key, which is the Node ID:
            genesetHierarchy.setNodeId((int) nodeId);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoGenesetHierarchyNode.class, connection, preparedStatement, null);
        }
	}

    /**
     * Retrieve gene set hierarchy object from geneset_hierarchy_node table in database to check if table if filled.
     * @throws DaoException 
     */
	public static boolean checkGenesetHierarchy() throws DaoException {
		Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        
        try {
        	// Open connection to database
            connection = JdbcUtil.getDbConnection(DaoGenesetHierarchyNode.class);
	        
	        // Prepare SQL statement
            preparedStatement = connection.prepareStatement("SELECT * FROM geneset_hierarchy_node LIMIT 1");
            
            // Execute statement
            resultSet = preparedStatement.executeQuery();
            
            // return false if result set is empty
            return resultSet.next();
            
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoGenesetHierarchyNode.class, connection, preparedStatement, resultSet);
        }
	}

    /**
     * Deletes all records from 'geneset_hierarchy_node' table in database.
     * This also deletes all records from related 'geneset_hierarchy_leaf' table, via ON DELETE CASCADE
     * constraint in DB.
     *
     * @throws DaoException 
     */   
	public static void deleteAllGenesetHierarchyRecords() throws DaoException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
        	connection = JdbcUtil.getDbConnection(DaoGenesetHierarchyNode.class);
        	preparedStatement = connection.prepareStatement("TRUNCATE TABLE geneset_hierarchy_node");
        	preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoGenesetHierarchyNode.class, connection, preparedStatement, resultSet);
        }
    }
}
