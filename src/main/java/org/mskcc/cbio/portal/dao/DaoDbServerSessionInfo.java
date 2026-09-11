/*
 * Copyright (c) 2026 Memorial Sloan Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan Kettering Cancer
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.mskcc.cbio.portal.util.ProgressMonitor;

/**
 * Provides database query functions which return information about the session created
 * by the database update process.
 */
public class DaoDbServerSessionInfo {

    /**
     * Make a simple query based on a provided expression. The return value must be a single String
     * @param expression : a valid SQL expression which will be evaluated to yield a single string.
     * @return a string from the database server's expression evaluation.
     * @throws DaoException
     */
    private static String queryDatabaseServerForString(String expression) throws DaoException {
        Connection connection = null;
        try {
            String query = String.format("SELECT %s AS expression_result", expression);
            connection = JdbcUtil.getDbConnection(DaoDbServerSessionInfo.class);
            try (
                    PreparedStatement preparedStatement = connection.prepareStatement(query);
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("expression_result");
                } else {
                    String errorMessage = String.format("unable to evaluate expression '%s' on database service", expression);
                    ProgressMonitor.setCurrentMessage(errorMessage);
                    throw new DaoException(errorMessage);
                }
            }
        } catch (SQLException e) {
            String errorMessage = String.format("unable to evaluate expression '%s' on database service", expression);
            ProgressMonitor.setCurrentMessage(errorMessage);
            throw new DaoException(errorMessage, e);
        } finally {
            JdbcUtil.closeAll(DaoDbServerSessionInfo.class, connection, null, null);
        }
    }

    /**
     * Get version of the database server
     * @return the database version (by evaluating 'version()')
     * @throws DaoException
     */
    public static String getServerVersion() throws DaoException {
        return queryDatabaseServerForString("version()");
    }

    /**
     * Get the name of the target database in use
     * @return the default / in-use database name (by evaluating 'current_database()')
     * @throws DaoException
     */
    public static String getDatabaseInUse() throws DaoException {
        return queryDatabaseServerForString("current_database()");
    }

    /**
     * Get the name of the database user we are connecting as
     * @return the name of the user that the update process is using to connect to the database.
     * @throws DaoException
     */
    public static String getDatabaseCurrentUser() throws DaoException {
        return queryDatabaseServerForString("current_user()");
    }

    /**
     * Get the privilege grants for the current user.
     * @return a list of GRANT privilege strings for the current user. Strings may contain multiple privileges listed (comma separated) on a single line.
     * @throws DaoException
     */
    public static List<String> getPrivilegesForCurrentUser() throws DaoException {
        Connection connection = null;
        try {
            String query = "SHOW GRANTS";
            connection = JdbcUtil.getDbConnection(DaoDbServerSessionInfo.class);
            try (
                    PreparedStatement preparedStatement = connection.prepareStatement(query);
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                List<String> privileges = new ArrayList<>();
                while (resultSet.next()) {
                    privileges.add(resultSet.getString("Grants"));
                }
                return privileges;
            }
        } catch (SQLException e) {
            String errorMessage = "unable to evaluate expression 'SHOW GRANTS' on database service";
            ProgressMonitor.setCurrentMessage(errorMessage);
            throw new DaoException(errorMessage, e);
        } finally {
            JdbcUtil.closeAll(DaoDbServerSessionInfo.class, connection, null, null);
        }
    }
}
