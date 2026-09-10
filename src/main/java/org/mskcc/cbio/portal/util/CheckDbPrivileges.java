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

package org.mskcc.cbio.portal.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Set;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoDbServerSessionInfo;
import org.mskcc.cbio.portal.util.ProgressMonitor;

// basic strategy : get current grants, iterate through map keys for any matches, collect values of matches, finally compare to the full list of all recommneded privileges
public class CheckDbPrivileges {

    public static class RecommendedPrivilege {
        String name;
        List<String> subsumingPrivileges;
        String onDatabase;
        String onTable;

        public static String CURRENT_DATABASE_SPECIAL_VALUE = "currentDatabase()";

        public RecommendedPrivilege(
                String name,
                List<String> subsumingPrivileges,
                String onDatabase,
                String onTable) {
            this.name = name;
            this.subsumingPrivileges = new ArrayList<>(subsumingPrivileges);
            this.onDatabase = onDatabase;
            this.onTable = onTable;
        }
    };

    private static Set<RecommendedPrivilege> recommendedPrivilegeSet = new HashSet<>();
    private static boolean versionDependentPrivilegesAdded = false;

    static {
        // construct list of recommended privileges for import (those independent of clickhouse version)
        CheckDbPrivileges.recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SHOW TABLES ON current_database.*
                "SHOW TABLES",
                new ArrayList<String>(List.of("ALL", "SHOW")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SHOW COLUMNS ON current_database.*
                "SHOW COLUMNS",
                new ArrayList<String>(List.of("ALL", "SHOW")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SELECT ON current_database.*
                "SELECT",
                new ArrayList<String>(List.of("ALL")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // INSERT ON current_database.*
                "INSERT",
                new ArrayList<String>(List.of("ALL")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // ALTER ON current_database.* (for ALTER TABLE, ALTER DELETE, ALTER UPDATE)
                "ALTER",
                new ArrayList<String>(List.of("ALL")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // CREATE TABLE ON current_database.*
                "CREATE TABLE",
                new ArrayList<String>(List.of("ALL", "CREATE")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // CREATE VIEW ON current_database.*
                "CREATE VIEW",
                new ArrayList<String>(List.of("ALL", "CREATE")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // DROP TABLE ON current_database.*
                "DROP TABLE",
                new ArrayList<String>(List.of("ALL", "DROP")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // DROP VIEW ON current_database.*
                "DROP VIEW",
                new ArrayList<String>(List.of("ALL", "DROP")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // TRUNCATE ON current_database.*
                "TRUNCATE",
                new ArrayList<String>(List.of("ALL")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // OPTIMIZE ON current_database.*
                "OPTIMIZE",
                new ArrayList<String>(List.of("ALL")),
                RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE, "*"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SHOW COLUMNS ON system.tables
                "SHOW COLUMNS",
                new ArrayList<String>(List.of("ALL", "SHOW")),
                "system", "tables"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SHOW COLUMNS ON system.parts
                "SHOW COLUMNS",
                new ArrayList<String>(List.of("ALL", "SHOW")),
                "system", "parts"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SHOW COLUMNS ON system.mutations
                "SHOW COLUMNS",
                new ArrayList<String>(List.of("ALL", "SHOW")),
                "system", "mutations"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SHOW COLUMNS ON system.one
                "SHOW COLUMNS",
                new ArrayList<String>(List.of("ALL", "SHOW")),
                "system", "one"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SELECT ON system.tables
                "SELECT",
                new ArrayList<String>(List.of("ALL")),
                "system", "tables"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SELECT ON system.parts
                "SELECT",
                new ArrayList<String>(List.of("ALL")),
                "system", "parts"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SELECT ON system.mutations
                "SELECT",
                new ArrayList<String>(List.of("ALL")),
                "system", "mutations"));
        recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // SELECT ON system.one
                "SELECT",
                new ArrayList<String>(List.of("ALL")),
                "system", "one"));
    }

    private static boolean versionEqualsOrPreceeds(String actualVersion, String compareVersion) throws DaoException {
        // check only major and minor
        String[] actual = actualVersion.split("\\.");
        String[] compare = compareVersion.split("\\.");
        try {
            int actualMajor = Integer.parseInt(actual[0]);
            int compareMajor = Integer.parseInt(compare[0]);
            if (actualMajor < compareMajor) {
                return true;
            }

            if (actualMajor > compareMajor) {
                return false;
            }
            int actualMinor = Integer.parseInt(actual[1]);
            int compareMinor = Integer.parseInt(compare[1]);
            if (actualMinor <= compareMinor) {
                return true;
            }

            if (actualMinor > compareMinor) {
                return false;
            }
            throw new DaoException("a non-evaluatable number (like NaN) was somehow reported by the database server : " + actualVersion);
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
            throw new DaoException("likely an invalid version was returned from database server : " + actualVersion + " " + e.getClass().getName());
        }
    }

    private static void addVersionDependentRecommendationsIfNeeded(String serverVersion) throws DaoException {
        if (versionDependentPrivilegesAdded) {
            return;
        }
        if (versionEqualsOrPreceeds(serverVersion, "11.6")) {
            recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // REMOTE ON *.*
                    "REMOTE",
                    new ArrayList<String>(List.of("ALL", "SOURCES")),
                    "*", "*"));
        } else {
            recommendedPrivilegeSet.add(new CheckDbPrivileges.RecommendedPrivilege( // READ ON REMOTE
                    "READ",
                    new ArrayList<String>(List.of("ALL", "SOURCES", "REMOTE")),
                    "REMOTE", "*"));  // special : a user with privilege REMOTE ON *.* inherently has READ ON REMOTE (no table reference), so wildcard matching should include "nothing" as well as "anything"
       }
        versionDependentPrivilegesAdded = true;
    }

    // Function discards unparsable privileges without raising an exception
    private static List<String> splitCombinedPrivilegeGrantStrings(List<String> combinedPrivilegesForCurrentUser) {
        List<String> splitPrivileges = new ArrayList<String>();
        if (combinedPrivilegesForCurrentUser == null || combinedPrivilegesForCurrentUser.size() == 0) {
            return splitPrivileges;
        }
        Pattern combinedPrivilegePattern = Pattern.compile("^(.*)\\s\\s*[Oo][Nn]\\s\\s*(\\S\\S*)\\s\\s*[Tt][Oo]\\s.*$");
        for (String combinedPrivilege : combinedPrivilegesForCurrentUser) {
            if (combinedPrivilege == null || combinedPrivilege.strip().isEmpty()) {
                continue;
            }
            String[] parts = combinedPrivilege.split(",");
            parts[0] = parts[0].replaceFirst("^\\s*[Gg][Rr][Aa][Nn][Tt]\\s*", "");
            Matcher combinedPrivilegeMatcher = combinedPrivilegePattern.matcher(parts[parts.length - 1]);
            if (!combinedPrivilegeMatcher.matches()) {
                continue;
            }
            parts[parts.length - 1] = combinedPrivilegeMatcher.group(1);
            String onClause = String.format(" ON %s", combinedPrivilegeMatcher.group(2));
            // add the "on clause" to all of the split privileges, and accumulate
            for (int i = 0; i < parts.length; i = i + 1) {
                splitPrivileges.add(parts[i].strip() + onClause);
            }
        }
        return splitPrivileges;
    }

    private static String collapseWhitespace(String s) {
        return s.replaceAll("\\s\\s*", " ");
    }

    private static boolean actualGrantMatchesOrSubsumesRecommendation(String actualGrant, CheckDbPrivileges.RecommendedPrivilege recommended) {
        if (actualGrant.equals(recommended.name)) {
            return true;
        }
        for (String subsumer : recommended.subsumingPrivileges) {
            if (actualGrant.equals(subsumer)) {
                return true;
            }
        }
        return false;
    }

    private static boolean actualDatabaseCoversRecommendedDatabase(String actualDatabase, CheckDbPrivileges.RecommendedPrivilege recommended, String currentDatabase) {
        if (actualDatabase.equals("*")) {
            return true; // everything is covered by this actual rule
        }
        if (recommended.onDatabase.equals(RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE)) {
            return actualDatabase.equals(currentDatabase);
        } else {
            return actualDatabase.equals(recommended.onDatabase);
        }
    }
    
    private static boolean actualTableCoversRecommendedTable(String actualTable, CheckDbPrivileges.RecommendedPrivilege recommended) {
        if (actualTable.equals("*")) {
            return true; // everything is covered by this actual rule
        }
        if (actualTable.equals("")) {
            // this happens for the special actual rules in the form "READ ON REMOTE" or "WRITE ON SOURCES" where no table is specified (not even a wildcard)
            return recommended.onTable.equals("*"); // recommendations for these special cases will have "*" as the table specified in order to cover subsuming grants like "REMOTE ON *.*"
        }
        return actualTable.equals(recommended.onTable);
    }
    
    private static boolean actualPrivilegeSatisfiesRecommendation(String privilegeString, CheckDbPrivileges.RecommendedPrivilege recommended, String currentDatabase) {
        int onPosition = privilegeString.lastIndexOf(" ON ");
        if (onPosition == -1) {
            return false;
        }
        String actualGrant = collapseWhitespace(privilegeString.substring(0, onPosition));
        String actualDatabaseAndTable = privilegeString.substring(onPosition + 4);
        String actualDatabase = null;
        String actualTable = null;
        int dotPosition = actualDatabaseAndTable.lastIndexOf(".");
        if (dotPosition == -1) {
            // no table specified
            actualDatabase = actualDatabaseAndTable.strip();
            actualTable = "";
        } else {
            actualDatabase = actualDatabaseAndTable.substring(0, dotPosition).strip();
            actualTable = actualDatabaseAndTable.substring(dotPosition + 1).strip();
        }
        boolean privilegeMatchedOrSubsumed = actualGrantMatchesOrSubsumesRecommendation(actualGrant.toUpperCase(), recommended);
        boolean privilegeDatabaseCovered = actualDatabaseCoversRecommendedDatabase(actualDatabase, recommended, currentDatabase);
        boolean privilegeTableCovered = actualTableCoversRecommendedTable(actualTable, recommended);
        return privilegeMatchedOrSubsumed && privilegeDatabaseCovered && privilegeTableCovered;
    }

    public static void logWarningIfRecommendedPrivilegeIsAbsent() throws DaoException {
        try {
            String dbServerVersion = DaoDbServerSessionInfo.getServerVersion();
            addVersionDependentRecommendationsIfNeeded(dbServerVersion);
            String currentDatabase = DaoDbServerSessionInfo.getDatabaseInUse();
            String currentUser = DaoDbServerSessionInfo.getDatabaseCurrentUser();
            List<String> privilegesForCurrentUser = DaoDbServerSessionInfo.getPrivilegesForCurrentUser();
            List<String> singlePrivilegesForCurrentUser = splitCombinedPrivilegeGrantStrings(privilegesForCurrentUser);
            Set<CheckDbPrivileges.RecommendedPrivilege> unsatisfiedRecommendations = new HashSet(recommendedPrivilegeSet);
            for (String p : singlePrivilegesForCurrentUser) {
                Set <CheckDbPrivileges.RecommendedPrivilege> satisfiedRecommendations = new HashSet<>();
                for (CheckDbPrivileges.RecommendedPrivilege recommended : unsatisfiedRecommendations) {
                    if (actualPrivilegeSatisfiesRecommendation(p, recommended, currentDatabase)) {
                        satisfiedRecommendations.add(recommended);
                    }
                }
                for (CheckDbPrivileges.RecommendedPrivilege satisfied : satisfiedRecommendations) {
                    unsatisfiedRecommendations.remove(satisfied);
                }
            }
            if (unsatisfiedRecommendations.size() > 0) {
                ProgressMonitor.setCurrentMessage("warning : recommended database privileges have not been granted; database interactions may fail.");
                ProgressMonitor.setCurrentMessage("          Executing the following SQL statements when connected to the database with a user that");
                ProgressMonitor.setCurrentMessage("          has the 'WITH GRANT' option enabled for all needed privileges may resolve this issue:");
                for (CheckDbPrivileges.RecommendedPrivilege recommended : unsatisfiedRecommendations) {
                    String onClause = null;
                    String onClauseDatabase = recommended.onDatabase;
                    if (onClauseDatabase.equals(RecommendedPrivilege.CURRENT_DATABASE_SPECIAL_VALUE)) {
                        onClauseDatabase = currentDatabase;
                    }
                    if (recommended.onTable == null || recommended.onTable.strip().isEmpty()) {
                        onClause = onClauseDatabase;
                    } else {
                        onClause = String.format("%s.%s", onClauseDatabase, recommended.onTable);
                    }
                    // special case
                    if (onClause.equals("REMOTE.*") || onClause.equals("SOURCES.*")) {
                        onClause = onClause.substring(0, onClause.length() - 2);
                    }
                    String grantCommand = String.format("    GRANT %s ON %s TO %s", recommended.name, onClause, currentUser);
                    ProgressMonitor.setCurrentMessage(grantCommand);
                }
                ProgressMonitor.setCurrentMessage("          Note : if you are running cBioPortal in a configuration which uses multiple databases,");
                ProgressMonitor.setCurrentMessage("                 all involved databases should be updated in a similar way.");
            }
        } catch (DaoException e) {
            String msg = "Error : exception occurred during CheckDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent()";
            ProgressMonitor.setCurrentMessage(msg);
            throw new DaoException(msg, e);
        }
    }

    public static void main(String[] args) {
        try {
            ProgressMonitor.setConsoleMode(true);
            logWarningIfRecommendedPrivilegeIsAbsent();
            System.exit(0);
        } catch (DaoException e) {
            e.printStackTrace();
        }
    }
}
