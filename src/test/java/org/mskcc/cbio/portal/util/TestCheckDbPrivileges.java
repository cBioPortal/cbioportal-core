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

import java.util.List;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.InjectMocks;
import org.mskcc.cbio.portal.dao.DaoDbServerSessionInfo;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.util.CheckDbPrivileges;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * JUnit tests for CheckDbPrivileges
 */
@RunWith(MockitoJUnitRunner.class)
public class TestCheckDbPrivileges {

    private DaoDbServerSessionInfo daoDbServerSessionInfo;
    private CheckDbPrivileges checkDbPrivileges;

    @Before
    public void setUp() {
        daoDbServerSessionInfo = Mockito.mock(DaoDbServerSessionInfo.class);
        checkDbPrivileges = new CheckDbPrivileges(daoDbServerSessionInfo);
    }

    boolean aggregateLogStringShowsWarning(String s) {
        return s.contains("recommended database privileges have not been granted");
    }

    /**
     * Tests user with all privileges under older REMOTE syntax
     */
    @Test
    public void testUserHasAllOlderPrivilegesDirectly() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("24.10");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO test_user",
                "GRANT REMOTE ON *.* TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected no privilege warning, but received one: %s", PMLog));
        }
    }

    /**
     * Tests user with all privileges under newer REMOTE syntax
     */
    @Test
    public void testUserHasAllNewerPrivilegesDirectly() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO test_user",
                "GRANT READ ON REMOTE TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected no privilege warning, but received one: %s", PMLog));
        }
    }

    /**
     * Tests user with all privileges except REMOTE
     */
    @Test
    public void testUserHasAllButRemotePrivilegesDirectly() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (!aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected privilege warning, but none was given. log message was: %s", PMLog));
        }
    }

    /**
     * Tests user has no privileges
     */
    @Test
    public void testUserHasNoPrivileges() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of();
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (!aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected privilege warning, but none was given. log message was: %s", PMLog));
        }
    }

    /**
     * Tests user only has role with all needed privileges
     */
    @Test
    public void testUserHasRoleWithAllPrivileges() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT role_1 TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        List<String> testPrivilegesForRole1 = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO role_1",
                "GRANT READ ON REMOTE TO role_1");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_1")).thenReturn(testPrivilegesForRole1);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected no privilege warning, but received one: %s", PMLog));
        }
    }

    /**
     * Tests user only has role with not all privileges
     */
    @Test
    public void testUserHasRoleWithAllMinusTwoPrivileges() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT role_1 TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        List<String> testPrivilegesForRole1 = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO role_1",
                "GRANT READ ON REMOTE TO role_1");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_1")).thenReturn(testPrivilegesForRole1);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (!aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected privilege warning, but none was given. log message was: %s", PMLog));
        }
    }

    /**
     * Tests user has two roles with all needed privileges
     */
    @Test
    public void testUserHasTwoRolesWithAllPrivileges() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT role_1, role_2 TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        List<String> testPrivilegesForRole1 = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO role_1",
                "GRANT READ ON REMOTE TO role_1");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_1")).thenReturn(testPrivilegesForRole1);
        List<String> testPrivilegesForRole2 = List.of(
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO role_2",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO role_2");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_2")).thenReturn(testPrivilegesForRole2);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected no privilege warning, but received one: %s", PMLog));
        }
    }

    /**
     * Tests user has one privilege and two roles with the other needed privileges
     */
    @Test
    public void testUserHasTwoRolesWithMostPrivilegesAndAlsoTheRest() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT role_1, role_2 TO test_user",
                "GRANT READ ON REMOTE TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        List<String> testPrivilegesForRole1 = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO role_1");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_1")).thenReturn(testPrivilegesForRole1);
        List<String> testPrivilegesForRole2 = List.of(
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO role_2",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO role_2");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_2")).thenReturn(testPrivilegesForRole2);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected no privilege warning, but received one: %s", PMLog));
        }
    }

    /**
     * Tests user has one privilege and two roles and one role has a different role granted -- privileges spread everywhere
     */
    @Test
    public void testUserHasTransitiveRoles() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT role_1, role_2 TO test_user",
                "GRANT READ ON REMOTE TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        List<String> testPrivilegesForRole1 = List.of(
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO role_1",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO role_1");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_1")).thenReturn(testPrivilegesForRole1);
        List<String> testPrivilegesForRole2 = List.of(
                "GRANT role_3 TO role_2",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO role_2");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_2")).thenReturn(testPrivilegesForRole2);
        List<String> testPrivilegesForRole3 = List.of(
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO role_3");
        when(daoDbServerSessionInfo.getPrivilegesForRole("role_3")).thenReturn(testPrivilegesForRole3);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected no privilege warning, but received one: %s", PMLog));
        }
    }

    /**
     * Tests user with all privileges plus some unparsable/unknown grants
     */
    @Test
    public void testUserHasAllPrivilegesPlusNonsense() throws DaoException {
        Mockito.lenient().when(daoDbServerSessionInfo.getServerVersion()).thenReturn("26.6");
        when(daoDbServerSessionInfo.getDatabaseInUse()).thenReturn("test_db");
        when(daoDbServerSessionInfo.getDatabaseCurrentUser()).thenReturn("test_user");
        List<String> testPrivilegesForUser = List.of(
                "GRANT this_looks_like_a_role_but_is_not_really TO test_user",
                "GRANT TO test_user",
                "GRANT ON test_user",
                "GRANT TO ON test_user",
                "Some random text that follows no real syntax",
                "GRANT SHOW TABLES, SHOW COLUMNS, SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON test_db.* TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.tables TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.parts TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.mutations TO test_user",
                "GRANT SHOW COLUMNS, SELECT ON system.one TO test_user",
                "GRANT READ ON REMOTE TO test_user");
        when(daoDbServerSessionInfo.getPrivilegesForCurrentUser()).thenReturn(testPrivilegesForUser);
        ProgressMonitor.resetLog();
        checkDbPrivileges.logWarningIfRecommendedPrivilegeIsAbsent();
        String PMLog = ProgressMonitor.getLog();
        if (aggregateLogStringShowsWarning(PMLog)) {
            Assert.fail(String.format("expected no privilege warning, but received one: %s", PMLog));
        }
    }

}
