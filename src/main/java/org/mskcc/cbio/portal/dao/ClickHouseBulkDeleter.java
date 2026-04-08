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

package org.mskcc.cbio.portal.dao;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bulk deleter that buffers IDs in memory, streams them into a ClickHouse staging
 * table via TSVWithNames, and issues a single DELETE ... WHERE id IN (SELECT id FROM
 * staging_table) statement. The staging table is a real MergeTree table (not temporary)
 * so that it persists across HTTP requests on ClickHouse Cloud.
 *
 * Mirrors the structure of ClickHouseBulkLoader.
 */
public class ClickHouseBulkDeleter {

    private static final Map<String, ClickHouseBulkDeleter> BULK_DELETERS = new LinkedHashMap<>();

    private final String targetTable;
    private final String idColumn;
    private final String stagingTable;
    private final List<Long> pendingIds = new ArrayList<>();

    private ClickHouseBulkDeleter(String targetTable, String idColumn) {
        this.targetTable = targetTable;
        this.idColumn = idColumn;
        this.stagingTable = "staging_delete_" + targetTable;
    }

    public static ClickHouseBulkDeleter getBulkDeleter(String targetTable, String idColumn) {
        String key = targetTable + ":" + idColumn;
        return BULK_DELETERS.computeIfAbsent(key, k -> new ClickHouseBulkDeleter(targetTable, idColumn));
    }

    public void addId(long id) {
        pendingIds.add(id);
    }

    public void addIds(Collection<? extends Number> ids) {
        for (Number id : ids) {
            pendingIds.add(id.longValue());
        }
    }

    public static int flushAll() throws DaoException {
        int totalDeleted = 0;
        for (ClickHouseBulkDeleter deleter : BULK_DELETERS.values()) {
            totalDeleted += deleter.flushPendingIds();
        }
        BULK_DELETERS.clear();
        return totalDeleted;
    }

    private int flushPendingIds() throws DaoException {
        if (pendingIds.isEmpty()) {
            return 0;
        }

        Connection con = null;
        try {
            con = JdbcUtil.getDbConnection(ClickHouseBulkDeleter.class);

            // Drop any leftover staging table from a previous crashed run
            try (PreparedStatement stmt = con.prepareStatement(
                    "DROP TABLE IF EXISTS " + stagingTable)) {
                stmt.executeUpdate();
            }

            // Create staging table
            try (PreparedStatement stmt = con.prepareStatement(
                    "CREATE TABLE " + stagingTable + " (id Int64) ENGINE = MergeTree() ORDER BY id")) {
                stmt.executeUpdate();
            }

            // Insert IDs into staging table via TSV stream
            byte[] payload = buildTsvPayload();
            try (PreparedStatement stmt = con.prepareStatement(
                    "INSERT INTO " + stagingTable + " (id) FORMAT TSVWithNames")) {
                stmt.setBinaryStream(1, new ByteArrayInputStream(payload));
                stmt.executeUpdate();
            }

            // Execute delete via staging table
            int deleted;
            try (PreparedStatement stmt = con.prepareStatement(
                    "DELETE FROM " + targetTable + " WHERE " + idColumn + " IN (SELECT id FROM " + stagingTable + ")")) {
                deleted = stmt.executeUpdate();
            }

            return deleted;
        } catch (SQLException | IOException e) {
            throw new DaoException(e);
        } finally {
            // Always drop staging table
            try {
                if (con != null) {
                    try (PreparedStatement drop = con.prepareStatement(
                            "DROP TABLE IF EXISTS " + stagingTable)) {
                        drop.executeUpdate();
                    }
                }
            } catch (SQLException ignored) {
            }
            JdbcUtil.closeAll(ClickHouseBulkDeleter.class, con, null, null);
            pendingIds.clear();
        }
    }

    private byte[] buildTsvPayload() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        // header row
        buffer.write("id\n".getBytes(StandardCharsets.UTF_8));
        // data rows
        for (Long id : pendingIds) {
            buffer.write((id.toString() + "\n").getBytes(StandardCharsets.UTF_8));
        }
        return buffer.toByteArray();
    }
}
