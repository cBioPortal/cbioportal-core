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
 * Bulk deleter that buffers IDs in memory, streams them into a ClickHouse temporary
 * table, and issues a single DELETE ... WHERE id IN (SELECT id FROM tmp) statement.
 * This avoids large numbers of parameterized IN-clause round-trips.
 *
 * Mirrors the structure of ClickHouseBulkLoader.
 */
public class ClickHouseBulkDeleter {

    private static final Map<String, ClickHouseBulkDeleter> BULK_DELETERS = new LinkedHashMap<>();

    private final String targetTable;
    private final String idColumn;
    private final String tmpTable;
    private final List<Long> pendingIds = new ArrayList<>();

    private ClickHouseBulkDeleter(String targetTable, String idColumn) {
        this.targetTable = targetTable;
        this.idColumn = idColumn;
        this.tmpTable = "tmp_delete_" + targetTable;
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
        PreparedStatement stmt = null;
        try {
            con = JdbcUtil.getDbConnection(ClickHouseBulkDeleter.class);

            // 1. Create temp table
            stmt = con.prepareStatement(
                "CREATE TEMPORARY TABLE " + tmpTable + " (id Int64) ENGINE = Memory");
            stmt.executeUpdate();
            stmt.close();

            // 2. Bulk-insert IDs into temp table via TSV stream
            byte[] payload = buildTsvPayload();
            stmt = con.prepareStatement(
                "INSERT INTO " + tmpTable + " (id) FORMAT TSVWithNames");
            stmt.setBinaryStream(1, new ByteArrayInputStream(payload));
            stmt.executeUpdate();
            stmt.close();

            // 3. Execute delete
            stmt = con.prepareStatement(
                "DELETE FROM " + targetTable + " WHERE " + idColumn + " IN (SELECT id FROM " + tmpTable + ")");
            int deleted = stmt.executeUpdate();
            stmt.close();

            return deleted;
        } catch (SQLException | IOException e) {
            throw new DaoException(e);
        } finally {
            // 4. Always drop temp table to avoid session state leak back into the connection pool
            try {
                if (con != null) {
                    PreparedStatement drop = con.prepareStatement(
                        "DROP TEMPORARY TABLE IF EXISTS " + tmpTable);
                    drop.executeUpdate();
                    drop.close();
                }
            } catch (SQLException ignored) {
            }
            JdbcUtil.closeAll(ClickHouseBulkDeleter.class, con, stmt, null);
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
