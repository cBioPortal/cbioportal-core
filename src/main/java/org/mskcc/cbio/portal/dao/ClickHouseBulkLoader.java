/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Minimal bulk loader that buffers rows in memory and streams them to ClickHouse via
 * TSVWithNames over the existing JDBC connection. This replaces the previous legacy
 * LOAD DATA LOCAL INFILE implementation.
 * <p>
 * The loader keeps the public API intact so that the various DAO classes do not need to
 * change their interaction pattern.
 */
public class ClickHouseBulkLoader {

    private static final Map<String, ClickHouseBulkLoader> BULK_LOADERS = new LinkedHashMap<>();

    private static boolean bulkLoad = false;
    private static boolean relaxedMode = false;

    private final String tableName;
    private final List<String[]> pendingRecords = new ArrayList<>();
    private String[] fieldNames = null;

    private ClickHouseBulkLoader(String tableName) {
        this.tableName = tableName;
    }

    public static ClickHouseBulkLoader getClickHouseBulkLoader(String tableName) {
        return BULK_LOADERS.computeIfAbsent(tableName, ClickHouseBulkLoader::new);
    }

    public static int flushAll() throws DaoException {
        int totalInserted = 0;
        for (ClickHouseBulkLoader loader : BULK_LOADERS.values()) {
            totalInserted += loader.flushPendingRecords();
        }
        BULK_LOADERS.clear();
        return totalInserted;
    }

    private int flushPendingRecords() throws DaoException {
        if (pendingRecords.isEmpty()) {
            return 0;
        }

        final int expectedRows = pendingRecords.size();
        Connection con = null;
        PreparedStatement stmt = null;
        try {
            con = JdbcUtil.getDbConnection(ClickHouseBulkLoader.class);
            List<String> columns = resolveColumnNames(con);
            validateRecordWidths(columns.size());

            stmt = con.prepareStatement(buildInsertStatement(columns));
            stmt.setBinaryStream(1, tsvPayload(columns));

            int rowsInserted = stmt.executeUpdate();
            if (rowsInserted <= 0) {
                rowsInserted = expectedRows;
            }
            ProgressMonitor.setCurrentMessage(" --> records inserted into `" + tableName + "` table: " + rowsInserted);

            if (!relaxedMode && rowsInserted != expectedRows) {
                throw new DaoException("DB Error: only " + rowsInserted + " of the " + expectedRows
                    + " records were inserted in `" + tableName + "`.");
            }

            pendingRecords.clear();
            return rowsInserted;
        } catch (SQLException exception) {
            throw new DaoException(exception);
        } finally {
            JdbcUtil.closeAll(ClickHouseBulkLoader.class, con, stmt, null);
        }
    }

    private String buildInsertStatement(List<String> columnNames) {
        final String columnsClause = columnNames.isEmpty() ? "" : " (" + String.join(",", columnNames) + ")";
        return "INSERT INTO " + tableName + columnsClause + " FORMAT TSVWithNames";
    }

    private List<String> resolveColumnNames(Connection con) throws SQLException, DaoException {
        if (fieldNames != null) {
            return Arrays.asList(fieldNames);
        }

        try (PreparedStatement stmt = con.prepareStatement("DESCRIBE TABLE " + tableName);
             ResultSet rs = stmt.executeQuery()) {
            List<String> columns = new ArrayList<>();
            while (rs.next()) {
                columns.add(rs.getString("name"));
            }
            if (columns.isEmpty()) {
                throw new DaoException("DB Error: unable to resolve columns for `" + tableName + "`.");
            }
            return columns;
        }
    }

    private void validateRecordWidths(int columnCount) throws DaoException {
        for (String[] record : pendingRecords) {
            if (record.length != columnCount) {
                throw new DaoException("DB Error: record column count (" + record.length + ") does not match expected column count ("
                    + columnCount + ") for `" + tableName + "`.");
            }
        }
    }

    // Rows are encoded as the driver reads the stream, so the payload is never held in memory.
    private InputStream tsvPayload(List<String> columnNames) {
        Iterator<InputStream> lines = Stream.concat(Stream.<String[]>of(columnNames.toArray(new String[0])), pendingRecords.stream())
            .map(values -> (InputStream) new ByteArrayInputStream(tsvLine(values)))
            .iterator();
        return new SequenceInputStream(new Enumeration<InputStream>() {
            public boolean hasMoreElements() {
                return lines.hasNext();
            }
            public InputStream nextElement() {
                return lines.next();
            }
        });
    }

    private byte[] tsvLine(String[] values) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                line.append('\t');
            }
            line.append(escapeTsvValue(values[i]));
        }
        return line.append('\n').toString().getBytes(StandardCharsets.UTF_8);
    }

    public void insertRecord(String... fieldValues) {
        if (fieldValues.length == 0) {
            return;
        }
        pendingRecords.add(fieldValues);
    }

    public static boolean isBulkLoad() {
        return bulkLoad;
    }

    public static void bulkLoadOn() {
        bulkLoad = true;
    }

    public static void bulkLoadOff() {
        bulkLoad = false;
    }

    public static void relaxedModeOn() {
        relaxedMode = true;
    }

    public static void relaxedModeOff() {
        relaxedMode = false;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    private String escapeTsvValue(String value) {
        if (value == null || "\\N".equals(value)) {
            return "\\N";
        }

        StringBuilder builder = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                default:
                    builder.append(ch);
                    break;
            }
        }
        return builder.toString();
    }
}
