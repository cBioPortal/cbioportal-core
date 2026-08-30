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
 * but WITHOUT ANY WARRANTY, without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * When enabled, matrix-style genetic alteration data is written directly to the exploded
     * {@code genetic_alteration_derived} table at import time instead of being stored packed in
     * {@code genetic_alteration} and rebuilt later by the ARRAY JOIN derive step. Defaults from the
     * {@code cbioportal.importer.no_explode} system property (or {@code CBIOPORTAL_IMPORTER_NO_EXPLODE}
     * env var) so it can be toggled from metaImport.py, and can also be set programmatically.
     */
    private static boolean noExplode = Boolean.parseBoolean(
        System.getProperty("cbioportal.importer.no_explode",
            System.getenv().getOrDefault("CBIOPORTAL_IMPORTER_NO_EXPLODE", "false")));

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

    /**
     * Flush this loader's buffered rows immediately. Lets a caller bound memory by flushing
     * periodically instead of accumulating an entire (potentially huge) batch before {@link #flushAll()}.
     */
    public int flush() throws DaoException {
        return flushPendingRecords();
    }

    /** Number of rows currently buffered (not yet flushed). */
    public int pendingSize() {
        return pendingRecords.size();
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

            byte[] payload = buildTsvPayload(columns);
            stmt = con.prepareStatement(buildInsertStatement(columns));
            stmt.setBinaryStream(1, new ByteArrayInputStream(payload));

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
        } catch (SQLException | IOException exception) {
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

    private byte[] buildTsvPayload(List<String> columnNames) throws DaoException, IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writeRow(buffer, columnNames);
        for (String[] record : pendingRecords) {
            writeRow(buffer, record);
        }
        return buffer.toByteArray();
    }

    private void writeRow(ByteArrayOutputStream buffer, List<String> values) throws IOException {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                buffer.write('\t');
            }
            buffer.write(escapeTsvValue(values.get(i)).getBytes(StandardCharsets.UTF_8));
        }
        buffer.write('\n');
    }

    private void writeRow(ByteArrayOutputStream buffer, String[] values) throws DaoException, IOException {
        if (values == null) {
            throw new DaoException("DB Error: encountered null record while preparing bulk insert for `" + tableName + "`.");
        }
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                buffer.write('\t');
            }
            buffer.write(escapeTsvValue(values[i]).getBytes(StandardCharsets.UTF_8));
        }
        buffer.write('\n');
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

    public static boolean isNoExplode() {
        return noExplode;
    }

    public static void noExplodeOn() {
        noExplode = true;
    }

    public static void noExplodeOff() {
        noExplode = false;
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
