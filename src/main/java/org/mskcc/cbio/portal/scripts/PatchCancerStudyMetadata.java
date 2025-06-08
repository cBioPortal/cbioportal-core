/*
 * Copyright (c) 2016 The Hyve B.V.
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

package org.mskcc.cbio.portal.scripts;

import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Command Line Tool to update the metadata of a Single Cancer Study.
 */
public class PatchCancerStudyMetadata extends ConsoleRunnable {

    public static final String CANCER_STUDY_IDENTIFIER_META_FIELD = "cancer_study_identifier";
    public static final String NAME_META_FIELD = "name";
    public static final String DESCRIPTION_META_FIELD = "description";
    public static final String CITATION_META_FIELD = "citation";
    public static final String PMID_META_FIELD = "pmid";
    public static final Set<String> PATCH_SUPPORTED_META_FIELDS = Set.of(NAME_META_FIELD, DESCRIPTION_META_FIELD, CITATION_META_FIELD, PMID_META_FIELD);

    public void run() {
        run(args);
    }

    public static void run(String[] args) {
        if (args.length < 1) {
            throw new UsageException(
                    PatchCancerStudyMetadata.class.getName(),
                    null,
                    "<cancer_study.txt>");
        }
        File file = new File(args[0]);
        try {
            run(file);
        } catch (Exception e) {
            throw new RuntimeException("File" + file, e);
        }
    }

    public static void run(File file) throws IOException, SQLException, DaoException {
        InputStream inputStream = new FileInputStream(file);
        run(inputStream);
    }

    public static void run(InputStream inputStream) throws IOException, SQLException, DaoException {
        TrimmedProperties properties = new TrimmedProperties();
        properties.load(inputStream);
        if (properties.isEmpty()) {
            throw new IllegalStateException("No fields were found");
        }
        if (!properties.containsKey(CANCER_STUDY_IDENTIFIER_META_FIELD)) {
            throw new IllegalStateException("No " + CANCER_STUDY_IDENTIFIER_META_FIELD + " field has been found");
        }
        if (properties.keySet().stream().noneMatch((PATCH_SUPPORTED_META_FIELDS::contains))) {
            throw new IllegalStateException("No field to patch has been found. Supported fields: "
                    + CANCER_STUDY_IDENTIFIER_META_FIELD);
        }

        Iterator<Map.Entry<Object, Object>> iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Object, Object> entry = iterator.next();
            Object field = entry.getKey();
            if (!CANCER_STUDY_IDENTIFIER_META_FIELD.equals(field) && !PATCH_SUPPORTED_META_FIELDS.contains(field)) {
                ProgressMonitor.logWarning("Patch functionality is not supported for '" + field + "' field. Skipping it.");
                iterator.remove();
            }
        }
        if (!patchCancerStudy(properties)) {
            throw new IllegalStateException("No study has been patched");
        }
        String message = "Patched cancer study:\n" +
                properties.keySet().stream().sorted().map(
                        (field) ->
                                " --> " + field + ": " + properties.getProperty((String) field)).collect(Collectors.joining("\n"));

        ProgressMonitor.setCurrentMessage(message);
        ProgressMonitor.setCurrentMessage("Done");
    }

    /**
     *
     * @param cancerStudyMetadata - metadata to patch
     * @return true - if record has been updated; false - otherwise
     * @throws SQLException
     */
    public static boolean patchCancerStudy(Properties cancerStudyMetadata) throws SQLException {
        Set<Object> orderedMetaFields = new TreeSet<>(cancerStudyMetadata.keySet());
        if (!orderedMetaFields.remove(CANCER_STUDY_IDENTIFIER_META_FIELD)) {
            throw new IllegalStateException("No " +  CANCER_STUDY_IDENTIFIER_META_FIELD + " field has found");
        }
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = JdbcUtil.getDbConnection(PatchCancerStudyMetadata.class);
            pstmt = con.prepareStatement("UPDATE cancer_study SET " +
                    orderedMetaFields.stream().map((field) -> "`" + getDbField(field) + "` = ?").collect(Collectors.joining(",")) +
                    "WHERE `CANCER_STUDY_IDENTIFIER` = ?");
            int parameterIndex = 1;
            for (Object field: orderedMetaFields) {
                pstmt.setString(parameterIndex++, cancerStudyMetadata.getProperty((String) field));
            }
            pstmt.setString(parameterIndex, cancerStudyMetadata.getProperty(CANCER_STUDY_IDENTIFIER_META_FIELD));
            return pstmt.executeUpdate() == 1;
        } finally {
            JdbcUtil.closeAll(PatchCancerStudyMetadata.class, con, pstmt, null);
        }
    }

    /**
     *
     * @param field - meta data field
     * @return corresponding database field
     */
    private static String getDbField(Object field) {
        return field.toString().toUpperCase();
    }

    /**
     * Makes an instance to run with the given command line arguments.
     *
     * @param args the command line arguments to be used
     */
    public PatchCancerStudyMetadata(String[] args) {
        super(args);
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new PatchCancerStudyMetadata(args);
        runner.runInConsole();
    }
}
