package org.mskcc.cbio.portal.util;

/**
 * Utils to parse and validate TSV lines
 * @author Ruslan Forostianov
 */
public class TsvUtil {
    /**
     * is the line has some data
     * e.g. blank line and comments do not
     * @param line
     * @return
     */
    public static boolean isDataLine(String line) {
        return !line.startsWith("#") && line.trim().length() > 0;
    }

    /**
     * Splits tsv line and does not trim empty values at the end.
     * @param line
     * @return
     */
    public static String[] splitTsvLine(String line) {
        return line.split("\t", -1);
    }

    /**
     * Makes sure header and row length match
     * @param headerParts
     * @param rowParts
     */
    public static void ensureHeaderAndRowMatch(String[] headerParts, String[] rowParts) {
        int headerColumns = headerParts.length;
        if (rowParts.length > headerColumns) {
            throw new IllegalArgumentException("Found line with more fields (" + rowParts.length
                    + ") than specified in the headers(" + headerColumns + "): \n" + rowParts[0]);
        }
        if (rowParts.length < headerColumns) {
            throw new IllegalArgumentException("Found line with less fields (" + rowParts.length
                    + ") than specified in the headers(" + headerColumns + "): \n" + rowParts[0]);
        }
    }
}
