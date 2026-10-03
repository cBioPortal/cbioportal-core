package org.mskcc.cbio.portal.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * De-identification rules for whole-slide-image data (contract wsi-serving-v5): the single core
 * definition of the specimen accession number pattern, shared by the resource-data and timeline
 * importers. Callers report columns or JSON paths, never the offending values.
 */
public final class WsiDeidentification {

    /** Specimen accession numbers (S##-#####, MSK:S...) must never reach ClickHouse. */
    public static final Pattern ACCESSION = Pattern.compile("(?i)(\\bS\\d{2}-\\d{3,}|MSK:S\\d)");

    private static final ObjectMapper JSON = new ObjectMapper();

    private WsiDeidentification() {
    }

    /** Whether the value contains a specimen accession number. */
    public static boolean containsAccession(String value) {
        return value != null && ACCESSION.matcher(value).find();
    }

    /**
     * Return the path ({@code root}, {@code root.key}, {@code root.key[0]}, ...) of the first
     * string or object key in a JSON document holding an accession number, or null. A document
     * that is not valid JSON is checked as plain text (and reported as {@code root}).
     */
    public static String findAccessionInJson(String json, String root) {
        if (json == null) {
            return null;
        }
        JsonNode node;
        try {
            node = JSON.readTree(json);
        } catch (JsonProcessingException e) {
            return containsAccession(json) ? root : null;
        }
        // Catch anything the parser normalizes away (e.g. escapes) as well as the parsed values.
        String found = findAccession(node, root);
        return found != null ? found : (containsAccession(json) ? root : null);
    }

    private static String findAccession(JsonNode node, String path) {
        if (node == null) {
            return null;
        }
        if (node.isTextual()) {
            return containsAccession(node.textValue()) ? path : null;
        }
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (containsAccession(field.getKey())) {
                    return path + ".<key>"; // the key itself is never echoed
                }
                String childPath = path + "." + field.getKey();
                String found = findAccession(field.getValue(), childPath);
                if (found != null) {
                    return found;
                }
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                String found = findAccession(node.get(i), path + "[" + i + "]");
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }
}
