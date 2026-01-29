package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Generates and executes ClickHouse SQL that reports referential-integrity violations
 * for the cBioPortal ClickHouse schema.
 * <p>
 * Output columns per violation row:
 * referee_table, referee_columns, referee_values, referred_table, referred_columns
 * <p>
 * Notes:
 * - ClickHouse does not enforce FKs, so orphans are detected via LEFT JOIN + isNull().
 * - Table/column names are normalized to lower-case in the generated SQL.
 * - Rows are checked only when all FK columns are non-NULL.
 * - "NULL" in referee_values means the FK column value was NULL.
 * <p>
 * The FK list is hard-coded for the ClickHouse schema (see clickhouse_cgds.sql).
 * Update {@link #schemaForeignKeys()} when the schema changes.
 */
public class ClickHouseReferentialIntegrityChecker {

    public static final String REFEREE_TABLE = "referee_table";
    public static final String REFEREE_COLUMNS = "referee_columns";
    public static final String REFEREE_VALUES = "referee_values";
    public static final String REFERRED_TABLE = "referred_table";
    public static final String REFERRED_COLUMNS = "referred_columns";
    private static final String REFEREE_VALUE_SEPARATOR = "|";

    private static final List<ForeignKey> SCHEMA_FOREIGN_KEYS = schemaForeignKeys();
    private static final String MASTER_UNION_QUERY = generateMasterUnionQuery(SCHEMA_FOREIGN_KEYS);

    private ClickHouseReferentialIntegrityChecker() {
    }

    public record ReferenceViolation(
            String refereeTable,
            String refereeColumns,
            String refereeValues,
            String referredTable,
            String referredColumns
    ) {
    }

    public static List<ReferenceViolation> findReferenceViolations() throws DaoException {
        if (MASTER_UNION_QUERY.isBlank()) {
            return List.of();
        }
        List<ReferenceViolation> violations = new ArrayList<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(ClickHouseReferentialIntegrityChecker.class);
            pstmt = con.prepareStatement(MASTER_UNION_QUERY);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                violations.add(new ReferenceViolation(
                        rs.getString(REFEREE_TABLE),
                        rs.getString(REFEREE_COLUMNS),
                        rs.getString(REFEREE_VALUES),
                        rs.getString(REFERRED_TABLE),
                        rs.getString(REFERRED_COLUMNS)
                ));
            }
            return violations;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(ClickHouseReferentialIntegrityChecker.class, con, pstmt, rs);
        }
    }

    /**
     * Represents one foreign-key relationship (supports composite keys).
     */
    private static final class ForeignKey {
        private final String childTable;       // referee_table
        private final List<String> childCols;   // referee_columns (possibly composite)
        private final String parentTable;      // referred_table
        private final List<String> parentCols; // referred_columns (same arity as childCols)

        private ForeignKey(String childTable, List<String> childCols, String parentTable, List<String> parentCols) {
            if (childCols.isEmpty() || parentCols.isEmpty()) {
                throw new IllegalArgumentException("FK column list cannot be empty: " + childTable + " -> " + parentTable);
            }
            if (childCols.size() != parentCols.size()) {
                throw new IllegalArgumentException("FK arity mismatch: childCols=" + childCols + " parentCols=" + parentCols);
            }
            this.childTable = childTable.toLowerCase(Locale.ROOT);
            this.childCols = childCols.stream().map(s -> s.toLowerCase(Locale.ROOT)).toList();
            this.parentTable = parentTable.toLowerCase(Locale.ROOT);
            this.parentCols = parentCols.stream().map(s -> s.toLowerCase(Locale.ROOT)).toList();
        }

        private String childColsCsv() {
            return String.join(",", childCols);
        }

        private String parentColsCsv() {
            return String.join(",", parentCols);
        }
    }

    /**
     * Foreign key list for the ClickHouse schema (lower-case names).
     * Keep this in sync with clickhouse_cgds.sql.
     */
    private static List<ForeignKey> schemaForeignKeys() {
        List<ForeignKey> fks = new ArrayList<>();

        // cancer_study
        fks.add(new ForeignKey("cancer_study", List.of("type_of_cancer_id"), "type_of_cancer", List.of("type_of_cancer_id")));
        fks.add(new ForeignKey("cancer_study", List.of("reference_genome_id"), "reference_genome", List.of("reference_genome_id")));

        // cancer_study_tags
        fks.add(new ForeignKey("cancer_study_tags", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));

        // patient
        fks.add(new ForeignKey("patient", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));

        // sample
        fks.add(new ForeignKey("sample", List.of("patient_id"), "patient", List.of("internal_id")));

        // sample_list
        fks.add(new ForeignKey("sample_list", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));

        // sample_list_list (only sample_id has an FK in the ClickHouse DDL)
        fks.add(new ForeignKey("sample_list_list", List.of("sample_id"), "sample", List.of("internal_id")));

        // gene
        fks.add(new ForeignKey("gene", List.of("genetic_entity_id"), "genetic_entity", List.of("id")));

        // gene_alias
        fks.add(new ForeignKey("gene_alias", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));

        // geneset
        fks.add(new ForeignKey("geneset", List.of("genetic_entity_id"), "genetic_entity", List.of("id")));

        // geneset_gene
        fks.add(new ForeignKey("geneset_gene", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));
        fks.add(new ForeignKey("geneset_gene", List.of("geneset_id"), "geneset", List.of("id")));

        // geneset_hierarchy_leaf
        fks.add(new ForeignKey("geneset_hierarchy_leaf", List.of("node_id"), "geneset_hierarchy_node", List.of("node_id")));
        fks.add(new ForeignKey("geneset_hierarchy_leaf", List.of("geneset_id"), "geneset", List.of("id")));

        // generic_entity_properties
        fks.add(new ForeignKey("generic_entity_properties", List.of("genetic_entity_id"), "genetic_entity", List.of("id")));

        // genetic_profile
        fks.add(new ForeignKey("genetic_profile", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));

        // genetic_profile_link
        fks.add(new ForeignKey("genetic_profile_link", List.of("referring_genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("genetic_profile_link", List.of("referred_genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));

        // genetic_alteration
        fks.add(new ForeignKey("genetic_alteration", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("genetic_alteration", List.of("genetic_entity_id"), "genetic_entity", List.of("id")));

        // genetic_profile_samples
        fks.add(new ForeignKey("genetic_profile_samples", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));

        // gene_panel_list
        fks.add(new ForeignKey("gene_panel_list", List.of("internal_id"), "gene_panel", List.of("internal_id")));
        fks.add(new ForeignKey("gene_panel_list", List.of("gene_id"), "gene", List.of("entrez_gene_id")));

        // sample_profile
        fks.add(new ForeignKey("sample_profile", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("sample_profile", List.of("sample_id"), "sample", List.of("internal_id")));
        fks.add(new ForeignKey("sample_profile", List.of("panel_id"), "gene_panel", List.of("internal_id")));

        // structural_variant
        fks.add(new ForeignKey("structural_variant", List.of("sample_id"), "sample", List.of("internal_id")));
        fks.add(new ForeignKey("structural_variant", List.of("site1_entrez_gene_id"), "gene", List.of("entrez_gene_id")));
        fks.add(new ForeignKey("structural_variant", List.of("site2_entrez_gene_id"), "gene", List.of("entrez_gene_id")));
        fks.add(new ForeignKey("structural_variant", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));

        // alteration_driver_annotation
        fks.add(new ForeignKey("alteration_driver_annotation", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("alteration_driver_annotation", List.of("sample_id"), "sample", List.of("internal_id")));

        // mutation_event
        fks.add(new ForeignKey("mutation_event", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));

        // mutation
        fks.add(new ForeignKey("mutation", List.of("mutation_event_id"), "mutation_event", List.of("mutation_event_id")));
        fks.add(new ForeignKey("mutation", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));
        fks.add(new ForeignKey("mutation", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("mutation", List.of("sample_id"), "sample", List.of("internal_id")));

        // mutation_count_by_keyword
        fks.add(new ForeignKey("mutation_count_by_keyword", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("mutation_count_by_keyword", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));

        // clinical_patient / clinical_sample
        fks.add(new ForeignKey("clinical_patient", List.of("internal_id"), "patient", List.of("internal_id")));
        fks.add(new ForeignKey("clinical_sample", List.of("internal_id"), "sample", List.of("internal_id")));

        // clinical_attribute_meta
        fks.add(new ForeignKey("clinical_attribute_meta", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));

        // mut_sig
        fks.add(new ForeignKey("mut_sig", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));
        fks.add(new ForeignKey("mut_sig", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));

        // gistic / gistic_to_gene
        fks.add(new ForeignKey("gistic", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));
        fks.add(new ForeignKey("gistic_to_gene", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));
        fks.add(new ForeignKey("gistic_to_gene", List.of("gistic_roi_id"), "gistic", List.of("gistic_roi_id")));

        // cna_event / sample_cna_event
        fks.add(new ForeignKey("cna_event", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));
        fks.add(new ForeignKey("sample_cna_event", List.of("cna_event_id"), "cna_event", List.of("cna_event_id")));
        fks.add(new ForeignKey("sample_cna_event", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("sample_cna_event", List.of("sample_id"), "sample", List.of("internal_id")));

        // copy_number_seg / copy_number_seg_file
        fks.add(new ForeignKey("copy_number_seg", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));
        fks.add(new ForeignKey("copy_number_seg", List.of("sample_id"), "sample", List.of("internal_id")));
        fks.add(new ForeignKey("copy_number_seg_file", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));

        // clinical_event / clinical_event_data
        fks.add(new ForeignKey("clinical_event", List.of("patient_id"), "patient", List.of("internal_id")));
        fks.add(new ForeignKey("clinical_event_data", List.of("clinical_event_id"), "clinical_event", List.of("clinical_event_id")));

        // reference_genome_gene
        fks.add(new ForeignKey("reference_genome_gene", List.of("reference_genome_id"), "reference_genome", List.of("reference_genome_id")));
        fks.add(new ForeignKey("reference_genome_gene", List.of("entrez_gene_id"), "gene", List.of("entrez_gene_id")));

        // data_access_tokens
        fks.add(new ForeignKey("data_access_tokens", List.of("username"), "users", List.of("email")));

        // allele_specific_copy_number
        fks.add(new ForeignKey("allele_specific_copy_number", List.of("mutation_event_id"), "mutation_event", List.of("mutation_event_id")));
        fks.add(new ForeignKey("allele_specific_copy_number", List.of("genetic_profile_id"), "genetic_profile", List.of("genetic_profile_id")));
        fks.add(new ForeignKey("allele_specific_copy_number", List.of("sample_id"), "sample", List.of("internal_id")));

        // resource_definition
        fks.add(new ForeignKey("resource_definition", List.of("cancer_study_id"), "cancer_study", List.of("cancer_study_id")));

        // resource_sample / resource_patient / resource_study
        fks.add(new ForeignKey("resource_sample", List.of("internal_id"), "sample", List.of("internal_id")));
        fks.add(new ForeignKey("resource_patient", List.of("internal_id"), "patient", List.of("internal_id")));
        fks.add(new ForeignKey("resource_study", List.of("internal_id"), "cancer_study", List.of("cancer_study_id")));

        return List.copyOf(fks);
    }

    /**
     * Builds a ClickHouse SELECT that returns one row per orphaned FK value.
     */
    private static String generateFkViolationSelect(ForeignKey fk, String childAlias, String parentAlias) {
        String joinPredicate = buildJoinPredicate(fk, childAlias, parentAlias);
        String childHasAllValues = buildAllNotNullPredicate(fk.childCols, childAlias);
        String parentMissing = buildAllNullPredicate(fk.parentCols, parentAlias);
        String valuesExpr = buildRefereeValuesExpression(fk, childAlias);

        return ""
                + "SELECT\n"
                + "  '" + fk.childTable + "' AS " + REFEREE_TABLE + ",\n"
                + "  '" + fk.childColsCsv() + "' AS " + REFEREE_COLUMNS + ",\n"
                + "  " + valuesExpr + " AS " + REFEREE_VALUES + ",\n"
                + "  '" + fk.parentTable + "' AS " + REFERRED_TABLE + ",\n"
                + "  '" + fk.parentColsCsv() + "' AS " + REFERRED_COLUMNS + "\n"
                + "FROM " + fk.childTable + " " + childAlias + "\n"
                + "LEFT JOIN " + fk.parentTable + " " + parentAlias + "\n"
                + "  ON " + joinPredicate + "\n"
                + "WHERE " + childHasAllValues + "\n"
                + "  AND " + parentMissing;
    }

    /**
     * Generates one UNION ALL block per referee table.
     * Each block is runnable SQL returning the five requested columns.
     */
    private static Map<String, String> generateUnionBlocksByRefereeTable(List<ForeignKey> fks) {
        Map<String, List<ForeignKey>> byChild = fks.stream()
                .collect(Collectors.groupingBy(fk -> fk.childTable, LinkedHashMap::new, Collectors.toList()));

        Map<String, String> blocks = new LinkedHashMap<>();
        for (Map.Entry<String, List<ForeignKey>> e : byChild.entrySet()) {
            String child = e.getKey();
            List<ForeignKey> childFks = e.getValue();

            List<String> selects = new ArrayList<>();
            for (ForeignKey fk : childFks) {
                selects.add(generateFkViolationSelect(fk, "c", "p"));
            }
            String union = String.join("\nUNION ALL\n", selects);
            blocks.put(child, "-- ===============================\n"
                    + "-- referee table: " + child + "\n"
                    + "-- ===============================\n"
                    + union + ";\n");
        }
        return blocks;
    }

    /**
     * Generates one master UNION ALL query for all foreign keys.
     */
    private static String generateMasterUnionQuery(List<ForeignKey> fks) {
        if (fks.isEmpty()) {
            return "";
        }
        Map<String, String> blocks = generateUnionBlocksByRefereeTable(fks);
        // Strip trailing semicolons and join with UNION ALL between blocks.
        List<String> blockSelects = blocks.values().stream()
                .map(s -> s.replaceAll(";\\s*$", ""))
                .toList();
        return String.join("\nUNION ALL\n", blockSelects) + ";\n";
    }

// -------------------------
// Helpers
// -------------------------

    private static String buildJoinPredicate(ForeignKey fk, String childAlias, String parentAlias) {
        return zip(fk.childCols, fk.parentCols).stream()
                .map(pair -> childAlias + "." + quote(pair.left) + " = " + parentAlias + "." + quote(pair.right))
                .collect(Collectors.joining(" AND "));
    }

    private static String buildAllNotNullPredicate(List<String> columns, String tableAlias) {
        return columns.stream()
                .map(col -> "NOT isNull(" + tableAlias + "." + quote(col) + ")")
                .collect(Collectors.joining(" AND ", "(", ")"));
    }

    private static String buildAllNullPredicate(List<String> columns, String tableAlias) {
        return columns.stream()
                .map(col -> "isNull(" + tableAlias + "." + quote(col) + ")")
                .collect(Collectors.joining(" AND ", "(", ")"));
    }

    /**
     * Builds the referee_values expression, joining child values with '|' and rendering NULL as "NULL".
     */
    private static String buildRefereeValuesExpression(ForeignKey fk, String childAlias) {
        List<String> values = fk.childCols.stream()
                .map(col -> "ifNull(toString(" + childAlias + "." + quote(col) + "), 'NULL')")
                .toList();
        return buildDelimitedConcat(values, REFEREE_VALUE_SEPARATOR);
    }

    private static String buildDelimitedConcat(List<String> args, String separatorLiteral) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("Cannot build concatenation for empty argument list.");
        }
        if (args.size() == 1) {
            return args.get(0);
        }
        StringBuilder sb = new StringBuilder("concat(");
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                sb.append(", '").append(separatorLiteral).append("', ");
            }
            sb.append(args.get(i));
        }
        sb.append(")");
        return sb.toString();
    }

    private static String quote(String identifier) {
        // ClickHouse supports backticks for identifiers; safe for reserved words.
        return "`" + identifier.replace("`", "``") + "`";
    }

    private record Pair(String left, String right) {
    }

    private static List<Pair> zip(List<String> a, List<String> b) {
        if (a.size() != b.size()) {
            throw new IllegalArgumentException("Mismatched list sizes: " + a.size() + " vs " + b.size());
        }
        List<Pair> out = new ArrayList<>();
        for (int i = 0; i < a.size(); i++) {
            out.add(new Pair(a.get(i), b.get(i)));
        }
        return out;
    }
}
