package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple sequence manager that keeps counters in memory and initializes them
 * by querying the current MAX(column) from the underlying ClickHouse tables.
 */
public final class ClickHouseAutoIncrement {

    private ClickHouseAutoIncrement() {
    }

    private static final class SequenceConfig {
        private final String tableName;
        private final String columnName;

        private SequenceConfig(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }
    }

    private static final Map<String, SequenceConfig> CONFIG = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> COUNTERS = new ConcurrentHashMap<>();

    static {
        register("seq_reference_genome", "reference_genome", "REFERENCE_GENOME_ID");
        register("seq_cancer_study", "cancer_study", "CANCER_STUDY_ID");
        register("seq_patient", "patient", "INTERNAL_ID");
        register("seq_sample", "sample", "INTERNAL_ID");
        register("seq_sample_list", "sample_list", "LIST_ID");
        register("seq_genetic_entity", "genetic_entity", "ID");
        register("seq_geneset", "geneset", "ID");
        register("seq_geneset_hierarchy_node", "geneset_hierarchy_node", "NODE_ID");
        register("seq_generic_entity_properties", "generic_entity_properties", "ID");
        register("seq_genetic_profile", "genetic_profile", "GENETIC_PROFILE_ID");
        register("seq_gene_panel", "gene_panel", "INTERNAL_ID");
        register("seq_structural_variant", "structural_variant", "INTERNAL_ID");
        register("seq_mutation_event", "mutation_event", "MUTATION_EVENT_ID");
        register("seq_gistic", "gistic", "GISTIC_ROI_ID");
        register("seq_cna_event", "cna_event", "CNA_EVENT_ID");
        register("seq_copy_number_seg", "copy_number_seg", "SEG_ID");
        register("seq_copy_number_seg_file", "copy_number_seg_file", "SEG_FILE_ID");
        register("seq_clinical_event", "clinical_event", "CLINICAL_EVENT_ID");
    }

    private static void register(String sequenceName, String tableName, String columnName) {
        CONFIG.put(sequenceName, new SequenceConfig(tableName, columnName));
    }

    public static long nextId(String sequenceName) throws DaoException {
        SequenceConfig config = CONFIG.get(sequenceName);
        if (config == null) {
            throw new DaoException("Unknown sequence: " + sequenceName);
        }
        AtomicLong counter = COUNTERS.get(sequenceName);
        if (counter == null) {
            synchronized (ClickHouseAutoIncrement.class) {
                counter = COUNTERS.get(sequenceName);
                if (counter == null) {
                    long current = fetchCurrentMax(config);
                    counter = new AtomicLong(current);
                    COUNTERS.put(sequenceName, counter);
                }
            }
        }
        return counter.incrementAndGet();
    }

    private static long fetchCurrentMax(SequenceConfig config) throws DaoException {
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(ClickHouseAutoIncrement.class);
            stmt = con.prepareStatement("SELECT max(" + config.columnName + ") FROM " + config.tableName);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        } catch (SQLException ex) {
            throw new DaoException(ex);
        } finally {
            JdbcUtil.closeAll(ClickHouseAutoIncrement.class, con, stmt, rs);
        }
    }
}
