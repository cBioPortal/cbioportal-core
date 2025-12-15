package org.mskcc.cbio.portal.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sequence manager that keeps counters in memory and persists the high-water mark
 * in ClickHouse so ids are never reused even if rows are deleted.
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
    private static final Map<String, AtomicLong> PERSISTED_COUNTERS = new ConcurrentHashMap<>();
    private static final String SEQUENCE_STATE_FILEPATH = "/data/cbioportal_sequence_state";

    static {
        register("seq_reference_genome", "reference_genome", "reference_genome_id");
        register("seq_cancer_study", "cancer_study", "cancer_study_id");
        register("seq_patient", "patient", "internal_id");
        register("seq_sample", "sample", "internal_id");
        register("seq_sample_list", "sample_list", "list_id");
        register("seq_genetic_entity", "genetic_entity", "id");
        register("seq_geneset", "geneset", "id");
        register("seq_geneset_hierarchy_node", "geneset_hierarchy_node", "node_id");
        register("seq_generic_entity_properties", "generic_entity_properties", "id");
        register("seq_genetic_profile", "genetic_profile", "genetic_profile_id");
        register("seq_gene_panel", "gene_panel", "internal_id");
        register("seq_structural_variant", "structural_variant", "internal_id");
        register("seq_mutation_event", "mutation_event", "mutation_event_id");
        register("seq_gistic", "gistic", "gistic_roi_id");
        register("seq_cna_event", "cna_event", "cna_event_id");
        register("seq_copy_number_seg", "copy_number_seg", "seg_id");
        register("seq_copy_number_seg_file", "copy_number_seg_file", "seg_file_id");
        register("seq_clinical_event", "clinical_event", "clinical_event_id");
        Thread persistingHook = new Thread(() -> ClickHouseAutoIncrement.persistSequences());
        Runtime.getRuntime().addShutdownHook(persistingHook);
    }

    private static void register(String sequenceName, String tableName, String columnName) {
        CONFIG.put(sequenceName, new SequenceConfig(tableName, columnName.toLowerCase(Locale.ROOT)));
    }

    public static long nextId(String sequenceName) throws DaoException {
        SequenceConfig config = CONFIG.get(sequenceName);
        if (config == null) {
            throw new DaoException("Unknown sequence: " + sequenceName);
        }
        initializeAllCountersIfNecessary();
        AtomicLong counter = COUNTERS.get(sequenceName);
        if (counter == null) {
            throw new DaoException("Unable to initialize sequence: " + sequenceName);
        }
        long next = counter.incrementAndGet();
        return next;
    }

    private static long initializeCounter(String sequenceName, SequenceConfig config) throws DaoException {
        long persisted = fetchPersistedMax(sequenceName);
        long tableMax = fetchCurrentMax(config);
        long current = Math.max(persisted, tableMax);
        return current;
    }

    public static void initializeAllCountersIfNecessary() throws DaoException {
        if (! COUNTERS.isEmpty()) {
            return;
        }
        synchronized (ClickHouseAutoIncrement.class) {
            for (Map.Entry<String, SequenceConfig> entry : CONFIG.entrySet()) {
                String sequenceName = entry.getKey();
                SequenceConfig config = entry.getValue();
                long current = initializeCounter(sequenceName, config);
                AtomicLong counter = new AtomicLong(current);
                COUNTERS.put(sequenceName, counter);
            }
        }
    }

    private static void readPersistedFileIfNecessary() throws DaoException{
        if (! PERSISTED_COUNTERS.isEmpty()) {
            return;
        }
        synchronized (ClickHouseAutoIncrement.class) {
            try {
                File f = new File(SEQUENCE_STATE_FILEPATH);
                if (!f.exists()) {
                    return;
                }
                FileInputStream fis = new FileInputStream(f);
                ObjectInputStream ois = new ObjectInputStream(fis);
                Map<String, AtomicLong> read_persisted_counters = (ConcurrentHashMap) ois.readObject();
                PERSISTED_COUNTERS.putAll(read_persisted_counters);
            } catch (Exception ex) {
                throw new DaoException(ex);
            }
        }
    }

    private static long fetchPersistedMax(String sequenceName) throws DaoException {
        readPersistedFileIfNecessary();
        AtomicLong persistedMax = PERSISTED_COUNTERS.get(sequenceName);
        if (persistedMax == null) {
            return 0L;
        }
        return persistedMax.get();
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

    public static void persistSequences() {
        if (COUNTERS.isEmpty()) {
            return;
        }
        try {
            FileOutputStream fos = new FileOutputStream(SEQUENCE_STATE_FILEPATH);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(COUNTERS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void reset() {
        COUNTERS.clear();
    }
}
