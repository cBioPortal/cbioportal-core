package org.mskcc.cbio.portal.scripts;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoEmbeddingData;
import org.mskcc.cbio.portal.dao.DaoEmbeddingDefinition;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ImportEmbeddingData extends ConsoleRunnable{
    private static final Logger log = LoggerFactory.getLogger(ImportEmbeddingData.class);
    private CancerStudy cancerStudy;
    private File embeddingDataFile;
    public static final String DELIMITER = "\t";
    public static final String SAMPLE_ID_COLUMN_NAME = "SAMPLE_ID";
    public static final String PATIENT_ID_COLUMN_NAME = "PATIENT_ID";
    public static final String EMBEDDING_ID_COLUMN_NAME = "EMBEDDING_ID";
    public static final String CUSTOM_ATTRIBUTE_COLUMN_NAME = "CUSTOM_ATTRIBUTES";
    public static final String X__COLUMN_NAME = "X";
    public static final String Y_COLUMN_NAME = "Y";
    private static Properties properties;
    Map<String, Integer> seenEmbeddingIds = new HashMap<>();
    public static final String TABLE = "embedding_data";
    /**
     * Instantiates a ConsoleRunnable to run with the given command line args.
     *
     * @param args the command line arguments to be used
     * @see {@link #run()}
     */
    public ImportEmbeddingData(String[] args) {
        super(args);
    }

    public void setFile(CancerStudy cancerStudy, File embeddingDataFile)
    {
        this.cancerStudy = cancerStudy;
        this.embeddingDataFile = embeddingDataFile;
    }

    // need to pay attention to the order at which it is passed to the method  must be order
    //might delete test for adding embedding data  and the model
    public void importData()throws Exception{
        ClickHouseBulkLoader.bulkLoadOn();
        FileReader reader = new FileReader(embeddingDataFile);
        BufferedReader buff = new BufferedReader(reader);
        String line = buff.readLine();

        // Get the headers to know which index belongs to which value
        String[] headerNames = splitFields(line);
        Map<String, Integer> headerIndexMap = makeHeaderIndexMap(headerNames);
        // need to fix this to not be hardcoded and to be create a single point
        // of entry a helper method to handle extracting this values
        int embeddingIdIndex = headerIndexMap.get(EMBEDDING_ID_COLUMN_NAME);
        int sampleIndex = headerIndexMap.get(SAMPLE_ID_COLUMN_NAME);
        int patientIndex = headerIndexMap.get(PATIENT_ID_COLUMN_NAME);
        int xIndex = headerIndexMap.get(X__COLUMN_NAME);
        int yIndex = headerIndexMap.get(Y_COLUMN_NAME);
        int customIndex = headerIndexMap.get(CUSTOM_ATTRIBUTE_COLUMN_NAME);

        while((line = buff.readLine())!=null){
            String[] fieldValues = getFieldValues(line, headerIndexMap);
            String embeddingId = fieldValues[embeddingIdIndex];
            int cancerStudyId =  cancerStudy.getInternalId();
            String sampleId = fieldValues[sampleIndex];
            String patientId = fieldValues[patientIndex];
            String x = fieldValues[xIndex];
            String y = fieldValues[yIndex];
            String customAttribute = fieldValues[customIndex];
            // check if we have gotten the embedding id before  by checking
            // the hashMap else query for embedding definition for the internal id
            Integer internalId = seenEmbeddingIds.get(embeddingId);

            if (internalId == null) {
                internalId = DaoEmbeddingDefinition.getDefinitionId(embeddingId);

                if (internalId <= 0) {
                    throw new IllegalArgumentException(
                            "Embedding definition not found: " + embeddingId
                    );
                }
                seenEmbeddingIds.put(embeddingId, internalId);
            }
            DaoEmbeddingData.addDatum(TABLE,Integer.toString(internalId), patientId,
                    sampleId,x,y,customAttribute, cancerStudyId);

        }
        if (ClickHouseBulkLoader.isBulkLoad()) {
            ClickHouseBulkLoader.flushAll();
            ClickHouseBulkLoader.relaxedModeOff();
        }
    }

    private String[] splitFields(String line) throws IOException {
        return line.split(DELIMITER, -1);
    }

    private String[] getFieldValues(String line, Map<String, Integer> headerIndexMap) {
        // split on delimiter:
        String[] fieldValues = line.split(DELIMITER, -1);

        // validate: if number of fields is incorrect, give exception
        if (fieldValues.length != headerIndexMap.size()) {
            throw new IllegalArgumentException("Number of columns in line is not as expected. Expected: "
                    + headerIndexMap.size() + " columns, found: " + fieldValues.length + ", for line: " + line);
        }

        // now iterate over lines and trim each value:
        for (int i = 0; i < fieldValues.length; i++) {
            fieldValues[i] = fieldValues[i].trim();
        }
        return fieldValues;
    }

    private Map<String, Integer> makeHeaderIndexMap(String[] headerNames) {
        Map<String, Integer> headerIndexMap = new HashMap<String, Integer>();
        for (int i= 0; i < headerNames.length; i++) {
            headerIndexMap.put(headerNames[i], i);
        }
        return headerIndexMap;
    }


    @Override
    public void run() {
        try{
            String progName = "importEmbeddingData";
            String description = "Import Embedding data files.";
            // usage: --data <data_file.txt> --meta <meta_file.txt> --loadMode [directLoad|bulkLoad (default)] [--noprogress]

            OptionParser parser = new OptionParser();
            OptionSpec<String> data = parser.accepts( "data",
                    "embedding data file" ).withRequiredArg().describedAs( "data_file.txt" ).ofType( String.class );
            OptionSpec<String> meta = parser.accepts( "meta",
                    "meta (description) file" ).withOptionalArg().describedAs( "meta_file.txt" ).ofType( String.class );
            OptionSpec<String> study = parser.accepts("study",
                    "cancer study id").withOptionalArg().describedAs("study").ofType(String.class);
            // might not need this parsed into the arguement because it always does bulk insert
            parser.accepts( "loadMode", "direct (per record) or bulk load of data" )
                    .withOptionalArg().describedAs( "[directLoad|bulkLoad (default)]" ).ofType( String.class );
            parser.accepts("noprogress", "this option can be given to avoid the messages regarding memory usage and % complete");
            OptionSpec<String> overWriteExistingFlag = parser.accepts("overwrite-existing",
                    "Flag that enables re-uploading data for the patient/sample entries that already exist in the database").withOptionalArg().describedAs("overwrite-existing").ofType(String.class);
            OptionSet options = null;

            try {
                options = parser.parse( args );
            } catch (OptionException e) {
                throw new UsageException(
                        progName, description, parser,
                        e.getMessage());
            }

            File embeddingData_f = null;
            if( options.has( data ) ){
                embeddingData_f = new File( options.valueOf( data ) );
            } else {
                throw new UsageException(
                        progName, description, parser,
                        "'data' argument required.");
            }
            boolean relaxed = false;
            String cancerStudyStableId = null;
            if( options.has ( study ) )
            {
                cancerStudyStableId = options.valueOf(study);
            }
            if( options.has ( meta ) )
            {
                properties = new TrimmedProperties();
                properties.load(new FileInputStream(options.valueOf(meta)));
                cancerStudyStableId = properties.getProperty("cancer_study_identifier");
            }

            CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId(cancerStudyStableId);
            if (cancerStudy == null) {
                throw new IllegalArgumentException("Unknown cancer study: " + cancerStudyStableId);
            }

            setFile(cancerStudy, embeddingData_f);
            importData();

        }catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void runInConsole() {
        try {
            Date start = new Date();
            ProgressMonitor.setConsoleModeAndParseShowProgress(this.args);
            this.run();
            ConsoleUtil.showMessages();
            System.err.println("Done.");
            Date end = new Date();
            long totalTime = end.getTime() - start.getTime();
            System.err.println ("Total time:  " + totalTime + " ms\n");

        }
        catch (UsageException e) {
            e.printUsageLine();
        }
        catch (Throwable t) {
            ConsoleUtil.showWarnings();
            System.err.println ("\nABORTED!");
            t.printStackTrace();
        }
    }

    public static void main(String[] args){
        ConsoleRunnable runner = new ImportEmbeddingData(args);
        runner.run();
    }

}
