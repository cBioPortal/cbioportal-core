package org.mskcc.cbio.portal.scripts;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.mskcc.cbio.portal.dao.DaoEmbeddingDefinition;
import org.mskcc.cbio.portal.model.EmbeddingDefinition;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ImportEmbeddingDefinition extends ConsoleRunnable{

    public static final String DELIMITER = "\t";
    public static final String EMBEDDING_ID_COLUMN_NAME = "EMBEDDING_ID";
    public static final String SHORT_NAME_COLUMN_NAME = "SHORT_NAME";
    public static final String NAME_COLUMN_NAME = "NAME";
    public static final String ENTITY_TYPE_COLUMN_NAME = "ENTITY_TYPE";
    public static final String REDUCTION_TECHNIQUE_COLUMN_NAME = "REDUCTION_TECHNIQUE";
    public static final String DESCRIPTION_COLUMN_NAME = "DESCRIPTION";

    private File embeddingDefinitionFile;

    /**
     * Instantiates a ConsoleRunnable to run with the given command line args.
     *
     * @param args the command line arguments to be used
     * @see {@link #run()}
     */
    public ImportEmbeddingDefinition(String[] args) {
        super(args);
    }

    public void setEmbeddingDefinitionFile(File embeddingDefinitionFile) {
        this.embeddingDefinitionFile = embeddingDefinitionFile;
    }

    //TODO need to enoforce checks so fields place correctly in the database
    public void importData() throws Exception{

        try(BufferedReader buff = new BufferedReader(new FileReader(embeddingDefinitionFile))) {
            String line = buff.readLine();
            String[] headerNames = splitFields(line);
            Map<String, Integer> headerIndexMap = makeHeaderIndexMap(headerNames);

            int embeddingIdIndex = findAndValidateEmbeddingIdColumn(headerIndexMap);
            int shortNameIndex = findAndValidateShortNameColumn(headerIndexMap);
            int nameIndex = findAndValidateNameColumn(headerIndexMap);
            int entityTypeIndex = findAndValidateEntityTypeColumn(headerIndexMap);
            int reductionTechniqueIndex = findAndValidateReductionTechniqueColumn(headerIndexMap);
            int descriptionIndex = findAndValidateDescriptionColumn(headerIndexMap);
            ProgressMonitor.setCurrentMessage("Loading embedding definition(Metadata) into database...");
            while((line = buff.readLine())!=null){
                String[] fieldValues = getFieldValues(line, headerIndexMap);


                String embeddingId = fieldValues[embeddingIdIndex].trim();
                String shortName = fieldValues[shortNameIndex].trim();
                String name = fieldValues[nameIndex].trim();
                String entityType = fieldValues[entityTypeIndex].trim();
                String reductionTechnique = fieldValues[reductionTechniqueIndex].trim();
                String description = fieldValues[descriptionIndex].trim();

                // Definition exists skip that line
                if(DaoEmbeddingDefinition.checkDefinitionExists(embeddingId)){
                    ProgressMonitor.logWarning("Embedding definition already exists, skipping: " + embeddingId);
                    continue;
                }

                // create and add to the database
                DaoEmbeddingDefinition.addDatum(new EmbeddingDefinition(embeddingId,
                        shortName,name,description,entityType,reductionTechnique));
            }
        }
    }

    private int findAndValidateEmbeddingIdColumn(Map<String, Integer> headerIndexMap) {
        if (!headerIndexMap.containsKey(EMBEDDING_ID_COLUMN_NAME)) {
            throw new RuntimeException("Missing required column: " + EMBEDDING_ID_COLUMN_NAME);
        }
        return headerIndexMap.get(EMBEDDING_ID_COLUMN_NAME);
    }

    private int findAndValidateDescriptionColumn(Map<String, Integer> headerIndexMap) {
        if (!headerIndexMap.containsKey(DESCRIPTION_COLUMN_NAME)) {
            throw new RuntimeException("Missing required column: " + EMBEDDING_ID_COLUMN_NAME);
        }
        return headerIndexMap.get(DESCRIPTION_COLUMN_NAME);
    }

    private int findAndValidateShortNameColumn(Map<String, Integer> headerIndexMap) {
        if (!headerIndexMap.containsKey(SHORT_NAME_COLUMN_NAME)) {
            throw new RuntimeException("Missing required column: " + SHORT_NAME_COLUMN_NAME);
        }
        return headerIndexMap.get(SHORT_NAME_COLUMN_NAME);
    }

    private int findAndValidateNameColumn(Map<String, Integer> headerIndexMap) {
        if (!headerIndexMap.containsKey(NAME_COLUMN_NAME)) {
            throw new RuntimeException("Missing required column: " + NAME_COLUMN_NAME);
        }
        return headerIndexMap.get(NAME_COLUMN_NAME);
    }

    private int findAndValidateEntityTypeColumn(Map<String, Integer> headerIndexMap) {
        if (!headerIndexMap.containsKey(ENTITY_TYPE_COLUMN_NAME)) {
            throw new RuntimeException("Missing required column: " + ENTITY_TYPE_COLUMN_NAME);
        }
        return headerIndexMap.get(ENTITY_TYPE_COLUMN_NAME);
    }

    private int findAndValidateReductionTechniqueColumn(Map<String, Integer> headerIndexMap) {
        if (!headerIndexMap.containsKey(REDUCTION_TECHNIQUE_COLUMN_NAME)) {
            throw new RuntimeException("Missing required column: " + REDUCTION_TECHNIQUE_COLUMN_NAME);
        }
        return headerIndexMap.get(REDUCTION_TECHNIQUE_COLUMN_NAME);
    }

    private String[] splitFields(String line) throws IOException {
        return line.split(DELIMITER, -1);
    }

    private Map<String, Integer> makeHeaderIndexMap(String[] headerNames) {
        Map<String, Integer> headerIndexMap = new HashMap<String, Integer>();
        for (int i= 0; i < headerNames.length; i++) {
            headerIndexMap.put(headerNames[i], i);
        }
        return headerIndexMap;
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

    @Override
    public void run() {
        String progName = "importEmbeddingDefinition";
        String description = "Import Embedding definition file";

        try{
            OptionParser parser = new OptionParser();
            OptionSpec<String> data = parser.accepts("data", "profile data file").withRequiredArg()
                    .describedAs("data_file.txt").ofType(String.class);
            parser.accepts("noprogress", "this option can be given to avoid the messages regarding memory usage and % complete");

            OptionSet options = null;
            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                throw new UsageException(
                        progName, description, parser,
                        e.getMessage());
            }
            File embeddingDefinitionFile = null;
            if (options.has(data)) {
                embeddingDefinitionFile = new File(options.valueOf(data));
            } else {
                throw new UsageException(
                        progName, description, parser,
                        "'data' argument required.");
            }

            setEmbeddingDefinitionFile(embeddingDefinitionFile);
            importData();


        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args){
        ConsoleRunnable runner = new ImportEmbeddingDefinition(args);
        runner.runInConsole();
    }
}
