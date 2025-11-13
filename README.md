# cbioportal-core
Many of the Java classes here are in use by the main [cbioportal/cbioportal codebase](https://github.com/cbioPortal/cbioportal), the `metaImport.py` Python scripts are all used for importing. We decided to move them all to a single repo so that we might deprecate them in the future once we have come up with a good plan for replacing them.

This repo contains:

- many old Java classes for interacting with the database
- The `metaImport.py` Python script used for importing

## Inclusion in main codebase
The `cbioportal-core` code is currently included in the final Docker image during the Docker build process: https://github.com/cBioPortal/cbioportal/blob/master/docker/web-and-data/Dockerfile#L48

## Running in docker

Build docker image with:
```bash
docker build -t cbioportal-core .
```

### Example of how to load `study_es_0` study

Import gene panels

```bash
docker run -it -v $(pwd)/tests/test_data/:/data/ -v $(pwd)/application.properties:/application.properties cbioportal-core \
perl importGenePanel.pl --data /data/study_es_0/data_gene_panel_testpanel1.txt
docker run -it -v $(pwd)/tests/test_data/:/data/ -v $(pwd)/application.properties:/application.properties cbioportal-core \
perl importGenePanel.pl --data /data/study_es_0/data_gene_panel_testpanel2.txt
```

Import gene sets and supplementary data

```bash
docker run -it -v $(pwd)/src/test/resources/:/data/ -v $(pwd)/application.properties:/application.properties cbioportal-core \
perl importGenesetData.pl --data /data/genesets/study_es_0_genesets.gmt --new-version msigdb_7.5.1 --supp /data/genesets/study_es_0_supp-genesets.txt
```

Import gene set hierarchy data

```bash
docker run -it -v $(pwd)/src/test/resources/:/data/ -v $(pwd)/application.properties:/application.properties cbioportal-core \
perl importGenesetHierarchy.pl --data /data/genesets/study_es_0_tree.yaml
```

Import study

```bash
docker run -it -v $(pwd)/tests/test_data/:/data/ -v $(pwd)/application.properties:/application.properties cbioportal-core \
python importer/metaImport.py -s /data/study_es_0 -p /data/api_json_system_tests -o
```

### Incremental upload of data

To add or update specific patient, sample, or molecular data in an already loaded study, you can perform an incremental upload. This process is quicker than reloading the entire study.

To execute an incremental upload, use the -d (or --data_directory) option instead of -s (or --study_directory). Here is an example command:
```bash
docker run -it -v $(pwd)/data/:/data/ -v $(pwd)/application.properties:/application.properties cbioportal-core python importer/metaImport.py -d /data/study_es_0_inc -p /data/api_json -o
```
**Note:**
While the directory should adhere to the standard cBioPortal file formats and study structure, incremental uploads are not supported for all data types though.
For instance, uploading study metadata, resources, or GSVA data incrementally is currently unsupported.

This method ensures efficient updates without the need for complete study reuploads, saving time and computational resources.

## How to run integration tests

This section guides you through the process of running integration tests by setting up a cBioPortal ClickHouse database environment using Docker. Please follow these steps carefully to ensure your testing environment is configured correctly.

### Preparing the cbioportal test database

1. **Prepare a ClickHouse Schema**: The loader now targets ClickHouse directly, so you need a ClickHouse-compatible schema that mirrors your portal version. Convert the upstream schema or export one from an existing installation and save it as `cgds.clickhouse.sql`.

2. **Launch the ClickHouse Server Container**: Use Docker to start a ClickHouse server and load the bundled schema. Run this command from the project root:

```
docker run -d --name cbio-clickhouse \
  -p 8123:8123 -p 9000:9000 \
  -v $(pwd)/db/clickhouse/cgds.clickhouse.sql:/docker-entrypoint-initdb.d/init.sql:ro \
  clickhouse/clickhouse-server:24.3
```

3. **Seed Reference Data (optional)**: If you need seed data such as the mini dataset used in integration tests, load it with the ClickHouse client and the bundled ClickHouse seed:

```
docker exec -i cbio-clickhouse clickhouse-client --multiquery < db/clickhouse/seed_mini.clickhouse.sql
```

### Run integration tests

With the database up and running, you are now ready to execute the integration tests.

Use Maven to run the integration tests. Ensure you are in the root directory of your project and run the following command:
```
mvn integration-test
```

Testcontainers automatically launches and seeds a ClickHouse container during the test phase, so ensure Docker is available before running the suite.

### Maintaining the ClickHouse schema and seed

The repository includes ClickHouse-native versions of the cgds schema and the `seed_mini` dataset under `db/clickhouse`. When the upstream MySQL assets change, regenerate the ClickHouse SQL by extracting the latest MySQL schema (either from the published cBioPortal sources or from the loader jar) and running:

```bash
jar xf core-*.jar db-scripts/cgds.sql

python scripts/tools/generate_clickhouse_sql.py \
  --mysql-schema db-scripts/cgds.sql \
  --mysql-seed src/test/resources/seed_mini.sql \
  --output-schema db/clickhouse/cgds.clickhouse.sql \
  --output-seed db/clickhouse/seed_mini.clickhouse.sql
```

The helper performs deterministic type conversions and syntax adjustments, ensuring the ClickHouse schema stays aligned with the canonical MySQL version.

## Development

### Prerequisites
To contribute to `cbioportal-core`, ensure you have the following tools installed:

- Python 3: Required for study validation and orchestration scripts. These scripts utilize the underlying loader jar.
- Perl: Specify the version required based on script compatibility. Necessary for data loading scripts interfacing with lookup tables.
- JDK 21: Essential for developing the data loader component.
- Maven 3.8.3: Used to compile and test the loader jar. Review this [issue](https://github.com/cBioPortal/cbioportal-core/issues/15) before starting.

### Setup

1. Create a Python virtual environment (first-time setup):
```bash
python -m venv .venv
```

2. Activate the virtual environment:
```bash
source .venv/bin/activate
```

3. Install required Python dependencies (first-time setup or when dependencies have changed):
```bash
pip install -r requirements.txt
```

### Building and Testing

After you are done with the setup, you can build and test the project.

1. Execute tests through the provided script:
```bash
./test_scripts.sh
```

2. Build the loader jar using Maven (includes testing):
```bash
mvn clean package
```
*Note:* The Maven configuration is set to place the jar in the project's root directory to ensure consistent paths in both development and production.

### Configuring Application Properties

The loader requires specific properties set to establish a connection to the database. These properties should be defined in the application.properties file within your project.

#### Creating the Properties File

1. Begin by creating your application.properties file. This can be done by copying from an example or template provided in the project:
```bash
cp application.properties.example application.properties
```

2. Open application.properties in your preferred text editor and modify the properties to match your database configuration and other environment-specific settings.

#### Setting the PORTAL_HOME Environment Variable

The PORTAL_HOME environment variable should be set to the directory containing your application.properties file, typically the root of your project:
```
export PORTAL_HOME=$(pwd)
```
Ensure this command is run in the root directory of your project, where the application.properties file is located. This setup is crucial for the loader to correctly access the required properties.

#### maven.properties
TODO: Document role of `maven.properties` file.

### Script Execution with Loader Jar

To run scripts that require the loader jar, ensure the jar file is in the project root.
The script will search for `core-*.jar` in the root of the project:
```bash
python scripts/importer/metaImport.py -s tests/test_data/study_es_0 -p tests/test_data/api_json_unit_tests -o
```
