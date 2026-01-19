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

This section guides you through the process of running integration tests by setting up a cBioPortal MySQL database environment using Docker. Please follow these steps carefully to ensure your testing environment is configured correctly.

### Preparing the cbioportal test database

The integration-test phase now prepares the test database automatically. When you run `mvn integration-test`, Maven executes `scripts/start_test_db.sh`, which:

- downloads `cgds.sql` for the `cbioportal.version` in `pom.xml` into `target/test-db/`
- starts a MySQL 5.7 container pre-loaded with `cgds.sql` and `src/test/resources/seed_mini.sql`

Maven then runs `scripts/stop_test_db.sh` after the integration tests to clean up the container.

To use an existing MySQL instance instead, set `CBIOPORTAL_TEST_DB_SKIP=true` and ensure your `application.properties` points at the database.
If the build exits early and the test container is still running, stop it with:
```
scripts/stop_test_db.sh
```

Optional manual startup (matches the script, assuming `target/test-db/cgds.sql` exists; see `scripts/start_test_db.sh` for the download URL):

```
docker run -p 3306:3306 \
-v $(pwd)/src/test/resources/seed_mini.sql:/docker-entrypoint-initdb.d/seed.sql:ro \
-v $(pwd)/target/test-db/cgds.sql:/docker-entrypoint-initdb.d/cgds.sql:ro \
-e MYSQL_ROOT_PASSWORD=root \
-e MYSQL_USER=cbio_user \
-e MYSQL_PASSWORD=somepassword \
-e MYSQL_DATABASE=cgds_test \
mysql:5.7
```

### Run integration tests

With the database up and running, you are now ready to execute the integration tests.

Use Maven to run the integration tests. Ensure you are in the root directory of your project and run the following command:
```
mvn integration-test
```

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
