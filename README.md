# cbioportal-core
Many of the Java classes here are in use by the main [cbioportal/cbioportal codebase](https://github.com/cbioPortal/cbioportal), the `metaImport.py` Python scripts are all used for importing. We decided to move them all to a single repo so that we might deprecate them in the future once we have come up with a good plan for replacing them.

This repo contains:

- many old Java classes for interacting with the database
- The `metaImport.py` Python script used for importing

## Inclusion in main codebase
The `cbioportal-core` code is currently included in the final Docker image during the Docker build process: https://github.com/cBioPortal/cbioportal/blob/master/docker/web-and-data/Dockerfile#L48

## How to run integration tests

This section guides you through the process of running integration tests by setting up a cBioPortal MySQL database environment using Docker. Please follow these steps carefully to ensure your testing environment is configured correctly.

### Preparing the cbioportal test database

1. **Download the cBioPortal Database Schema**: To begin, you need to download the database schema for the version of cBioPortal you are interested in testing.
Locate the pom.xml file in your project directory and check the values of `<db.version>` and `<cbioportal.version>` to determine the correct version.
Replace `v6.0.3` in the command below with your desired cBioPortal version:
```
curl -o cgds.sql https://raw.githubusercontent.com/cBioPortal/cbioportal/v6.0.3/src/main/resources/db-scripts/cgds.sql
```

2. **Launch the MySQL Server Container**: Use Docker to start a MySQL server pre-loaded with the cBioPortal schema. Execute the following command from the root of your project directory.
It is recommended to open a separate terminal tab or window for this operation as it will occupy the console until stopped:

```
docker run -p 3306:3306 \
-v $(pwd)/src/test/resources/seed_mini.sql:/docker-entrypoint-initdb.d/seed.sql:ro \
-v $(pwd)/cgds.sql:/docker-entrypoint-initdb.d/cgds.sql:ro \
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
