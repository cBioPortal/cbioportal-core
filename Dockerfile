FROM maven:3-eclipse-temurin-21 as jar_builder

# Set the working directory in the Maven image
WORKDIR /app

# Copy the java source files and the pom.xml file into the image
COPY src ./src
COPY pom.xml .

# Build the application
RUN mvn clean package -DskipTests

FROM maven:3-eclipse-temurin-21

# download system dependencies first to take advantage of docker caching
RUN apt-get update; apt-get install -y --no-install-recommends \
        build-essential \
        default-mysql-client \
        default-libmysqlclient-dev \
        python3 \
        python3-setuptools \
        python3-dev \
        python3-pip \
        unzip \
        perl \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install wheel

# Install any needed packages specified in requirements.txt
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN ln -s $(which python3) /usr/local/bin/python || true

COPY --from=jar_builder /app/core-*.jar /
COPY scripts/ scripts/
RUN chmod -R a+x /scripts/

# Set the working directory in the container
WORKDIR /scripts/

ENV PORTAL_HOME=/

# This file is empty. It has to be overriden by bind mounting the actual application.properties
RUN touch /application.properties
