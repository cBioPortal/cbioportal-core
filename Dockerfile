# Stage 1: Build the application
FROM maven:3.9.6-amazoncorretto-21 as builder

# Set the working directory in the Maven image
WORKDIR /app

# Copy the java source files and the pom.xml file into the image
COPY src ./src
COPY pom.xml .

# Build the application
RUN mvn clean package

FROM python:3.11

# Install any needed packages specified in requirements.txt
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Perl. Some scripts are written in perl
RUN apt-get update && apt-get install -y perl

COPY scripts/ scripts/
RUN chmod -R a+x /scripts/

# Set the working directory in the container
WORKDIR /scripts/

ENV PORTAL_HOME=/

COPY --from=builder /app/core-*.jar /
# Set environment variables for Java version and installation paths
ENV JAVA_VERSION 21
ENV JAVA_HOME /usr/java/openjre-$JAVA_VERSION

# Install necessary packages for adding repositories over HTTPS
RUN apt-get update && \
    apt-get install -y --no-install-recommends apt-transport-https ca-certificates wget dirmngr gnupg software-properties-common && \
    rm -rf /var/lib/apt/lists/*

# Download and install OpenJRE 21
# Note: The exact URL might change based on the latest available version, so replace it with the correct URL for JRE 21
RUN mkdir -p "$JAVA_HOME" && \
    wget -O jdk21.tar.gz "https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.tar.gz" && \
    tar xvf jdk21.tar.gz --strip-components=1 -C "$JAVA_HOME" && \
    rm jdk21.tar.gz

# Add java to PATH
ENV PATH $JAVA_HOME/bin:$PATH

# This file is empty. It has to be overriden by bind mounting the actual application.properties
RUN touch /application.properties
