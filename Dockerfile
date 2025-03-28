# Stage 1: Build the Java application JAR
FROM maven:3-eclipse-temurin-21 as jar_builder

# Set the working directory in the Maven image
WORKDIR /app

# Copy the Java source files and the pom.xml file into the image
COPY src ./src
COPY pom.xml .

# Build the application
RUN mvn clean package -DskipTests

# Stage 2: Prepare the final image
FROM maven:3-eclipse-temurin-21

# Download system dependencies first to take advantage of Docker caching
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        default-mysql-client \
        default-libmysqlclient-dev \
        python3 \
        python3-venv \
        python3-dev \
        unzip \
        perl \
    && rm -rf /var/lib/apt/lists/*

# Set up a Python virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python packages in the virtual environment
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Link python3 to python for compatibility
RUN ln -s /opt/venv/bin/python /usr/local/bin/python || true

# Copy the built JAR from the first stage
COPY --from=jar_builder /app/core-*.jar /

# Copy and set permissions for scripts
COPY scripts/ scripts/
RUN chmod -R a+x /scripts/

# Set the working directory in the container
WORKDIR /scripts/

# Environment variable
ENV PORTAL_HOME=/

# Create an empty application.properties file
RUN touch /application.properties

# Entry command
CMD ["python", "your_script.py"]
