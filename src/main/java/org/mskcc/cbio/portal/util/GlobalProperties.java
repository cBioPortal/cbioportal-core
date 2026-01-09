/*
 * Copyright (c) 2015 - 2018 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * Utility class for getting / setting global properties.
 */
public class GlobalProperties {

    public static final String HOME_DIR = "PORTAL_HOME";
    private static final String PORTAL_PROPERTIES_FILE_NAME = "application.properties";
    private static final String MAVEN_PROPERTIES_FILE_NAME = "maven.properties";

    public static final String APP_VERSION = "app.version";
    public static final String DB_VERSION = "db.version";
    public static final String SPECIES = "species";
    public static final String DEFAULT_SPECIES = "human";
    public static final String NCBI_BUILD = "ncbi.build";
    public static final String DEFAULT_NCBI_BUILD = "37";
    public static final String UCSC_BUILD = "ucsc.build";
    public static final String DEFAULT_UCSC_BUILD = "hg19";

    private static Logger LOG = LoggerFactory.getLogger(GlobalProperties.class);
    private static ConfigPropertyResolver portalProperties = new ConfigPropertyResolver();
    private static Properties mavenProperties = initializeProperties(MAVEN_PROPERTIES_FILE_NAME);

    /**
     * Minimal portal property resolver that takes system property overrides.
     *
     * Provides properties from runtime or the baked-in
     * application.properties config file, but takes overrides from <code>-D</code>
     * system properties.
     */
    private static class ConfigPropertyResolver {
        private Properties configFileProperties;
        /**
         * Finds the config file for properties not overridden by system props.
         *
         * Either the runtime or buildtime application.properties file.
         */
        public ConfigPropertyResolver() {
            configFileProperties = initializeProperties(PORTAL_PROPERTIES_FILE_NAME);
        }
        /**
         * Finds the property with the specified key, or returns the default.
         */
        public String getProperty(String key, String defaultValue) {
            String propertyValue = configFileProperties.getProperty(key, defaultValue);
            return System.getProperty(key, propertyValue);
        }
        /**
         * Finds the property with the specified key, or returns null.
         */
        public String getProperty(String key) {
            return getProperty(key, null);
        }
        /**
        * Tests if a property has been specified for this key.
        *
        * @return true iff the property was specified, even if blank.
        */
        public boolean containsKey(String key) {
            return getProperty(key) != null;
        }
    }

    private static Properties initializeProperties(String propertiesFileName)
    {
        return loadProperties(getResourceStream(propertiesFileName));
    }

    private static InputStream getResourceStream(String propertiesFileName)
    {
        String resourceFilename = null;
        InputStream resourceFIS = null;

        try {
            String home = System.getenv(HOME_DIR);
            if (home != null) {
                 resourceFilename =
                    home + File.separator + propertiesFileName;
                if (LOG.isInfoEnabled()) {
                    LOG.info("Attempting to read properties file: " + resourceFilename);
                }
                resourceFIS = new FileInputStream(resourceFilename);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Successfully read properties file");
                }
            }
        }
        catch (FileNotFoundException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Failed to read properties file: " + resourceFilename);
            }
        }

        if (resourceFIS == null) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Attempting to read properties file from classpath");
            }
            resourceFIS = GlobalProperties.class.getClassLoader().
                getResourceAsStream(propertiesFileName);
            if (LOG.isInfoEnabled()) {
                LOG.info("Successfully read properties file");
            }
        }
         
        return resourceFIS;
    }

    private static Properties loadProperties(InputStream resourceInputStream)
    {
        Properties properties = new Properties();

        try {
            properties.load(resourceInputStream);
            resourceInputStream.close();
        }
        catch (IOException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Error loading properties file: " + e.getMessage());
            }
        }

        return properties;
    }

	public static String getProperty(String property)
	{
		return (portalProperties.containsKey(property)) ? portalProperties.getProperty(property) : null;
	}

    public static String getAppVersion()
    {
        String appVersion = mavenProperties.getProperty(APP_VERSION);
        return (appVersion == null) ? "1.0" : appVersion;
    }
    
    public static String getSpecies(){
    	String species = portalProperties.getProperty(SPECIES);
    	return species == null ? DEFAULT_SPECIES : species;
    	}

    public static String getDbVersion() {
        String version = mavenProperties.getProperty(DB_VERSION);
        if (version == null)
        {
            return "0";
        }
        return version;
    }

    public static String getReferenceGenomeName() {
        return portalProperties.getProperty(UCSC_BUILD, DEFAULT_UCSC_BUILD);
    }
    
    public static void main(String[] args)
    {
        System.out.println(getAppVersion());    
    }


}
