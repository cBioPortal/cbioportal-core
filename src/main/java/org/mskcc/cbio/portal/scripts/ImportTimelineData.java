/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
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

package org.mskcc.cbio.portal.scripts;

import joptsimple.OptionSet;
import org.mskcc.cbio.portal.dao.DaoClinicalEvent;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoPatient;
import org.mskcc.cbio.portal.dao.SQLiteBulkLoader;
import org.mskcc.cbio.portal.model.ClinicalEvent;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Imports timeline data for display in patient view
 * 
 * @author jgao, inodb
 */
public class ImportTimelineData extends ConsoleRunnable {

	private static void importData(String dataFile, int cancerStudyId, boolean overwriteExisting) throws IOException, DaoException {
		SQLiteBulkLoader.bulkLoadOn();

		ProgressMonitor.setCurrentMessage("Reading file " + dataFile);
		FileReader reader = new FileReader(dataFile);
		BufferedReader buff = new BufferedReader(reader);
		try {
			String line = buff.readLine();
	
			// Check event category agnostic headers
			String[] headers = line.split("\t");
			int indexCategorySpecificField = -1;
			if (headers[0].equals("PATIENT_ID") && headers[1].equals("START_DATE")) {
				if ("STOP_DATE".equals(headers[2]) && "EVENT_TYPE".equals(headers[3])) {
					indexCategorySpecificField = 4;
				} else if (headers[2].equals("EVENT_TYPE")) {
					indexCategorySpecificField = 3;
				}
			}
			if (indexCategorySpecificField == -1) {
				throw new RuntimeException("The first line must start with\n'PATIENT_ID\tSTART_DATE\tEVENT_TYPE'\nor\n"
					+ "PATIENT_ID\tSTART_DATE\tSTOP_DATE\tEVENT_TYPE");
			}

			long clinicalEventId = DaoClinicalEvent.getLargestClinicalEventId();
			Set<Integer> processedPatientIds = new HashSet<>();

			while ((line = buff.readLine()) != null) {
				line = line.trim();
	
				String[] fields = line.split("\t");
				if (fields.length > headers.length) {
					//TODO - should better throw an exception here...
					ProgressMonitor.logWarning("more attributes than header: " + line + ". Skipping entry.");
					continue;
				}
				String patientId = fields[0];
				Patient patient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudyId, patientId);
				if (patient == null) {
					ProgressMonitor.logWarning("Patient " + patientId + " not found in study " + cancerStudyId + ". Skipping entry.");
					continue;
				}
				if (overwriteExisting && processedPatientIds.add(patient.getInternalId())) {
					DaoClinicalEvent.deleteByPatientId(patient.getInternalId());
				}
				ClinicalEvent event = new ClinicalEvent();
				event.setClinicalEventId(++clinicalEventId);
				event.setPatientId(patient.getInternalId());
				event.setStartDate(Long.valueOf(fields[1]));
				if (indexCategorySpecificField != 3 && !fields[2].isEmpty()) {
					event.setStopDate(Long.valueOf(fields[2]));
				}
				event.setEventType(fields[indexCategorySpecificField - 1]);
				Map<String, String> eventData = new HashMap<String, String>();
				for (int i = indexCategorySpecificField; i < fields.length; i++) {
					if (!fields[i].isEmpty()) {
						eventData.put(headers[i], fields[i]);
					}
				}
				event.setEventData(eventData);
	
				DaoClinicalEvent.addClinicalEvent(event);
			}
	
			SQLiteBulkLoader.flushAll();
		}
		finally {
			buff.close();
		}
	}

    public void run() {
        try {
		    String description = "Import 'timeline' data";

		    OptionSet options = ConsoleUtil.parseStandardDataAndMetaOptions(args, description, true);
			if (options.has("loadMode") && !"bulkLoad".equalsIgnoreCase((String) options.valueOf("loadMode"))) {
				throw new UnsupportedOperationException("This loader supports bulkLoad load mode only, but "
						+ options.valueOf("loadMode")
						+ " has been supplied.");
			}
			String dataFile = (String) options.valueOf("data");
		    File descriptorFile = new File((String) options.valueOf("meta"));
			boolean overwriteExisting = options.has("overwrite-existing");
            
			Properties properties = new TrimmedProperties();
			properties.load(new FileInputStream(descriptorFile));
            
			int cancerStudyInternalId = ValidationUtils.getInternalStudyId(properties.getProperty("cancer_study_identifier"));
            
			importData(dataFile, cancerStudyInternalId, overwriteExisting);
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException|DaoException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Makes an instance to run with the given command line arguments.
     *
     * @param args  the command line arguments to be used
     */
    public ImportTimelineData(String[] args) {
        super(args);
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args  the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new ImportTimelineData(args);
        runner.runInConsole();
    }
}
