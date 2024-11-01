/*
 * Copyright (c) 2015 - 2022 Memorial Sloan Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan Kettering Cancer
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

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.mskcc.cbio.maf.MafRecord;
import org.mskcc.cbio.maf.MafUtil;
import org.mskcc.cbio.portal.util.*;

import java.io.*;
import java.util.*;

/**
 * Read MAF records, filter records of interest and writes back to the file. The script backs up original file under {filename with extension}_backup.
 *
 * @author Ruslan Forostianov
 */
public class FilterMutationData extends ConsoleRunnable {

    /**
     * Instantiates a ConsoleRunnable to run with the given command line args.
     *
     * @param args the command line arguments to be used
     * @see {@link #run()}
     */
    public FilterMutationData(String[] args) {
        super(args);
    }

    public void run() {
        String description = "Filter MAF file for records of interest and rewrites it with selected mutations.";
        OptionParser parser = new OptionParser();
        OptionSpec<String> data = parser.accepts( "data",
                "MAF data file" ).withRequiredArg().describedAs( "data_mutations.txt" ).ofType( String.class );
        OptionSpec<String> meta = parser.accepts( "meta",
                "meta (description) file" ).withOptionalArg().describedAs( "meta_mutations.txt" ).ofType( String.class );

        OptionSet options = null;
        File originalMutationFile;
        Set<String> namespaces = null;
        Set<String> filteredMutations = null;

        try {
            options = parser.parse( args );
            originalMutationFile = new File((String) options.valueOf("data"));
            if (options.has("meta")) {
                File descriptorFile = new File((String) options.valueOf( "meta" ) );
                filteredMutations = GeneticProfileReader.getVariantClassificationFilter(descriptorFile);
                namespaces = GeneticProfileReader.getNamespaces(descriptorFile);
            }
        } catch (OptionException e) {
            throw new UsageException(
                    this.getClass().getName(), description, parser,
                    e.getMessage());
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
        ProgressMonitor.setCurrentMessage("Start filtering mutation records in the MAF file ...");
        File resultMutationFile = new File(originalMutationFile.getAbsolutePath() + "_filtered");
        final MutationFilter mutationFilter = new MutationFilter();
        try (
                BufferedReader originalFileBufferedReader = new BufferedReader(new FileReader(originalMutationFile));
                BufferedWriter resultFileBufferedWriter = new BufferedWriter(new FileWriter(resultMutationFile))
        ) {
            String line;
            MafUtil mafUtil = null;
            while ((line = originalFileBufferedReader.readLine()) != null) {
                ProgressMonitor.incrementCurValue();
                ConsoleUtil.showProgress();

                if (TsvUtil.isDataLine(line)) {
                    if (mafUtil == null) {
                        mafUtil = new MafUtil(line, namespaces);
                    } else {
                        MafRecord record = mafUtil.parseRecord(line);
                        if (!mutationFilter.acceptMutation(record, filteredMutations)) {
                            continue;
                        }
                    }
                }
                resultFileBufferedWriter.write(line);
                resultFileBufferedWriter.write(System.lineSeparator());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        File backupMutationFile = new File(originalMutationFile.getAbsolutePath() + "_backup");
        if (originalMutationFile.renameTo(backupMutationFile)) {
            ProgressMonitor.setCurrentMessage("The original file is backed up to:"
                    + backupMutationFile.getAbsolutePath());
            if (resultMutationFile.renameTo(originalMutationFile)) {
                ProgressMonitor.setCurrentMessage("The MAF file has been overwritten with filtered records.");
            } else {
                throw new RuntimeException("Failed to rename the filtered MAF file ("
                        + resultMutationFile.getAbsolutePath() + ") to the input MAF file ("
                        + originalMutationFile.getAbsolutePath() + ").");
            }
        } else {
            throw new RuntimeException("Failed to rename MAF file ("
                    + originalMutationFile.getAbsolutePath() + ") for backup.");
        }
        ProgressMonitor.setCurrentMessage(mutationFilter.getStatistics());
    }
}