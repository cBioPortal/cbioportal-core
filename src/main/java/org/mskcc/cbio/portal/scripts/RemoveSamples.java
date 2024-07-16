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
import org.jetbrains.annotations.NotNull;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Command Line Tool to Remove Samples in Cancer Studies
 */
public class RemoveSamples extends ConsoleRunnable {

    public static final String COMMA = ",";
    private Set<String> studyIds;
    private Set<String> sampleIds;

    public void run() {
        JdbcUtil.getTransactionTemplate().execute(status -> {
            try {
                doRun();
            } catch (Throwable e) {
                status.setRollbackOnly();
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private void doRun() {
        ProgressMonitor.setCurrentMessage("Start removing sample(s) from study(ies).");
        parseArgs();
        ProgressMonitor.logDebug("Reading study id(s) from the database.");
        final Set<CancerStudy> cancerStudies = studyIds.stream().map(studyId -> {
            try {
                CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId(studyId);
                if (cancerStudy == null) {
                    throw new NoSuchElementException("Cancer study with stable id=" + studyId + " not found.");
                }
                return cancerStudy;
            } catch (DaoException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toUnmodifiableSet());
        try {
            for (CancerStudy cancerStudy : cancerStudies) {
                ProgressMonitor.setCurrentMessage("Removing sample with stable id(s) ("
                        + String.join(", ", sampleIds)
                        + ") from study with stable id=" + cancerStudy.getCancerStudyStableId() + " ...");
                DaoSample.deleteSamples(cancerStudy.getInternalId(), sampleIds);
            }
        } catch (DaoException e) {
            throw new RuntimeException(e);
        }
        ProgressMonitor.setCurrentMessage("Done removing sample(s) from study(ies).");
    }

    private void parseArgs() {
        OptionParser parser = new OptionParser();
        OptionSpec<String> studyIdsOpt = parser.accepts("study_ids", "Cancer Study ID(s; comma separated) to remove samples for.")
                .withRequiredArg()
                .describedAs("comma separated study ids")
                .ofType(String.class);
        OptionSpec<String> sampleIdsOpt = parser.accepts("sample_ids", "Samples Stable ID(s; comma separated) to remove.")
                .withRequiredArg()
                .describedAs("comma separated sample ids")
                .ofType(String.class);
        OptionSpec<Void> help = parser.accepts("help", "print this help info");
        String progName = this.getClass().getSimpleName();
        String description = "Removes clinical sample(s) information by their stable id(s) and cancer study id(s).";

        OptionSet options;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            throw new UsageException(progName, description, parser,
                    e.getMessage());
        }

        if (options.has(help)) {
            throw new UsageException(progName, description, parser);
        }
        if (!options.has(studyIdsOpt) || options.valueOf(studyIdsOpt) == null || "".equals(options.valueOf(studyIdsOpt).trim())) {
            throw new UsageException(progName, description, parser, "'--study_ids' argument has to specify study id(s).");
        }
        if (!options.has(sampleIdsOpt) || options.valueOf(sampleIdsOpt) == null || "".equals(options.valueOf(sampleIdsOpt).trim())) {
            throw new UsageException(progName, description, parser, "'--sample_ids' argument has to specify sample id(s).");
        }
        this.studyIds = parseCsvAsSet(options.valueOf(studyIdsOpt));
        this.sampleIds = parseCsvAsSet(options.valueOf(sampleIdsOpt));
    }

    @NotNull
    private Set<String> parseCsvAsSet(String s) {
        return Arrays.stream(s.trim().split(COMMA)).filter(val -> !"".equals(val)).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Makes an instance to run with the given command line arguments.
     *
     * @param args the command line arguments to be used
     */
    public RemoveSamples(String[] args) {
        super(args);
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new RemoveSamples(args);
        runner.runInConsole();
    }
}
