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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.mskcc.cbio.portal.dao.ClickHouseAutoIncrement;
import org.mskcc.cbio.portal.dao.ClickHouseBulkDeleter;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.mskcc.cbio.portal.util.VersionUtil;

/**
 * Runs a list of import steps, written by cbioportalImporter.py, in one JVM.
 *
 * Each step is the class name and arguments that would otherwise be passed to
 * its own {@code java} process. Steps run in order, and the runner stops at the
 * first step that fails and exits with that step's exit status. Between steps
 * the static state that a fresh JVM would start without is reset; the gene
 * cache is kept, since avoiding its reload is the point of running in one JVM.
 *
 * Manifest format:
 * <pre>
 * {"version": 1, "steps": [{"class": "...", "args": ["...", ...]}, ...]}
 * </pre>
 * If {@code --result} is given, a JSON file is written with the exit status and
 * the 0-based index of the failed step ({@code null} if none failed).
 */
public class ImportStepRunner {

    static final int EX_OK = 0;
    static final int EX_USAGE = 64;
    static final int EX_SOFTWARE = 70;

    static final int MANIFEST_VERSION = 1;

    static final String VERSION_UTIL_CLASS = VersionUtil.class.getName();

    /**
     * Classes cbioportalImporter.py may run as a step. Each one other than
     * VersionUtil is a ConsoleRunnable with a (String[] args) constructor.
     */
    static final Set<String> STEP_CLASSES = Set.of(
        VERSION_UTIL_CLASS,
        ImportTypesOfCancers.class.getName(),
        RemoveCancerStudy.class.getName(),
        ImportCancerStudy.class.getName(),
        UpdateCancerStudy.class.getName(),
        RemoveSamples.class.getName(),
        RemovePatients.class.getName(),
        ImportSampleList.class.getName(),
        AddCaseList.class.getName(),
        UpdateCaseListsSampleIds.class.getName(),
        ImportClinicalData.class.getName(),
        ImportProfileData.class.getName(),
        ImportCopyNumberSegmentData.class.getName(),
        ImportGisticData.class.getName(),
        ImportMutSigData.class.getName(),
        ImportTimelineData.class.getName(),
        ImportGenePanelProfileMap.class.getName(),
        ImportResourceData.class.getName(),
        ImportResourceDefinition.class.getName()
    );

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Step(@JsonProperty("class") String className, List<String> args) {
        String[] argArray() {
            return args.toArray(new String[0]);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Manifest(int version, List<Step> steps) {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    /**
     * Runs the steps in the manifest named on the command line.
     *
     * @return  the exit status: 0, the status of the failed step, or 64 if the
     *          command line or manifest is invalid
     */
    public static int run(String[] argv) {
        OptionParser parser = new OptionParser();
        OptionSpec<String> manifestOption = parser.accepts("manifest",
            "JSON file listing the import steps to run").withRequiredArg().required();
        OptionSpec<String> resultOption = parser.accepts("result",
            "JSON file to write the exit status and failed step to").withRequiredArg();
        OptionSet options;
        try {
            options = parser.parse(argv);
        } catch (OptionException e) {
            new UsageException(ImportStepRunner.class.getSimpleName(),
                "Run import steps in one JVM", parser, e.getMessage()).printUsageLine();
            return EX_USAGE;
        }
        File resultFile = options.has(resultOption) ? new File(options.valueOf(resultOption)) : null;

        List<Step> steps;
        try {
            steps = readManifest(new File(options.valueOf(manifestOption)));
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Invalid import step manifest: " + e.getMessage());
            writeResult(resultFile, EX_USAGE, null);
            return EX_USAGE;
        }
        return runSteps(steps, resultFile);
    }

    static List<Step> readManifest(File manifestFile) throws IOException {
        Manifest manifest = new ObjectMapper().readValue(manifestFile, Manifest.class);
        if (manifest.version() != MANIFEST_VERSION) {
            throw new IllegalArgumentException("unsupported manifest version " + manifest.version());
        }
        if (manifest.steps() == null) {
            throw new IllegalArgumentException("manifest has no 'steps' list");
        }
        for (int i = 0; i < manifest.steps().size(); i++) {
            Step step = manifest.steps().get(i);
            if (step == null || step.className() == null || step.args() == null) {
                throw new IllegalArgumentException("step " + (i + 1) + " needs a 'class' and an 'args' list");
            }
            if (!STEP_CLASSES.contains(step.className())) {
                throw new IllegalArgumentException(
                    "step " + (i + 1) + " class cannot be run as an import step: " + step.className());
            }
        }
        return manifest.steps();
    }

    static int runSteps(List<Step> steps, File resultFile) {
        for (int i = 0; i < steps.size(); i++) {
            Step step = steps.get(i);
            System.out.println("> " + step.className() + " " + String.join(" ", step.args()));
            System.out.flush();
            if (i > 0) {
                // a fresh JVM loads these caches when DaoCancerStudy is first used
                DaoCancerStudy.reCacheAll();
            }
            int status = EX_SOFTWARE;
            try {
                status = runStep(step);
            } finally {
                resetAfterStep(status);
            }
            if (status != EX_OK) {
                writeResult(resultFile, status, i);
                return status;
            }
        }
        writeResult(resultFile, EX_OK, null);
        return EX_OK;
    }

    private static int runStep(Step step) {
        if (VERSION_UTIL_CLASS.equals(step.className())) {
            try {
                return VersionUtil.check();
            } catch (Throwable t) {
                t.printStackTrace();
                return 1;
            }
        }
        ConsoleRunnable runnable;
        try {
            runnable = Class.forName(step.className())
                .asSubclass(ConsoleRunnable.class)
                .getConstructor(String[].class)
                .newInstance((Object) step.argArray());
        } catch (ReflectiveOperationException | ClassCastException e) {
            System.err.println("\nABORTED!");
            e.printStackTrace();
            return EX_SOFTWARE;
        }
        return runnable.runInProcess();
    }

    /**
     * Resets the static state a step can leave behind, so the next step starts
     * as it would in a fresh JVM.
     */
    static void resetAfterStep(int status) {
        int discardedRecords = ClickHouseBulkLoader.discardAll();
        if (status == EX_OK && discardedRecords > 0) {
            System.err.println("WARNING: discarded " + discardedRecords
                + " bulk load records that the step did not flush");
        }
        ClickHouseBulkDeleter.discardAll();
        // in a separate JVM the shutdown hook persists these on exit
        ClickHouseAutoIncrement.persistAndResetCounters();
        ProgressMonitor.reset();
    }

    private static void writeResult(File resultFile, int status, Integer failedStep) {
        if (resultFile == null) {
            return;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", status);
        result.put("failed_step", failedStep);
        try {
            new ObjectMapper().writeValue(resultFile, result);
        } catch (IOException e) {
            System.err.println("Could not write import step result file " + resultFile + ": " + e.getMessage());
        }
    }
}
