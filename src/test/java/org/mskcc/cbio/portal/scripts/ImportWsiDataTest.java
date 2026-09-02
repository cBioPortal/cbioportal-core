package org.mskcc.cbio.portal.scripts;

import static org.junit.Assert.assertArrayEquals;

import java.util.List;
import org.junit.Test;

public class ImportWsiDataTest {

    @Test
    public void patientTotalsIncludeUnmatchedSlides() {
        String[] blockMatched = placement("101", "BLOCK");
        String[] unmatched = placement("101", "UNMATCHED");
        String[] otherPatient = placement("202", "PART");

        var counts = ImportWsiData.countPatientSlidePlacements(
            List.of(blockMatched, unmatched, otherPatient));

        assertArrayEquals(new int[] {2, 0, 1}, counts.get(101L));
        assertArrayEquals(new int[] {1, 1, 0}, counts.get(202L));
    }

    private static String[] placement(String patientId, String matchLevel) {
        String[] placement = new String[9];
        placement[1] = patientId;
        placement[5] = "UNMATCHED".equals(matchLevel) ? null : "7";
        placement[6] = matchLevel;
        return placement;
    }
}
