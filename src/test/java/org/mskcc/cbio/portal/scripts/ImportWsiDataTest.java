package org.mskcc.cbio.portal.scripts;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

    @Test
    public void rejectsMalformedTileMetadataBeforeImport() throws Exception {
        Method validator = ImportWsiData.class.getDeclaredMethod(
            "requireValidTileMetadata", String.class, int.class);
        validator.setAccessible(true);

        try {
            validator.invoke(null,
                "{\"dimensions\":{\"width\":\"bad\",\"height\":256},"
                    + "\"levels\":1,\"level_dimensions\":[{\"width\":256,\"height\":256}],"
                    + "\"max_zoom\":0,\"tile_size\":256}", 7);
            fail("malformed tile metadata must be rejected");
        } catch (InvocationTargetException exception) {
            assertTrue(exception.getCause() instanceof IllegalArgumentException);
            assertTrue(exception.getCause().getMessage().contains("valid tile contract"));
        }
    }

    private static String[] placement(String patientId, String matchLevel) {
        String[] placement = new String[9];
        placement[1] = patientId;
        placement[5] = "UNMATCHED".equals(matchLevel) ? null : "7";
        placement[6] = matchLevel;
        return placement;
    }
}
