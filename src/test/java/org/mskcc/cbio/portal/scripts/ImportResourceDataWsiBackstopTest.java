package org.mskcc.cbio.portal.scripts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class ImportResourceDataWsiBackstopTest {

    private static final String URL =
        "https://portal.example.org/wsi/patient/P-1?studyId=s&slideKey=0123456789abcdef0123456789abcdef";
    private static final String NAME = "H&E \u00b7 Specimen 1 / Block 2";
    private static final String METADATA =
        "{\"slide_key\":\"0123456789abcdef0123456789abcdef\",\"part_description\":\"Specimen 1\","
            + "\"wsi_serving\":{\"image_id\":\"1\",\"source_url\":\"s3://bucket/1.svs\","
            + "\"tile_metadata_json\":{\"vendor\":[\"ok\"]}}}";

    @Test
    public void cleanWholeSlideImageRowIsAccepted() {
        ImportResourceData.validateWholeSlideImageRow("WSI_SAMPLE", "WHOLE_SLIDE_IMAGE", URL, NAME, METADATA, 2);
        ImportResourceData.validateWholeSlideImageRow("WSI_PATIENT", null, URL, null, null, 2);
    }

    @Test
    public void accessionInUrlIsRejectedWithoutEchoingIt() {
        assertRejected("Line 4: WHOLE_SLIDE_IMAGE URL contains a specimen accession number", "S21-12345",
            "WSI_SAMPLE", "WHOLE_SLIDE_IMAGE", URL + "&x=S21-12345", NAME, METADATA, 4);
    }

    @Test
    public void accessionInDisplayNameIsRejectedWithoutEchoingIt() {
        assertRejected("Line 5: WHOLE_SLIDE_IMAGE DISPLAY_NAME contains a specimen accession number", "MSK:S1",
            "WSI_PATIENT", "WHOLE_SLIDE_IMAGE", URL, "MSK:S1 H&E", METADATA, 5);
    }

    @Test
    public void accessionAnywhereInMetadataIsRejectedWithoutEchoingIt() {
        String[][] cases = {
            {"part_description", "\"Specimen 1\"", "\"S19-12345 A1\"", "METADATA.part_description", "S19-12345"},
            {"image_id", "\"1\"", "\"S19-12345\"", "METADATA.wsi_serving.image_id", "S19-12345"},
            {"source_url", "\"s3://bucket/1.svs\"", "\"s3://bucket/S19-12345.svs\"",
                "METADATA.wsi_serving.source_url", "S19-12345"},
            {"vendor", "[\"ok\"]", "[\"ok\",\"msk:s9\"]", "METADATA.wsi_serving.tile_metadata_json.vendor[1]",
                "msk:s9"},
        };
        for (String[] testCase : cases) {
            String metadata = METADATA.replace("\"" + testCase[0] + "\":" + testCase[1],
                "\"" + testCase[0] + "\":" + testCase[2]);
            assertRejected("Line 6: WHOLE_SLIDE_IMAGE " + testCase[3] + " contains a specimen accession number",
                testCase[4],
                "WSI_SAMPLE", "WHOLE_SLIDE_IMAGE", URL, NAME, metadata, 6);
        }
    }

    @Test
    public void accessionInMetadataKeyOrEscapedTextIsRejected() {
        assertRejected("Line 7: WHOLE_SLIDE_IMAGE METADATA.<key> contains a specimen accession number", "S19-12345",
            "WSI_SAMPLE", "WHOLE_SLIDE_IMAGE", URL, NAME, "{\"S19-12345\":1}", 7);
        // \u0053 is S: only the parsed value reveals the accession
        assertRejected("Line 7: WHOLE_SLIDE_IMAGE METADATA.stain_name contains a specimen accession number", "19-12345",
            "WSI_SAMPLE", "WHOLE_SLIDE_IMAGE", URL, NAME, "{\"stain_name\":\"\\u005319-12345\"}", 7);
        assertRejected("Line 7: WHOLE_SLIDE_IMAGE METADATA contains a specimen accession number", "S19-12345",
            "WSI_SAMPLE", "WHOLE_SLIDE_IMAGE", URL, NAME, "not json S19-12345", 7);
    }

    @Test
    public void wsiResourceIdIsCheckedEvenWithAnotherType() {
        assertRejected("Line 3: WHOLE_SLIDE_IMAGE URL contains a specimen accession number", "S21-12345",
            "WSI_PATIENT", "LINK", "https://x/S21-12345", null, null, 3);
    }

    @Test
    public void otherResourcesAreNotAffected() {
        ImportResourceData.validateWholeSlideImageRow("PATHOLOGY_REPORT", "PDF",
            "https://reports.example.org/S21-12345.pdf", "S21-12345", "{\"accession\":\"S21-12345\"}", 2);
    }

    private static void assertRejected(String expected, String value, String resourceId, String type,
                                       String url, String name, String metadata, int line) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> ImportResourceData.validateWholeSlideImageRow(resourceId, type, url, name, metadata, line));
        assertEquals(expected, error.getMessage());
        assertFalse(error.getMessage().toLowerCase().contains(value.toLowerCase()));
    }
}
