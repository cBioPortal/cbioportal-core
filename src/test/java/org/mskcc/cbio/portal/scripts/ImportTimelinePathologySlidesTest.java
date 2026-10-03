package org.mskcc.cbio.portal.scripts;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ImportTimelinePathologySlidesTest {

    private static final String[] HEADERS = {
        "PATIENT_ID", "START_DATE", "STOP_DATE", "EVENT_TYPE", "SPECIMEN", "LINKOUT"
    };

    @Test
    public void cleanPathologyEventIsAccepted() {
        ImportTimelineData.validatePathologySlidesEvent(
            HEADERS,
            new String[] {"P-1", "-5", "", "PATHOLOGY SLIDES", "Part 1", "/patient/wsiHESlides?specimenKey=part%3A%3Aab"},
            "PATHOLOGY SLIDES", 2);
    }

    @Test
    public void accessionInPathologyEventIsRejectedWithoutEchoingIt() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> ImportTimelineData.validatePathologySlidesEvent(
                HEADERS,
                new String[] {"P-1", "-5", "", "PATHOLOGY SLIDES", "S19-12345 A1", ""},
                "PATHOLOGY SLIDES", 7));
        assertTrue(error.getMessage().contains("Line 7"));
        assertTrue(error.getMessage().contains("SPECIMEN"));
        assertFalse(error.getMessage().contains("S19-12345"));
    }

    @Test
    public void imageIdsAttributeIsRejected() {
        String[] headers = {"PATIENT_ID", "START_DATE", "STOP_DATE", "EVENT_TYPE", "IMAGE_IDS"};
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> ImportTimelineData.validatePathologySlidesEvent(
                headers,
                new String[] {"P-1", "-5", "", "PATHOLOGY SLIDES", "[\"3735444\"]"},
                "PATHOLOGY SLIDES", 3));
        assertFalse(error.getMessage().contains("3735444"));
    }

    @Test
    public void otherEventTypesAreNotAffected() {
        ImportTimelineData.validatePathologySlidesEvent(
            HEADERS,
            new String[] {"P-1", "-5", "", "SPECIMEN", "S19-12345", ""},
            "SPECIMEN", 2);
    }
}
