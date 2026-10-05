package org.mskcc.cbio.portal.scripts;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ImportTimelineDeidTest {

    private static final String[] SLIDE_HEADERS = {
        "PATIENT_ID", "START_DATE", "STOP_DATE", "EVENT_TYPE", "SPECIMEN", "LINKOUT"
    };

    @Test
    public void cleanPathologySlidesEventIsAccepted() {
        ImportTimelineData.validateTimelineEventDeid(
            SLIDE_HEADERS,
            new String[] {"P-1", "-5", "", "PATHOLOGY SLIDES", "Part 1", "/patient/wsiHESlides?specimenKey=part%3A%3Aab"},
            "PATHOLOGY SLIDES", 2);
    }

    @Test
    public void specimenAccessionValueIsRejectedInAnyEventWithoutEchoingIt() {
        for (String eventType : new String[] {"PATHOLOGY SLIDES", "SPECIMEN", "TREATMENT"}) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ImportTimelineData.validateTimelineEventDeid(
                    SLIDE_HEADERS,
                    new String[] {"P-1", "-5", "", eventType, "S19-12345 A1", ""},
                    eventType, 7));
            assertTrue(error.getMessage().contains("Line 7"));
            assertTrue(error.getMessage().contains("SPECIMEN"));
            assertFalse(error.getMessage().contains("S19-12345"));
        }
    }

    @Test
    public void accessionNumberColumnIsRejectedForEveryEventTypeWithoutEchoingIt() {
        String[] headers = {"PATIENT_ID", "START_DATE", "STOP_DATE", "EVENT_TYPE", "ACCESSION_NUMBER", "TUMOR_SITE"};
        // Pathology case numbers and radiology exam numbers (no recognizable value pattern).
        String[][] cases = {
            {"PATHOLOGY", "S19-12345"},
            {"Diagnosis", "R12345678"},
            {"Diagnosis", "1234567890"},
        };
        for (String[] c : cases) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ImportTimelineData.validateTimelineEventDeid(
                    headers, new String[] {"P-1", "-5", "", c[0], c[1], "Lung"}, c[0], 3));
            assertTrue(error.getMessage().contains("ACCESSION_NUMBER"));
            assertFalse(error.getMessage().contains(c[1]));
        }
    }

    @Test
    public void emptyAccessionColumnIsAccepted() {
        String[] headers = {"PATIENT_ID", "START_DATE", "STOP_DATE", "EVENT_TYPE", "ACCESSION_NUMBER", "TUMOR_SITE"};
        ImportTimelineData.validateTimelineEventDeid(
            headers, new String[] {"P-1", "-5", "", "Diagnosis", "", "Lung"}, "Diagnosis", 2);
    }

    @Test
    public void imageIdsAttributeIsRejectedOnPathologySlides() {
        String[] headers = {"PATIENT_ID", "START_DATE", "STOP_DATE", "EVENT_TYPE", "IMAGE_IDS"};
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> ImportTimelineData.validateTimelineEventDeid(
                headers,
                new String[] {"P-1", "-5", "", "PATHOLOGY SLIDES", "[\"3735444\"]"},
                "PATHOLOGY SLIDES", 3));
        assertFalse(error.getMessage().contains("3735444"));
    }

    @Test
    public void ordinaryTimelineEventsAreAccepted() {
        String[] headers = {"PATIENT_ID", "START_DATE", "STOP_DATE", "EVENT_TYPE", "SUBTYPE", "GLEASON_SCORE"};
        ImportTimelineData.validateTimelineEventDeid(
            headers, new String[] {"P-1", "-5", "", "PATHOLOGY", "Gleason", "7"}, "PATHOLOGY", 2);
    }
}
