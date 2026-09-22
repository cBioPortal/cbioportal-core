package org.mskcc.cbio.portal.scripts;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    @Test
    public void rejectsNonFiniteDownsampleBeforeImport() throws Exception {
        Method validator = ImportWsiData.class.getDeclaredMethod(
            "requireValidTileMetadata", String.class, int.class);
        validator.setAccessible(true);
        String metadata = "{\"dimensions\":{\"width\":256,\"height\":256},"
            + "\"levels\":1,\"level_dimensions\":[{\"width\":256,\"height\":256}],"
            + "\"max_zoom\":0,\"tile_size\":256,"
            + "\"tile_metadata_schema_version\":2,\"safe_min_level\":0,"
            + "\"level_downsamples\":[1e309],"
            + "\"decode_policy_version\":\"geometry-v2;tile-max=16777216;thumbnail-max=16777216\","
            + "\"max_decode_pixels\":16777216,\"thumbnail_max_decode_pixels\":16777216}";

        try {
            validator.invoke(null, metadata, 8);
            fail("non-finite tile metadata must be rejected");
        } catch (InvocationTargetException exception) {
            assertTrue(exception.getCause() instanceof IllegalArgumentException);
            assertTrue(exception.getCause().getMessage().contains("valid tile contract"));
        }
    }

    @Test
    public void artifactUrisAcceptDeploymentSpecificSchemesAndExtensions() {
        String unsetPrefixVariable = "CBIOPORTAL_WSI_TEST_PREFIXES_UNSET";
        assertTrue(ImportWsiData.safeArtifactUrl(
            "https://slides.example/scan.custom", unsetPrefixVariable));
        assertTrue(ImportWsiData.safeArtifactUrl(
            "gs://thumbnails.example/thumbnail.webp", unsetPrefixVariable));
        assertTrue(ImportWsiData.safeArtifactUrl(
            "https://slides.example/100%25.jpg", unsetPrefixVariable));
        assertTrue(ImportWsiData.safeArtifactUrl(
            "https://slides.example/%25ZZ.jpg", unsetPrefixVariable));
        assertTrue(ImportWsiData.safeArtifactUrl(
            "https://slides.example/%2525.jpg", unsetPrefixVariable));
        assertTrue(ImportWsiData.safeArtifactUrl(
            "https://slides.example/a+b.jpg", unsetPrefixVariable));
        assertTrue(ImportWsiData.safeArtifactUrl(
            "https://slides.example/caf%C3%A9.jpg", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://user:password@example/slide.svs", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/slide.svs?token=secret", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/../slide.svs", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/slides/%252e%252e/slide.svs", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/100%.jpg", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/short%2.jpg", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/bad%ZZ.jpg", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/slides/%2e%2e/slide.svs", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/slides/%2E%2e/slide.svs", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/slides/%2e%2e%2fsecret.svs", unsetPrefixVariable));
        assertFalse(ImportWsiData.safeArtifactUrl(
            "https://example/slides/%252e%252e%252fsecret.svs", unsetPrefixVariable));
    }

    @Test
    public void thumbnailContentTypeAcceptsGenericImageMediaTypes() {
        assertTrue(ImportWsiData.isImageContentType("image/webp"));
        assertTrue(ImportWsiData.isImageContentType("image/avif"));
        assertTrue(ImportWsiData.isImageContentType("image/svg+xml"));
        assertFalse(ImportWsiData.isImageContentType("text/plain"));
        assertFalse(ImportWsiData.isImageContentType("image/"));
        assertFalse(ImportWsiData.isImageContentType("image/foo=bar"));
        assertFalse(ImportWsiData.isImageContentType("image/jpeg; charset=utf-8"));
        assertFalse(ImportWsiData.isImageContentType("image/jpëg"));
    }

    @Test
    public void tileMetadataAcceptsProducerSpecificFields() throws Exception {
        String metadata = "{\"dimensions\":{\"width\":256,\"height\":256},"
            + "\"levels\":1,\"level_dimensions\":[{\"width\":256,\"height\":256}],"
            + "\"max_zoom\":0,\"tile_size\":256,"
            + "\"producer_specific\":{\"profile\":\"custom\"}}";
        assertTrue(ImportWsiData.validTileMetadata(new ObjectMapper().readTree(metadata)));
    }

    private static String[] placement(String patientId, String matchLevel) {
        String[] placement = new String[9];
        placement[1] = patientId;
        placement[5] = "UNMATCHED".equals(matchLevel) ? null : "7";
        placement[6] = matchLevel;
        return placement;
    }
}
