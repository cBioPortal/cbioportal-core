package org.mskcc.cbio.portal.scripts;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.model.shared.GeneticAlterationType;
import org.mskcc.cbio.portal.model.shared.GeneticEntity;
import org.mskcc.cbio.portal.util.GeneticProfileReader;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestImportStaleReads {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Before
    public void emptyDatabase() {
        JdbcUtil.setDataSource(mock(DataSource.class, RETURNS_DEEP_STUBS));
    }

    @Test
    public void entityImportReturnsNewIdsAndDoesNotInsertDuplicateStableIds() throws Exception {
        File data = temporary.newFile("entities.tsv");
        Files.writeString(data.toPath(), "ENTITY_STABLE_ID\tSAMPLE\nexisting\t1\nnew\t2\nnew\t3\n");
        try (MockedStatic<GenericAssayMetaUtils> reads = mockStatic(GenericAssayMetaUtils.class);
             MockedStatic<DaoGeneticEntity> entities = mockStatic(DaoGeneticEntity.class)) {
            reads.when(GenericAssayMetaUtils::buildGenericAssayStableIdToEntityIdMap)
                .thenReturn(new HashMap<>(Map.of("existing", 5)));
            entities.when(() -> DaoGeneticEntity.addNewGeneticEntity(any()))
                .thenReturn(new GeneticEntity(6, "GENERIC_ASSAY", "new"));
            Map<String, Integer> ids = ImportGenericAssayEntity.importData(
                data, GeneticAlterationType.GENERIC_ASSAY, null, false);
            assertEquals(Map.of("existing", 5, "new", 6), ids);
            reads.verify(GenericAssayMetaUtils::buildGenericAssayStableIdToEntityIdMap, times(1));
            entities.verify(() -> DaoGeneticEntity.addNewGeneticEntity(any()), times(1));
        }
    }

    @Test
    public void missingSourceProfileFailsWithoutReloadingCachedProfiles() throws Exception {
        GeneticProfile profile = new GeneticProfile();
        profile.setStableId("study_derived");
        Properties metadata = new Properties();
        metadata.setProperty("source_stable_id", "study_missing");
        profile.setOtherMetadataFields(metadata);
        Method link = GeneticProfileReader.class.getDeclaredMethod("createGeneticProfileLink", GeneticProfile.class);
        link.setAccessible(true);
        try (MockedStatic<DaoGeneticProfile> profiles = mockStatic(DaoGeneticProfile.class)) {
            InvocationTargetException error = assertThrows(InvocationTargetException.class,
                () -> link.invoke(null, profile));
            assertTrue(error.getCause().getMessage().contains("Referred genetic profile not found"));
            profiles.verify(DaoGeneticProfile::reCache, never());
        }
    }

    @Test
    public void staleExistingAllListCannotMakeCreationSilentlySucceed() throws Exception {
        Method add = AddCaseList.class.getDeclaredMethod("addCaseList", String.class, CancerStudy.class,
            SampleListCategory.class, String.class, String.class, ArrayList.class);
        add.setAccessible(true);
        try (MockedConstruction<DaoSampleList> lists = mockConstruction(DaoSampleList.class,
                (mock, context) -> when(mock.getSampleListByStableId("study_all")).thenReturn(new SampleList()))) {
            InvocationTargetException error = assertThrows(InvocationTargetException.class,
                () -> add.invoke(null, "study_all", new CancerStudy("study", "study", "study", "brca", true), null, "All", "All", new ArrayList<>()));
            assertTrue(error.getCause() instanceof IllegalArgumentException);
            assertTrue(error.getCause().getMessage().contains("Refusing to skip creation"));
            verify(lists.constructed().get(0), never()).addSampleList(any());
        }
    }
}
