package org.mskcc.cbio.portal.testsupport;

import java.util.List;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;

public class ClickHouseTestContextCustomizerFactory implements ContextCustomizerFactory {

    @Override
    public ContextCustomizer createContextCustomizer(
        Class<?> testClass,
        List<ContextConfigurationAttributes> configAttributes
    ) {
        return new ClickHouseContextCustomizer();
    }

    private static class ClickHouseContextCustomizer implements ContextCustomizer {

        @Override
        public void customizeContext(
            ConfigurableApplicationContext context,
            MergedContextConfiguration mergedConfig
        ) {
            ClickHouseTestContainerManager.ensureStarted();
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && getClass() == obj.getClass();
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }
    }
}
