package org.mskcc.cbio.portal.testsupport;

import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

public class ClickHouseDatabaseResetTestExecutionListener implements TestExecutionListener {

    @Override
    public void beforeTestMethod(TestContext testContext) {
        ClickHouseTestContainerManager.resetDatabaseState();
    }
}
