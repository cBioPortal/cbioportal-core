/*
 * Copyright (c) 2015 - 2026 Memorial Sloan Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.mskcc.cbio.portal.util;

import java.lang.StringBuilder;
import org.mskcc.cbio.portal.dao.DaoInfo;
import org.mskcc.cbio.portal.util.CheckDbPrivileges;

public class VersionUtil {

    public static void main(String[] args) {
        System.exit(check());
    }

    /**
     * Checks that the database schema version matches this application.
     *
     * @return  0 if the versions match, 1 otherwise
     */
    public static int check() {
        CheckDbPrivileges.getInstance().logWarningIfRecommendedPrivilegeIsAbsent();
        StringBuilder logMessageBuilder = new StringBuilder(117);
        int versionCheck = DaoInfo.checkVersion(logMessageBuilder) ? 0 : 1;
        System.out.println(logMessageBuilder.toString());
        System.out.flush();
        return versionCheck;
    }
}
