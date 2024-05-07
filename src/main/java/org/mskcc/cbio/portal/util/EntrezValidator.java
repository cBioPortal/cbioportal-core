package org.mskcc.cbio.portal.util;

public class EntrezValidator {
    public static boolean isaValidEntrezId(String entrez) {
        return entrez.matches("[0-9]+");
    }
}
