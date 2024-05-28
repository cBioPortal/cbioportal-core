/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.scripts;

import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoClinicalData;
import org.mskcc.cbio.portal.dao.DaoClinicalEvent;
import org.mskcc.cbio.portal.dao.DaoCopyNumberSegmentFile;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.dao.DaoGeneset;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoGeneticProfileSamples;
import org.mskcc.cbio.portal.dao.DaoInfo;
import org.mskcc.cbio.portal.dao.DaoMutSig;
import org.mskcc.cbio.portal.dao.DaoMutation;
import org.mskcc.cbio.portal.dao.DaoPatient;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.DaoSampleList;
import org.mskcc.cbio.portal.dao.DaoSampleProfile;
import org.mskcc.cbio.portal.dao.DaoTypeOfCancer;
import org.mskcc.cbio.portal.dao.DaoUser;
import org.mskcc.cbio.portal.dao.DaoUserAuthorities;

/**
 * Empty the database.
 *
 * @author Ethan Cerami
 * @author Arthur Goldberg goldberg@cbio.mskcc.org
 */
public class ResetDatabase {

    public static final int MAX_RESET_SIZE = 6;

    /**
     * Remove all records from any size database.
     * Whenever a new Dao* class is defined, must add its deleteAllRecords() method here.
     *
     * @throws DaoException
     */
    public static void resetAnySizeDatabase() throws DaoException {
        System.out.print("resetting all database.");
        DaoUser.deleteAllRecords();
        DaoUserAuthorities.deleteAllRecords();
        DaoTypeOfCancer.deleteAllRecords();
        DaoCancerStudy.deleteAllRecords();
        DaoGeneOptimized daoGene = DaoGeneOptimized.getInstance();
        daoGene.deleteAllRecords();
        DaoGeneset.deleteAllRecords();
        DaoInfo.clearVersion();
        DaoSampleProfile.deleteAllRecords();
        DaoGeneticAlteration daoGenetic = DaoGeneticAlteration.getInstance();
        daoGenetic.deleteAllRecords();
        DaoMutSig.deleteAllRecords();
        DaoGeneticProfile.deleteAllRecords();
        DaoSampleList daoSampleList = new DaoSampleList();
        daoSampleList.deleteAllRecords();
        DaoClinicalData.deleteAllRecords();
        DaoClinicalEvent.deleteAllRecords();
        DaoCopyNumberSegmentFile.deleteAllRecords();
        DaoMutation.deleteAllRecords();
        DaoGeneticProfileSamples.deleteAllRecords();
        DaoPatient.deleteAllRecords();
        DaoSample.deleteAllRecords();
        System.out.println("all database reset.");
    }

    public static void resetDatabase() throws DaoException {

        // safety measure: don't reset a big database
        if (MAX_RESET_SIZE < DaoCancerStudy.getCount()) {
            throw new DaoException("Database has " + DaoCancerStudy.getCount() +
                    " studies, and we don't reset a database with more than " + MAX_RESET_SIZE + " records.");
        } else {
            resetAnySizeDatabase();
        }

    }

    public static void main(String[] args) throws DaoException {
        StatDatabase.statDb();
        ResetDatabase.resetDatabase();
        System.err.println("Database Cleared and Reset.");
    }
}
