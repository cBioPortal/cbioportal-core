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

package org.mskcc.cbio.portal.validate;

import java.util.*;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.model.Gistic;

public class ValidateGistic {

    /**
     * Validates a gistic bean object according to some basic "business logic".
     * @param gistic
     * @throws validationException
     */
    public static void validateBean(Gistic gistic) throws validationException {
        
        int chromosome = gistic.getChromosome();
        int peakStart = gistic.getPeakStart();
        int peakEnd = gistic.getPeakEnd();
        double qValue = gistic.getqValue();
        ArrayList<CanonicalGene> genes_in_ROI = gistic.getGenes_in_ROI();

        if (chromosome < 1 || chromosome > 22) {
            throw new validationException("Invalid chromosome: " + chromosome);
        }

        if (peakStart <= 0) {
            throw new validationException("Invalid peak start: " + peakStart);
        }

        if (peakEnd <= 0) {
            throw new validationException("Invalid peak end: " + peakEnd);
        }

        if (peakEnd <= peakStart) {
            throw new validationException("Peak end is <= peak start: (start=" + peakStart + ", end=" + peakEnd + ")");
        }

        if (qValue < 0 || qValue > 1) {
            throw new validationException("Invalid qValue=" + qValue);
        }

        if (genes_in_ROI.isEmpty()){
            throw new validationException("No genes in ROI");
        }

        // todo: how do you validate ampdel?
    }
}
