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

package org.mskcc.cbio.portal.servlet;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONArray;
import org.mskcc.cbio.portal.dao.DaoClinicalAttributeMeta;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.ClinicalAttribute;
import org.mskcc.cbio.portal.util.AccessControl;
import org.mskcc.cbio.portal.util.SpringUtil;
import org.mskcc.cbio.portal.util.WebserviceParserUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;

public class ClinicalAttributesServlet extends HttpServlet {
    private static Logger log = LoggerFactory.getLogger(ClinicalAttributesServlet.class);
    
    // class which process access control to cancer studies
    private AccessControl accessControl;

    /**
     * Initializes the servlet.
     *
     * @throws ServletException
     */
    public void init() throws ServletException {
        super.init();
        accessControl = SpringUtil.getAccessControl();
    }

    /**
     * Takes any sort of patient list parameter in the request
     *
     * Returns a list of clinical attributes in json format
     *
     * @param request
     * @param response
     * @throws ServletException
     */
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        try {
            JSONArray toWrite = new JSONArray();
            response.setContentType("text/json");
            
            String cancerStudyId = WebserviceParserUtils.getCancerStudyId(request);
            
            if(cancerStudyId != null) {
            	CancerStudy cancerStudy = DaoCancerStudy
                        .getCancerStudyByStableId(cancerStudyId);
            	if(cancerStudy != null && accessControl.isAccessibleCancerStudy(cancerStudy.getCancerStudyStableId()).size() == 1) {
                    List<ClinicalAttribute> clinicalAttributes = DaoClinicalAttributeMeta.getDataByStudy(cancerStudy.getInternalId());


                    for (ClinicalAttribute attr : clinicalAttributes) {
                        toWrite.add(ClinicalJSON.reflectToMap(attr));
                    }
            	}
            }

            
            PrintWriter out = response.getWriter();
            JSONArray.writeJSONString(toWrite, out);
        } catch (DaoException e) {
            throw new ServletException(e);
        } catch (IOException e) {
            throw new ServletException(e);
        }
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        doGet(request, response);
    }
}