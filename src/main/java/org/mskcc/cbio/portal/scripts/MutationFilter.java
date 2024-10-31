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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.mskcc.cbio.maf.MafRecord;
import org.mskcc.cbio.portal.util.ExtendedMutationUtil;

/**
 * Filter mutations as they're imported into the CGDS dbms.
 * <p>
 * @author Arthur Goldberg goldberg@cbio.mskcc.org
 */
public class MutationFilter {
   
   private final Set<Long> whiteListGenesForPromoterMutations;

   private int accepts=0;
    public int decisions=0;
   private int mutationStatusNoneRejects=0;
   private int invalidChromosome=0;
   private int invalidGeneInfo=0;
   private int lohOrWildTypeRejects=0;
    private int redactedOrWildTypeRejects =0;
   public Map<String,Integer> rejectionMap = new HashMap<String, Integer>();

   private static final Map<String,String> VALID_CHR_VALUES = new HashMap<>();
   static {
       for (int lc = 1; lc<=24; lc++) {
           VALID_CHR_VALUES.put(Integer.toString(lc),Integer.toString(lc));
           VALID_CHR_VALUES.put("CHR" + Integer.toString(lc),Integer.toString(lc));
       }
       VALID_CHR_VALUES.put("X","23");
       VALID_CHR_VALUES.put("CHRX","23");
       VALID_CHR_VALUES.put("Y","24");
       VALID_CHR_VALUES.put("CHRY","24");
       VALID_CHR_VALUES.put("NA","NA");
       VALID_CHR_VALUES.put("MT","MT"); // mitochondria
   }

   /**
    * Construct a MutationFilter with no white lists. 
    * This filter will 
    * <br>
    * REJECT Silent, LOH, Intron and Wildtype mutations, and
    * <br>
    * KEEP all other mutations.
    */
   public MutationFilter() throws IllegalArgumentException{
      whiteListGenesForPromoterMutations = new HashSet<Long>();
      whiteListGenesForPromoterMutations.add(7015L); // TERT
   }
   
   /**
    * Indicate whether the specified mafRecord should be accepted as input to
    * the CGDS Database.
    * <p>
    * @param mafRecord
    *           a MAF line/record.
    * <br>
    * @return true if the mafRecord should be imported into the dbms
    */
   public boolean acceptMutation(MafRecord mafRecord, Set<String> filteredMutations) {
      this.decisions++;
      
      /*
       * Mutation types from Firehose:
         +------------------------+
         | De_novo_Start          | 
         | Frame_Shift_Del        | 
         | Frame_Shift_Ins        | 
         | Indel                  | 
         | In_Frame_Del           | 
         | In_Frame_Ins           | 
         | Missense               | 
         | Missense_Mutation      | 
         | Nonsense_Mutation      | 
         | Nonstop_Mutation       | 
         | Splice_Site            | 
         | Stop_Codon_Del         | 
         | Translation_Start_Site | 
         +------------------------+
       */
      if (ExtendedMutationUtil.isBlankEntrezGeneId(mafRecord.getGivenEntrezGeneId())
              && ExtendedMutationUtil.isBlankHugoGeneSymbol(mafRecord.getHugoGeneSymbol())) {
          invalidGeneInfo++;
          return false;
      }
      long entrezGeneId;
      try {
         entrezGeneId = Long.parseLong(mafRecord.getGivenEntrezGeneId());
         if (entrezGeneId < 0) {
             invalidGeneInfo++;
             return false;
         }
      } catch (NumberFormatException e) {
          invalidGeneInfo++;
          return false;
      }
      // Do not accept mutations with invalid chromosome symbol
      if (normalizeChr(mafRecord.getChr()) ==  null) {
          invalidChromosome++;
          return false;
      }
      // Do not accept mutations with Mutation_Status of None
      if (safeStringTest( mafRecord.getMutationStatus(), "None" )) {
          mutationStatusNoneRejects++;
          return false;
      }
      
      // Do not accept LOH or Wildtype Mutations
      if( safeStringTest( mafRecord.getMutationStatus(), "LOH" ) ||
               safeStringTest( mafRecord.getMutationStatus(), "Wildtype" ) ){
         lohOrWildTypeRejects++;
         return false;
      }
      
      // Do not accept Redacted or Wildtype mutations
      if (safeStringTest(mafRecord.getValidationStatus(), "Redacted") ||
              safeStringTest( mafRecord.getValidationStatus(), "Wildtype" )) {
          redactedOrWildTypeRejects++;
          return false;
      }
      
      //Filter by types if specified in the meta file, else filter for the default types
      String mutationType = ExtendedMutationUtil.getMutationType(mafRecord);
      if (filteredMutations != null) {
          if (filteredMutations.contains(mutationType)) {
              addRejectedVariant(mutationType);
              return false;
          } else {
              if( safeStringTest( mutationType, "5'Flank" ) ) {
                  mafRecord.setProteinChange("Promoter");
              }
              return true;
          }
      } else {
          // Do not accept Silent, Intronic, 3'UTR, 5'UTR, IGR or RNA Mutations
          if( safeStringTest( mutationType, "Silent" ) ||
                   safeStringTest( mutationType, "Intron" ) ||
                   safeStringTest( mutationType, "3'UTR" ) ||
                   safeStringTest( mutationType, "3'Flank" ) ||
                   safeStringTest( mutationType, "5'UTR" ) ||
                   safeStringTest( mutationType, "IGR" ) ||
                   safeStringTest( mutationType, "RNA")){
              addRejectedVariant(mutationType);
              return false;
          }
          
          if( safeStringTest( mutationType, "5'Flank" ) ) {
                if (whiteListGenesForPromoterMutations.contains(entrezGeneId)){
                      mafRecord.setProteinChange("Promoter");
                } else {
                    addRejectedVariant(mutationType);
                    return false;
                }
          }
    
         this.accepts++;
         return true;
      }
   }

   public static String normalizeChr(String strChr) {
       if (strChr == null) {
           return null;
       }
       return VALID_CHR_VALUES.get(strChr.toUpperCase());
   }

   /**
    * Provide number of decisions made by this MutationFilter.
    * @return the number of decisions made by this MutationFilter
    */
   public int getDecisions(){
      return this.decisions;
   }

   /**
    * Provide number of ACCEPT (return true) decisions made by this MutationFilter.
    * @return the number of ACCEPT (return true) decisions made by this MutationFilter
    */
   public int getAccepts(){
      return this.accepts;
   }

    public int getMutationStatusNoneRejects() {
        return mutationStatusNoneRejects;
    }

    /**
     * Provide number of REJECT decisions for LOH or Wild Type Mutations.
     * @return number of REJECT decisions for LOH or Wild Type Mutations.
     */
   public int getLohOrWildTypeRejects() {
       return this.lohOrWildTypeRejects;
   }

   public int getInvalidChromosome() {
       return invalidChromosome;
   }

   public int getInvalidGeneInfo() {
       return invalidGeneInfo;
   }

   public int getRedactedOrWildTypeRejects()
	{
		return this.redactedOrWildTypeRejects;
	}
	
   public Map<String, Integer> getRejectionMap() {
       return this.rejectionMap;
   }
   
   public void addRejectedVariant(String mutation) {
       this.rejectionMap.putIfAbsent(mutation, 0);
       this.rejectionMap.computeIfPresent(mutation, (k, v) -> v + 1);
   }

   /**
    * Provide number of REJECT (return false) decisions made by this MutationFilter.
    * @return the number of REJECT (return false) decisions made by this MutationFilter
    */
   public int getRejects(){
      return this.decisions - this.accepts;
   }
   
   public String getStatistics(){
      String statistics = "Mutation filter decisions: " + this.getDecisions() +
            "\nRejects: " + this.getRejects() +
            "\nMutation Status 'None' Rejects:  " + this.getMutationStatusNoneRejects() +
            "\nLOH or Wild Type Mutation Status Rejects:  " + this.getLohOrWildTypeRejects() +
            "\nRedacted or Wild Type Validation Status Rejects:  " + this.getRedactedOrWildTypeRejects() +
            "\nInvalid Choromosome Rejects:  " + this.getInvalidChromosome() +
            "\nInvalid Gene Info Rejects:  " + this.getInvalidGeneInfo();

      Map<String, Integer> variantsRejected = this.getRejectionMap();
      for (Map.Entry<String, Integer> variant : variantsRejected.entrySet()) {
          statistics = statistics + "\n" + variant.getKey() + " Rejects: " + variant.getValue();
      }
      
      return statistics;
   }

   /**
    * Carefully look for pattern in data.
    * <p>
    * @param data
    * @param pattern
    * @return false if data is null; true if data starts with pattern, independent of case
    */
   private boolean safeStringTest( String data, String pattern ){
      if( null == data){
         return false;
      }
      return data.toLowerCase().startsWith( pattern.toLowerCase() );
   }
   
   @Override
   public String toString(){
      StringBuffer sb = new StringBuffer();
      return( sb.toString() );
   }
}