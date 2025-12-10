--
-- Copyright (c) 2016 - 2025 Memorial Sloan Kettering Cancer Center.
--
-- This library is distributed in the hope that it will be useful, but WITHOUT
-- ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
-- FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
-- is on an "as is" basis, and Memorial Sloan Kettering Cancer Center has no
-- obligations to provide maintenance, support, updates, enhancements or
-- modifications. In no event shall Memorial Sloan Kettering Cancer Center be
-- liable to any party for direct, indirect, special, incidental or
-- consequential damages, including lost profits, arising out of the use of this
-- software and its documentation, even if Memorial Sloan Kettering Cancer
-- Center has been advised of the possibility of such damage.
--
-- This file is part of cBioPortal.
--
-- cBioPortal is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as
-- published by the Free Software Foundation, either version 3 of the
-- License.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU Affero General Public License for more details.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.
-- ----------------------------------------------------------------------------
--
-- Database: `cgds` - SQLite Version
--
-- --------------------------------------------------------
-- Database table schemas and version number (SQLite Compatible)
-- --------------------------------------------------------
-- Converted from MySQL schema for SQLite compatibility
-- --------------------------------------------------------

PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS `resource_study`;
DROP TABLE IF EXISTS `resource_patient`;
DROP TABLE IF EXISTS `resource_sample`;
DROP TABLE IF EXISTS `resource_definition`;
DROP TABLE IF EXISTS `info`;
DROP TABLE IF EXISTS `allele_specific_copy_number`;
DROP TABLE IF EXISTS `data_access_tokens`;
DROP TABLE IF EXISTS `reference_genome_gene`;
DROP TABLE IF EXISTS `clinical_event_data`;
DROP TABLE IF EXISTS `clinical_event`;
DROP TABLE IF EXISTS `copy_number_seg_file`;
DROP TABLE IF EXISTS `copy_number_seg`;
DROP TABLE IF EXISTS `sample_cna_event`;
DROP TABLE IF EXISTS `cna_event`;
DROP TABLE IF EXISTS `gistic_to_gene`;
DROP TABLE IF EXISTS `gistic`;
DROP TABLE IF EXISTS `mut_sig`;
DROP TABLE IF EXISTS `clinical_attribute_meta`;
DROP TABLE IF EXISTS `clinical_sample`;
DROP TABLE IF EXISTS `clinical_patient`;
DROP TABLE IF EXISTS `mutation_count_by_keyword`;
DROP TABLE IF EXISTS `mutation`;
DROP TABLE IF EXISTS `mutation_event`;
DROP TABLE IF EXISTS `alteration_driver_annotation`;
DROP TABLE IF EXISTS `structural_variant`;
DROP TABLE IF EXISTS `sample_profile`;
DROP TABLE IF EXISTS `gene_panel_list`;
DROP TABLE IF EXISTS `gene_panel`;
DROP TABLE IF EXISTS `genetic_profile_samples`;
DROP TABLE IF EXISTS `genetic_alteration`;
DROP TABLE IF EXISTS `genetic_profile_link`;
DROP TABLE IF EXISTS `genetic_profile`;
DROP TABLE IF EXISTS `generic_entity_properties`;
DROP TABLE IF EXISTS `geneset_hierarchy_leaf`;
DROP TABLE IF EXISTS `geneset_hierarchy_node`;
DROP TABLE IF EXISTS `geneset_gene`;
DROP TABLE IF EXISTS `geneset`;
DROP TABLE IF EXISTS `gene_alias`;
DROP TABLE IF EXISTS `gene`;
DROP TABLE IF EXISTS `genetic_entity`;
DROP TABLE IF EXISTS `sample_list_list`;
DROP TABLE IF EXISTS `sample_list`;
DROP TABLE IF EXISTS `sample`;
DROP TABLE IF EXISTS `patient`;
DROP TABLE IF EXISTS `authorities`;
DROP TABLE IF EXISTS `users`;
DROP TABLE IF EXISTS `cancer_study_tags`;
DROP TABLE IF EXISTS `cancer_study`;
DROP TABLE IF EXISTS `reference_genome`;
DROP TABLE IF EXISTS `type_of_cancer`;

CREATE TABLE `type_of_cancer` (
  `TYPE_OF_CANCER_ID` varchar(63) NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `DEDICATED_COLOR` char(31) NOT NULL,
  `SHORT_NAME` varchar(127),
  `PARENT` varchar(63),
  PRIMARY KEY (`TYPE_OF_CANCER_ID`)
);

-- --------------------------------------------------------
CREATE TABLE `reference_genome` (
  `REFERENCE_GENOME_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `SPECIES` varchar(64) NOT NULL,
  `NAME` varchar(64) NOT NULL,
  `BUILD_NAME` varchar(64) NOT NULL,
  `GENOME_SIZE` bigint NULL,
  `URL` varchar(256) NOT NULL,
  `RELEASE_DATE` datetime DEFAULT NULL,
  UNIQUE (`BUILD_NAME`)
);

-- --------------------------------------------------------
CREATE TABLE `cancer_study` (
  `CANCER_STUDY_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `CANCER_STUDY_IDENTIFIER` varchar(255),
  `TYPE_OF_CANCER_ID` varchar(63) NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `DESCRIPTION` varchar(1024) NOT NULL,
  `PUBLIC` BOOLEAN NOT NULL,
  `PMID` varchar(1024) DEFAULT NULL,
  `CITATION` varchar(200) DEFAULT NULL,
  `GROUPS` varchar(200) DEFAULT NULL,
  `STATUS` INTEGER DEFAULT NULL,
  `IMPORT_DATE` datetime DEFAULT NULL,
  `REFERENCE_GENOME_ID` INTEGER DEFAULT 1,
  UNIQUE (`CANCER_STUDY_IDENTIFIER`),
  FOREIGN KEY (`TYPE_OF_CANCER_ID`) REFERENCES `type_of_cancer` (`TYPE_OF_CANCER_ID`),
  FOREIGN KEY (`REFERENCE_GENOME_ID`) REFERENCES `reference_genome` (`REFERENCE_GENOME_ID`) ON DELETE RESTRICT
);

-- --------------------------------------------------------
CREATE TABLE `cancer_study_tags` (
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `TAGS` TEXT NOT NULL,
  PRIMARY KEY (`CANCER_STUDY_ID`),
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `users` (
  `EMAIL` varchar(128) NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `ENABLED` BOOLEAN NOT NULL,
  PRIMARY KEY (`EMAIL`)
);

-- --------------------------------------------------------
CREATE TABLE `authorities` (
  `EMAIL` varchar(128) NOT NULL,
  `AUTHORITY` varchar(255) NOT NULL
);

-- --------------------------------------------------------
CREATE TABLE `patient` (
  `INTERNAL_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `STABLE_ID` varchar(50) NOT NULL,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `sample` (
  `INTERNAL_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `STABLE_ID` varchar(63) NOT NULL,
  `SAMPLE_TYPE` varchar(255) NOT NULL,
  `PATIENT_ID` INTEGER NOT NULL,
  FOREIGN KEY (`PATIENT_ID`) REFERENCES `patient` (`INTERNAL_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `sample_list` (
  `LIST_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `STABLE_ID` varchar(255) NOT NULL,
  `CATEGORY` varchar(255) NOT NULL,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `DESCRIPTION` TEXT,
  UNIQUE (`STABLE_ID`),
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `sample_list_list` (
  `LIST_ID` INTEGER NOT NULL,
  `SAMPLE_ID` INTEGER NOT NULL,
  PRIMARY KEY (`LIST_ID`,`SAMPLE_ID`),
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `genetic_entity` (
  `ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `ENTITY_TYPE` varchar(45) NOT NULL,
  `STABLE_ID` varchar(255) DEFAULT NULL
);

-- --------------------------------------------------------
CREATE TABLE `gene` (
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `HUGO_GENE_SYMBOL` varchar(255) NOT NULL,
  `GENETIC_ENTITY_ID` INTEGER NOT NULL,
  `TYPE` varchar(50),
  PRIMARY KEY (`ENTREZ_GENE_ID`),
  UNIQUE (`GENETIC_ENTITY_ID`),
  FOREIGN KEY (`GENETIC_ENTITY_ID`) REFERENCES `genetic_entity` (`ID`) ON DELETE CASCADE
);

CREATE INDEX `idx_gene_hugo` ON `gene` (`HUGO_GENE_SYMBOL`);

-- --------------------------------------------------------
CREATE TABLE `gene_alias` (
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `GENE_ALIAS` varchar(255) NOT NULL,
  PRIMARY KEY (`ENTREZ_GENE_ID`,`GENE_ALIAS`),
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`)
);

-- --------------------------------------------------------
CREATE TABLE `geneset` (
  `ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `GENETIC_ENTITY_ID` INTEGER NOT NULL,
  `EXTERNAL_ID` VARCHAR(200) NOT NULL,
  `NAME` VARCHAR(200) NOT NULL,
  `DESCRIPTION` VARCHAR(300) NOT NULL,
  `REF_LINK` TEXT,
  UNIQUE (`NAME`),
  UNIQUE (`EXTERNAL_ID`),
  UNIQUE (`GENETIC_ENTITY_ID`),
  FOREIGN KEY (`GENETIC_ENTITY_ID`) REFERENCES `genetic_entity` (`ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `geneset_gene` (
  `GENESET_ID` INTEGER NOT NULL,
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  PRIMARY KEY (`GENESET_ID`,`ENTREZ_GENE_ID`),
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`GENESET_ID`) REFERENCES `geneset` (`ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `geneset_hierarchy_node` (
  `NODE_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `NODE_NAME` VARCHAR(200) NOT NULL,
  `PARENT_ID` INTEGER NULL DEFAULT NULL,
  UNIQUE (`NODE_NAME`, `PARENT_ID`)
);

-- --------------------------------------------------------
CREATE TABLE `geneset_hierarchy_leaf` (
  `NODE_ID` INTEGER NOT NULL,
  `GENESET_ID` INTEGER NOT NULL,
  PRIMARY KEY (`NODE_ID`,`GENESET_ID`),
  FOREIGN KEY (`NODE_ID`) REFERENCES `geneset_hierarchy_node` (`NODE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`GENESET_ID`) REFERENCES `geneset` (`ID`) ON DELETE CASCADE
);

-- ------------------------------------------------------
CREATE TABLE `generic_entity_properties` (
  `ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `GENETIC_ENTITY_ID` INTEGER NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `VALUE` varchar(5000) NOT NULL,
  UNIQUE (`GENETIC_ENTITY_ID`,`NAME`),
  FOREIGN KEY (`GENETIC_ENTITY_ID`) REFERENCES `genetic_entity` (`ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `genetic_profile` (
  `GENETIC_PROFILE_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `STABLE_ID` varchar(255) NOT NULL,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `GENETIC_ALTERATION_TYPE` varchar(255) NOT NULL,
  `GENERIC_ASSAY_TYPE` varchar(255) DEFAULT NULL,
  `DATATYPE` varchar(255) NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `DESCRIPTION` TEXT,
  `SHOW_PROFILE_IN_ANALYSIS_TAB` INTEGER NOT NULL,
  `PIVOT_THRESHOLD` REAL DEFAULT NULL,
  `SORT_ORDER` TEXT CHECK(`SORT_ORDER` IN ('ASC','DESC')) DEFAULT NULL,
  `PATIENT_LEVEL` INTEGER DEFAULT 0,
  UNIQUE (`STABLE_ID`),
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `genetic_profile_link` (
  `REFERRING_GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `REFERRED_GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `REFERENCE_TYPE` VARCHAR(45) NULL,
  PRIMARY KEY (`REFERRING_GENETIC_PROFILE_ID`,`REFERRED_GENETIC_PROFILE_ID`),
  FOREIGN KEY (`REFERRING_GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`REFERRED_GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- --------------------------------------------------------
CREATE TABLE `genetic_alteration` (
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `GENETIC_ENTITY_ID` INTEGER NOT NULL,
  `VALUES` TEXT NOT NULL,
  PRIMARY KEY (`GENETIC_PROFILE_ID`,`GENETIC_ENTITY_ID`),
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`GENETIC_ENTITY_ID`) REFERENCES `genetic_entity` (`ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `genetic_profile_samples` (
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `ORDERED_SAMPLE_LIST` TEXT NOT NULL,
  UNIQUE (`GENETIC_PROFILE_ID`),
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `gene_panel` (
  `INTERNAL_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `STABLE_ID` varchar(255) NOT NULL,
  `DESCRIPTION` TEXT,
  UNIQUE (`STABLE_ID`)
);

-- --------------------------------------------------------
CREATE TABLE `gene_panel_list` (
  `INTERNAL_ID` INTEGER NOT NULL,
  `GENE_ID` INTEGER NOT NULL,
  PRIMARY KEY (`INTERNAL_ID`,`GENE_ID`),
  FOREIGN KEY (`INTERNAL_ID`) REFERENCES `gene_panel` (`INTERNAL_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `sample_profile` (
  `SAMPLE_ID` INTEGER NOT NULL,
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `PANEL_ID` INTEGER DEFAULT NULL,
  UNIQUE (`SAMPLE_ID`,`GENETIC_PROFILE_ID`),
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`PANEL_ID`) REFERENCES `gene_panel` (`INTERNAL_ID`) ON DELETE RESTRICT
);

CREATE INDEX `idx_sample_profile_sample` ON `sample_profile` (`SAMPLE_ID`);

-- --------------------------------------------------------
CREATE TABLE `structural_variant` (
  `INTERNAL_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `SAMPLE_ID` INTEGER NOT NULL,
  `SITE1_ENTREZ_GENE_ID` INTEGER,
  `SITE1_ENSEMBL_TRANSCRIPT_ID` varchar(25),
  `SITE1_CHROMOSOME` varchar(5),
  `SITE1_REGION` varchar(25),
  `SITE1_REGION_NUMBER` INTEGER,
  `SITE1_CONTIG` varchar(100),
  `SITE1_POSITION` INTEGER,
  `SITE1_DESCRIPTION` varchar(255),
  `SITE2_ENTREZ_GENE_ID` INTEGER,
  `SITE2_ENSEMBL_TRANSCRIPT_ID` varchar(25),
  `SITE2_CHROMOSOME` varchar(5),
  `SITE2_REGION` varchar(25),
  `SITE2_REGION_NUMBER` INTEGER,
  `SITE2_CONTIG` varchar(100),
  `SITE2_POSITION` INTEGER,
  `SITE2_DESCRIPTION` varchar(255),
  `SITE2_EFFECT_ON_FRAME` varchar(25),
  `NCBI_BUILD` varchar(10),
  `DNA_SUPPORT` varchar(3),
  `RNA_SUPPORT` varchar(3),
  `NORMAL_READ_COUNT` INTEGER,
  `TUMOR_READ_COUNT` INTEGER,
  `NORMAL_VARIANT_COUNT` INTEGER,
  `TUMOR_VARIANT_COUNT` INTEGER,
  `NORMAL_PAIRED_END_READ_COUNT` INTEGER,
  `TUMOR_PAIRED_END_READ_COUNT` INTEGER,
  `NORMAL_SPLIT_READ_COUNT` INTEGER,
  `TUMOR_SPLIT_READ_COUNT` INTEGER,
  `ANNOTATION` varchar(255),
  `BREAKPOINT_TYPE` varchar(25),
  `CONNECTION_TYPE` varchar(25),
  `EVENT_INFO` varchar(255),
  `CLASS` varchar(25),
  `LENGTH` INTEGER,
  `COMMENTS` varchar(255),
  `SV_STATUS` varchar(25) NOT NULL DEFAULT 'SOMATIC',
  `ANNOTATION_JSON` TEXT,
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SITE1_ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SITE2_ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `alteration_driver_annotation` (
  `ALTERATION_EVENT_ID` INTEGER NOT NULL,
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `SAMPLE_ID` INTEGER NOT NULL,
  `DRIVER_FILTER` VARCHAR(20),
  `DRIVER_FILTER_ANNOTATION` VARCHAR(80),
  `DRIVER_TIERS_FILTER` VARCHAR(50),
  `DRIVER_TIERS_FILTER_ANNOTATION` VARCHAR(80),
  PRIMARY KEY (`ALTERATION_EVENT_ID`,`GENETIC_PROFILE_ID`,`SAMPLE_ID`),
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

CREATE INDEX `idx_driver_filter` ON `alteration_driver_annotation` (`DRIVER_FILTER`);
CREATE INDEX `idx_driver_tiers_filter` ON `alteration_driver_annotation` (`DRIVER_TIERS_FILTER`);

-- --------------------------------------------------------
CREATE TABLE `mutation_event` (
  `MUTATION_EVENT_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `CHR` varchar(5),
  `START_POSITION` bigint,
  `END_POSITION` bigint,
  `REFERENCE_ALLELE` TEXT,
  `TUMOR_SEQ_ALLELE` TEXT,
  `PROTEIN_CHANGE` varchar(255),
  `MUTATION_TYPE` varchar(255),
  `NCBI_BUILD` varchar(10),
  `STRAND` varchar(2),
  `VARIANT_TYPE` varchar(15),
  `DB_SNP_RS` varchar(25),
  `DB_SNP_VAL_STATUS` varchar(255),
  `REFSEQ_MRNA_ID` varchar(64),
  `CODON_CHANGE` varchar(255),
  `UNIPROT_ACCESSION` varchar(64),
  `PROTEIN_POS_START` INTEGER,
  `PROTEIN_POS_END` INTEGER,
  `CANONICAL_TRANSCRIPT` INTEGER,
  `KEYWORD` varchar(255) DEFAULT NULL,
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`)
);

CREATE INDEX `idx_mutation_event_keyword` ON `mutation_event` (`KEYWORD`);
CREATE INDEX `idx_mutation_event_details` ON `mutation_event` (`CHR`,`START_POSITION`,`END_POSITION`,`ENTREZ_GENE_ID`,`PROTEIN_CHANGE`,`MUTATION_TYPE`);
CREATE INDEX `idx_mutation_type` ON `mutation_event` (`MUTATION_TYPE`);

-- --------------------------------------------------------
CREATE TABLE `mutation` (
  `MUTATION_EVENT_ID` INTEGER NOT NULL,
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `SAMPLE_ID` INTEGER NOT NULL,
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `CENTER` varchar(100),
  `SEQUENCER` varchar(255),
  `MUTATION_STATUS` varchar(25),
  `VALIDATION_STATUS` varchar(25),
  `TUMOR_SEQ_ALLELE1` TEXT,
  `TUMOR_SEQ_ALLELE2` TEXT,
  `MATCHED_NORM_SAMPLE_BARCODE` varchar(255),
  `MATCH_NORM_SEQ_ALLELE1` TEXT,
  `MATCH_NORM_SEQ_ALLELE2` TEXT,
  `TUMOR_VALIDATION_ALLELE1` TEXT,
  `TUMOR_VALIDATION_ALLELE2` TEXT,
  `MATCH_NORM_VALIDATION_ALLELE1` TEXT,
  `MATCH_NORM_VALIDATION_ALLELE2` TEXT,
  `VERIFICATION_STATUS` varchar(10),
  `SEQUENCING_PHASE` varchar(100),
  `SEQUENCE_SOURCE` varchar(255) NOT NULL,
  `VALIDATION_METHOD` varchar(255),
  `SCORE` varchar(100),
  `BAM_FILE` varchar(255),
  `TUMOR_ALT_COUNT` INTEGER,
  `TUMOR_REF_COUNT` INTEGER,
  `NORMAL_ALT_COUNT` INTEGER,
  `NORMAL_REF_COUNT` INTEGER,
  `AMINO_ACID_CHANGE` varchar(255),
  `ANNOTATION_JSON` TEXT,
  UNIQUE (`MUTATION_EVENT_ID`,`GENETIC_PROFILE_ID`,`SAMPLE_ID`),
  FOREIGN KEY (`MUTATION_EVENT_ID`) REFERENCES `mutation_event` (`MUTATION_EVENT_ID`),
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`),
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

CREATE INDEX `idx_mutation_genetic_profile_gene` ON `mutation` (`GENETIC_PROFILE_ID`,`ENTREZ_GENE_ID`);
CREATE INDEX `idx_mutation_genetic_profile_sample` ON `mutation` (`GENETIC_PROFILE_ID`,`SAMPLE_ID`);
CREATE INDEX `idx_mutation_genetic_profile` ON `mutation` (`GENETIC_PROFILE_ID`);
CREATE INDEX `idx_mutation_gene` ON `mutation` (`ENTREZ_GENE_ID`);
CREATE INDEX `idx_mutation_sample` ON `mutation` (`SAMPLE_ID`);
CREATE INDEX `idx_mutation_event` ON `mutation` (`MUTATION_EVENT_ID`);

-- --------------------------------------------------------
CREATE TABLE `mutation_count_by_keyword` (
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `KEYWORD` varchar(255) DEFAULT NULL,
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `KEYWORD_COUNT` INTEGER NOT NULL,
  `GENE_COUNT` INTEGER NOT NULL,
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`) ON DELETE CASCADE
);

CREATE INDEX `idx_mutation_count_profile_keyword` ON `mutation_count_by_keyword` (`GENETIC_PROFILE_ID`,`KEYWORD`);

-- --------------------------------------------------------
CREATE TABLE `clinical_patient` (
  `INTERNAL_ID` INTEGER NOT NULL,
  `ATTR_ID` varchar(255) NOT NULL,
  `ATTR_VALUE` varchar(255) NOT NULL,
  PRIMARY KEY (`INTERNAL_ID`,`ATTR_ID`),
  FOREIGN KEY (`INTERNAL_ID`) REFERENCES `patient` (`INTERNAL_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `clinical_sample` (
  `INTERNAL_ID` INTEGER NOT NULL,
  `ATTR_ID` varchar(255) NOT NULL,
  `ATTR_VALUE` varchar(255) NOT NULL,
  PRIMARY KEY (`INTERNAL_ID`,`ATTR_ID`),
  FOREIGN KEY (`INTERNAL_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `clinical_attribute_meta` (
  `ATTR_ID` varchar(255) NOT NULL,
  `DISPLAY_NAME` varchar(255) NOT NULL,
  `DESCRIPTION` varchar(2048) NOT NULL,
  `DATATYPE` varchar(255) NOT NULL,
  `PATIENT_ATTRIBUTE` INTEGER NOT NULL,
  `PRIORITY` varchar(255) NOT NULL,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  PRIMARY KEY (`ATTR_ID`,`CANCER_STUDY_ID`),
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `mut_sig` (
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `RANK` INTEGER NOT NULL,
  `NumBasesCovered` INTEGER NOT NULL,
  `NumMutations` INTEGER NOT NULL,
  `P_VALUE` REAL NOT NULL,
  `Q_VALUE` REAL NOT NULL,
  PRIMARY KEY (`CANCER_STUDY_ID`,`ENTREZ_GENE_ID`),
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`)
);

-- --------------------------------------------------------
CREATE TABLE `gistic` (
  `GISTIC_ROI_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `CHROMOSOME` INTEGER NOT NULL,
  `CYTOBAND` varchar(255) NOT NULL,
  `WIDE_PEAK_START` INTEGER NOT NULL,
  `WIDE_PEAK_END` INTEGER NOT NULL,
  `Q_VALUE` REAL NOT NULL,
  `AMP` INTEGER NOT NULL,
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `gistic_to_gene` (
  `GISTIC_ROI_ID` INTEGER NOT NULL,
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  PRIMARY KEY(`GISTIC_ROI_ID`,`ENTREZ_GENE_ID`),
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`),
  FOREIGN KEY (`GISTIC_ROI_ID`) REFERENCES `gistic` (`GISTIC_ROI_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `cna_event` (
  `CNA_EVENT_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `ALTERATION` INTEGER NOT NULL,
  UNIQUE (`ENTREZ_GENE_ID`,`ALTERATION`),
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`)
);

CREATE INDEX `idx_cna_alteration` ON `cna_event` (`ALTERATION`);

-- --------------------------------------------------------
CREATE TABLE `sample_cna_event` (
  `CNA_EVENT_ID` INTEGER NOT NULL,
  `SAMPLE_ID` INTEGER NOT NULL,
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `ANNOTATION_JSON` TEXT,
  PRIMARY KEY (`CNA_EVENT_ID`,`SAMPLE_ID`,`GENETIC_PROFILE_ID`),
  FOREIGN KEY (`CNA_EVENT_ID`) REFERENCES `cna_event` (`CNA_EVENT_ID`),
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

CREATE INDEX `idx_sample_cna_profile_sample` ON `sample_cna_event` (`GENETIC_PROFILE_ID`,`SAMPLE_ID`);

-- --------------------------------------------------------
CREATE TABLE `copy_number_seg` (
  `SEG_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `SAMPLE_ID` INTEGER NOT NULL,
  `CHR` varchar(5) NOT NULL,
  `START` INTEGER NOT NULL,
  `END` INTEGER NOT NULL,
  `NUM_PROBES` INTEGER NOT NULL,
  `SEGMENT_MEAN` REAL NOT NULL,
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

CREATE INDEX `idx_copy_number_seg_study_sample` ON `copy_number_seg` (`CANCER_STUDY_ID`,`SAMPLE_ID`);

-- --------------------------------------------------------
CREATE TABLE `copy_number_seg_file` (
  `SEG_FILE_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `REFERENCE_GENOME_ID` varchar(10) NOT NULL,
  `DESCRIPTION` varchar(255) NOT NULL,
  `FILENAME` varchar(255) NOT NULL,
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `clinical_event` (
  `CLINICAL_EVENT_ID` INTEGER PRIMARY KEY AUTOINCREMENT,
  `PATIENT_ID` INTEGER NOT NULL,
  `START_DATE` INTEGER NOT NULL,
  `STOP_DATE` INTEGER,
  `EVENT_TYPE` varchar(50) NOT NULL,
  FOREIGN KEY (`PATIENT_ID`) REFERENCES `patient` (`INTERNAL_ID`) ON DELETE CASCADE
);

CREATE INDEX `idx_clinical_event_patient_type` ON `clinical_event` (`PATIENT_ID`,`EVENT_TYPE`);

-- --------------------------------------------------------
CREATE TABLE `clinical_event_data` (
  `CLINICAL_EVENT_ID` INTEGER NOT NULL,
  `KEY` varchar(255) NOT NULL,
  `VALUE` varchar(5000) NOT NULL,
  FOREIGN KEY (`CLINICAL_EVENT_ID`) REFERENCES `clinical_event` (`CLINICAL_EVENT_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `reference_genome_gene` (
  `ENTREZ_GENE_ID` INTEGER NOT NULL,
  `REFERENCE_GENOME_ID` INTEGER NOT NULL,
  `CHR` varchar(5) DEFAULT NULL,
  `CYTOBAND` varchar(64) DEFAULT NULL,
  `START` bigint DEFAULT NULL,
  `END` bigint DEFAULT NULL,
  PRIMARY KEY (`ENTREZ_GENE_ID`,`REFERENCE_GENOME_ID`),
  FOREIGN KEY (`REFERENCE_GENOME_ID`) REFERENCES `reference_genome` (`REFERENCE_GENOME_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`ENTREZ_GENE_ID`) REFERENCES `gene` (`ENTREZ_GENE_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `data_access_tokens` (
  `TOKEN` varchar(50) NOT NULL,
  `USERNAME` varchar(128) NOT NULL,
  `EXPIRATION` datetime NOT NULL,
  `CREATION` datetime NOT NULL,
  PRIMARY KEY (`TOKEN`),
  FOREIGN KEY (`USERNAME`) REFERENCES `users` (`EMAIL`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `allele_specific_copy_number` (
  `MUTATION_EVENT_ID` INTEGER NOT NULL,
  `GENETIC_PROFILE_ID` INTEGER NOT NULL,
  `SAMPLE_ID` INTEGER NOT NULL,
  `ASCN_INTEGER_COPY_NUMBER` INTEGER DEFAULT NULL,
  `ASCN_METHOD` varchar(24) NOT NULL,
  `CCF_EXPECTED_COPIES_UPPER` REAL DEFAULT NULL,
  `CCF_EXPECTED_COPIES` REAL DEFAULT NULL,
  `CLONAL` varchar(16) DEFAULT NULL,
  `MINOR_COPY_NUMBER` INTEGER DEFAULT NULL,
  `EXPECTED_ALT_COPIES` INTEGER DEFAULT NULL,
  `TOTAL_COPY_NUMBER` INTEGER DEFAULT NULL,
  UNIQUE (`MUTATION_EVENT_ID`,`GENETIC_PROFILE_ID`,`SAMPLE_ID`),
  FOREIGN KEY (`MUTATION_EVENT_ID`) REFERENCES `mutation_event` (`MUTATION_EVENT_ID`),
  FOREIGN KEY (`GENETIC_PROFILE_ID`) REFERENCES `genetic_profile` (`GENETIC_PROFILE_ID`) ON DELETE CASCADE,
  FOREIGN KEY (`SAMPLE_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `info` (
  `DB_SCHEMA_VERSION` varchar(24),
  `GENESET_VERSION` varchar(24),
  `DERIVED_TABLE_SCHEMA_VERSION` varchar(24),
  `GENE_TABLE_VERSION` varchar(24)
);

-- --------------------------------------------------------
CREATE TABLE `resource_definition` (
  `RESOURCE_ID` varchar(255) NOT NULL,
  `DISPLAY_NAME` varchar(255) NOT NULL,
  `DESCRIPTION` varchar(2048) DEFAULT NULL,
  `RESOURCE_TYPE` TEXT CHECK(`RESOURCE_TYPE` IN ('STUDY','PATIENT','SAMPLE')) NOT NULL,
  `OPEN_BY_DEFAULT` INTEGER DEFAULT 0,
  `PRIORITY` INTEGER NOT NULL,
  `CANCER_STUDY_ID` INTEGER NOT NULL,
  `CUSTOM_METADATA` TEXT,
  PRIMARY KEY (`RESOURCE_ID`,`CANCER_STUDY_ID`),
  FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `resource_sample` (
  `INTERNAL_ID` INTEGER NOT NULL,
  `RESOURCE_ID` varchar(255) NOT NULL,
  `URL` varchar(255) NOT NULL,
  PRIMARY KEY (`INTERNAL_ID`,`RESOURCE_ID`,`URL`),
  FOREIGN KEY (`INTERNAL_ID`) REFERENCES `sample` (`INTERNAL_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `resource_patient` (
  `INTERNAL_ID` INTEGER NOT NULL,
  `RESOURCE_ID` varchar(255) NOT NULL,
  `URL` varchar(255) NOT NULL,
  PRIMARY KEY (`INTERNAL_ID`,`RESOURCE_ID`,`URL`),
  FOREIGN KEY (`INTERNAL_ID`) REFERENCES `patient` (`INTERNAL_ID`) ON DELETE CASCADE
);

-- --------------------------------------------------------
CREATE TABLE `resource_study` (
  `INTERNAL_ID` INTEGER NOT NULL,
  `RESOURCE_ID` varchar(255) NOT NULL,
  `URL` varchar(255) NOT NULL,
  PRIMARY KEY (`INTERNAL_ID`,`RESOURCE_ID`,`URL`),
  FOREIGN KEY (`INTERNAL_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
);

-- DB_SCHEMA_VERSION AND DERIVED_TABLE_SCHEMA_VERSION MUST BE KEPT IN SYNC WITH THE db.version AND derived_table.version PROPERTIES IN pom.xml
INSERT INTO `info` (`DB_SCHEMA_VERSION`, `GENESET_VERSION`, `DERIVED_TABLE_SCHEMA_VERSION`, `GENE_TABLE_VERSION`)
  VALUES ('2.14.5', NULL, '1.0.2', NULL);

PRAGMA foreign_keys = ON;
