-- LICENSE_TBD --

SET SESSION sql_mode = 'ANSI_QUOTES';

/* we should not clean these tables
-- DELETE FROM IM_cell;
-- DELETE FROM IM_cell_entity;
*/

DELETE FROM IM_cell_alias;
DELETE FROM IM_cell_profile;
DELETE FROM IM_cell_alteration;

-- Insertion based on meta_linear_CRA.txt
-- one existing cell profile
INSERT INTO `IM_cell_profile` VALUES (1, 'linear_CRA', 1, 'CELL_RELATIVE_ABUNDANCE', 'CONTINUOUS', 'Relative immune cell abundance values from CiberSort', 'Relative linear relative abundance values (0 to 1) for each cell type', 0);
INSERT INTO `IM_cell_profile` VALUES (2, 'linear_CRA_test', 1, 'CELL_RELATIVE_ABUNDANCE', 'CONTINUOUS', 'Relative immune cell abundance values from CiberSort', 'Relative linear relative abundance values (0 to 1) for each cell type', 0);

-- Insertion based on data_linear_CRA.txt
-- six samples were measured in the cell profile
INSERT INTO `IM_sample_cell_profile` VALUES(1, 1, NULL);
INSERT INTO `IM_sample_cell_profile` VALUES(2, 1, NULL);
INSERT INTO `IM_sample_cell_profile` VALUES(3, 1, NULL);
INSERT INTO `IM_sample_cell_profile` VALUES(4, 1, NULL);
INSERT INTO `IM_sample_cell_profile` VALUES(5, 1, NULL);
INSERT INTO `IM_sample_cell_profile` VALUES(6, 1, NULL);
-- also insert into IM_cell_profile_samples;
INSERT INTO `IM_cell_profile_samples` VALUES(1, '1,2,3,4,5,6,');

-- Insertion based on data_linear_CRA.txt
-- 5 cell entity were measured by the cell profile
-- must use single quote
INSERT INTO `IM_cell_alteration` VALUES(1, 1, '0.65985,NA,NA,NA,NA,NA,');
INSERT INTO `IM_cell_alteration` VALUES(1, 2, '0.63916,0.25981,0.08660,0.08660,0.08660,0.08660,');
INSERT INTO `IM_cell_alteration` VALUES(1, 3, 'NA,NA,NA,NA,NA,NA,');
INSERT INTO `IM_cell_alteration` VALUES(1, 4, '0.6205,0.08660,0.25981,0.08660,0.08660,0.08660,');
INSERT INTO `IM_cell_alteration` VALUES(1, 5, '0.9240,0.04330,0.08660,0.08660,0.08660,0.08660,');
