-- LICENSE_TBD --

SET SESSION sql_mode = 'ANSI_QUOTES';

/* we should not clean these tables
-- DELETE FROM IM_cell;
-- DELETE FROM IM_cell_entity;
*/

DELETE FROM IM_cell_alias;
DELETE FROM IM_cell_profile;
DELETE FROM IM_cell_alteration;

INSERT INTO `IM_cell_profile` VALUES ('1', 'linear_CRA', '1', 'CELL_RELATIVE_ABUNDANCE', 'CONTINUOUS', 'Relative immune cell abundance values from CiberSort', 'Relative linear relative abundance values (0 to 1) for each cell type', '0');

--
