/* LICENSE_TBD */
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;
-- MySQL dump 10.13  Distrib 5.7.25, for osx10.14 (x86_64)
--
-- Host: localhost    Database: cgds_im
-- ------------------------------------------------------
-- Server version	5.7.25

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `IM_cell_entity`
--

DROP TABLE IF EXISTS `IM_cell_entity`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `IM_cell_entity` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `ENTITY_TYPE` varchar(45) NOT NULL DEFAULT '',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `IM_cell_entity`
--

/*!40000 ALTER TABLE `IM_cell_entity` DISABLE KEYS */;
/*!40000 ALTER TABLE `IM_cell_entity` ENABLE KEYS */;

--
-- Table structure for table `IM_cell`
--

DROP TABLE IF EXISTS `IM_cell`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `IM_cell` (
  `CELL_ENTITY_ID` int(11) NOT NULL,
  `UNIQUE_CELL_ID` int(11) NOT NULL,
  `UNIQUE_CELL_NAME` varchar(255) NOT NULL,
  `TYPE` varchar(50) DEFAULT NULL,
  `ORGAN` varchar(64) DEFAULT NULL,
  `CPID` int(11) DEFAULT NULL,
  `ANATOMY_ID` varchar(255) DEFAULT NULL,
  `CELL_TYPE_ID` int(11) DEFAULT NULL,
  PRIMARY KEY (`CELL_ENTITY_ID`),
  UNIQUE KEY `UNIQUE_CELL_ID` (`UNIQUE_CELL_ID`),
  KEY `UNIQUE_CELL_NAME` (`UNIQUE_CELL_NAME`),
  CONSTRAINT `im_cell_x_entity` FOREIGN KEY (`CELL_ENTITY_ID`) REFERENCES `IM_cell_entity` (`ID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `IM_cell`
--

/*!40000 ALTER TABLE `IM_cell` DISABLE KEYS */;
/*!40000 ALTER TABLE `IM_cell` ENABLE KEYS */;

--
-- Table structure for table `IM_cell_profile`
--

DROP TABLE IF EXISTS `IM_cell_profile`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `IM_cell_profile` (
  `CELL_PROFILE_ID` int(11) NOT NULL AUTO_INCREMENT,
  `STABLE_ID` varchar(255) NOT NULL,
  `CANCER_STUDY_ID` int(11) NOT NULL,
  `CELL_ALTERATION_TYPE` varchar(255) NOT NULL,
  `DATATYPE` varchar(255) NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `DESCRIPTION` mediumtext,
  `SHOW_PROFILE_IN_ANALYSIS_TAB` tinyint(1) NOT NULL,
  PRIMARY KEY (`CELL_PROFILE_ID`),
  UNIQUE KEY `STABLE_ID` (`STABLE_ID`),
  KEY `CANCER_STUDY_ID` (`CANCER_STUDY_ID`),
  CONSTRAINT `im_cell_x_profile` FOREIGN KEY (`CANCER_STUDY_ID`) REFERENCES `cancer_study` (`CANCER_STUDY_ID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `IM_cell_profile`
--

/*!40000 ALTER TABLE `IM_cell_profile` DISABLE KEYS */;
/*!40000 ALTER TABLE `IM_cell_profile` ENABLE KEYS */;

--
-- Table structure for table `IM_cell_alteration`
--

DROP TABLE IF EXISTS `IM_cell_alteration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `IM_cell_alteration` (
  `CELL_PROFILE_ID` int(11) NOT NULL,
  `CELL_ENTITY_ID` int(11) NOT NULL,
  `VALUES` longtext NOT NULL,
  PRIMARY KEY (`CELL_PROFILE_ID`,`CELL_ENTITY_ID`),
  KEY `CELL_ENTITY_ID` (`CELL_ENTITY_ID`),
  CONSTRAINT `im_cell_alteration_x_entity` FOREIGN KEY (`CELL_ENTITY_ID`) REFERENCES `IM_cell_entity` (`ID`),
  CONSTRAINT `im_cell_alteration_x_profile` FOREIGN KEY (`CELL_PROFILE_ID`) REFERENCES `IM_cell_profile` (`CELL_PROFILE_ID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `IM_cell_alteration`
--

/*!40000 ALTER TABLE `IM_cell_alteration` DISABLE KEYS */;
/*!40000 ALTER TABLE `IM_cell_alteration` ENABLE KEYS */;

--
-- Table structure for table `IM_cell_alias`
--

DROP TABLE IF EXISTS `IM_cell_alias`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `IM_cell_alias` (
  `UNIQUE_CELL_ID` int(11) NOT NULL,
  `CELL_ALIAS` varchar(255) NOT NULL,
  PRIMARY KEY (`UNIQUE_CELL_ID`,`CELL_ALIAS`),
  CONSTRAINT `im_cell_alias_x_cell` FOREIGN KEY (`UNIQUE_CELL_ID`) REFERENCES `IM_cell` (`UNIQUE_CELL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `IM_cell_alias`
--

/*!40000 ALTER TABLE `IM_cell_alias` DISABLE KEYS */;
/*!40000 ALTER TABLE `IM_cell_alias` ENABLE KEYS */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-03-21 19:20:11
SET FOREIGN_KEY_CHECKS = 1;
