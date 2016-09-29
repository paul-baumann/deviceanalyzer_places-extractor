# ************************************************************
# Sequel Pro SQL dump
# Version 4135
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: 127.0.0.1 (MySQL 5.5.52-0ubuntu0.14.04.1)
# Database: Cambridge_PB
# Generation Time: 2016-09-29 12:53:24 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table _network_to_placeid
# ------------------------------------------------------------

DROP TABLE IF EXISTS `_network_to_placeid`;

CREATE TABLE `_network_to_placeid` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `place_id` int(11) DEFAULT NULL,
  `network_id` varchar(50) DEFAULT '',
  `is_connected` tinyint(4) DEFAULT NULL,
  `is_wifi` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `place_id` (`place_id`),
  KEY `is_connected` (`is_connected`),
  KEY `is_wifi` (`is_wifi`),
  KEY `network_id` (`network_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table _timeslot_to_placeid
# ------------------------------------------------------------

DROP TABLE IF EXISTS `_timeslot_to_placeid`;

CREATE TABLE `_timeslot_to_placeid` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `unique_index` bigint(20) DEFAULT NULL,
  `place_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `unique_index` (`unique_index`),
  KEY `place_id` (`place_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



# Dump of table _timeslot_to_placeid_all
# ------------------------------------------------------------

DROP TABLE IF EXISTS `_timeslot_to_placeid_all`;

CREATE TABLE `_timeslot_to_placeid_all` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `unique_index` bigint(20) DEFAULT NULL,
  `place_id_wifi_connected` int(11) DEFAULT NULL,
  `place_id_wifi` int(11) DEFAULT NULL,
  `place_id_cell` int(11) DEFAULT NULL,
  `place_id_all_combined` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
