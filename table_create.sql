CREATE TABLE `alpha` (
  `id` varchar(36) NOT NULL,
  `symbol` varchar(10) DEFAULT NULL,
  `quantity` decimal(10,3) DEFAULT NULL,
  `purchasedate` varchar(10) DEFAULT NULL,
  `purchaseprice` decimal(10,2) DEFAULT NULL,
  `saledate` varchar(10) DEFAULT NULL,
  `saleprice` decimal(10,2) DEFAULT NULL,
  `purchaseorderid` varchar(100) DEFAULT NULL,
  `saleorderid` varchar(100) DEFAULT NULL,
  `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;