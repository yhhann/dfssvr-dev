/*
schema:

CREATE DATABASE file CHARSET=UTF8;

CREATE TABLE file (
  id varchar(50) NOT NULL,
  name varchar(255) DEFAULT NULL,
  biz varchar(50) NOT NULL,
  md5 varchar(50) NOT NULL,
  uid varchar(30) NOT NULL,
  domain bigint(20) UNSIGNED NOT NULL,
  size int(10) UNSIGNED NOT NULL,
  chunksize int(10) UNSIGNED NOT NULL,
  refcnt int(10) UNSIGNED NOT NULL,
  uploadtime timestamp(6) NULL DEFAULT NULL,
  entitytype tinyint(4) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_md5_domain_uploadtime (md5, domain, uploadtime),
  KEY idx_domain (domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE dupl (
  did varchar(50) NOT NULL,
  fid varchar(50) NOT NULL,
  createdtime timestamp(6) NULL DEFAULT NULL,
  PRIMARY KEY (dId),
  KEY idx_dupl_fid (fid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE attr (
  id bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  fid varchar(50) NOT NULL,
  k varchar(255) DEFAULT NULL,
  v varchar(255) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY idx_attr_fid (fid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

*/

package sql
