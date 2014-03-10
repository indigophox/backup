PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS host_mappings;
DROP TABLE IF EXISTS backups;

create table host_mappings (
  host             TEXT      UNIQUE  PRIMARY KEY  NOT NULL,
  connect_as       TEXT      NOT NULL
);

create table backups (
  /* Required */
  name             TEXT      UNIQUE  PRIMARY KEY  NOT NULL,
  host             TEXT      REFERENCES host_mappings(host)  NOT NULL,
  zpool            TEXT      NOT NULL,
  enable           TEXT      NOT NULL  CHECK (enable IN ('enabled','disabled','error')),

  /* Optional */                             
  previous_backup  TEXT      DEFAULT NULL,
  next_backup      TEXT      DEFAULT NULL,

  /* Should not be edited by user */
  lock_pid         INT       DEFAULT NULL,
  failed_trylocks  INT       DEFAULT 0  CHECK(failed_trylocks>=0),
  last_error       TEXT      DEFAULT NULL,
  last_start       INT       DEFAULT NULL,
  last_finish      INT       DEFAULT NULL,
  first_backup     TEXT      DEFAULT NULL,

  /* DO NOT manually set this - this is automatic bookkeeping */
  added            DATETIME  DEFAULT CURRENT_TIMESTAMP
);
