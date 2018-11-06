-- The option of replication will be adjusted according to ops.
CREATE KEYSPACE dfs WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE dupl (
    id text,
    refid text,
    size bigint,
    omain bigint,
    date timestamp,
    RIMARY KEY(id)
) WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'};

CREATE TABLE rc (
    id text,
    refcnt counter,
    PRIMARY KEY(id)
) WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'};

CREATE TABLE dfs.files (
    id text,
    domain bigint,
    attrs map<text, text>,
    biz text,
    cksize int,
    fn text,
    md5 text,
    size bigint,
    type tinyint,
    udate timestamp,
    uid text,
    PRIMARY KEY (id, domain)
) WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'};

CREATE MATERIALIZED VIEW dfs.md5a AS
    SELECT *
    FROM dfs.files
    WHERE id IS NOT NULL AND md5 IS NOT NULL AND domain IS NOT NULL
    PRIMARY KEY (md5, domain, id)
    WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'};

CREATE TABLE health (
    id text,
    magic bigint,
    PRIMARY KEY(id)
);
