CREATE TABLE ztf.dr24_meta
(
    `oid` UInt64 CODEC(Delta(8), LZ4),
    `nobs` UInt16 CODEC(T64, LZ4),
    `ngoodobs` UInt16 CODEC(T64, LZ4),
    `filter` UInt8 CODEC(T64, LZ4),
    `fieldid` UInt16 CODEC(T64, LZ4),
    `rcid` UInt8,
    `ra` Float64,
    `dec` Float64,
    `h3index10` UInt64,
    `durgood` Float64,
    `mingoodmag` Float32,
    `maxgoodmag` Float32,
    `meangoodmag` Float32
)
ENGINE = MergeTree
ORDER BY oid
SETTINGS index_granularity = 8192;

INSERT INTO ztf.dr24_meta
SELECT
    oid,
    nobs_w_bad AS nobs,
    ngoodobs,
    filter,
    fieldid,
    rcid,
    ra,
    dec,
    h3index10,
    durgood,
    mingoodmag,
    maxgoodmag,
    meangoodmag
FROM ztf.dr24_olc;
