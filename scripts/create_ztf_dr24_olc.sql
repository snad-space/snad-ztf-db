CREATE TABLE ztf.dr24_olc
(
    `oid` UInt64 CODEC(Delta(8), LZ4),
    `filter` UInt8 CODEC(T64, LZ4),
    `fieldid` UInt16 CODEC(T64, LZ4),
    `rcid` UInt8 CODEC(Delta(1), LZ4),
    `ra` Float64 CODEC(Gorilla(8)),
    `dec` Float64 CODEC(Gorilla(8)),
    `nobs_w_bad` UInt16 CODEC(T64, LZ4),
    `h3index10` UInt64 MATERIALIZED geoToH3(ra, dec, 10) CODEC(Delta(8), LZ4),
    `mjd` Array(Float64),
    `mag` Array(Float32),
    `magerr` Array(Float32),
    `clrcoeff` Array(Float32),
    `ngoodobs` UInt16 MATERIALIZED length(mjd) CODEC(T64, LZ4),
    `durgood` Float64 MATERIALIZED arrayMax(mjd) - arrayMin(mjd) CODEC(Gorilla(8)),
    `mingoodmag` Float32 MATERIALIZED arrayMin(mag) CODEC(Gorilla(4)),
    `maxgoodmag` Float32 MATERIALIZED arrayMax(mag) CODEC(Gorilla(4)),
    `meangoodmag` Float32 MATERIALIZED arrayAvg(mag) CODEC(Gorilla(4))
)
ENGINE = MergeTree
PARTITION BY fieldid
PRIMARY KEY h3index10
ORDER BY (h3index10, oid)
SETTINGS index_granularity = 8192
