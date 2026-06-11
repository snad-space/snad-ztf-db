CREATE VIEW ztf.dr24
(
    `oid` UInt64,
    `nobs` UInt16,
    `filter` UInt8,
    `fieldid` UInt16,
    `rcid` UInt8,
    `ra` Float64,
    `dec` Float64,
    `h3index10` UInt64,
    `mjd` Float64,
    `mag` Float32,
    `magerr` Float32,
    `clrcoeff` Float32,
    `catflags` UInt8
)
AS SELECT
    oid,
    nobs_w_bad AS nobs,
    filter,
    fieldid,
    rcid,
    ra,
    dec,
    h3index10,
    mjd,
    mag,
    magerr,
    clrcoeff,
    0 AS catflags
FROM ztf.dr24_olc
ARRAY JOIN
    mjd,
    mag,
    magerr,
    clrcoeff
ORDER BY
    h3index10 ASC,
    oid ASC,
    mjd ASC
