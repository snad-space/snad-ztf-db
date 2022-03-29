from typing import Iterable, Optional

from aiochclient import ChClient
from aiohttp import ClientSession, ClientConnectorError
from aiohttp.web import Application, Response, json_response, RouteTableDef, Request, HTTPInternalServerError, \
    HTTPNotFound

from .util import oids_from_request, ra_dec_radius_from_request, try_for_a_while

MAX_RADIUS = 60


FILTERS = {1: 'zg', 2: 'zr', 3: 'zi'}


LC_FIELDS = {'mjd', 'mag', 'magerr', 'clrcoeff'}


AVAILABLE_DRS = ('dr2', 'dr3', 'dr4', 'dr8', 'latest')
SHORT_META_DRS = ('dr2', 'dr3')
LATEST_DR = 'dr8'
AVAILABLE_DRS_HTML = ', '.join(f"<font face='monospace'>{dr}</font>" for dr in AVAILABLE_DRS)


HELP = f'''
    <h1>Available resources</h1>
    <h2><font face='monospace'>/api/v3/dr/list/json</font></h2>
        <p>List all available ZTF data release identifiers</p>
        <p>Example: <font face='monospace'><a href="/api/v3/dr/list/json">/api/v3/dr/list/json</a></font></p>
    <h2><font face='monospace'>/api/v3/data/:dr/oid/full/json</font></h2>
        <p>Get json with the whole objects data by their identifiers</p>
        <p>Path parameters:</p>
        <ul>
            <li>
                <font face='monospace'>:dr</font>
                &mdash;
                ZTF data release specifier.
                Could be one of: {AVAILABLE_DRS_HTML}
            </li>
        </ul>
        <p>Query parameters:</p>
        <ul>
            <li>
                <font face='monospace'>oid</font>
                &mdash;
                object identifier (OID).
                Mandatory, multiple values accepted
            </li>
        </ul>
        <p>Example: <font face='monospace'><a href="/api/v3/data/latest/oid/full/json?oid=830202400008402">/api/v3/data/latest/oid/full/json?oid=830202400008402</a></font></p>
    <h2><font face='monospace'>/api/v3/data/:dr/circle/full/json</font></h2>
        <p>Find objects in circle and return json with the whole data</p>
        <p>Path parameters:</p>
        <ul>
            <li>
                <font face='monospace'>:dr</font>
                &mdash;
                ZTF data release specifier.
                Could be one of: {AVAILABLE_DRS_HTML}
            </li>
        </ul>
        <p>Query parameters:</p>
        <ul>
            <li>
                <font face='monospace'>ra</font>
                &mdash;
                right ascension of the circle center, degrees.
                Mandatory
            </li>
            <li>
                <font face='monospace'>dec</font>
                &mdash;
                declination of the circle center, degrees.
                Mandatory
            </li>
            <li>
                <font face='monospace'>radius_arcsec</font>
                &mdash;
                circle radius, acrseconds. Should be positive and less than {MAX_RADIUS}.
                Mandatory
            </li>
        </ul>
        <p>Example: <font face='monospace'><a href="/api/v3/data/latest/circle/full/json?ra=10&dec=30&radius_arcsec=10">/api/v3/data/latest/circle/full/json?ra=10&dec=30&radius_arcsec=10</a></font>
'''


routes = RouteTableDef()


@routes.get('/api/v3/help')
async def api_help(request) -> Response:
    return Response(
        text=HELP,
        content_type='text/html',
    )


@routes.get('/api/v3/dr/list/json')
async def dr_list_json(request) -> Response:
    return json_response(AVAILABLE_DRS)


def _table_name_from_dr(dr: str) -> str:
    if dr not in AVAILABLE_DRS:
        msg = f"ZTF data release identify {dr} isn't supported"
        raise HTTPNotFound(reason=msg, body=msg)
    if dr == 'latest':
        dr = LATEST_DR
    return dr


def observation_table(dr: str) -> str:
    return _table_name_from_dr(dr)


def meta_table(dr: str) -> str:
    dr = _table_name_from_dr(dr)
    return f'{dr}_meta'


def meta_short_table(dr: str) -> Optional[str]:
    dr = _table_name_from_dr(dr)
    if dr not in SHORT_META_DRS:
        return None
    return f'{dr}_meta_short'


async def get_meta_for_oids(client: ChClient, table: str, oids: Iterable[int]) -> dict:
    oids_array = f'({",".join(map(str, oids))})'
    records = await client.fetch(f"""
        SELECT *
        FROM {table}
        WHERE oid IN {oids_array} AND ngoodobs > 0
    """)
    records = (dict(r) for r in records)
    return {r['oid']: r for r in records}


def prepare_meta(long: dict, short: Optional[dict]) -> dict:
    record = dict(
        nobs=long['nobs'],
        ngoodobs=long['ngoodobs'],
        duration=long['durgood'],
        filter=FILTERS[long['filter']],
        fieldid=long['fieldid'],
        rcid=long['rcid'],
        coord=dict(ra=long['ra'], dec=long['dec']),
        h3={10: long['h3index10']},
    )
    if short is not None:
        record.update(dict(
            ngoodobs_short=short['ngoodobs'],
            duration_short=short['durgood'],
        ))
    return record


async def get_lc_for_oid_h3index10(client: ChClient, dr: str, oid: int, h3index10: int) -> [dict]:
    table = observation_table(dr)
    records = await client.fetch(f"""
        SELECT *
        FROM {table}
        WHERE h3index10 = {h3index10:d} AND oid = {oid:d} AND catflags = 0 AND magerr > 0
        ORDER BY mjd
    """)
    return [dict(r) for r in records]


@routes.get('/api/v3/data/{dr}/oid/full/json')
async def data_dr_oid_full_json(request: Request) -> Response:
    dr = request.match_info['dr']
    oids = oids_from_request(request)
    metas = await get_meta_for_oids(request.app['ch_client'], meta_table(dr), oids)

    if metas_short := meta_short_table(dr):
        metas_short = await get_meta_for_oids(request.app['ch_client'], metas_short, oids)
    else:
        metas_short = {}

    data = {}
    for oid, meta in metas.items():
        lc = await get_lc_for_oid_h3index10(request.app['ch_client'], dr, oid, meta['h3index10'])
        data[oid] = dict(
            meta=prepare_meta(meta, metas_short.get(oid, None)),
            lc=[{k: v for k, v in obs.items() if k in LC_FIELDS} for obs in lc],
        )
    return json_response(data)


async def get_lcs_in_circle(client: ChClient, dr, ra: float, dec: float, radius_arcsec: float) -> [dict]:
    table = observation_table(dr)
    radius_deg = radius_arcsec / 3600.0
    records = await client.fetch(f"""
        SELECT *
        FROM {table}
        WHERE h3index10 IN
        (
            SELECT arrayJoin(h3kRing(geoToH3({ra:f}, {dec:f}, 10), toUInt8({radius_deg:f} / h3EdgeAngle(10)) + 1))
        ) AND greatCircleAngle({ra:f}, {dec:f}, ra, dec) < {radius_deg:f} AND catflags = 0 AND magerr > 0
        ORDER BY h3index10, oid, mjd
    """)
    return [dict(r) for r in records]


@routes.get('/api/v3/data/{dr}/circle/full/json')
async def data_dr_circle_full_json(request: Request) -> Response:
    dr = request.match_info['dr']
    ra, dec, radius = ra_dec_radius_from_request(request, MAX_RADIUS)
    lcs = await get_lcs_in_circle(request.app['ch_client'], dr, ra, dec, radius)

    if not lcs:
        return json_response({})

    oids = set(obs['oid'] for obs in lcs)

    metas = await get_meta_for_oids(request.app['ch_client'], meta_table(dr), oids)
    if oids != set(metas):
        raise HTTPInternalServerError(reason='observation and meta requests returned different oids')

    if metas_short := meta_short_table(dr):
        metas_short = await get_meta_for_oids(request.app['ch_client'], metas_short, oids)
    else:
        metas_short = {}

    data = {}
    for obs in lcs:
        oid = obs['oid']
        obj = data.setdefault(oid, dict(meta=prepare_meta(metas[oid], metas_short.get(oid, None))))
        lc = obj.setdefault('lc', [])
        lc.append({k: v for k, v in obs.items() if k in LC_FIELDS})
    return json_response(data)


async def app_on_startup(app: Application):
    app['ch_http_session'] = ClientSession()

    async def ch_client():
        client = ChClient(app['ch_http_session'], url=f'http://sai.snad.space:8123', database='ztf', user='api')
        await client.fetch('SELECT 1')
        return client

    app['ch_client'] = await try_for_a_while(
        ch_client,
        wait_for=900,
        interval=1,
        exception=ClientConnectorError,
    )


async def app_on_cleanup(app: Application):
    await app['ch_http_session'].close()
