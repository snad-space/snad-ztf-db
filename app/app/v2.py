from typing import Iterable

from aiochclient import ChClient
from aiohttp import ClientSession, ClientConnectorError
from aiohttp.web import Application, Response, json_response, RouteTableDef, Request, HTTPInternalServerError

from .clichouse_host import CLICKHOUSE_HOST
from .util import oids_from_request, ra_dec_radius_from_request, try_for_a_while

MAX_RADIUS = 60


FILTERS = {1: 'zg', 2: 'zr', 3: 'zi'}


LC_FIELDS = {'mjd', 'mag', 'magerr', 'clrcoeff', 'catflags'}


routes = RouteTableDef()


@routes.get('/api/v2/help')
async def api_help(request) -> Response:
    return Response(
        text=f'''
            <h1>Available resources</h1>
            <h2><font face='monospace'>/api/v2/oid/full/json</font></h2>
                <p>Get json with the whole objects data by their identifiers</p>
                <p>Query parameters:</p>
                <ul>
                    <li>
                        <font face='monospace'>oid</font>
                        &mdash;
                        object identifier (OID).
                        Mandatory, multiple values accepted
                    </li>
                </ul>
                <p>Example: <font face='monospace'><a href="/api/v2/oid/full/json?oid=830202400008402">/api/v2/oid/full/json?oid=830202400008402</a></font></p>
            <h2><font face='monospace'>/api/v2/circle/full/json</font></h2>
                <p>Find objects in circle and return json with the whole data</p>
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
                <p>Example: <font face='monospace'><a href="/api/v2/circle/full/json?ra=10&dec=30&radius_arcsec=10">/api/v2/circle/full/json?ra=10&dec=30&radius_arcsec=10</a></font>
        ''',
        content_type='text/html',
    )


async def get_meta_for_oids(client: ChClient, oids: Iterable[int]) -> [dict]:
    oids_array = '(' + ', '.join(map(str, oids)) + ')'
    records = await client.fetch(f"""
        SELECT *
        FROM dr2_meta
        WHERE oid IN {oids_array}
    """)
    return [dict(r) for r in records]


def prepare_meta(meta: dict) -> dict:
    return dict(
        nobs=meta['nobs'],
        ngoodobs=meta['ngoodobs'],
        duration=meta['durgood'],
        filter=FILTERS[meta['filter']],
        fieldid=meta['fieldid'],
        rcid=meta['rcid'],
        coord=dict(ra=meta['ra'], dec=meta['dec']),
        h3={10: meta['h3index10']},
    )


async def get_lc_for_oid_h3index10(client: ChClient, oid: int, h3index10: int) -> [dict]:
    records = await client.fetch(f"""
        SELECT *
        FROM dr2
        WHERE h3index10 = {h3index10:d} AND oid = {oid:d} AND catflags = 0
        ORDER BY mjd
    """)
    return [dict(r) for r in records]


@routes.get('/api/v2/oid/full/json')
async def oid_full_json(request: Request) -> Response:
    oids = oids_from_request(request)
    metas = await get_meta_for_oids(request.app['ch_client'], oids)
    data = {}
    for meta in metas:
        oid = meta['oid']
        h3index10 = meta['h3index10']
        lc = await get_lc_for_oid_h3index10(request.app['ch_client'], oid, h3index10)
        data[oid] = dict(
            meta=prepare_meta(meta),
            lc=[{k: v for k, v in obs.items() if k in LC_FIELDS} for obs in lc],
        )
    return json_response(data)


async def get_lcs_in_circle(client: ChClient, ra: float, dec: float, radius_arcsec: float) -> [dict]:
    radius_deg = radius_arcsec / 3600.0
    records = await client.fetch(f"""
        SELECT *
        FROM dr2
        WHERE h3index10 IN
        (
            SELECT arrayJoin(h3kRing(geoToH3({ra:f}, {dec:f}, 10), toUInt8({radius_deg:f} / h3EdgeAngle(10)) + 1))
        ) AND greatCircleAngle({ra:f}, {dec:f}, ra, dec) < {radius_deg:f}
        ORDER BY (oid, mjd)
    """)
    return [dict(r) for r in records]


@routes.get('/api/v2/circle/full/json')
async def circle_full_json(request: Request) -> Response:
    ra, dec, radius = ra_dec_radius_from_request(request, MAX_RADIUS)
    lcs = await get_lcs_in_circle(request.app['ch_client'], ra, dec, radius)
    oids = set(obs['oid'] for obs in lcs)
    metas = await get_meta_for_oids(request.app['ch_client'], oids)
    metas = {meta['oid']: meta for meta in metas}
    if oids != set(metas):
        raise HTTPInternalServerError(reason='dr2 and dr2_meta return different oids')
    data = {}
    for obs in lcs:
        obj = data.setdefault(obs['oid'], dict(meta=prepare_meta(metas[obs['oid']])))
        lc = obj.setdefault('lc', [])
        lc.append({k: v for k, v in obs.items() if k in LC_FIELDS})
    return json_response(data)


async def app_on_startup(app: Application):
    app['ch_http_session'] = ClientSession()

    async def ch_client():
        client = ChClient(app['ch_http_session'], url=f'http://{CLICKHOUSE_HOST}:8123', database='ztf', user='api')
        await client.fetch('SELECT 1')
        return client

    app['ch_client'] = await try_for_a_while(
        ch_client,
        wait_for=1,
        interval=1,
        exception=ClientConnectorError,
    )


async def app_on_cleanup(app: Application):
    await app['ch_http_session'].close()
