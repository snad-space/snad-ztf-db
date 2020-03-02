from typing import Optional

from aiochclient import ChClient, Record
from aiohttp import ClientSession
from aiohttp.web import Application, Response, json_response, RouteTableDef, Request


MAX_RADIUS = 60


FILTERS = {1: 'zg', 2: 'zr', 3: 'zi'}


LC_FIELDS = {'mjd', 'mag', 'magerr', 'clrcoeff', 'catflags'}


routes = RouteTableDef()


def records_to_dict(records: [Record]) -> dict:
    if len(records) == 0:
        return {}
    first = records[0]
    meta = dict(
        nobs=first['nobs'],
        ngoodobs=len(records),
        filter=FILTERS[first['filter']],
        fieldid=first['fieldid'],
        rcid=first['rcid'],
        coord=dict(ra=first['ra'], dec=first['dec']),
    )
    lc = [dict(
        mjd=record['mjd'],
        mag=record['mag'],
        magerr=record['magerr'],
        clrcoeff=record['clrcoeff'],
        catflags=record['catflags'],
    ) for record in records]
    return dict(meta=meta, lc=lc)


async def get_by_oid(client: ChClient, oid: int) -> dict:
    records = await client.fetch(f"""
        WITH
        (
            SELECT h3index10
            FROM dr2_meta
            WHERE oid = {oid:d}
        ) AS h3
        SELECT *
        FROM dr2
        WHERE h3index10 = h3 AND oid = {oid:d} AND catflags = 0
        ORDER BY mjd
    """)
    return records_to_dict(records)


async def get_meta_for_oids(client: ChClient, oids: [str]) -> [dict]:
    oids_array = '(' + ', '.join(oids) + ')'
    records = await client.fetch(f"""
        SELECT *
        FROM dr2_meta
        WHERE oid IN {oids_array}
    """)
    return [dict(r) for r in records]


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
    oids = request.query.getall('oid', None)
    if oids is None:
        return Response(text='Query string should has at least one "oid" field', status=404)
    for oid in oids:
        try:
            oid = int(oid)
        except ValueError:
            return Response(text=f'oid value "{oid}" cannot be converted to int', status=404)
    metas = await get_meta_for_oids(request.app['ch_client'], oids)
    data = {}
    for meta in metas:
        oid = meta['oid']
        h3index10 = meta['h3index10']
        lc = await get_lc_for_oid_h3index10(request.app['ch_client'], oid, h3index10)
        data[oid] = dict(
            meta=dict(
                nobs=meta['nobs'],
                ngoodobs=meta['ngoodobs'],
                filter=FILTERS[meta['filter']],
                fieldid=meta['fieldid'],
                rcid=meta['rcid'],
                coord=dict(ra=meta['ra'], dec=meta['dec']),
                h3={10: h3index10},
            ),
            lc=[{k: v for k, v in obs.items() if k in LC_FIELDS} for obs in lc],
        )
    return json_response(data)


async def app_on_startup(app: Application):
    app['ch_http_session'] = ClientSession()
    app['ch_client'] = ChClient(app['ch_http_session'], url='http://snad.sai.msu.ru:8123', database='ztf', user='api')


async def app_on_cleanup(app: Application):
    await app['ch_http_session'].close()
