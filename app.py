from __future__ import annotations

import json
import math as m
from dataclasses import dataclass
from functools import partial
from typing import Optional, Dict, List, Any

from aiohttp.web import Response, json_response, RouteTableDef, Application, Request
from asyncpg import create_pool, Connection, BitString


MAX_RADIUS = 60


@dataclass
class SPoint:
    ra: float
    dec: float

    @property
    def ra_rad(self) -> float:
        return m.radians(self.ra)

    @property
    def dec_rad(self) -> float:
        return m.radians(self.dec)

    def to_sql(self) -> str:
        return f'({self.ra_rad}, {self.dec_rad})'

    @staticmethod
    def from_sql(s) -> SPoint:
        s = s.strip('()')
        ra, dec = (m.degrees(float(x)) for x in s.split(','))
        return SPoint(ra=ra, dec=dec)

    def to_dict(self) -> dict:
        return {'ra': self.ra, 'dec': self.dec}


@dataclass
class SCircle:
    point: SPoint
    radius: float

    @property
    def radius_rad(self) -> float:
        return m.radians(self.radius / 3600.)

    def to_sql(self) -> str:
        return f'<{self.point.to_sql()}, {self.radius_rad}>'

    @staticmethod
    def from_sql(s) -> SCircle:
        s = s.strip('<>')
        point, radius = s.rsplit(',', maxsplit=1)
        point = SPoint.from_sql(point)
        radius = m.degrees(float(radius)) * 3600.
        return SCircle(point=point, radius=radius)


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BitString):
            return obj.to_int()
        if isinstance(obj, SPoint):
            return obj.to_dict()
        return super().default(obj)


routes = RouteTableDef()


@routes.get('/')
async def index(request) -> Response:
    return Response(
        text='''
            <p>
                Welcome on <a href="//snad.space">SNAD</a> <a href="https://www.ztf.caltech.edu/page/dr1">ZTF dr1</a></a>
            mirror page.
            </p>
            <p>
                See API details on <a href="/api/v1/help">/api/v1/help</a>
            </p>
        ''',
        content_type='text/html',
    )


@routes.get('/api/v1/help')
async def help(request) -> Response:
    return Response(
        text=f'''
            <h1>Available resources</h1>
            <h2><font face='monospace'>/api/v1/oid/full/json</font></h2>
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
            <h2><font face='monospace'>/api/v1/circle/full/json</font></h2>
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
                        circle radius, acrseconds. Should be positive and less that {MAX_RADIUS}.
                        Mandatory
                    </li>
                    <li>
                        <font face='monospace'>filter</font>
                        &mdash;
                        pathband to search.
                        Optional, multiple values accepted. If not specified then all filters are used
                    </li>
                    <li>
                        <font face='monospace'>not_filter</font>
                        &mdash;
                        excluded pathband from search. 
                        Optional, multiple values accepted
                    </li>
                    <li>
                        <font face='monospace'>fieldid</font>
                        &mdash;
                        field id to search.
                        Optional, multiple values accepted. If not specified then all field ids are used
                    </li>
                    <li>
                        <font face='monospace'>not_filter</font>
                        &mdash;
                        excluded field ids from search. 
                        Optional, multiple values accepted
                    </li>
                </ul>
        ''',
        content_type='text/html',
    )


async def get_meta_for_oid(con: Connection, oid: int, remove_oid: bool = True) -> Optional[Dict[str, Any]]:
    meta = await con.fetchrow('''
        SELECT *
        FROM dr1_meta
        INNER JOIN dr1_info USING(oid)
        WHERE oid = $1
    ''', oid)
    if meta is None:
        return None
    meta = dict(meta)
    if remove_oid:
        assert oid == meta.pop('oid')
    return meta


async def get_lc_for_oid(con: Connection, oid: int, remove_oid: bool = True) -> Optional[List[Dict[str, Any]]]:
    lc = await con.fetch('''
        SELECT *
        FROM dr1_good_lc
        WHERE oid = $1
        ORDER BY mjd
    ''', oid)
    lc = [dict(obs) for obs in lc]
    if remove_oid:
        if len(lc) > 0:
            assert {oid} == set(obs.pop('oid') for obs in lc)
    return lc


@routes.get('/api/v1/oid/full/json')
async def oid_full_json(request: Request) -> Response:
    oids = request.query.getall('oid', None)
    if oids is None:
        return Response(text='Query string should has at least one "oid" field', status=404)
    data = {}
    for oid in oids:
        try:
            oid = int(oid)
        except ValueError:
            return Response(text=f'oid value "{oid}" cannot be converted to int', status=404)
        async with request.app['pool'].acquire() as con:  # type: Connection
            meta = await get_meta_for_oid(con, oid)
            if meta is None:
                continue
            lc = await get_lc_for_oid(con, oid)
            data[oid] = dict(meta=meta, lc=lc)
    return json_response(data, dumps=partial(json.dumps, cls=JSONEncoder))


@routes.get('/api/v1/circle/full/json')
async def circle_full_json(request: Request) -> Response:
    try:
        ra = float(request.query['ra'])
        dec = float(request.query['dec'])
        radius = float(request.query['radius_arcsec'])
    except KeyError:
        return Response(text='All of "ra", "dec" and "radius_arcsec" fields should be specified', status=404)
    except ValueError:
        return Response(text='All or "ra", "dec" and "radius_arcsec" fields should be floats', status=404)
    if radius <= 0 or radius > MAX_RADIUS:
        return Response(text='"radius" should be positive and less than 60')
    filters = request.query.getall('filter', [])
    not_filters = request.query.getall('not_filter', [])
    try:
        fieldids = [int(x) for x in request.query.getall('fieldid', [])]
        not_fieldids = [int(x) for x in request.query.getall('not_fieldid', [])]
    except ValueError:
        return Response(text='All "fieldid" and "not_fieldid" values should be int', status=404)
    circle = SCircle(point=SPoint(ra=ra, dec=dec), radius=radius)

    where = [
        (circle, 'coord @ ${i}::scircle'),
        (filters, 'filter = ANY(${i}::FILTER[])'),
        (not_filters, 'NOT filter = ANY(${i}::FILTER[])'),
        (fieldids, 'fieldid = ANY(${i}::int[])'),
        (not_fieldids, 'NOT fieldid = ANY(${i}::int[])'),
    ]
    i = 0
    where_parts = []
    values = []
    for value, part in where:
        if value:
            i += 1
            where_parts.append(part.format(i=i))
            values.append(value)
    where = ' AND '.join(where_parts)

    data = {}
    async with request.app['pool'].acquire() as con:  # type: Connection
        oids = await con.fetch(
            f'''
                SELECT oid
                FROM dr1_meta
                WHERE {where}
                ORDER BY oid
            ''',
            *values
        )
        if oids is None:
            return json_response(data)
        for oid in (int(record[0]) for record in oids):
            meta = await get_meta_for_oid(con, oid)
            assert meta is not None
            lc = await get_lc_for_oid(con, oid)
            data[oid] = dict(meta=meta, lc=lc)
    return json_response(data, dumps=partial(json.dumps, cls=JSONEncoder))


async def connection_setup(con: Connection):
    await con.set_type_codec(
        'spoint',
        encoder=SPoint.to_sql,
        decoder=SPoint.from_sql,
        format='text',
    )
    await con.set_type_codec(
        'scircle',
        encoder=SCircle.to_sql,
        decoder=SCircle.from_sql,
        format='text',
    )


async def get_app():
    app = Application()
    app['pool'] = await create_pool(database='ztf', user='api', setup=connection_setup)
    app.add_routes(routes)
    return app
