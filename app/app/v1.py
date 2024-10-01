from __future__ import annotations

import json
import math as m
from dataclasses import dataclass
from functools import partial
from typing import Optional, Dict, List, Any

from aiohttp.web import Application, Response, json_response, RouteTableDef, Request, HTTPBadRequest
from asyncpg import create_pool, Connection, BitString

from .util import oids_from_request, ra_dec_radius_from_request


MAX_RADIUS = 60


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BitString):
            return obj.to_int()
        if isinstance(obj, SPoint):
            return obj.to_dict()
        return super().default(obj)


json_response = partial(json_response, dumps=partial(json.dumps, cls=JSONEncoder))


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


routes = RouteTableDef()


@routes.get('/api/v1/help')
async def api_help(request) -> Response:
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
                <p>Example: <font face='monospace'><a href="/api/v1/oid/full/json?oid=830202400008402">/api/v1/oid/full/json?oid=830202400008402</a></font></p>
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
                        circle radius, acrseconds. Should be positive and less than {MAX_RADIUS}.
                        Mandatory
                    </li>
                    <li>
                        <font face='monospace'>filter</font>
                        &mdash;
                        passband to search.
                        Optional, multiple values accepted. If not specified then all filters are used
                    </li>
                    <li>
                        <font face='monospace'>not_filter</font>
                        &mdash;
                        excluded passband from search. 
                        Optional, multiple values accepted
                    </li>
                    <li>
                        <font face='monospace'>fieldid</font>
                        &mdash;
                        field id to search.
                        Optional, multiple values accepted. If not specified then all field ids are used
                    </li>
                    <li>
                        <font face='monospace'>not_fieldid</font>
                        &mdash;
                        excluded field ids from search.
                        Optional, multiple values accepted
                    </li>
                </ul>
                <p>Example: <font face='monospace'><a href="/api/v1/circle/full/json?ra=10&dec=30&radius_arcsec=10">/api/v1/circle/full/json?ra=10&dec=30&radius_arcsec=10</a></font>
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
    oids = await oids_from_request(request)
    data = {}
    for oid in oids:
        async with request.app['pg_pool'].acquire() as con:  # type: Connection
            meta = await get_meta_for_oid(con, oid)
            if meta is None:
                continue
            lc = await get_lc_for_oid(con, oid)
            data[oid] = dict(meta=meta, lc=lc)
    return json_response(data)


async def circle_oids(request: Request) -> [int]:
    ra, dec, radius = ra_dec_radius_from_request(request, MAX_RADIUS)
    filters = request.query.getall('filter', [])
    not_filters = request.query.getall('not_filter', [])
    try:
        fieldids = [int(x) for x in request.query.getall('fieldid', [])]
        not_fieldids = [int(x) for x in request.query.getall('not_fieldid', [])]
    except ValueError:
        raise HTTPBadRequest(reason='All "fieldid" and "not_fieldid" values should be int')
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
    
    async with request.app['pg_pool'].acquire() as con:  # type: Connection
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
            oids = []
        oids = [int(record[0]) for record in oids]
    return oids


@routes.get('/api/v1/circle/full/json')
async def circle_full_json(request: Request) -> Response:
    oids = await circle_oids(request)
    data = {}
    async with request.app['pg_pool'].acquire() as con:  # type: Connection
        for oid in oids:
            meta = await get_meta_for_oid(con, oid)
            assert meta is not None
            lc = await get_lc_for_oid(con, oid)
            data[oid] = dict(meta=meta, lc=lc)
    return json_response(data)


@routes.get('/api/v1/circle/oid/json')
async def circe_oid_json(request: Request) -> Response:
    return json_response(circle_oids(request))


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


async def app_on_startup(app: Application):
    app['pg_pool'] = await create_pool(database='ztf', user='api', setup=connection_setup)


async def app_on_cleanup(app: Application):
    await app['pg_pool'].close()
