import asyncio
import time
from collections import namedtuple
from json import JSONDecodeError

from aiohttp.web import HTTPBadRequest, Request


def oid_to_int(oid: str) -> int:
    try:
        return int(oid)
    except ValueError:
        raise HTTPBadRequest(reason=f'oid value "{oid}" cannot be converted to int')


def oids_from_get_request(request: Request) -> [int]:
    oids = request.query.getall('oid', None)
    if oids is None:
        raise HTTPBadRequest(reason='Query string should has at least one "oid" field')
    return oids


async def oids_from_post_request(request: Request) -> [int]:
    try:
        data = await request.json()
    except JSONDecodeError as e:
        raise HTTPBadRequest(reason='Error during JSON parsing') from e
    if not isinstance(data, list):
        raise HTTPBadRequest(reason='JSON should be a list of {"oid": "value"} objects')

    oids = []
    for record in data:
        try:
            oid = record['oid']
        except KeyError:
            raise HTTPBadRequest(reason='All objects in JSON should have "oid" field')
        oids.append(oid)
    return oids


async def oids_from_request(request: Request) -> [int]:
    if request.method == 'GET':
        oids = oids_from_get_request(request)
    elif request.method == 'POST':
        oids = await oids_from_post_request(request)
    else:
        raise HTTPBadRequest(reason='Only GET and POST methods are supported for OID parsing')
    return [oid_to_int(oid) for oid in oids]


RaDecRadius = namedtuple('RaDecRadius', ('ra', 'dec', 'radius'))


def ra_dec_radius_from_request(request: Request, max_radius: float) -> RaDecRadius:
    try:
        ra = float(request.query['ra'])
        dec = float(request.query['dec'])
        radius = float(request.query['radius_arcsec'])
    except KeyError:
        raise HTTPBadRequest(reason='All of "ra", "dec" and "radius_arcsec" fields should be specified')
    except ValueError:
        raise HTTPBadRequest(reason='All or "ra", "dec" and "radius_arcsec" fields should be floats')
    if radius <= 0 or radius > max_radius:
        raise HTTPBadRequest(reason=f'"radius" should be positive and less than {max_radius}')
    return RaDecRadius(ra=ra, dec=dec, radius=radius)


async def try_for_a_while(f, wait_for, interval=None, exception=Exception):
    if interval is None:
        interval = wait_for / 11
    t_start = time.monotonic()
    while time.monotonic() - t_start < wait_for:
        try:
            if asyncio.iscoroutinefunction(f):
                return await f()
            else:
                return f()
        except exception as e:
            await asyncio.sleep(interval)
    raise RuntimeError(f'Function {f} calls are timed out') from e
