from collections import namedtuple

from aiohttp.web import HTTPBadRequest, Request


def oid_to_int(oid: str) -> int:
    try:
        return int(oid)
    except ValueError:
        raise HTTPBadRequest(reason=f'oid value "{oid}" cannot be converted to int')


def oids_from_request(request: Request) -> [int]:
    oids = request.query.getall('oid', None)
    if oids is None:
        raise HTTPBadRequest(reason='Query string should has at least one "oid" field')
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
