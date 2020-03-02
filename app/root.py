from aiohttp.web import Response, RouteTableDef


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
