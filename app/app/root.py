from aiohttp.web import Response, RouteTableDef

from .api_version import get_api_versions

routes = RouteTableDef()


_template = '''
<p>
    Welcome on <a href="//snad.space">SNAD</a> <a href="https://www.ztf.caltech.edu">ZTF</a> data releases
    light curves page.
</p>
{help}
<p>
    See source code <a href="https://github.com/hombit/snad-ztf-db/">on GitHub</a>.
</p>
'''


_help_links = dict(
    v3='''
        <p>
            API v3 provides access to ZTF DR2, DR3, DR4, DR8 & DR13, see details on
            <a href="/api/v3/help">/api/v3/help</a>.
        </p>
    ''',
    v2='''
        <p>
            API v2 provides access to ZTF DR2, see details on <a href="/api/v2/help">/api/v2/help</a>.
        </p>
    ''',
    v1='''
        <p>
            API v1 provides access to ZTF DR1, see details on <a href="/api/v1/help">/api/v1/help</a>.
        </p> 
    ''',
)


@routes.get('/')
async def index(request) -> Response:
    api_versions = get_api_versions()
    links = '\n'.join(text for version, text in _help_links.items() if version in api_versions)
    return Response(
        text=_template.format(help=links),
        content_type='text/html',
    )
