"""Access ZTF DRs, API v1 goes to Postgres, API v2 goes to ClickHouse

Specify environment variable $API_VERSION to 'v1', 'v2', 'all', or 'v1:v2'.
If no variable is presented, all versions will be used
"""
from aiohttp.web import Application

from . import root
from .api_version import get_api_versions


async def get_app():
    app = Application()

    app.add_routes(root.routes)

    api_versions = get_api_versions()

    if 'v1' in api_versions:
        from . import v1
        app.on_startup.append(v1.app_on_startup)
        app.on_cleanup.append(v1.app_on_cleanup)
        app.add_routes(v1.routes)

    if 'v2' in api_versions:
        from . import v2
        app.on_startup.append(v2.app_on_startup)
        app.on_cleanup.append(v2.app_on_cleanup)
        app.add_routes(v2.routes)

    return app
