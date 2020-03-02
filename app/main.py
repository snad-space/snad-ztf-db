from aiohttp.web import Application

from . import root, v1, v2


async def get_app():
    app = Application()

    app.add_routes(root.routes)

    app.on_startup.append(v1.app_on_startup)
    app.on_cleanup.append(v1.app_on_cleanup)
    app.add_routes(v1.routes)

    app.on_startup.append(v2.app_on_startup)
    app.on_cleanup.append(v2.app_on_cleanup)
    app.add_routes(v2.routes)

    return app
