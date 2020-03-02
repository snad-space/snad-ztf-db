from aiohttp.web import Application

from . import root, v1


async def get_app():
    app = Application()
    await v1.configure_app(app)
    app.add_routes(root.routes)
    app.add_routes(v1.routes)
    return app
