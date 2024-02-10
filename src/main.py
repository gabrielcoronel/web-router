import asyncio # run
from router import Router

if __name__ == "__main__":
    app_router = Router()

    asyncio.run(app_router.serve())
