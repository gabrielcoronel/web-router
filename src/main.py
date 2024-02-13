import asyncio # run
from sys import argv as program_arguments
from router import Router

def get_port():
    try:
        return program_arguments[1]
    except IndexError:
        return 8000

if __name__ == "__main__":
    app_router = Router()
    port = get_port()

    asyncio.run(app_router.serve(port))
