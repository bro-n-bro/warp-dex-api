
import uvicorn
from fastapi import FastAPI

from services.warp_service import WarpService


def start_application():
    app = FastAPI()
    return app


app = start_application()


@app.get("/pairs/")
def get_pairs():
    return WarpService().get_pairs()


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
