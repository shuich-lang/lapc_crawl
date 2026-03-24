import sys
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI
from bill_002008 import router as bill_002008_router
from bill_002009 import router as bill_002009_router
# from bill_002011 import router as bill_002011_router
# from bill_031003 import router as bill_031003_router
# from bill_031009 import router as bill_031009_router
# from bill_031017 import router as bill_031017_router

app = FastAPI(
    title="LAPC Bill Scraper API",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {
        "ok": True,
        "message": "국회도서관 수집기 API 서버",
    }

app.include_router(bill_002008_router)
app.include_router(bill_002009_router)
# app.include_router(bill_002011_router)
# app.include_router(bill_031003_router)
# app.include_router(bill_031009_router)
# app.include_router(bill_031017_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8900, reload=False)