from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
import json
import os
import datetime
from urllib.parse import urlparse, parse_qs

app = FastAPI()

# 수집 중단 플래그
stop_scraping = False

@app.get("/")
async def root():
    return {
        "ok": True,
        "endpoints": [
            "/031003/scrape",
            "/031003/scrapeView",
            "/031003/list",
            "/031003/view_data",
            "/stop",
        ],
    }

async def scrape_bills_goyang():
    global stop_scraping
    stop_scraping = False

    url = "https://www.goyangcouncil.go.kr/promote/bill.do"
    data_list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 초기 페이지 로드
        await page.goto(url)
        await page.wait_for_load_state("networkidle")

        # 최대 페이지 수 계산 (마지막 페이지 링크에서 가져옴)
        max_pages = 1
        last_link = await page.query_selector("a.num_last")
        if last_link:
            href = await last_link.get_attribute("href")
            if href:
                parsed = parse_qs(urlparse(href).query)
                max_pages = int(parsed.get("page", ["1"])[0])

        page_num = 1
        while page_num <= max_pages:
            if stop_scraping:
                break

            print(f"Scraping page {page_num}/{max_pages}")

            # 테이블 데이터 추출
            rows = await page.query_selector_all("table.board_list.bbs_bill tbody tr")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 6:
                    continue

                bill_num = (await cells[0].inner_text()).strip()

                # 제목 + uid
                title = ""
                uid = ""
                link = await cells[1].query_selector("a")
                if link:
                    title = (await link.inner_text()).strip()
                    href = await link.get_attribute("href")
                    if href:
                        parsed = parse_qs(urlparse(href).query)
                        uid = parsed.get("uid", [""])[0]

                proposer = (await cells[2].inner_text()).strip()
                committee = (await cells[3].inner_text()).strip()
                session = (await cells[4].inner_text()).strip()
                result = (await cells[5].inner_text()).strip()

                data_list.append({
                    "bill_num": bill_num,
                    "title": title,
                    "uid": uid,
                    "proposer": proposer,
                    "committee": committee,
                    "session": session,
                    "result": result,
                })

            # 다음 페이지로 이동
            page_num += 1
            if page_num <= max_pages:
                next_url = f"{url}?page={page_num}"
                await page.goto(next_url)
                await page.wait_for_load_state("networkidle")

        await browser.close()

    # JSON 파일에 저장
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs("download", exist_ok=True)
    filename = f"download/bill_031003_list_{date_str}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)

    print(f"List data scraped and saved to {filename}")

@app.api_route("/031003/scrape", methods=["GET", "POST"])
async def scrape_endpoint(background_tasks: BackgroundTasks):
    global stop_scraping
    stop_scraping = False
    background_tasks.add_task(scrape_bills_goyang)
    return {"message": "Scraping started in background"}

async def scrape_view_details_goyang():
    global stop_scraping
    stop_scraping = False

    url = "https://www.goyangcouncil.go.kr/promote/bill.do"
    details = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 초기 페이지 로드
        await page.goto(url)
        await page.wait_for_load_state("networkidle")

        # 모든 페이지를 순회하며 uid 수집
        uids = []
        page_num = 1
        max_pages = 1
        last_link = await page.query_selector("a.num_last")
        if last_link:
            href = await last_link.get_attribute("href")
            if href:
                parsed = parse_qs(urlparse(href).query)
                max_pages = int(parsed.get("page", ["1"])[0])

        while page_num <= max_pages:
            if stop_scraping:
                break

            print(f"Collecting UIDs from page {page_num}/{max_pages}")

            links = await page.query_selector_all("table.board_list.bbs_bill tbody tr td a")
            for a in links:
                href = await a.get_attribute("href")
                if not href:
                    continue
                parsed = parse_qs(urlparse(href).query)
                uid = parsed.get("uid", [""])[0]
                if uid and uid not in uids:
                    uids.append(uid)

            page_num += 1
            if page_num <= max_pages:
                next_url = f"{url}?page={page_num}"
                await page.goto(next_url)
                await page.wait_for_load_state("networkidle")

        # 각 uid 별로 상세 페이지 수집
        for uid in uids:
            if stop_scraping:
                break

            try:
                view_url = f"https://www.goyangcouncil.go.kr/promote/billview.do?uid={uid}"
                await page.goto(view_url)
                await page.wait_for_load_state("networkidle")

                rows = await page.query_selector_all("div#sub_board table.normal_list tr")
                item = {"uid": uid}

                for row in rows:
                    th_el = await row.query_selector("th")
                    td_el = await row.query_selector("td.con")
                    if not th_el or not td_el:
                        continue
                    key = (await th_el.inner_text()).strip()
                    val = (await td_el.inner_text()).strip()
                    item[key] = val

                details.append(item)
                print(f"Collected view data for uid {uid}")

            except Exception as e:
                print(f"Failed to get view {uid}: {e}")
                continue

        await browser.close()

    # 저장
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs("download", exist_ok=True)
    filename = f"download/bill_031003_view_{date_str}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False, indent=4)

    print(f"View data scraped and saved to {filename}")

@app.api_route("/031003/scrapeView", methods=["GET", "POST"])
async def scrape_view_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(scrape_view_details_goyang)
    return {"message": "View scraping started in background"}


def _latest_file(prefix: str):
    if not os.path.exists("download"):
        return None
    files = [f for f in os.listdir("download") if f.startswith(prefix) and f.endswith(".json")]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join("download", files[0])

@app.get("/031003/list")
async def get_list_data():
    filename = _latest_file("bill_031003_list_")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    return {"error": "Data not found"}

@app.get("/031003/view_data")
async def get_view_data():
    filename = _latest_file("bill_031003_view_")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    return {"error": "Data not found"}

@app.get("/stop")
async def stop_scraping_endpoint():
    global stop_scraping
    stop_scraping = True
    return {"message": "Stop signal set. Current scraping will stop after current item."}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8902)
