from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
import json
import asyncio
import os
import datetime
from urllib.parse import urlparse, parse_qs

app = FastAPI()

# 수집 중단 플래그
stop_scraping = False

@app.get("/")
async def root():
    return {"ok": True, "endpoints": ["/002009/scrape", "/002009/scrapeView", "/stop", "/002009/list", "/002009/view"]}

async def scrape_bills_geumcheon():
    global stop_scraping
    stop_scraping = False
    
    url = "https://council.geumcheon.go.kr/council/kr/minutes/bill.do"
    data_list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 초기 페이지 로드
        await page.goto(url)
        await page.wait_for_load_state('networkidle')

        page_num = 1
        max_pages = 500  # 최대 페이지 수 (동적으로 감지 가능)

        while page_num <= max_pages:
            if stop_scraping:
                break

            print(f"Scraping page {page_num}")

            # 테이블 데이터 추출
            rows = await page.query_selector_all('table.normal_list tbody tr')

            if len(rows) == 0:
                break

            for row in rows:
                cells = await row.query_selector_all('td')
                if len(cells) >= 5:
                    bill_num = await cells[0].inner_text()
                    th = await cells[1].inner_text()
                    session = await cells[2].inner_text()
                    
                    # 제목과 uid 추출
                    title_link = await row.query_selector('td a')
                    if title_link:
                        title = await title_link.inner_text()
                        href = await title_link.get_attribute('href')
                        # uid 파라미터 추출
                        parsed = parse_qs(urlparse(href).query)
                        uid = parsed.get('uid', [''])[0]
                    else:
                        title = await cells[3].inner_text()
                        uid = ""

                    proposer = await cells[4].inner_text()
                    co_proposer = await cells[5].inner_text()
                    committee = await cells[6].inner_text() if len(cells) > 6 else ""
                    result = await cells[7].inner_text() if len(cells) > 7 else ""

                    data_list.append({
                        "bill_num": bill_num.strip(),
                        "th": th.strip(),
                        "session": session.strip(),
                        "title": title.strip(),
                        "uid": uid.strip(),
                        "proposer": proposer.strip(),
                        "co_proposer": co_proposer.strip(),
                        "committee": committee.strip(),
                        "result": result.strip()
                    })

            # 다음 페이지로 이동
            try:
                # 다음 페이지 링크 찾기
                next_link = await page.query_selector('a.num_right')
                if next_link:
                    # 링크의 href에서 page 파라미터 추출
                    href = await next_link.get_attribute('href')
                    if href:
                        await page.goto(f"https://council.geumcheon.go.kr{href}")
                        await page.wait_for_load_state('networkidle')
                        page_num += 10
                    else:
                        break
                else:
                    break
            except Exception as e:
                print(f"Failed to go to next page: {e}")
                break

        await browser.close()

    # JSON 파일에 저장
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs('download', exist_ok=True)
    filename = f"download/bill_002009_list_{date_str}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)

    print(f"List data scraped and saved to {filename}")

@app.api_route("/002009/scrape", methods=["GET", "POST"])
async def scrape_endpoint(background_tasks: BackgroundTasks):
    global stop_scraping
    stop_scraping = False
    background_tasks.add_task(scrape_bills_geumcheon)
    return {"message": "Scraping started in background"}

async def scrape_view_details_geumcheon():
    global stop_scraping
    stop_scraping = False

    url = "https://council.geumcheon.go.kr/council/kr/minutes/bill.do"
    details = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 초기 페이지 로드
        await page.goto(url)
        await page.wait_for_load_state('networkidle')

        # 모든 페이지를 돌면서 각 항목의 uid 수집
        view_ids = []
        page_num = 1
        max_pages = 500

        while page_num <= max_pages:
            if stop_scraping:
                break

            print(f"Collecting UIDs from page {page_num}")

            # 각 행의 링크에서 uid 추출
            links = await page.query_selector_all('table.normal_list tbody tr td a')
            for a in links:
                href = await a.get_attribute('href')
                if href and 'billview.do' in href:
                    parsed = parse_qs(urlparse(href).query)
                    uid = parsed.get('uid', [''])[0]
                    if uid:
                        view_ids.append(uid)

            # 다음 페이지로 이동
            try:
                next_link = await page.query_selector('a.num_right')
                if next_link:
                    href = await next_link.get_attribute('href')
                    if href:
                        await page.goto(f"https://council.geumcheon.go.kr{href}")
                        await page.wait_for_load_state('networkidle')
                        page_num += 10
                    else:
                        break
                else:
                    break
            except Exception as e:
                print(f"Failed to go to next page: {e}")
                break

        # view 페이지별로 수집
        for uid in view_ids:
            if stop_scraping:
                break

            try:
                # view 페이지로 이동
                view_url = f"https://council.geumcheon.go.kr/council/kr/minutes/billview.do?uid={uid}"
                await page.goto(view_url)
                await page.wait_for_load_state('networkidle')

                # sub_board 테이블 긁어오기
                rows = await page.query_selector_all('div#sub_board table.normal_list tr')
                item = {"uid": uid}
                
                for row in rows:
                    th_el = await row.query_selector('th')
                    td_el = await row.query_selector('td.con')
                    
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
    os.makedirs('download', exist_ok=True)
    filename = f"download/bill_002009_view_{date_str}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(details, f, ensure_ascii=False, indent=4)

    print(f"View data scraped and saved to {filename}")

@app.api_route("/002009/scrapeView", methods=["GET", "POST"])
@app.api_route("/002009/view", methods=["GET", "POST"])
async def scrape_view_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(scrape_view_details_geumcheon)
    return {"message": "View scraping started in background"}

def _latest_file(prefix: str):
    if not os.path.exists('download'):
        return None
    files = [f for f in os.listdir('download') if f.startswith(prefix) and f.endswith('.json')]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join('download', files[0])

@app.get("/002009/list")
async def get_list_data():
    filename = _latest_file("bill_002009_list_")
    if filename and os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    return {"error": "Data not found"}

@app.get("/002009/view_data")
async def get_view_data():
    filename = _latest_file("bill_002009_view_")
    if filename and os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
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
    uvicorn.run(app, host="0.0.0.0", port=8901)
