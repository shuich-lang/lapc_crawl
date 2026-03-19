from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
import json
import asyncio
import os
import datetime

app = FastAPI()

# 볼링 중단 플래그
stop_scraping = False

@app.get("/")
async def root():
    return {"ok": True, "endpoints": ["/002008/scrape", "/002008/scrapeView", "/stop", "/bill_002008/list"]}

async def scrape_bills():
    url = "https://www.guroc.go.kr/meeting/bill/search.do"
    data_list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 페이지 로드
        await page.goto(url)

        # 폼 데이터 설정 (기본값 사용)
        form_data = {
            "series": "9",
            "ntime": "0",
            "kindz": "",
            "member": "",
            "keyword": "",
            "ntimedtl": "0",
            "code": "",
            "typestr": ""
        }

        # 폼 제출 (POST)
        await page.select_option('select[name="series"]', form_data["series"])
        await page.select_option('select[name="ntime"]', form_data["ntime"])
        await page.select_option('select[name="kindSch"]', form_data["kindz"])
        await page.select_option('select[name="memberSch"]', form_data["member"])
        await page.fill('input[name="keywordSch"]', form_data["keyword"])

        # 검색 버튼 클릭
        await page.click('#btnSearch')

        # 페이지 로드 대기
        await page.wait_for_load_state('networkidle')

        # 총 페이지 수 확인 (HTML에서 파싱)
        total_pages = 64  # HTML에서 64페이지라고 나와 있음

        for page_num in range(1, total_pages + 1):
            print(f"Scraping page {page_num}")

            # 테이블 데이터 추출
            rows = await page.query_selector_all('table.stable tbody tr')

            for row in rows:
                cells = await row.query_selector_all('td')
                if len(cells) >= 5:
                    bill_num = await cells[0].inner_text()
                    bill_name = await cells[1].inner_text()
                    proposer = await cells[2].inner_text()
                    session = await cells[3].inner_text()
                    result = await cells[4].inner_text()

                    data_list.append({
                        "bill_num": bill_num.strip(),
                        "bill_name": bill_name.strip(),
                        "proposer": proposer.strip(),
                        "session": session.strip(),
                        "result": result.strip()
                    })

            # 다음 페이지로 이동
            if page_num < total_pages:
                if page_num % 10 == 0:
                    # 10, 20, 30... 페이지에서 "다음" 버튼 클릭
                    try:
                        await page.click('a.num_right')
                        await page.wait_for_load_state('networkidle')
                    except Exception as e:
                        print(f"Failed to click next: {e}")
                        break
                else:
                    # 개별 페이지 링크 클릭
                    try:
                        next_link = page.locator(f'a.num[onclick*="fn_egov_link_page({page_num + 1})"]')
                        await next_link.click()
                        await page.wait_for_load_state('networkidle')
                    except Exception as e:
                        print(f"Failed to go to page {page_num + 1}: {e}")
                        break

        await browser.close()

    # JSON 파일에 저장
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs('download', exist_ok=True)
    filename = f"download/bill_002008_list_{date_str}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)

    print(f"Data scraped and saved to {filename}")

@app.api_route("/002008/scrape", methods=["GET", "POST"])
async def scrape_endpoint(background_tasks: BackgroundTasks):
    global stop_scraping
    stop_scraping = False
    background_tasks.add_task(scrape_bills)
    return {"message": "Scraping started in background"}

async def scrape_view_details():
    global stop_scraping
    stop_scraping = False

    url = "https://www.guroc.go.kr/meeting/bill/search.do"
    details = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 목록 페이지 로드 + 검색
        await page.goto(url)
        await page.select_option('select[name="series"]', "9")
        await page.select_option('select[name="ntime"]', "0")
        await page.click('#btnSearch')
        await page.wait_for_load_state('networkidle')

        # 각 항목의 view 링크를 수집
        view_ids = []
        total_pages = 4
        for page_num in range(1, total_pages + 1):
            if stop_scraping:
                break

            # 각 행의 view 호출 파라미터(009XXXX) 추출
            links = await page.query_selector_all('table.stable tbody tr td a[onclick^="fn_view_page"]')
            for a in links:
                onclick = await a.get_attribute('onclick')
                if onclick and "fn_view_page" in onclick:
                    start = onclick.find("('")
                    end = onclick.find("')", start)
                    if start != -1 and end != -1:
                        view_id = onclick[start+2:end]
                        view_ids.append(view_id)

            # 페이지 이동
            if page_num < total_pages:
                if page_num % 10 == 0:
                    await page.click('a.num_right')
                else:
                    next_link = page.locator(f'a.num[onclick*="fn_egov_link_page({page_num + 1})"]')
                    await next_link.click()
                await page.wait_for_load_state('networkidle')

        # view 페이지별로 수집
        for vid in view_ids:
            if stop_scraping:
                break

            # view 페이지로 이동 (직접 URL 이동)
            try:
                await page.goto(f"https://www.guroc.go.kr/meeting/bill/billview.do?code={vid}")
                await page.wait_for_load_state('networkidle')
            except Exception as e:
                print(f"Failed navigating to view {vid}: {e}")
                continue

            # sub_detail 테이블 긁어오기
            rows = await page.query_selector_all('#sub_detail table.board_view tr')
            item = {"view_id": vid}
            for row in rows:
                key_el = await row.query_selector('th')
                val_el = await row.query_selector('td.con')
                if not key_el or not val_el:
                    continue
                key = (await key_el.inner_text()).strip()
                val = (await val_el.inner_text()).strip()
                item[key] = val

            details.append(item)

            # 목록으로 돌아오기 (목록 버튼이 있으면 클릭, 없으면 뒤로가기)
            try:
                await page.click('#btnList')
            except Exception:
                await page.go_back()
            await page.wait_for_load_state('networkidle')

        await browser.close()

    # 저장
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs('download', exist_ok=True)
    filename = f"download/bill_002008_view_{date_str}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(details, f, ensure_ascii=False, indent=4)

    print(f"View data scraped and saved to {filename}")

@app.api_route("/002008/scrapeView", methods=["GET", "POST"])
@app.api_route("/bill_002008/view", methods=["GET", "POST"])
async def scrape_view_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(scrape_view_details)
    return {"message": "View scraping started in background"}

def _latest_file(prefix: str):
    if not os.path.exists('download'):
        return None
    files = [f for f in os.listdir('download') if f.startswith(prefix) and f.endswith('.json')]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join('download', files[0])

@app.get("/bill_002008/list")
async def get_data():
    filename = _latest_file("bill_002008_list_")
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
    uvicorn.run(app, host="0.0.0.0", port=8900)
