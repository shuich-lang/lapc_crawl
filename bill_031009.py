from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json
import os
import datetime
import re
from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP

app = FastAPI()

stop_scraping = False


@app.get("/")
async def root():
    return {
        "ok": True,
        "endpoints": [
            "/031009/scrape",
            "/031009/scrapeView",
            "/031009/list",
            "/031009/view_data",
            "/stop",
        ],
    }


def normalize_key(key: str) -> str:
    if not key:
        return ""
    return " ".join(key.strip().replace("\n", " ").split())


def map_field(section: str, key: str) -> str:
    section = normalize_key(section)
    key = normalize_key(key)

    if section and section in SECTION_FIELD_MAP:
        return SECTION_FIELD_MAP[section].get(key, FIELD_MAP.get(key, key))

    return FIELD_MAP.get(key, key)


def extract_page_num(js_text: str, func_name: str) -> str:
    """
    예: javascript:goPage('2') 또는 goViewPage('12345')
    """
    if not js_text:
        return ""
    m = re.search(rf"{func_name}\('([^']+)'\)", js_text)
    return m.group(1) if m else ""


async def goto_list_page(page, page_num: int):
    """
    페이지 이동 전용 함수
    """
    link = page.locator(f'a[href*="goPage(\'{page_num}\')"]').first

    if await link.count() == 0:
        raise Exception(f"{page_num} 페이지 링크를 찾지 못함")

    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
            await link.click()
    except PlaywrightTimeoutError:
        await link.click()
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(1500)

    await page.wait_for_load_state("networkidle")


async def scrape_bills_gimpo():
    global stop_scraping
    stop_scraping = False

    url = "https://gimpocouncil.go.kr/cnts/bls/billList.php?bbsCd=mnt&bbsSubCd=mnt04"
    data_list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        max_pages = 1
        last_link = page.locator("a.end").first
        if await last_link.count() > 0:
            href = await last_link.get_attribute("href")
            page_str = extract_page_num(href or "", "goPage")
            if page_str.isdigit():
                max_pages = int(page_str)

        page_num = 1
        while page_num <= max_pages:
            if stop_scraping:
                break

            print(f"Scraping page {page_num}/{max_pages}")

            await page.wait_for_selector("div.board_body table tbody tr", timeout=10000)

            rows = page.locator("div.board_body table tbody tr")
            row_count = await rows.count()

            for i in range(row_count):
                if stop_scraping:
                    break

                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = await cells.count()

                if cell_count < 7:
                    continue

                bill_num = (await cells.nth(0).inner_text()).strip()

                title = ""
                uid = ""

                link = cells.nth(1).locator("a").first
                if await link.count() > 0:
                    title = (await link.inner_text()).strip()
                    href = await link.get_attribute("href")
                    uid = extract_page_num(href or "", "goViewPage")

                proposer = (await cells.nth(2).inner_text()).strip()
                committee = (await cells.nth(3).inner_text()).strip()
                committee_result = (await cells.nth(4).inner_text()).strip()
                plenary_result = (await cells.nth(5).inner_text()).strip()
                proposal_date = (await cells.nth(6).inner_text()).strip()

                data_list.append({
                    "BI_NO": bill_num,
                    "BI_SJ": title,
                    "CONTS_SEQ": uid,
                    "PROPSR": proposer,
                    "JRSD_CMIT_NM": committee,
                    "CMIT_RESULT": committee_result,
                    "PLNMT_RESULT": plenary_result,
                    "ITNC_DE": proposal_date,
                    "TITLE": title,
                })

            page_num += 1
            if page_num <= max_pages:
                try:
                    await goto_list_page(page, page_num)
                except Exception as e:
                    print(f"Failed to go to page {page_num}: {e}")
                    break

        await browser.close()

    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs("download", exist_ok=True)
    filename = f"download/bill_031009_list_{date_str}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)

    print(f"List data scraped and saved to {filename}")


@app.api_route("/031009/scrape", methods=["GET", "POST"])
async def scrape_endpoint(background_tasks: BackgroundTasks):
    global stop_scraping
    stop_scraping = False
    background_tasks.add_task(scrape_bills_gimpo)
    return {"message": "Scraping started in background"}


async def scrape_view_details_gimpo():
    global stop_scraping
    stop_scraping = False

    url = "https://gimpocouncil.go.kr/cnts/bls/billList.php?bbsCd=mnt&bbsSubCd=mnt04"
    details = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        uids = []
        page_num = 1
        max_pages = 1

        last_link = page.locator("a.end").first
        if await last_link.count() > 0:
            href = await last_link.get_attribute("href")
            page_str = extract_page_num(href or "", "goPage")
            if page_str.isdigit():
                max_pages = int(page_str)

        while page_num <= max_pages:
            if stop_scraping:
                break

            print(f"Collecting UIDs from page {page_num}/{max_pages}")

            await page.wait_for_selector("div.board_body table tbody tr td a", timeout=10000)
            links = page.locator("div.board_body table tbody tr td a")
            link_count = await links.count()

            for i in range(link_count):
                href = await links.nth(i).get_attribute("href")
                uid = extract_page_num(href or "", "goViewPage")
                if uid and uid not in uids:
                    uids.append(uid)

            page_num += 1
            if page_num <= max_pages:
                try:
                    await goto_list_page(page, page_num)
                except Exception as e:
                    print(f"Failed to go to next page: {e}")
                    break

        for uid in uids:
            if stop_scraping:
                break

            try:
                view_url = (
                    f"https://gimpocouncil.go.kr/cnts/bls/billView.php?"
                    f"bbsSn={uid}&mbrSn=&flSn=&totalCnt=4457&pageNo=1&schGnrtn=0&schSesn=0"
                    f"&schBillNo=0&schCmtCd=&schSrtCd=&schPrpslCd=&schCmtRsltCd=&schCpsRsltCd="
                    f"&schTle=&schMbrCd=&schMbrPrpslCd=&bbsCd=mnt&bbsSubCd=mnt04"
                )

                await page.goto(view_url, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle")

                rows = page.locator("div.billcontent div.bill_view div.pcView table.table_bill tbody tr")
                row_count = await rows.count()

                item = {
                    "CONTS_SEQ": uid,
                    "URL": view_url,
                }

                for i in range(row_count):
                    row = rows.nth(i)
                    th_el = row.locator("th").first
                    td_el = row.locator("td").first

                    if await th_el.count() == 0 or await td_el.count() == 0:
                        continue

                    key = normalize_key((await th_el.inner_text()).strip())
                    val = (await td_el.inner_text()).strip()

                    eng_key = map_field("", key)
                    item[eng_key] = val

                if "BI_SJ" in item:
                    item["TITLE"] = item["BI_SJ"]
                else:
                    item["TITLE"] = ""

                if "BI_OUTLINE" in item:
                    item["CONTENT"] = item["BI_OUTLINE"]
                else:
                    item["CONTENT"] = ""

                if "REG_DATE" not in item:
                    item["REG_DATE"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                details.append(item)
                print(f"Collected view data for uid {uid}")

            except Exception as e:
                print(f"Failed to get view {uid}: {e}")
                continue

        await browser.close()

    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs("download", exist_ok=True)
    filename = f"download/bill_031009_view_{date_str}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False, indent=4)

    print(f"View data scraped and saved to {filename}")


@app.api_route("/031009/scrapeView", methods=["GET", "POST"])
async def scrape_view_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(scrape_view_details_gimpo)
    return {"message": "View scraping started in background"}


def _latest_file(prefix: str):
    if not os.path.exists("download"):
        return None
    files = [f for f in os.listdir("download") if f.startswith(prefix) and f.endswith(".json")]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join("download", files[0])


@app.get("/031009/list")
async def get_list_data():
    filename = _latest_file("bill_031009_list_")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    return {"error": "Data not found"}


@app.get("/031009/view_data")
async def get_view_data():
    filename = _latest_file("bill_031009_view_")
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
    uvicorn.run(app, host="0.0.0.0", port=8903)