from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse, parse_qs
import json
import os
import datetime
import re
from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP

app = FastAPI()

# 수집 중단 플래그
stop_scraping = False

BASE_URL = "https://www.anseongcl.go.kr"
LIST_URL = "https://www.anseongcl.go.kr/kr/bill.do"

@app.get("/")
async def root():
    return {
        "ok": True,
        "site": "anseongcl",
        "endpoints": [
            "/031017/scrape",
            "/031017/scrapeView",
            "/031017/list",
            "/031017/view_data",
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

    if not section:
        return FIELD_MAP.get(key, key)

    if section == "위원회":
        if key == "소관위":
            return "JRSD_CMIT_NM"
        elif key == "회부일":
            return "FRWRD_DE"
        elif key == "보고일":
            return "CMIT_REPORT_DE"
        elif key == "상정일":
            return "CMIT_SBMISN_DE"
        elif key == "의결일":
            return "CMIT_PROCESS_DE"
        elif key == "처리 결과":
            return "CMIT_RESULT"
        elif key == "비고":
            return "CMIT_UPDT_OUTLINE"
        elif key == "관련 회의록":
            return "CMIT_RELATED_MEETING"

    if section == "본회의":
        if key == "접수일":
            return "PLNMT_FRWRD_DE"
        elif key == "회부일":
            return "PLNMT_FRWRD_DE"
        elif key == "보고일":
            return "PLNMT_REPORT_DE"
        elif key == "상정일":
            return "PLNMT_SBMISN_DE"
        elif key == "의결일":
            return "PLNMT_PROCESS_DE"
        elif key == "처리 결과":
            return "PLNMT_RESULT"
        elif key == "비고":
            return "PLNMT_REMARK"
        elif key == "관련 회의록":
            return "PLNMT_RELATED_MEETING"

    return FIELD_MAP.get(key, key)


def extract_uid_from_href(href: str) -> str:
    if not href:
        return ""
    try:
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        return qs.get("uid", [""])[0]
    except Exception:
        return ""


def extract_page_from_href(href: str) -> int:
    if not href:
        return 1
    try:
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        page = qs.get("page", ["1"])[0]
        return int(page)
    except Exception:
        return 1


def build_list_page_url(page_num: int) -> str:
    return f"{LIST_URL}?begin_dt=&pln_result_cd=&end_dt=&flag=all&cl_cd_bill=&bill_sch=&schwrd=&cmt_result_cd=&th_sch=&prop_cd_sch=&prop_sch=&res_sch=&page={page_num}&cmt_sch=&list_style="


async def get_max_pages(page) -> int:
    max_pages = 1

    last_link = page.locator("#pagingNav a.num_last").first
    if await last_link.count() > 0:
        href = await last_link.get_attribute("href")
        if href:
            full_url = urljoin(BASE_URL, href)
            max_pages = extract_page_from_href(full_url)

    return max_pages


async def scrape_bills_anseong():
    global stop_scraping
    stop_scraping = False

    data_list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 첫 페이지 진입
        await page.goto(build_list_page_url(1), wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        max_pages = await get_max_pages(page)

        for page_num in range(1, max_pages + 1):
            if stop_scraping:
                break

            target_url = build_list_page_url(page_num)
            print(f"Scraping page {page_num}/{max_pages} : {target_url}")

            await page.goto(target_url, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle")

            rows = page.locator("table.board_list.bbs_bill tbody tr")
            row_count = await rows.count()

            for i in range(row_count):
                if stop_scraping:
                    break

                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = await cells.count()

                if cell_count < 4:
                    continue

                bill_num = (await cells.nth(0).inner_text()).strip()

                title = ""
                proposer = ""
                proposal_date = ""
                uid = ""
                view_url = ""

                link = cells.nth(1).locator("a").first
                if await link.count() > 0:
                    title = (await link.inner_text()).strip()
                    href = await link.get_attribute("href")
                    if href:
                        view_url = urljoin(BASE_URL, href)
                        uid = extract_uid_from_href(view_url)

                proposer = (await cells.nth(2).inner_text()).strip()
                proposal_date = (await cells.nth(3).inner_text()).strip()

                data_list.append({
                    "BI_NO": bill_num,
                    "BI_SJ": title,
                    "CONTS_SEQ": uid,
                    "PROPSR": proposer,
                    "ITNC_DE": proposal_date,
                    "URL": view_url,
                    "TITLE": title,
                })

        await browser.close()

    # JSON 파일 저장
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs("download", exist_ok=True)
    filename = f"download/bill_031017_list_{date_str}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)

    print(f"List data scraped and saved to {filename}")


@app.api_route("/031017/scrape", methods=["GET", "POST"])
async def scrape_endpoint(background_tasks: BackgroundTasks):
    global stop_scraping
    stop_scraping = False
    background_tasks.add_task(scrape_bills_anseong)
    return {"message": "Anseong list scraping started in background"}


async def parse_view_table(page, uid: str, view_url: str) -> dict:
    item = {
        "CONTS_SEQ": uid,
        "URL": view_url
    }

    table = page.locator("#sub_board table.normal_list").first
    if await table.count() == 0:
        return item

    rows = table.locator("tbody tr")
    row_count = await rows.count()

    for i in range(row_count):
        row = rows.nth(i)

        first_th = row.locator("th").first
        if await first_th.count() > 0:
            first_key = (await first_th.inner_text()).strip()

            # 발의 의원
            if first_key == "발의 의원":
                members = row.locator("td a")
                member_count = await members.count()
                member_names = []
                for j in range(member_count):
                    name = (await members.nth(j).inner_text()).strip()
                    if name:
                        member_names.append(name)
                item["PROPSR_MEMBER_LIST"] = member_names
                continue

            # 첨부파일
            if first_key == "첨부":
                files = row.locator("td a")
                file_count = await files.count()
                file_names = []
                file_urls = []
                attach_list = []

                for j in range(file_count):
                    a = files.nth(j)
                    name = (await a.inner_text()).strip()
                    href = await a.get_attribute("href")
                    full_url = urljoin(BASE_URL, href) if href else ""

                    if name:
                        file_names.append(name)
                    if full_url:
                        file_urls.append(full_url)

                    attach_list.append({
                        "name": name,
                        "url": full_url
                    })

                item["BI_FILE_NM"] = file_names
                item["BI_FILE_URL"] = file_urls
                item["ATTACH_LIST"] = attach_list
                continue

            # 관련 회의록
            if first_key == "관련 회의록":
                meetings = row.locator("td a")
                meeting_count = await meetings.count()
                meeting_list = []
                for j in range(meeting_count):
                    a = meetings.nth(j)
                    name = (await a.inner_text()).strip()
                    href = await a.get_attribute("href")
                    meeting_list.append({
                        "name": name,
                        "url": urljoin(BASE_URL, href) if href else ""
                    })
                if meeting_list:
                    if "RELATED_MEETING_LIST" not in item:
                        item["RELATED_MEETING_LIST"] = []
                    item["RELATED_MEETING_LIST"].extend(meeting_list)

        headers = row.locator("th")
        values = row.locator("td")

        header_count = await headers.count()
        value_count = await values.count()

        if header_count == 0 or value_count == 0:
            continue

        header_texts = []
        for h in range(header_count):
            txt = (await headers.nth(h).inner_text()).strip()
            if txt:
                header_texts.append(txt)

        value_texts = []
        for v in range(value_count):
            txt = (await values.nth(v).inner_text()).strip()
            value_texts.append(txt)

        # 단일형
        if len(header_texts) == 1 and len(value_texts) >= 1:
            key = header_texts[0]
            if key not in ["발의 의원", "첨부", "관련 회의록"]:
                eng_key = map_field("", key)
                item[eng_key] = value_texts[0]
            continue

        # 2쌍 구조
        if len(header_texts) == 2 and len(value_texts) == 2:
            eng_key1 = map_field("", header_texts[0])
            eng_key2 = map_field("", header_texts[1])
            item[eng_key1] = value_texts[0]
            item[eng_key2] = value_texts[1]
            continue

        # 위원회 / 본회의 묶음 rows
        if len(header_texts) >= 2 and len(value_texts) >= 1:
            section_name = ""
            sub_keys = header_texts[:]

            if header_texts[0] in ["위원회", "본회의"]:
                section_name = header_texts[0]
                sub_keys = header_texts[1:]

            if section_name:
                for idx, key in enumerate(sub_keys):
                    if not key:
                        continue
                    eng_key = map_field(section_name, key)
                    val = value_texts[idx] if idx < len(value_texts) else ""
                    item[eng_key] = val
            else:
                for idx, key in enumerate(header_texts):
                    if not key:
                        continue
                    eng_key = map_field("", key)
                    val = value_texts[idx] if idx < len(value_texts) else ""
                    item[eng_key] = val

    # 보조 필드 세팅
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

    return item


async def scrape_view_details_anseong():
    global stop_scraping
    stop_scraping = False

    details = []
    uids = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 1. 전체 목록 돌면서 uid, view_url 수집
        await page.goto(build_list_page_url(1), wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")
        max_pages = await get_max_pages(page)

        view_targets = []

        for page_num in range(1, 3 + 1):
            if stop_scraping:
                break

            target_url = build_list_page_url(page_num)
            print(f"Collecting view urls from page {page_num}/{max_pages}")

            await page.goto(target_url, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle")

            links = page.locator("table.board_list.bbs_bill tbody tr td.sbj a")
            link_count = await links.count()

            for i in range(link_count):
                href = await links.nth(i).get_attribute("href")
                title = (await links.nth(i).inner_text()).strip()

                if not href:
                    continue

                full_url = urljoin(BASE_URL, href)
                uid = extract_uid_from_href(full_url)

                if uid and uid not in uids:
                    uids.append(uid)
                    view_targets.append({
                        "uid": uid,
                        "title": title,
                        "view_url": full_url
                    })

        # 2. 상세 페이지 수집
        for idx, target in enumerate(view_targets, start=1):
            if stop_scraping:
                break

            uid = target["uid"]
            view_url = target["view_url"]

            try:
                print(f"Scraping view {idx}/{len(view_targets)} uid={uid}")

                await page.goto(view_url, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle")

                item = await parse_view_table(page, uid, view_url)
                if "BI_SJ" not in item and target.get("title"):
                    item["BI_SJ"] = target["title"]
                    item["TITLE"] = target["title"]

                details.append(item)

            except Exception as e:
                print(f"Failed to get view uid={uid}: {e}")
                continue

        await browser.close()

    # 저장
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs("download", exist_ok=True)
    filename = f"download/bill_031017_view_{date_str}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False, indent=4)

    print(f"View data scraped and saved to {filename}")


@app.api_route("/031017/scrapeView", methods=["GET", "POST"])
async def scrape_view_endpoint(background_tasks: BackgroundTasks):
    global stop_scraping
    stop_scraping = False
    background_tasks.add_task(scrape_view_details_anseong)
    return {"message": "Anseong view scraping started in background"}


def _latest_file(prefix: str):
    if not os.path.exists("download"):
        return None
    files = [f for f in os.listdir("download") if f.startswith(prefix) and f.endswith(".json")]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join("download", files[0])


@app.get("/031017/list")
async def get_list_data():
    filename = _latest_file("bill_031017_list_")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    return {"error": "List data not found"}


@app.get("/031017/view_data")
async def get_view_data():
    filename = _latest_file("bill_031017_view_")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    return {"error": "View data not found"}


@app.get("/stop")
async def stop_scraping_endpoint():
    global stop_scraping
    stop_scraping = True
    return {"message": "Stop signal set. Current scraping will stop after current item."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8904)