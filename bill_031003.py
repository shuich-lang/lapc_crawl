from fastapi import FastAPI, BackgroundTasks, Query
from playwright.async_api import async_playwright
from typing import Optional, List, Dict, Any
import json
import os
import datetime
from urllib.parse import urlparse, parse_qs
from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP
from urllib.parse import urljoin
import re

app = FastAPI()

# 수집 중단 플래그
stop_scraping = False

DOWNLOAD_DIR = "download"
BASE_LIST_URL = "https://www.goyangcouncil.go.kr/promote/bill.do"
BASE_VIEW_URL = "https://www.goyangcouncil.go.kr/promote/billview.do"


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
        "example": {
            "list": "http://localhost:8903/031003/scrape?rasmbly_numpr=8",
            "view": "http://localhost:8903/031003/scrapeView?rasmbly_numpr=8",
        }
    }


def make_list_url(rasmbly_numpr: Optional[str] = None, page: int = 1) -> str:
    th_sch = str(rasmbly_numpr).strip() if rasmbly_numpr else "9"
    return f"{BASE_LIST_URL}?th_sch={th_sch}&page={page}"


def save_json(data, prefix: str) -> str:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = os.path.join(DOWNLOAD_DIR, f"{prefix}_{date_str}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return filename


def _latest_file(prefix: str):
    if not os.path.exists(DOWNLOAD_DIR):
        return None
    files = [
        f for f in os.listdir(DOWNLOAD_DIR)
        if f.startswith(prefix) and f.endswith(".json")
    ]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join(DOWNLOAD_DIR, files[0])


async def scrape_bills_goyang(
    rasmbly_numpr: Optional[str] = None,
    save_file: bool = True
) -> List[Dict[str, Any]]:
    global stop_scraping
    stop_scraping = False

    start_url = make_list_url(rasmbly_numpr=rasmbly_numpr, page=1)
    data_list: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print(f"[LIST] open url: {start_url}")

        # 초기 페이지 로드
        await page.goto(start_url)
        await page.wait_for_load_state("networkidle")

        # 최대 페이지 수 계산
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

            current_url = make_list_url(rasmbly_numpr=rasmbly_numpr, page=page_num)
            print(f"[LIST] Scraping page {page_num}/{max_pages} : {current_url}")

            if page_num > 1:
                await page.goto(current_url)
                await page.wait_for_load_state("networkidle")

            rows = await page.query_selector_all("table.board_list.bbs_bill tbody tr")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 6:
                    continue

                bill_num = (await cells[0].inner_text()).strip()

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

                item = {
                    "uid": uid,
                    "view_id": uid,
                    "view_url": f"{BASE_VIEW_URL}?uid={uid}",
                    "BI_NO": bill_num,
                    "BI_SJ": title,
                    "PROPSR": proposer,
                    "JRSD_CMIT_NM": committee,
                    "RASMBLY_SESN": session,
                    "RESULT": result,
                }
                if rasmbly_numpr:
                    item["RASMBLY_NUMPR"] = str(rasmbly_numpr)

                data_list.append(item)

        await browser.close()

    if save_file:
        suffix = f"_thsch_{rasmbly_numpr}" if rasmbly_numpr else ""
        filename = save_json(data_list, f"bill_031003_list{suffix}")
        print(f"[LIST] data scraped and saved to {filename}")

    return data_list


async def scrape_view_details_goyang(
    rasmbly_numpr: Optional[str] = None,
    save_file: bool = True
) -> List[Dict[str, Any]]:
    global stop_scraping
    stop_scraping = False

    details: List[Dict[str, Any]] = []

    list_url = make_list_url(rasmbly_numpr=rasmbly_numpr, page=1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print(f"[VIEW] open url: {list_url}")

        # 초기 페이지 로드
        await page.goto(list_url)
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
                #max_pages = int(parsed.get("page", ["1"])[0])
                max_pages = 3 # 테스트용으로 최대 3페이지만 수집하도록 제한, 실제 운영 시에는 위 라인으로 변경하여 전체 페이지 수집

        while page_num <= max_pages:
            if stop_scraping:
                break

            current_url = make_list_url(rasmbly_numpr=rasmbly_numpr, page=page_num)
            print(f"[VIEW] Collecting UIDs from page {page_num}/{max_pages} : {current_url}")

            if page_num > 1:
                await page.goto(current_url)
                await page.wait_for_load_state("networkidle")

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

        print(f"[VIEW] collected uid count = {len(uids)}")

        # 각 uid 별로 상세 페이지 수집
        for uid in uids:
            if stop_scraping:
                break

            try:
                view_url = f"{BASE_VIEW_URL}?uid={uid}"
                await page.goto(view_url)
                await page.wait_for_load_state("networkidle")

                item: Dict[str, Any] = {
                    "uid": uid,
                    "view_id": uid,
                    "view_url": view_url,
                }

                if rasmbly_numpr:
                    item["RASMBLY_NUMPR"] = str(rasmbly_numpr)

                try:
                    await parse_general_view_table_goyang(page, item)
                except Exception as e:
                    print(f"[VIEW] general parse failed uid={uid}: {e}")

                try:
                    await parse_section_tables_goyang(page, item)
                except Exception as e:
                    print(f"[VIEW] section parse failed uid={uid}: {e}")

                details.append(item)
                print(f"[VIEW] Collected view data for uid {uid}")

            except Exception as e:
                print(f"[VIEW] Failed to get view {uid}: {e}")
                continue

        await browser.close()

    if save_file:
        suffix = f"_thsch_{rasmbly_numpr}" if rasmbly_numpr else ""
        filename = save_json(details, f"bill_031003_view{suffix}")
        print(f"[VIEW] data scraped and saved to {filename}")

    return details

def normalize_label(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = text.replace("&nbsp;", " ")
    text = text.replace("\n", " ")
    text = text.replace("\r", " ")
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def append_value(item: dict, key: str, value: str):
    value = clean_text(value)
    if not key or not value:
        return

    if key in item and item[key]:
        old = str(item[key]).strip()
        if value not in old.split(", "):
            item[key] = f"{old}, {value}"
    else:
        item[key] = value

async def extract_cell_text(td) -> str:
    try:
        text = await td.inner_text()
        return clean_text(text)
    except Exception:
        return ""


async def extract_links_from_td(td, base_url: str) -> list:
    results = []
    try:
        links = await td.query_selector_all("a")
        for a in links:
            href = await a.get_attribute("href")
            name = await a.inner_text()
            name = clean_text(name)
            href = urljoin(base_url, href) if href else ""
            if name or href:
                results.append({
                    "name": name,
                    "url": href
                })
    except Exception:
        pass
    return results

async def parse_general_view_table_goyang(page, item: dict):
    rows = await page.query_selector_all("div#sub_board table.normal_list tbody tr")

    for row in rows:
        ths = await row.query_selector_all("th")
        tds = await row.query_selector_all("td.con")

        if not ths or not tds:
            continue

        # 섹션행은 여기서 스킵하고 section parser에서 처리
        first_th_text = clean_text(await ths[0].inner_text())
        first_th_rowspan = await ths[0].get_attribute("rowspan")
        if first_th_rowspan and len(ths) >= 2:
            continue

        # 일반적으로 1쌍 또는 2쌍 존재
        pair_count = min(len(ths), len(tds))
        for i in range(pair_count):
            raw_label = clean_text(await ths[i].inner_text())
            mapped_key = FIELD_MAP.get(raw_label)

            if not mapped_key:
                continue

            td = tds[i]

            # 첨부파일류는 링크 목록 우선 처리
            if mapped_key == "BI_FILE_NM":
                links = await extract_links_from_td(td, BASE_LIST_URL)
                if links:
                    file_names = [x["name"] for x in links if x.get("name")]
                    file_urls = [x["url"] for x in links if x.get("url")]

                    if file_names:
                        append_value(item, "BI_FILE_NM", " | ".join(file_names))
                    if file_urls:
                        append_value(item, "BI_FILE_URL", " | ".join(file_urls))
                else:
                    value = await extract_cell_text(td)
                    append_value(item, mapped_key, value)
            else:
                value = await extract_cell_text(td)
                append_value(item, mapped_key, value)

async def parse_section_tables_goyang(page, item: dict):
    rows = await page.query_selector_all("div#sub_board table.normal_list tbody tr")

    current_section = None

    for row in rows:
        ths = await row.query_selector_all("th")
        tds = await row.query_selector_all("td.con")

        if not ths or not tds:
            continue

        # 케이스 1: 첫 th가 rowspan 가진 섹션 시작행
        # 예) <th rowspan="4">위원회</th><th>소관위</th><td>본회의</td><th>회부일</th><td>2026-03-19</td>
        first_th_rowspan = await ths[0].get_attribute("rowspan")
        if first_th_rowspan and len(ths) >= 2:
            section_name = clean_text(await ths[0].inner_text())
            current_section = section_name

            section_map = SECTION_FIELD_MAP.get(section_name)
            if not section_map:
                continue

            sub_labels = []
            for th in ths[1:]:
                sub_labels.append(clean_text(await th.inner_text()))

            for idx, sub_label in enumerate(sub_labels):
                mapped_key = section_map.get(sub_label)
                if not mapped_key:
                    continue
                if idx >= len(tds):
                    continue

                td = tds[idx]

                # 관련 회의록 링크 처리
                if mapped_key in ("CMIT_RELATED_MEETING", "PLNMT_RELATED_MEETING"):
                    links = await extract_links_from_td(td, BASE_LIST_URL)
                    if links:
                        names = [x["name"] for x in links if x.get("name")]
                        append_value(item, mapped_key, " | ".join(names))
                    else:
                        value = await extract_cell_text(td)
                        append_value(item, mapped_key, value)
                else:
                    value = await extract_cell_text(td)
                    append_value(item, mapped_key, value)

            continue

        # 케이스 2: 이전 섹션의 다음 줄
        # 예) <th>보고일</th><td>...</td><th>상정일</th><td>...</td>
        if current_section:
            section_map = SECTION_FIELD_MAP.get(current_section)
            if not section_map:
                continue

            pair_count = min(len(ths), len(tds))
            for i in range(pair_count):
                sub_label = clean_text(await ths[i].inner_text())
                mapped_key = section_map.get(sub_label)
                if not mapped_key:
                    continue

                td = tds[i]

                if mapped_key in ("CMIT_RELATED_MEETING", "PLNMT_RELATED_MEETING"):
                    links = await extract_links_from_td(td, BASE_LIST_URL)
                    if links:
                        names = [x["name"] for x in links if x.get("name")]
                        append_value(item, mapped_key, " | ".join(names))
                    else:
                        value = await extract_cell_text(td)
                        append_value(item, mapped_key, value)
                else:
                    value = await extract_cell_text(td)
                    append_value(item, mapped_key, value)

@app.api_route("/031003/scrape", methods=["GET", "POST"])
async def scrape_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None)
):
    data = await scrape_bills_goyang(
        rasmbly_numpr=rasmbly_numpr,
        save_file=True
    )
    return {
        "ok": True,
        "message": "List scraping completed",
        "count": len(data),
        "rasmbly_numpr": rasmbly_numpr,
        "data": data
    }


@app.api_route("/031003/scrapeView", methods=["GET", "POST"])
async def scrape_view_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None)
):
    data = await scrape_view_details_goyang(
        rasmbly_numpr=rasmbly_numpr,
        save_file=True
    )
    return {
        "ok": True,
        "message": "View scraping completed",
        "count": len(data),
        "rasmbly_numpr": rasmbly_numpr,
        "data": data
    }


@app.get("/031003/list")
async def get_list_data():
    filename = _latest_file("bill_031003_list")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    return {"error": "Data not found"}


@app.get("/031003/view_data")
async def get_view_data():
    filename = _latest_file("bill_031003_view")
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