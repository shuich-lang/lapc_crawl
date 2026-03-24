from fastapi import FastAPI, Query
from playwright.async_api import async_playwright
from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP

import json
import os
import datetime
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, parse_qs, urljoin
from fastapi import APIRouter

#app = FastAPI()

router = APIRouter(
    prefix="/002009",
    tags=["002009"]
)

# 크롤링 중단 플래그
stop_scraping = False

BASE_URL = "https://council.geumcheon.go.kr"
LIST_URL = "https://council.geumcheon.go.kr/council/kr/minutes/bill.do"
DOWNLOAD_DIR = "download"


@router.get("/")
async def root():
    return {
        "ok": True,
        "endpoints": [
            "/002009/scrape",
            "/002009/scrapeView",
            "/002009/list",
            "/002009/view_data",
            "/stop"
        ]
    }


# ---------------------------------------------------------
# 공통 유틸
# ---------------------------------------------------------
def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def save_json(data: Any, prefix: str) -> str:
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


def map_general_field(label: str) -> str:
    label = clean_text(label)
    return FIELD_MAP.get(label, label)


def map_section_field(section_name: str, label: str) -> str:
    section_name = clean_text(section_name)
    label = clean_text(label)

    # 처리일 보정
    if section_name == "위원회" and label in ["처리일", "처리일자", "의결일", "의결일자"]:
        return "CMIT_PROCESS_DE"
    if section_name == "본회의" and label in ["처리일", "처리일자", "의결일", "의결일자"]:
        return "PLNMT_PROCESS_DE"

    section_map = SECTION_FIELD_MAP.get(section_name, {})
    if label in section_map:
        return section_map[label]

    return FIELD_MAP.get(label, label)


def append_mapped_value(target: Dict[str, Any], key: str, value: Any):
    if value is None:
        return

    if isinstance(value, str):
        value = clean_text(value)

    if value == "":
        if key not in target:
            target[key] = ""
        return

    if key not in target or target[key] in ("", None):
        target[key] = value
    else:
        if isinstance(target[key], list):
            if value not in target[key]:
                target[key].append(value)
        else:
            if target[key] != value:
                target[key] = [target[key], value]


def normalize_section_name(name: str) -> str:
    name = clean_text(name)
    if "위원회" in name:
        return "위원회"
    if "본회의" in name:
        return "본회의"
    return name


def extract_uid_from_href(href: str) -> str:
    if not href:
        return ""
    try:
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        return qs.get("uid", [""])[0]
    except Exception:
        return ""


async def extract_links_from_td(td) -> List[Dict[str, str]]:
    results = []
    links = await td.query_selector_all("a")
    for link in links:
        text = clean_text(await link.inner_text())
        href = await link.get_attribute("href")
        if href:
            results.append({
                "name": text,
                "url": urljoin(BASE_URL, href)
            })
    return results


async def move_to_next_block(page) -> bool:
    """
    금천구의회는 a.num_right 로 다음 블록 이동하는 구조
    """
    try:
        next_link = await page.query_selector("a.num_right")
        if not next_link:
            return False

        href = await next_link.get_attribute("href")
        if not href:
            return False

        await page.goto(urljoin(BASE_URL, href), wait_until="networkidle")
        return True
    except Exception:
        return False


# ---------------------------------------------------------
# 리스트 수집
# ---------------------------------------------------------
async def scrape_bills_geumcheon(
    save_file: bool = True,
    rasmbly_numpr: Optional[str] = None
):
    global stop_scraping
    stop_scraping = False

    data_list: List[Dict[str, Any]] = []
    target_list_url = build_list_url(rasmbly_numpr)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(target_list_url, wait_until="networkidle")

        page_block = 1
        max_blocks = 500

        while page_block <= max_blocks:
            if stop_scraping:
                break

            print(f"Scraping list block {page_block} / url={target_list_url}")

            rows = await page.query_selector_all("table.normal_list tbody tr")
            if not rows:
                break

            for row in rows:
                if stop_scraping:
                    break

                cells = await row.query_selector_all("td")
                if len(cells) < 6:
                    continue

                title_link = await row.query_selector("td a")
                title = ""
                uid = ""
                if title_link:
                    title = clean_text(await title_link.inner_text())
                    href = await title_link.get_attribute("href")
                    uid = extract_uid_from_href(href or "")

                raw_item = {
                    "의안번호": clean_text(await cells[0].inner_text()) if len(cells) > 0 else "",
                    "대수": clean_text(await cells[1].inner_text()) if len(cells) > 1 else "",
                    "회기": clean_text(await cells[2].inner_text()) if len(cells) > 2 else "",
                    "의안명": title if title else (clean_text(await cells[3].inner_text()) if len(cells) > 3 else ""),
                    "대표 발의자": clean_text(await cells[4].inner_text()) if len(cells) > 4 else "",
                    "공동발의자": clean_text(await cells[5].inner_text()) if len(cells) > 5 else "",
                    "소관위원회": clean_text(await cells[6].inner_text()) if len(cells) > 6 else "",
                    "처리결과": clean_text(await cells[7].inner_text()) if len(cells) > 7 else "",
                }

                mapped_item = {}
                for raw_key, value in raw_item.items():
                    mapped_key = map_general_field(raw_key)
                    append_mapped_value(mapped_item, mapped_key, value)

                if uid:
                    mapped_item["view_id"] = uid
                    mapped_item["uid"] = uid
                    mapped_item["view_url"] = f"{BASE_URL}/council/kr/minutes/billview.do?uid={uid}"

                data_list.append(mapped_item)

            moved = await move_to_next_block(page)
            if not moved:
                break

            page_block += 1

        await browser.close()

    if save_file:
        suffix = f"_{rasmbly_numpr}" if rasmbly_numpr else ""
        filename = save_json(data_list, f"bill_002009_list{suffix}")
        print(f"List data scraped and saved to {filename}")

    return data_list


# @app.api_route("/002009/scrape", methods=["GET", "POST"])
# async def scrape_endpoint(
#     rasmbly_numpr: Optional[str] = Query(default=None)
# ):
#     global stop_scraping
#     stop_scraping = False

#     data = await scrape_bills_geumcheon(
#         save_file=True,
#         rasmbly_numpr=rasmbly_numpr
#     )
#     return data


# ---------------------------------------------------------
# 상세(View) 수집
# ---------------------------------------------------------
async def collect_view_ids_geumcheon(
    page,
    rasmbly_numpr: Optional[str] = None
) -> List[str]:
    global stop_scraping

    view_ids = []
    page_block = 1
    max_blocks = 500
    target_list_url = build_list_url(rasmbly_numpr)

    await page.goto(target_list_url, wait_until="networkidle")

    while page_block <= max_blocks:
        if stop_scraping:
            break

        print(f"Collecting UIDs from block {page_block} / url={target_list_url}")

        links = await page.query_selector_all("table.normal_list tbody tr td a")
        for a in links:
            href = await a.get_attribute("href")
            if href and "billview.do" in href:
                uid = extract_uid_from_href(href)
                if uid:
                    view_ids.append(uid)

        moved = await move_to_next_block(page)
        if not moved:
            break

        page_block += 1

    return list(dict.fromkeys(view_ids))


async def parse_general_view_table_geumcheon(page, item: Dict[str, Any]):
    """
    금천구의회 일반 상세 테이블 파싱
    - 한 행에 th/td 쌍이 여러 개 있는 구조 대응
    - 첨부파일 별도 처리
    - 섹션형(위원회/본회의)은 제외
    """
    rows = await page.query_selector_all("div#sub_board table.normal_list tr, #sub_board table tr")

    attachment_aliases = {
        "첨부파일", "첨부", "의안파일", "의안원문", "원안",
        "접수의안", "심의안건", "발의(제출)안", "제안안(원안)",
        "의안", "관련자료"
    }

    section_aliases = {"위원회", "본회의"}

    section_detail_aliases = {
        "소관위원회", "소관위", "소관 위원회", "소관위원회명",
        "회부일", "회부일자",
        "접수일", "접수일자",
        "상정일", "상정일자",
        "처리일", "처리일자",
        "의결일", "의결일자",
        "처리결과", "처리 결과",
        "심사결과", "심사 결과",
        "관련 회의록", "비고", "보고일", "보고일자", "심사보고일"
    }

    for row in rows:
        ths = await row.query_selector_all("th")
        tds = await row.query_selector_all("td")

        if not ths or not tds:
            continue

        # 섹션 시작행은 일반 파싱 제외
        first_text = clean_text(await ths[0].inner_text())
        rowspan = await ths[0].get_attribute("rowspan")
        if rowspan and normalize_section_name(first_text) in section_aliases:
            continue

        # 일반 행에서 th/td 쌍을 모두 처리
        pair_count = min(len(ths), len(tds))

        for i in range(pair_count):
            key = clean_text(await ths[i].inner_text())
            td = tds[i]
            val = clean_text(await td.inner_text())

            if not key:
                continue

            # 섹션 내부 상세키는 여기서 제외
            if key in section_aliases or key in section_detail_aliases:
                continue

            # 첨부파일
            if key in attachment_aliases:
                try:
                    await parse_attachment_row(row, item)
                except Exception as e:
                    print(f"Attachment parse failed: {e}")
                continue

            mapped_key = map_general_field(key)
            append_mapped_value(item, mapped_key, val)


async def parse_section_tables_geumcheon(page, item: Dict[str, Any]):
    """
    금천구의회 상세 뷰에서 위원회/본회의 영역 파싱
    - rowspan 기반 섹션 구조 대응
    - 한 행의 th/td 여러 쌍 처리
    """
    item["sections"] = {}

    rows = await page.query_selector_all("div#sub_board table.normal_list tr, #sub_board table tr")
    current_section = None

    section_detail_keys = {
        "소관위원회", "소관위", "소관 위원회", "소관위원회명",
        "회부일", "회부일자",
        "접수일", "접수일자",
        "상정일", "상정일자",
        "처리일", "처리일자",
        "의결일", "의결일자",
        "처리결과", "처리 결과",
        "심사결과", "심사 결과",
        "관련 회의록", "비고", "보고일", "보고일자", "심사보고일"
    }

    for row in rows:
        ths = await row.query_selector_all("th")
        tds = await row.query_selector_all("td")

        if not ths or not tds:
            continue

        # 섹션 시작행
        first_th_text = clean_text(await ths[0].inner_text())
        first_th_rowspan = await ths[0].get_attribute("rowspan")
        normalized_first = normalize_section_name(first_th_text)

        if first_th_rowspan and normalized_first in SECTION_FIELD_MAP:
            current_section = normalized_first
            section_obj = item["sections"].setdefault(current_section, {})

            sub_ths = ths[1:]
            pair_count = min(len(sub_ths), len(tds))

            for i in range(pair_count):
                key = clean_text(await sub_ths[i].inner_text())
                td = tds[i]
                val = clean_text(await td.inner_text())

                if not key:
                    continue

                mapped_key = map_section_field(current_section, key)
                append_mapped_value(section_obj, mapped_key, val)

                if key == "관련 회의록":
                    links = await extract_links_from_td(td)
                    if links:
                        names = [x["name"] for x in links if x["name"]]
                        urls = [x["url"] for x in links if x["url"]]
                        if names:
                            append_mapped_value(section_obj, "RELATED_MEETING_NM", names[0] if len(names) == 1 else names)
                        if urls:
                            append_mapped_value(section_obj, "RELATED_MEETING_URL", urls[0] if len(urls) == 1 else urls)
            continue

        # 섹션 내부행
        if current_section:
            pair_count = min(len(ths), len(tds))
            handled = False
            section_obj = item["sections"].setdefault(current_section, {})

            for i in range(pair_count):
                key = clean_text(await ths[i].inner_text())
                td = tds[i]
                val = clean_text(await td.inner_text())

                if not key or key not in section_detail_keys:
                    continue

                handled = True
                mapped_key = map_section_field(current_section, key)
                append_mapped_value(section_obj, mapped_key, val)

                if key == "관련 회의록":
                    links = await extract_links_from_td(td)
                    if links:
                        names = [x["name"] for x in links if x["name"]]
                        urls = [x["url"] for x in links if x["url"]]
                        if names:
                            append_mapped_value(section_obj, "RELATED_MEETING_NM", names[0] if len(names) == 1 else names)
                        if urls:
                            append_mapped_value(section_obj, "RELATED_MEETING_URL", urls[0] if len(urls) == 1 else urls)

            # 섹션 내부행이 아니면 current_section 유지하되 아무 처리 안 함
            if handled:
                continue


async def scrape_view_details_geumcheon(
    save_file: bool = True,
    rasmbly_numpr: Optional[str] = None
):
    global stop_scraping
    stop_scraping = False

    details: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        view_ids = await collect_view_ids_geumcheon(page, rasmbly_numpr)

        for uid in view_ids:
            if stop_scraping:
                break

            view_url = f"{BASE_URL}/council/kr/minutes/billview.do?uid={uid}"
            print(f"Scraping view: {view_url}")

            item: Dict[str, Any] = {
                "view_id": uid,
                "uid": uid,
                "view_url": view_url
            }

            if rasmbly_numpr:
                item["RASMBLY_NUMPR"] = str(rasmbly_numpr)

            try:
                await page.goto(view_url, wait_until="networkidle")
            except Exception as e:
                print(f"Failed navigating to view {uid}: {e}")
                continue

            try:
                await parse_general_view_table_geumcheon(page, item)
            except Exception as e:
                print(f"General parse failed for uid {uid}: {e}")

            try:
                await parse_section_tables_geumcheon(page, item)
            except Exception as e:
                print(f"Section parse failed for uid {uid}: {e}")

            details.append(item)

        await browser.close()

    if save_file:
        suffix = f"_{rasmbly_numpr}" if rasmbly_numpr else ""
        filename = save_json(details, f"bill_002009_view{suffix}")
        print(f"View data scraped and saved to {filename}")

    return details

def build_list_url(rasmbly_numpr: Optional[str] = None) -> str:
    """
    rasmbly_numpr 값이 있으면 begin_th / end_th 조건을 붙인 URL 반환
    없으면 기본 LIST_URL 반환
    """
    if rasmbly_numpr:
        rasmbly_numpr = str(rasmbly_numpr).strip()
        return f"{LIST_URL}?begin_th={rasmbly_numpr}&end_th={rasmbly_numpr}"
    return LIST_URL

async def parse_attachment_row(row, item: Dict[str, Any]):
    """
    첨부파일 행에서 파일명/링크 추출
    FIELD_MAP 기준:
    - 첨부파일 -> BI_FILE_NM
    - 첨부파일링크 -> BI_FILE_URL
    """
    file_name_key = FIELD_MAP.get("첨부파일", "BI_FILE_NM")
    file_url_key = FIELD_MAP.get("첨부파일링크", "BI_FILE_URL")

    links = await row.query_selector_all("td.con a, td a")
    if not links:
        return

    file_names = []
    file_urls = []
    attachments = []

    for link in links:
        name = clean_text(await link.inner_text())
        href = await link.get_attribute("href")
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)

        if name:
            file_names.append(name)
        file_urls.append(full_url)
        attachments.append({
            "name": name,
            "url": full_url
        })

    if file_names:
        item[file_name_key] = file_names[0] if len(file_names) == 1 else file_names

    if file_urls:
        item[file_url_key] = file_urls[0] if len(file_urls) == 1 else file_urls

    if attachments:
        item["attachments"] = attachments

@router.api_route("/scrapeView", methods=["GET", "POST"])
@router.api_route("/view", methods=["GET", "POST"])
async def scrape_view_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None)
):
    global stop_scraping
    stop_scraping = False

    data = await scrape_view_details_geumcheon(
        save_file=True,
        rasmbly_numpr=rasmbly_numpr
    )
    return data

@router.api_route("/scrape", methods=["GET", "POST"])
async def scrape_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None)
):
    global stop_scraping
    stop_scraping = False

    data = await scrape_bills_geumcheon(
        save_file=True,
        rasmbly_numpr=rasmbly_numpr
    )
    return data

# @app.api_route("/002009/scrapeView", methods=["GET", "POST"])
# @app.api_route("/002009/view", methods=["GET", "POST"])
# async def scrape_view_endpoint(
#     rasmbly_numpr: Optional[str] = Query(default=None)
# ):
#     global stop_scraping
#     stop_scraping = False

#     data = await scrape_view_details_geumcheon(
#         save_file=True,
#         rasmbly_numpr=rasmbly_numpr
#     )
#     return data


# ---------------------------------------------------------
# 파일 조회 API
# ---------------------------------------------------------
@router.get("/list")
async def get_list_data():
    filename = _latest_file("bill_002009_list")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"error": "List data not found"}


@router.get("/view_data")
async def get_view_data():
    filename = _latest_file("bill_002009_view")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"error": "View data not found"}


@router.get("/stop")
async def stop_scraping_endpoint():
    global stop_scraping
    stop_scraping = True
    return {"message": "Stop signal set. Current scraping will stop after current item."}

# @app.get("/002009/list")
# async def get_list_data():
#     filename = _latest_file("bill_002009_list")
#     if filename and os.path.exists(filename):
#         with open(filename, "r", encoding="utf-8") as f:
#             return json.load(f)
#     return {"error": "List data not found"}


# @app.get("/002009/view_data")
# async def get_view_data():
#     filename = _latest_file("bill_002009_view")
#     if filename and os.path.exists(filename):
#         with open(filename, "r", encoding="utf-8") as f:
#             return json.load(f)
#     return {"error": "View data not found"}


# @app.get("/stop")
# async def stop_scraping_endpoint():
#     global stop_scraping
#     stop_scraping = True
#     return {"message": "Stop signal set. Current scraping will stop after current item."}


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8901)