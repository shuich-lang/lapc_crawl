from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP

import json
import os
import datetime
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin
from fastapi import APIRouter

#app = FastAPI()

router = APIRouter(
    prefix="/002008",
    tags=["002008"]
)

# 크롤링 중단 플래그
stop_scraping = False

BASE_URL = "https://www.guroc.go.kr"
LIST_URL = "https://www.guroc.go.kr/meeting/bill/search.do"
DOWNLOAD_DIR = "download"


@router.get("/")
async def root():
    return {
        "ok": True,
        "site": "guroc",
        "endpoints": [
            "/002008/scrape",
            "/002008/scrapeView",
            "/002008/list",
            "/002008/view_data",
            "/002008/stop",
        ],
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
    """
    FIELD_MAP 에 정의된 공통 필드명으로 변환.
    없으면 원본 label 그대로 사용.
    """
    label = clean_text(label)
    return FIELD_MAP.get(label, label)


def map_section_field(section_name: str, label: str) -> str:
    """
    SECTION_FIELD_MAP[섹션명] 에 정의된 필드명으로 변환.
    없으면 FIELD_MAP -> 원본 순으로 fallback.
    """
    section_name = clean_text(section_name)
    label = clean_text(label)

    section_map = SECTION_FIELD_MAP.get(section_name, {})
    if label in section_map:
        return section_map[label]

    return FIELD_MAP.get(label, label)


def append_mapped_value(target: Dict[str, Any], key: str, value: Any):
    """
    같은 키가 중복될 경우 안전하게 처리.
    - 빈값은 가능하면 무시
    - 기존 값 없으면 단일값 저장
    - 기존 값 있으면 list 로 누적
    """
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


async def safe_select_option(page, selector: str, value: str):
    try:
        await page.select_option(selector, value)
    except Exception:
        pass


async def safe_fill(page, selector: str, value: str):
    try:
        await page.fill(selector, value)
    except Exception:
        pass


async def goto_list_and_search(page):
    await page.goto(LIST_URL, wait_until="networkidle")

    # 검색 조건
    await safe_select_option(page, 'select[name="series"]', "9")
    await safe_select_option(page, 'select[name="ntime"]', "0")
    await safe_select_option(page, 'select[name="kindSch"]', "")
    await safe_select_option(page, 'select[name="memberSch"]', "")
    await safe_fill(page, 'input[name="keywordSch"]', "")

    await page.click("#btnSearch")
    await page.wait_for_load_state("networkidle")


async def extract_total_pages(page) -> int:
    """
    페이지 네비게이션에서 마지막 숫자 페이지를 찾아 총 페이지 수 추정
    실패 시 기본값 1
    """
    try:
        links = await page.query_selector_all("div.paging a, .paging a, .page a, a.num")
        nums = []
        for link in links:
            txt = clean_text(await link.inner_text())
            if txt.isdigit():
                nums.append(int(txt))
        return max(nums) if nums else 1
    except Exception:
        return 1


async def move_to_page(page, next_page_num: int) -> bool:
    """
    다음 페이지로 이동
    """
    try:
        locator = page.locator(f'a[onclick*="fn_egov_link_page({next_page_num})"]')
        if await locator.count() > 0:
            await locator.first.click()
            await page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass

    # 10페이지 단위 넘김 대응
    try:
        next_btn_candidates = [
            "a.num_right",
            "a.next",
            "a[title='다음']",
            "a[aria-label='다음']"
        ]
        for selector in next_btn_candidates:
            loc = page.locator(selector)
            if await loc.count() > 0:
                await loc.first.click()
                await page.wait_for_load_state("networkidle")
                return True
    except Exception:
        pass

    return False


# ---------------------------------------------------------
# 리스트 수집
# ---------------------------------------------------------
async def scrape_bills():
    global stop_scraping
    stop_scraping = False

    data_list: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await goto_list_and_search(page)

        # 실제 페이지에서 자동 확인
        total_pages = await extract_total_pages(page)
        if total_pages < 1:
            total_pages = 1

        for page_num in range(1, total_pages + 1):
            if stop_scraping:
                break

            print(f"Scraping list page {page_num}")

            rows = await page.query_selector_all("table.stable tbody tr")

            for row in rows:
                if stop_scraping:
                    break

                cells = await row.query_selector_all("td")
                if len(cells) < 5:
                    continue

                # 원본 라벨 기준 값 구성
                raw_item = {
                    "의안번호": clean_text(await cells[0].inner_text()),
                    "의안명": clean_text(await cells[1].inner_text()),
                    "제안자": clean_text(await cells[2].inner_text()),
                    "회기": clean_text(await cells[3].inner_text()),
                    "처리결과": clean_text(await cells[4].inner_text()),
                }

                # FIELD_MAP 기준으로 표준 키 변환
                mapped_item = {}
                for raw_key, value in raw_item.items():
                    mapped_key = map_general_field(raw_key)
                    mapped_item[mapped_key] = value

                # 상세 링크 코드 추출 시도
                try:
                    link = await cells[1].query_selector('a[onclick^="fn_view_page"]')
                    if link:
                        onclick = await link.get_attribute("onclick")
                        if onclick:
                            match = re.search(r"fn_view_page\('([^']+)'\)", onclick)
                            if match:
                                mapped_item["view_id"] = match.group(1)
                except Exception:
                    pass

                data_list.append(mapped_item)

            if page_num < total_pages:
                moved = await move_to_page(page, page_num + 1)
                if not moved:
                    print(f"Failed to move to page {page_num + 1}")
                    break

        await browser.close()

    filename = save_json(data_list, "bill_002008_list")
    print(f"List data scraped and saved to {filename}")


# @app.api_route("/002008/scrape", methods=["GET", "POST"])
# async def scrape_endpoint(background_tasks: BackgroundTasks):
#     global stop_scraping
#     stop_scraping = False
#     background_tasks.add_task(scrape_bills)
#     return {"message": "List scraping started in background"}


# ---------------------------------------------------------
# 상세(View) 수집
# ---------------------------------------------------------
async def collect_view_ids(page, total_pages: int) -> List[str]:
    view_ids = []

    for page_num in range(1, total_pages + 1):
        global stop_scraping
        if stop_scraping:
            break

        print(f"Collecting view ids from page {page_num}")

        links = await page.query_selector_all(
            'table.stable tbody tr td a[onclick^="fn_view_page"]'
        )

        for a in links:
            onclick = await a.get_attribute("onclick")
            if not onclick:
                continue

            match = re.search(r"fn_view_page\('([^']+)'\)", onclick)
            if match:
                view_id = match.group(1)
                view_ids.append(view_id)

        if page_num < total_pages:
            moved = await move_to_page(page, page_num + 1)
            if not moved:
                print(f"Failed to move to page {page_num + 1}")
                break

    # 중복 제거
    return list(dict.fromkeys(view_ids))


async def parse_general_view_table(page, item: Dict[str, Any]):
    """
    일반 상세 테이블 파싱
    - colspan=2 형태의 일반 단일 항목만 처리
    - 위원회/본회의 rowspan 섹션은 여기서 제외
    - 첨부파일은 a 태그에서 파일명/링크 별도 추출
    """
    rows = await page.query_selector_all("#sub_detail table.board_view tbody tr")

    attachment_aliases = {
        "첨부파일", "첨부", "의안파일", "의안원문", "원안",
        "접수의안", "심의안건", "발의(제출)안", "제안안(원안)",
        "의안", "관련자료"
    }

    for row in rows:
        ths = await row.query_selector_all("th")
        tds = await row.query_selector_all("td")

        if not ths or not tds:
            continue

        # 섹션형 행(rowspan 시작 또는 섹션 내부 행)은 일반 파싱에서 제외
        # 구로구의회 HTML 기준:
        # - 일반행: th 1개 + td 1개 (colspan=2)
        # - 섹션시작행: th 2개 + td 1개 (첫 th가 위원회/본회의, rowspan 존재)
        # - 섹션내부행: th 1개 + td 1개 (하지만 현재 섹션 context 필요)
        # 일반행만 안전하게 처리하려면 th[0] colspan=2 또는 th 1개이면서 섹션명이 아닌 경우만 처리
        first_th = ths[0]
        first_th_text = clean_text(await first_th.inner_text())
        rowspan = await first_th.get_attribute("rowspan")
        colspan = await first_th.get_attribute("colspan")

        # 섹션 시작행은 제외
        if rowspan and first_th_text in SECTION_FIELD_MAP:
            continue

        # 섹션 내부행도 제외해야 함
        # (소관위원회, 회부일, 상정일, 처리결과 등은 section parser 에서만 처리)
        if first_th_text in {
            "소관위원회", "소관위", "소관 위원회", "소관위원회명",
            "회부일", "회부일자",
            "접수일", "접수일자",
            "상정일", "상정일자",
            "처리일", "처리일자",
            "의결일", "의결일자",
            "처리결과", "처리 결과",
            "심사결과", "심사 결과",
            "관련 회의록", "비고", "보고일", "보고일자", "심사보고일"
        }:
            continue

        # 일반 단일행만 처리
        key = first_th_text
        td = tds[0]
        val = clean_text(await td.inner_text())

        if not key:
            continue

        # 첨부파일 전용 처리
        if key in attachment_aliases:
            await parse_attachment_row(row, item)
            continue

        mapped_key = map_general_field(key)
        append_mapped_value(item, mapped_key, val)


async def parse_section_tables(page, item: Dict[str, Any]):
    """
    구로구의회 board_view 테이블의 rowspan 기반 섹션 파싱
    결과:
    item["sections"]["위원회"] = {...}
    item["sections"]["본회의"] = {...}

    최상위에는 섹션 데이터 넣지 않음.
    """
    item["sections"] = {}

    rows = await page.query_selector_all("#sub_detail table.board_view tbody tr")
    current_section = None

    for row in rows:
        ths = await row.query_selector_all("th")
        tds = await row.query_selector_all("td")

        if not ths or not tds:
            continue

        # case 1) 섹션 시작 행
        # <th rowspan="6">위원회</th><th>소관위원회</th><td>...</td>
        if len(ths) >= 2:
            first_th = ths[0]
            second_th = ths[1]

            first_text = normalize_section_name(clean_text(await first_th.inner_text()))
            second_text = clean_text(await second_th.inner_text())

            rowspan = await first_th.get_attribute("rowspan")

            if rowspan and first_text in SECTION_FIELD_MAP:
                current_section = first_text
                section_obj = item["sections"].setdefault(current_section, {})

                td = tds[0]
                val = clean_text(await td.inner_text())
                mapped_key = map_section_field(current_section, second_text)
                append_mapped_value(section_obj, mapped_key, val)

                # 관련 회의록 링크 처리
                if second_text == "관련 회의록":
                    links = await extract_links_from_td(td)
                    if links:
                        names = [x["name"] for x in links if x["name"]]
                        urls = [x["url"] for x in links if x["url"]]
                        if names:
                            section_obj["RELATED_MEETING_NM"] = names[0] if len(names) == 1 else names
                        if urls:
                            section_obj["RELATED_MEETING_URL"] = urls[0] if len(urls) == 1 else urls
                continue

        # case 2) 섹션 내부 행
        # <th>회부일</th><td>...</td>
        if len(ths) == 1 and current_section:
            key = clean_text(await ths[0].inner_text())
            td = tds[0]
            val = clean_text(await td.inner_text())

            section_obj = item["sections"].setdefault(current_section, {})
            mapped_key = map_section_field(current_section, key)
            append_mapped_value(section_obj, mapped_key, val)

            # 관련 회의록 링크 처리
            if key == "관련 회의록":
                links = await extract_links_from_td(td)
                if links:
                    names = [x["name"] for x in links if x["name"]]
                    urls = [x["url"] for x in links if x["url"]]
                    if names:
                        section_obj["RELATED_MEETING_NM"] = names[0] if len(names) == 1 else names
                    if urls:
                        section_obj["RELATED_MEETING_URL"] = urls[0] if len(urls) == 1 else urls
            continue


async def scrape_view_details(save_file: bool = True):
    global stop_scraping
    stop_scraping = False

    details: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 목록 페이지에서 view_id 수집
        await goto_list_and_search(page)
        total_pages = await extract_total_pages(page)
        if total_pages < 1:
            total_pages = 1

        view_ids = await collect_view_ids(page, total_pages)

        # 상세 페이지 개별 진입
        for vid in view_ids:
            if stop_scraping:
                break

            try:
                view_url = f"{BASE_URL}/meeting/bill/billview.do?code={vid}"
                print(f"Scraping view: {view_url}")

                await page.goto(view_url, wait_until="networkidle")

                item: Dict[str, Any] = {
                    "view_id": vid,
                    "view_url": view_url
                }

                await parse_general_view_table(page, item)
                await parse_section_tables(page, item)

                details.append(item)

            except Exception as e:
                print(f"Failed navigating to view {vid}: {e}")
                continue

        await browser.close()

    if save_file:
        filename = save_json(details, "bill_002008_view")
        print(f"View data scraped and saved to {filename}")

    return details

def normalize_section_name(name: str) -> str:
    """
    섹션명 표준화
    """
    name = clean_text(name)
    if "위원회" in name:
        return "위원회"
    if "본회의" in name:
        return "본회의"
    return name


async def extract_links_from_td(td) -> List[Dict[str, str]]:
    """
    td 내부 a 태그들의 텍스트/링크 추출
    """
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

    for link in links:
        name = clean_text(await link.inner_text())
        href = await link.get_attribute("href")

        if not href:
            continue

        full_url = urljoin(BASE_URL, href)

        if name:
            file_names.append(name)
        file_urls.append(full_url)

    if file_names:
        item[file_name_key] = file_names[0] if len(file_names) == 1 else file_names

    if file_urls:
        item[file_url_key] = file_urls[0] if len(file_urls) == 1 else file_urls


# @app.api_route("/002008/scrapeView", methods=["GET", "POST"])
# async def scrape_view_endpoint():
#     global stop_scraping
#     stop_scraping = False

#     data = await scrape_view_details(save_file=True)
#     return data


# ---------------------------------------------------------
# 파일 조회 API
# ---------------------------------------------------------
@router.api_route("/scrapeView", methods=["GET", "POST"])
async def scrape_view_endpoint():
    global stop_scraping
    stop_scraping = False

    data = await scrape_view_details(save_file=True)
    return data


@router.api_route("/scrape", methods=["GET", "POST"])
async def scrape_endpoint(background_tasks: BackgroundTasks):
    global stop_scraping
    stop_scraping = False
    background_tasks.add_task(scrape_bills)
    return {"message": "List scraping started in background"}

@router.get("/list")
async def list_data():
    return {"message": "002008 list"}

@router.get("/view_data")
async def view_data():
    return {"message": "002008 view_data"}

@router.get("/stop")
async def stop():
    return {"message": "002008 stop"}


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8900)