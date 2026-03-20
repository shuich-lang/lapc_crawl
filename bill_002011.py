from fastapi import FastAPI, Query
from playwright.async_api import async_playwright
from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP

import json
import os
import datetime
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, parse_qs, urljoin

app = FastAPI()

# 크롤링 중단 플래그
stop_scraping = False

BASE_URL = "https://www.council-dobong.seoul.kr"
LIST_URL = "https://www.council-dobong.seoul.kr/meeting/bill/bill.do"
DOWNLOAD_DIR = "download"


@app.get("/")
async def root():
    return {
        "ok": True,
        "endpoints": [
            "/002011/scrape",
            "/002011/scrapeView",
            "/002011/list",
            "/002011/view_data",
            "/stop",
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
    label = clean_text(label)
    return FIELD_MAP.get(label, label)


def map_section_field(section_name: str, label: str) -> str:
    section_name = clean_text(section_name)
    label = clean_text(label)

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


def extract_view_id_from_onclick(onclick_text: str) -> str:
    if not onclick_text:
        return ""
    m = re.search(r"fn_view_page\('([^']+)'\)", onclick_text)
    return m.group(1) if m else ""


def normalize_date(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""
    return text.replace(".", "-").replace("--", "-").strip("-")


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


async def safe_click_search(page):
    """
    도봉/구로 계열 공통:
    - #btnSearch 우선
    - 없으면 '검색' 버튼 텍스트 fallback
    """
    if await page.query_selector("#btnSearch"):
        await page.click("#btnSearch")
        return

    btn = page.locator("button:has-text('검색')")
    if await btn.count() > 0:
        await btn.first.click()
        return

    inp = page.locator("input[type='submit']")
    if await inp.count() > 0:
        await inp.first.click()
        return


async def wait_list_rendered(page):
    candidates = [
        "div.table_wrap table.stable tbody tr",
        "table.stable tbody tr",
        "table.normal_list tbody tr",
        "table tbody tr"
    ]
    for selector in candidates:
        try:
            await page.wait_for_selector(selector, timeout=8000)
            return selector
        except Exception:
            continue
    return ""


async def detect_list_row_selector(page) -> str:
    candidates = [
        "table tbody tr"
    ]

    for selector in candidates:
        try:
            rows = await page.query_selector_all(selector)
            for row in rows:
                tds = await row.query_selector_all("td")
                if len(tds) >= 2:
                    return selector
        except Exception:
            pass
    return ""


async def detect_detail_row_selector(page) -> str:
    candidates = [
        "#sub_detail table.board_view tbody tr",
        "#sub_detail table tbody tr",
        "table.board_view tbody tr",
        "div#sub_board table.normal_list tr",
        "#sub_board table tr",
        "table tbody tr"
    ]
    for selector in candidates:
        try:
            count = await page.locator(selector).count()
            if count > 0:
                return selector
        except Exception:
            pass
    return ""


def build_list_url() -> str:
    return LIST_URL


# ---------------------------------------------------------
# 도봉구의회 전용/공용 유틸
# ---------------------------------------------------------
async def open_list_page_dobong(
    page,
    rasmbly_numpr: Optional[str] = None,
    ntime: Optional[str] = None,
    bill_kind: Optional[str] = None,
    proposer: Optional[str] = None,
    keyword: Optional[str] = None
):
    await page.goto(LIST_URL, wait_until="networkidle")

    async def try_select(selectors: List[str], value: str):
        for selector in selectors:
            try:
                if await page.locator(selector).count() > 0:
                    await page.select_option(selector, str(value))
                    return True
            except Exception:
                continue
        return False

    async def try_fill(selectors: List[str], value: str):
        for selector in selectors:
            try:
                if await page.locator(selector).count() > 0:
                    await page.fill(selector, str(value))
                    return True
            except Exception:
                continue
        return False

    if rasmbly_numpr:
        await try_select(["#seriesSch", "select[name='series']"], rasmbly_numpr)

    if ntime:
        await try_select(["#ntimeSch", "select[name='ntime']"], ntime)

    if bill_kind is not None:
        await try_select(["#kindSch", "select[name='kindSch']"], bill_kind)

    if proposer is not None:
        await try_select(["#memberSch", "select[name='memberSch']"], proposer)

    if keyword is not None:
        await try_fill(["#keywordSch", "input[name='keywordSch']"], keyword)

    # 도봉구는 검색 버튼 클릭 시 fn_egov_link_page(1) 호출
    try:
        await page.evaluate("fn_egov_link_page(1)")
    except Exception:
        if await page.locator("#btnSearch").count() > 0:
            await page.locator("#btnSearch").click()

    await page.wait_for_load_state("networkidle")
    await page.wait_for_selector("table tbody tr")


async def get_last_page_dobong(page) -> int:
    try:
        nav = await page.query_selector("#pagingNav")
        if nav:
            last_link = await nav.query_selector("a.num_last")
            if last_link:
                onclick = await last_link.get_attribute("onclick")
                if onclick:
                    nums = re.findall(r"\d+", onclick)
                    if nums:
                        return int(nums[-1])

            text = clean_text(await nav.inner_text())
            nums = re.findall(r"\b\d+\b", text)
            if nums:
                return max(map(int, nums))

        # fallback: 현재 페이지 하나로 처리
        return 1
    except Exception:
        return 1


async def move_to_page_dobong(page, page_no: int) -> bool:
    try:
        # fn_egov_link_page 구조 우선
        try:
            await page.evaluate(f"fn_egov_link_page({int(page_no)})")
            await page.wait_for_load_state("networkidle")
            await wait_list_rendered(page)
            return True
        except Exception:
            pass

        # paging 링크 클릭 fallback
        nav_link = page.locator(f"#pagingNav a[onclick*='fn_egov_link_page({int(page_no)})']")
        if await nav_link.count() > 0:
            await nav_link.first.click()
            await page.wait_for_load_state("networkidle")
            await wait_list_rendered(page)
            return True

        return False
    except Exception:
        return False


async def extract_list_meta_from_link(link, page_no: int) -> Dict[str, Any]:
    title = clean_text(await link.inner_text())
    href = await link.get_attribute("href")
    onclick = await link.get_attribute("onclick")

    view_id = extract_view_id_from_onclick(onclick or "")
    uid = extract_uid_from_href(href or "")
    final_id = view_id or uid

    view_url = ""
    if href and href not in ["#none", "#", "javascript:void(0);"]:
        view_url = urljoin(BASE_URL, href)

    return {
        "title": title,
        "view_id": final_id,
        "uid": final_id,
        "href": href or "",
        "onclick": onclick or "",
        "view_url": view_url,
        "page_index": page_no,
    }


async def open_detail_from_meta(page, meta: Dict[str, Any]) -> bool:
    """
    1) 직접 URL 있으면 direct open
    2) onclick view_id 있으면 현재 목록 페이지에서 클릭
    """
    view_url = meta.get("view_url", "")
    view_id = meta.get("view_id", "")

    if view_url:
        try:
            await page.goto(view_url, wait_until="networkidle")
            selector = await detect_detail_row_selector(page)
            return bool(selector)
        except Exception:
            pass

    if view_id:
        candidates = [
            f"a[onclick*=\"fn_view_page('{view_id}')\"]",
            f"a[onclick*='fn_view_page(\"{view_id}\")']",
            f"a[onclick*='{view_id}']"
        ]
        for selector in candidates:
            try:
                if await page.locator(selector).count() > 0:
                    await page.locator(selector).first.click()
                    await page.wait_for_load_state("networkidle")
                    detail_selector = await detect_detail_row_selector(page)
                    if detail_selector:
                        return True
            except Exception:
                continue

    return False


# ---------------------------------------------------------
# 리스트 수집
# ---------------------------------------------------------
async def scrape_bills_dobong(
    save_file: bool = True,
    rasmbly_numpr: Optional[str] = None,
    ntime: Optional[str] = None,
    bill_kind: Optional[str] = None,
    proposer: Optional[str] = None,
    keyword: Optional[str] = None
):
    global stop_scraping
    stop_scraping = False

    data_list: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await open_list_page_dobong(
            page,
            rasmbly_numpr=rasmbly_numpr,
            ntime=ntime,
            bill_kind=bill_kind,
            proposer=proposer,
            keyword=keyword
        )

        list_row_selector = await detect_list_row_selector(page)
        if not list_row_selector:
            await browser.close()
            return data_list

        last_page = await get_last_page_dobong(page)

        for page_no in range(1, last_page + 1):
            if stop_scraping:
                break

            if page_no > 1:
                moved = await move_to_page_dobong(page, page_no)
                if not moved:
                    break

            print(f"Scraping list page {page_no}/{last_page}")

            list_row_selector = await detect_list_row_selector(page)
            rows = await page.query_selector_all(list_row_selector)
            if not rows:
                continue

            for row in rows:
                if stop_scraping:
                    break

                cells = await row.query_selector_all("td")
                if len(cells) < 4:
                    continue

                title_link = await row.query_selector("td a")

                title = ""
                link_meta = {
                    "view_id": "",
                    "uid": "",
                    "view_url": "",
                    "page_index": page_no
                }

                if title_link:
                    link_meta = await extract_list_meta_from_link(title_link, page_no)
                    title = link_meta["title"]
                else:
                    title = clean_text(await cells[1].inner_text()) if len(cells) > 1 else ""

                # 도봉구 현재 공개 페이지 기준 리스트 컬럼:
                # 의안No / 의안명 / 발의자 / 소관위원회 / 처리일 / 처리결과
                raw_item = {
                    "의안번호": clean_text(await cells[0].inner_text()) if len(cells) > 0 else "",
                    "의안명": title,
                    "대표 발의자": clean_text(await cells[2].inner_text()) if len(cells) > 2 else "",
                    "소관위원회": clean_text(await cells[3].inner_text()) if len(cells) > 3 else "",
                    "처리일": clean_text(await cells[4].inner_text()) if len(cells) > 4 else "",
                    "처리결과": clean_text(await cells[5].inner_text()) if len(cells) > 5 else "",
                }

                mapped_item = {}
                for raw_key, value in raw_item.items():
                    mapped_key = map_general_field(raw_key)
                    if raw_key in ["처리일"]:
                        append_mapped_value(mapped_item, mapped_key, normalize_date(value))
                    else:
                        append_mapped_value(mapped_item, mapped_key, value)

                if rasmbly_numpr:
                    mapped_item["RASMBLY_NUMPR"] = str(rasmbly_numpr)

                if link_meta.get("view_id"):
                    mapped_item["view_id"] = link_meta["view_id"]
                    mapped_item["uid"] = link_meta["uid"]

                mapped_item["page_index"] = page_no
                if link_meta.get("view_url"):
                    mapped_item["view_url"] = link_meta["view_url"]
                elif link_meta.get("view_id"):
                    mapped_item["view_url"] = f"{LIST_URL}#view_id={link_meta['view_id']}"
                else:
                    mapped_item["view_url"] = ""

                data_list.append(mapped_item)

        await browser.close()

    if save_file:
        suffix_parts = []
        if rasmbly_numpr:
            suffix_parts.append(str(rasmbly_numpr))
        if ntime:
            suffix_parts.append(f"ntime_{ntime}")
        suffix = f"_{'_'.join(suffix_parts)}" if suffix_parts else ""
        filename = save_json(data_list, f"bill_002011_list{suffix}")
        print(f"List data scraped and saved to {filename}")

    return data_list


@app.api_route("/002011/scrape", methods=["GET", "POST"])
async def scrape_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None),
    ntime: Optional[str] = Query(default=None),
    bill_kind: Optional[str] = Query(default=None),
    proposer: Optional[str] = Query(default=None),
    keyword: Optional[str] = Query(default=None),
):
    global stop_scraping
    stop_scraping = False

    data = await scrape_bills_dobong(
        save_file=True,
        rasmbly_numpr=rasmbly_numpr,
        ntime=ntime,
        bill_kind=bill_kind,
        proposer=proposer,
        keyword=keyword
    )
    return data


# ---------------------------------------------------------
# 상세(View) 수집용 리스트 메타 수집
# ---------------------------------------------------------
async def collect_view_items_dobong(
    page,
    rasmbly_numpr: Optional[str] = None,
    ntime: Optional[str] = None,
    bill_kind: Optional[str] = None,
    proposer: Optional[str] = None,
    keyword: Optional[str] = None
) -> List[Dict[str, Any]]:
    global stop_scraping

    view_items: List[Dict[str, Any]] = []

    await open_list_page_dobong(
        page,
        rasmbly_numpr=rasmbly_numpr,
        ntime=ntime,
        bill_kind=bill_kind,
        proposer=proposer,
        keyword=keyword
    )

    # last_page = await get_last_page_dobong(page)
    last_page = 3  # 테스트용 상한, 실제론 위에서 get_last_page_dobong() 호출

    for page_no in range(1, last_page + 1):
        if stop_scraping:
            break

        if page_no > 1:
            moved = await move_to_page_dobong(page, page_no)
            if not moved:
                break

            current_page = await get_current_page_index(page)
            if current_page != page_no:
                print(f"page mismatch: requested={page_no}, actual={current_page}")
                break

        print(f"Collecting view ids from page {page_no}/{last_page}")

        rows = await page.query_selector_all("table tbody tr")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 2:
                continue

            title_link = await cells[1].query_selector("a")
            if not title_link:
                continue

            title = clean_text(await title_link.inner_text())
            href = await title_link.get_attribute("href")
            onclick = await title_link.get_attribute("onclick")

            view_id = extract_view_id_from_onclick(onclick or "")
            uid = extract_uid_from_href(href or "")
            final_id = view_id or uid

            bill_no = clean_text(await cells[0].inner_text()) if len(cells) > 0 else ""
            proposer_text = clean_text(await cells[2].inner_text()) if len(cells) > 2 else ""
            committee = clean_text(await cells[3].inner_text()) if len(cells) > 3 else ""
            process_date = clean_text(await cells[4].inner_text()) if len(cells) > 4 else ""
            result = clean_text(await cells[5].inner_text()) if len(cells) > 5 else ""

            if final_id:
                view_items.append({
                    "view_id": final_id,
                    "uid": final_id,
                    "page_index": page_no,
                    "BI_NO": bill_no,
                    "BI_SJ": title,
                    "PROPSR": proposer_text,
                    "JRSD_CMIT_NM": committee,
                    "PROCESS_DE": normalize_date(process_date),
                    "RESULT": result,
                })

    dedup = {}
    for item in view_items:
        dedup[item["view_id"]] = item

    return list(dedup.values())


# ---------------------------------------------------------
# 상세(View) 일반 테이블 파싱
# ---------------------------------------------------------
async def parse_general_view_table_dobong(page, item: Dict[str, Any]):
    rows_selector = await detect_detail_row_selector(page)
    if not rows_selector:
        return

    rows = page.locator(rows_selector)
    row_count = await rows.count()

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

    for i in range(row_count):
        try:
            row = rows.nth(i)
            ths = row.locator("th")
            tds = row.locator("td")

            th_count = await ths.count()
            td_count = await tds.count()

            if th_count == 0 or td_count == 0:
                continue

            first_text = clean_text(await ths.nth(0).inner_text())
            first_rowspan = await ths.nth(0).get_attribute("rowspan")

            # 위원회/본회의 섹션 시작행 제외
            if first_rowspan and normalize_section_name(first_text) in section_aliases:
                continue

            # 일반 필드: 마지막 td를 값으로 사용
            if th_count >= 1 and td_count >= 1:
                key = clean_text(await ths.nth(0).inner_text())
                val = clean_text(await tds.nth(td_count - 1).inner_text())

                if not key:
                    continue

                if key in section_aliases or key in section_detail_aliases:
                    continue

                if key in attachment_aliases:
                    await parse_attachment_row_locator(row, item)
                    continue

                mapped_key = map_general_field(key)

                if key == "의안번호":
                    bill_no = val
                    session_no = ""

                    m_bill = re.search(r"^\s*([^\s(]+)", val)
                    if m_bill:
                        bill_no = m_bill.group(1)

                    m_session = re.search(r"제\s*(\d+)\s*회", val)
                    if m_session:
                        session_no = m_session.group(1)

                    append_mapped_value(item, mapped_key, bill_no)
                    if session_no:
                        append_mapped_value(item, map_general_field("회기"), session_no)
                    continue

                if key in ["발의(제출)일", "공포일", "처리일"]:
                    append_mapped_value(item, mapped_key, normalize_date(val))
                else:
                    append_mapped_value(item, mapped_key, val)

        except Exception as e:
            print(f"row parse skip: {e}")
            continue

async def parse_attachment_row_locator(row_locator, item: Dict[str, Any]):
    file_name_key = FIELD_MAP.get("첨부파일", "BI_FILE_NM")
    file_url_key = FIELD_MAP.get("첨부파일링크", "BI_FILE_URL")

    links = row_locator.locator("td.con a, td a")
    count = await links.count()
    if count == 0:
        return

    file_names = []
    file_urls = []
    attachments = []

    for i in range(count):
        link = links.nth(i)
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

# ---------------------------------------------------------
# 상세(View) 섹션 파싱
# ---------------------------------------------------------
async def parse_section_tables_dobong(page, item: Dict[str, Any]):
    item["sections"] = {}

    headings = page.locator("#sub_detail h5")
    heading_count = await headings.count()

    for i in range(heading_count):
        try:
            heading = headings.nth(i)
            heading_text = clean_text(await heading.inner_text())
            section_name = normalize_heading_section_name(heading_text)

            if section_name not in ["위원회", "본회의"]:
                continue

            # h5 바로 다음 table.board_view
            table = heading.locator("xpath=following-sibling::table[1]")
            if await table.count() == 0:
                continue

            rows = table.locator("tbody tr")
            row_count = await rows.count()
            if row_count == 0:
                continue

            section_obj = item["sections"].setdefault(section_name, {})

            for j in range(row_count):
                row = rows.nth(j)
                ths = row.locator("th")
                tds = row.locator("td")

                th_count = await ths.count()
                td_count = await tds.count()

                if th_count == 0 or td_count == 0:
                    continue

                key = clean_text(await ths.nth(0).inner_text())
                td = tds.nth(td_count - 1)
                val = clean_text(await td.inner_text())

                if not key:
                    continue

                mapped_key = map_section_field(section_name, key)

                if key in ["회부일", "접수일", "상정일", "처리일", "의결일", "보고일", "심사보고일"]:
                    append_mapped_value(section_obj, mapped_key, normalize_date(val))
                else:
                    append_mapped_value(section_obj, mapped_key, val)

                if key == "관련 회의록":
                    await append_related_meeting_from_td_locator(td, section_obj)

        except Exception as e:
            print(f"section heading parse skip: {e}")
            continue

async def append_related_meeting_from_td_locator(td_locator, section_obj: Dict[str, Any]):
    links = td_locator.locator("a")
    count = await links.count()
    if count == 0:
        return

    names = []
    urls = []

    for i in range(count):
        link = links.nth(i)
        text = clean_text(await link.inner_text())
        href = await link.get_attribute("href")
        if text:
            names.append(text)
        if href:
            urls.append(urljoin(BASE_URL, href))

    if names:
        append_mapped_value(section_obj, "RELATED_MEETING_NM", names[0] if len(names) == 1 else names)
    if urls:
        append_mapped_value(section_obj, "RELATED_MEETING_URL", urls[0] if len(urls) == 1 else urls)

async def parse_attachment_row(row, item: Dict[str, Any]):
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


# ---------------------------------------------------------
# 상세(View) 수집
# ---------------------------------------------------------
async def scrape_view_details_dobong(
    save_file: bool = True,
    rasmbly_numpr: Optional[str] = None,
    ntime: Optional[str] = None,
    bill_kind: Optional[str] = None,
    proposer: Optional[str] = None,
    keyword: Optional[str] = None
):
    global stop_scraping
    stop_scraping = False

    details: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        view_items = await collect_view_items_dobong(
            page,
            rasmbly_numpr=rasmbly_numpr,
            ntime=ntime,
            bill_kind=bill_kind,
            proposer=proposer,
            keyword=keyword
        )

        for meta in view_items:
            if stop_scraping:
                break

            target_page = int(meta.get("page_index", 1))
            view_id = meta.get("view_id", "")

            print(f"Scraping view: page={target_page}, view={view_id}")

            # 매건마다 검색화면 새로 열기
            await open_list_page_dobong(
                page,
                rasmbly_numpr=rasmbly_numpr,
                ntime=ntime,
                bill_kind=bill_kind,
                proposer=proposer,
                keyword=keyword
            )

            if target_page > 1:
                moved = await move_to_page_dobong(page, target_page)
                if not moved:
                    print(f"Failed moving to page {target_page}")
                    continue

                current_page = await get_current_page_index(page)
                if current_page != target_page:
                    print(f"page mismatch: requested={target_page}, actual={current_page}")
                    continue

            opened = await open_detail_by_view_id_dobong(page, view_id)
            if not opened:
                print(f"Failed opening detail for {view_id}")
                continue

            item: Dict[str, Any] = {
                "view_id": view_id,
                "uid": view_id,
                "view_url": f"{LIST_URL}#view_id={view_id}",
                "BI_NO": meta.get("BI_NO", ""),
                "BI_SJ": meta.get("BI_SJ", ""),
            }

            if meta.get("PROPSR"):
                item["PROPSR"] = meta["PROPSR"]
            if meta.get("JRSD_CMIT_NM"):
                item["JRSD_CMIT_NM"] = meta["JRSD_CMIT_NM"]
            if meta.get("PROCESS_DE"):
                item["PROCESS_DE"] = meta["PROCESS_DE"]
            if meta.get("RESULT"):
                item["RESULT"] = meta["RESULT"]
            if rasmbly_numpr:
                item["RASMBLY_NUMPR"] = str(rasmbly_numpr)

            try:
                await parse_general_view_table_dobong(page, item)
            except Exception as e:
                print(f"General parse failed for view {view_id}: {e}")

            try:
                await parse_section_tables_dobong(page, item)
            except Exception as e:
                print(f"Section parse failed for view {view_id}: {e}")

            details.append(item)

        await browser.close()

    if save_file:
        suffix_parts = []
        if rasmbly_numpr:
            suffix_parts.append(str(rasmbly_numpr))
        if ntime:
            suffix_parts.append(f"ntime_{ntime}")
        suffix = f"_{'_'.join(suffix_parts)}" if suffix_parts else ""
        filename = save_json(details, f"bill_002011_view{suffix}")
        print(f"View data scraped and saved to {filename}")

    return details

async def open_detail_by_view_id_dobong(page, view_id: str) -> bool:
    selectors = [
        f"a[onclick*=\"fn_view_page('{view_id}')\"]",
        f"a[onclick*='{view_id}']",
        f"a[href*='uid={view_id}']",
    ]

    for selector in selectors:
        try:
            if await page.locator(selector).count() > 0:
                await page.locator(selector).first.click()
                await page.wait_for_load_state("networkidle")
                await page.wait_for_selector("table tbody tr", timeout=10000)
                return True
        except Exception:
            continue

    return False

async def get_current_page_index(page) -> int:
    try:
        if await page.locator("#pageIndex").count() > 0:
            value = await page.locator("#pageIndex").input_value()
            return int(value)
    except Exception:
        pass
    return 1

def normalize_heading_section_name(name: str) -> str:
    name = clean_text(name)

    if "소관위원회" in name and "처리결과" in name:
        return "위원회"
    if "위원회" in name and "처리결과" in name:
        return "위원회"
    if "본회의" in name and "처리결과" in name:
        return "본회의"

    return name

@app.api_route("/002011/scrapeView", methods=["GET", "POST"])
@app.api_route("/002011/view", methods=["GET", "POST"])
async def scrape_view_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None),
    ntime: Optional[str] = Query(default=None),
    bill_kind: Optional[str] = Query(default=None),
    proposer: Optional[str] = Query(default=None),
    keyword: Optional[str] = Query(default=None),
):
    global stop_scraping
    stop_scraping = False

    data = await scrape_view_details_dobong(
        save_file=True,
        rasmbly_numpr=rasmbly_numpr,
        ntime=ntime,
        bill_kind=bill_kind,
        proposer=proposer,
        keyword=keyword
    )
    return data


# ---------------------------------------------------------
# 파일 조회 API
# ---------------------------------------------------------
@app.get("/002011/list")
async def get_list_data():
    filename = _latest_file("bill_002011_list")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"error": "List data not found"}


@app.get("/002011/view_data")
async def get_view_data():
    filename = _latest_file("bill_002011_view")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"error": "View data not found"}


@app.get("/stop")
async def stop_scraping_endpoint():
    global stop_scraping
    stop_scraping = True
    return {"message": "Stop signal set. Current scraping will stop after current item."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8902)