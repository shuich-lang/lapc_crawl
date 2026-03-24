from fastapi import FastAPI, Query
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json
import os
import datetime
import re
from typing import Optional, List, Dict, Any
from field_maps.field_map import FIELD_MAP, SECTION_FIELD_MAP
from urllib.parse import urljoin

app = FastAPI()

stop_scraping = False

BASE_LIST_URL = "https://gimpocouncil.go.kr/cnts/bls/billList.php?bbsCd=mnt&bbsSubCd=mnt04"
BASE_VIEW_URL = "https://gimpocouncil.go.kr/cnts/bls/billView.php"


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
        "example": {
            "list": "http://localhost:8903/031009/scrape?rasmbly_numpr=8",
            "view": "http://localhost:8903/031009/scrapeView?rasmbly_numpr=8",
        }
    }


def make_list_url(rasmbly_numpr: Optional[str] = None) -> str:
    sch_gnrtn = str(rasmbly_numpr).strip() if rasmbly_numpr else "8"
    return f"{BASE_LIST_URL}&schGnrtn={sch_gnrtn}"


def make_view_url(uid: str, rasmbly_numpr: Optional[str] = None) -> str:
    sch_gnrtn = str(rasmbly_numpr).strip() if rasmbly_numpr else "0"
    return (
        f"{BASE_VIEW_URL}?"
        f"bbsSn={uid}&mbrSn=&flSn=&totalCnt=4457&pageNo=1&schGnrtn={sch_gnrtn}&schSesn=0"
        f"&schBillNo=0&schCmtCd=&schSrtCd=&schPrpslCd=&schCmtRsltCd=&schCpsRsltCd="
        f"&schTle=&schMbrCd=&schMbrPrpslCd=&bbsCd=mnt&bbsSubCd=mnt04"
    )


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
    if not js_text:
        return ""
    m = re.search(rf"{func_name}\('([^']+)'\)", js_text)
    return m.group(1) if m else ""


def save_json(data, prefix: str) -> str:
    os.makedirs("download", exist_ok=True)
    date_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"download/{prefix}_{date_str}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    return filename


async def goto_list_page(page, page_num: int):
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


async def scrape_bills_gimpo(
    rasmbly_numpr: Optional[str] = None,
    save_file: bool = True
) -> List[Dict[str, Any]]:
    global stop_scraping
    stop_scraping = False

    url = make_list_url(rasmbly_numpr)
    data_list: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print(f"[LIST] open url: {url}")

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

            print(f"[LIST] Scraping page {page_num}/{max_pages}")

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

                item = {
                    "view_id": uid,
                    "view_url": make_view_url(uid, rasmbly_numpr) if uid else "",
                    "BI_NO": bill_num,
                    "BI_SJ": title,
                    "PROPSR": proposer,
                    "JRSD_CMIT_NM": committee,
                    "CMIT_RESULT": committee_result,
                    "PLNMT_RESULT": plenary_result,
                    "ITNC_DE": proposal_date,
                    "TITLE": title,
                }

                if rasmbly_numpr:
                    item["RASMBLY_NUMPR"] = str(rasmbly_numpr)

                data_list.append(item)

            page_num += 1
            if page_num <= max_pages:
                try:
                    await goto_list_page(page, page_num)
                except Exception as e:
                    print(f"[LIST] Failed to go to page {page_num}: {e}")
                    break

        await browser.close()

    if save_file:
        suffix = f"_{rasmbly_numpr}" if rasmbly_numpr else ""
        filename = save_json(data_list, f"bill_031009_list{suffix}")
        print(f"[LIST] data scraped and saved to {filename}")

    return data_list


async def scrape_view_details_gimpo(
    rasmbly_numpr: Optional[str] = None,
    save_file: bool = True
) -> List[Dict[str, Any]]:
    global stop_scraping
    stop_scraping = False

    url = make_list_url(rasmbly_numpr)
    details: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        base_url = "https://gimpocouncil.go.kr"

        print(f"[VIEW] open list url: {url}")

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
                #max_pages = int(page_str)
                max_pages = 2 # 테스트용으로 최대 3페이지만 수집하도록 제한, 실제 운영 시에는 위 라인으로 변경하여 전체 페이지 수집

        while page_num <= max_pages:
            if stop_scraping:
                break

            print(f"[VIEW] Collecting UIDs from page {page_num}/{max_pages}")

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
                    print(f"[VIEW] Failed to go to next page: {e}")
                    break

        print(f"[VIEW] uid count = {len(uids)}")

        for uid in uids:
            if stop_scraping:
                break

            try:
                view_url = make_view_url(uid, rasmbly_numpr)

                await page.goto(view_url, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle")

                rows = page.locator("div.billcontent div.bill_view div.pcView table.table_bill tbody tr")
                row_count = await rows.count()

                item: Dict[str, Any] = {
                    "uid": uid,
                    "view_id": uid,
                    "view_url": view_url,
                }

                if rasmbly_numpr:
                    item["RASMBLY_NUMPR"] = str(rasmbly_numpr)

                current_section = ""

                for i in range(row_count):
                    row = rows.nth(i)

                    ths = row.locator("th")
                    tds = row.locator("td")

                    th_count = await ths.count()
                    td_count = await tds.count()

                    if th_count == 0 or td_count == 0:
                        continue

                    th_texts = [clean_text(await ths.nth(j).inner_text()) for j in range(th_count)]

                    # ---------------------------------
                    # 1. 제안대수/회기 분리
                    # ---------------------------------
                    if "제안대수/회기" in th_texts:
                        for j in range(min(th_count, td_count)):
                            key = clean_text(await ths.nth(j).inner_text())
                            val = clean_text(await tds.nth(j).inner_text())

                            if key == "제안대수/회기":
                                numpr, sesn = split_rasmbly_session(val)
                                if numpr:
                                    item["RASMBLY_NUMPR"] = numpr
                                if sesn:
                                    item["RASMBLY_SESN"] = sesn
                            else:
                                eng_key = map_field("", key)
                                if eng_key and eng_key not in ("관련위원회", "재이송일"):
                                    append_value(item, eng_key, val)
                        continue

                    # ---------------------------------
                    # 2. 첨부 > 의안
                    # ---------------------------------
                    # 구조:
                    # <th rowspan="2">첨부</th>
                    # <th>의안</th>
                    # <td ...> ... 파일링크들 ... </td>
                    # 그리고 다음 행은 보고서/첨부파일
                    # 여기서는 "의안"만 BI_FILE_NM / BI_FILE_URL 로 저장
                    if th_count >= 2:
                        first_th = th_texts[0]
                        second_th = th_texts[1] if th_count > 1 else ""

                        if first_th == "첨부" and second_th == "의안":
                            td = tds.nth(0)
                            files = await extract_multi_files(td, base_url)

                            if files:
                                item["BI_FILE_NM"] = ", ".join([f["name"] for f in files])
                                item["BI_FILE_URL"] = ", ".join([f["url"] for f in files])
                            continue

                    # ---------------------------------
                    # 3. 회의록보기 -> CMIT_RELATED_MEETING
                    # ---------------------------------
                    if "회의록보기" in th_texts:
                        td = tds.nth(0)
                        names = await extract_meeting_names_from_td(td)
                        if names:
                            item["CMIT_RELATED_MEETING"] = ", ".join(names)
                        continue

                    # ---------------------------------
                    # 4. 섹션 처리
                    # 위원회 처리사항 / 본회의 처리사항 / 재의 처리사항
                    # ---------------------------------
                    if th_count >= 2 and td_count >= 1:
                        first_th = th_texts[0]

                        if first_th in SECTION_FIELD_MAP:
                            current_section = first_th

                            # 첫 th는 섹션명이고, 나머지 th부터 실제 key
                            sub_ths = th_texts[1:]

                            for j, sub_key in enumerate(sub_ths):
                                if j >= td_count:
                                    continue

                                # 제외 대상
                                if sub_key in ("관련위원회", "재이송일"):
                                    continue

                                eng_key = map_field(current_section, sub_key)
                                if not eng_key:
                                    continue

                                val = clean_text(await tds.nth(j).inner_text())
                                append_value(item, eng_key, val)

                            continue

                    # ---------------------------------
                    # 5. 이전 섹션의 이어지는 행 처리
                    # ---------------------------------
                    if current_section and th_count == td_count:
                        for j in range(min(th_count, td_count)):
                            key = th_texts[j]

                            # 제외 대상
                            if key in ("관련위원회", "재이송일"):
                                continue

                            eng_key = map_field(current_section, key)
                            if not eng_key:
                                continue

                            val = clean_text(await tds.nth(j).inner_text())
                            append_value(item, eng_key, val)
                        continue

                    # ---------------------------------
                    # 6. 일반 1:1 / 다중 쌍 처리
                    # ---------------------------------
                    pair_count = min(th_count, td_count)
                    for j in range(pair_count):
                        key = th_texts[j]
                        val = clean_text(await tds.nth(j).inner_text())

                        # 제외 대상
                        if key in ("관련위원회", "재이송일"):
                            continue

                        # 제안대수/회기 별도 처리
                        if key == "제안대수/회기":
                            numpr, sesn = split_rasmbly_session(val)
                            if numpr:
                                item["RASMBLY_NUMPR"] = numpr
                            if sesn:
                                item["RASMBLY_SESN"] = sesn
                            continue

                        # 회의록보기 별도 처리
                        if key == "회의록보기":
                            names = await extract_meeting_names_from_td(tds.nth(j))
                            if names:
                                item["CMIT_RELATED_MEETING"] = ", ".join(names)
                            continue

                        eng_key = map_field("", key)

                        # map_field 결과가 한글 그대로면 불필요 컬럼일 가능성 큼
                        if eng_key in ("관련위원회", "재이송일"):
                            continue

                        append_value(item, eng_key, val)

                # 후처리
                item["TITLE"] = item.get("BI_SJ", "")
                item["CONTENT"] = item.get("BI_OUTLINE", "")
                if "REG_DATE" not in item:
                    item["REG_DATE"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # 불필요/제외 대상 키 제거
                for drop_key in ["제안대수/회기", "관련위원회", "재이송일", "회의록보기", "첨부파일", "첨부"]:
                    item.pop(drop_key, None)

                details.append(item)
                print(f"[VIEW] Collected view data for uid {uid}")

            except Exception as e:
                print(f"[VIEW] Failed to get view {uid}: {e}")
                continue

        await browser.close()

    if save_file:
        suffix = f"_{rasmbly_numpr}" if rasmbly_numpr else ""
        filename = save_json(details, f"bill_031009_view{suffix}")
        print(f"[VIEW] data scraped and saved to {filename}")

    return details

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
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


def split_rasmbly_session(text: str):
    """
    예: '제8대 제265회' -> ('8', '265')
    """
    text = clean_text(text)
    m = re.search(r"제\s*(\d+)\s*대\s*제\s*(\d+)\s*회", text)
    if m:
        return m.group(1), m.group(2)

    nums = re.findall(r"\d+", text)
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], ""

    return "", ""


async def extract_multi_files(td, base_url):
    results = []

    items = td.locator("li")
    li_count = await items.count()

    for i in range(li_count):
        li = items.nth(i)

        links = li.locator("a")
        link_count = await links.count()

        for j in range(link_count):
            a = links.nth(j)

            href = await a.get_attribute("href") or ""
            title = (await a.inner_text()).strip()

            # 🔥 fileDownLoad만 필터
            if "fileDownLoad" not in href:
                continue

            if not title:
                continue

            # fileDownLoad('18635','bill')
            m = re.search(r"fileDownLoad\('([^']+)','([^']+)'\)", href)
            if not m:
                continue

            flSn = m.group(1)
            flCd = m.group(2)

            # 🔥 핵심 추가: 의안(bill)만 허용
            if flCd != "bill":
                continue

            file_url = urljoin(
                base_url,
                f"/sma/utl/FileDownLoad.php?flSn={flSn}&flCd={flCd}"
            )

            results.append({
                "name": title,
                "url": file_url
            })

    return results

async def extract_meeting_names_from_td(td):
    names = []

    links = td.locator("a")  # ✅ 변경
    count = await links.count()

    for i in range(count):
        a = links.nth(i)
        text = clean_text(await a.inner_text())
        if text:
            names.append(text)

    return names

@app.api_route("/031009/scrape", methods=["GET", "POST"])
async def scrape_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None)
):
    global stop_scraping
    stop_scraping = False

    data = await scrape_bills_gimpo(
        rasmbly_numpr=rasmbly_numpr,
        save_file=True
    )

    return {
        "ok": True,
        "message": "Scraping completed",
        "count": len(data),
        "rasmbly_numpr": rasmbly_numpr,
        "data": data
    }


@app.api_route("/031009/scrapeView", methods=["GET", "POST"])
async def scrape_view_endpoint(
    rasmbly_numpr: Optional[str] = Query(default=None)
):
    global stop_scraping
    stop_scraping = False

    data = await scrape_view_details_gimpo(
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
    filename = _latest_file("bill_031009_list")
    if filename and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    return {"error": "Data not found"}


@app.get("/031009/view_data")
async def get_view_data():
    filename = _latest_file("bill_031009_view")
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
    uvicorn.run(app, host="0.0.0.0", port=8904)