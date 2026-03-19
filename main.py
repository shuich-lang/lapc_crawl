"""
전주시의회 의안 전체 수집 스크립트 (FastAPI)
URL: http://council.jeonju.go.kr/source/kr/assembly/bill.html
총 6144건 / 615페이지
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
import time
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import uvicorn

app = FastAPI(title="전주시의회 의안 수집 API")

BASE_URL = "https://council.jeonju.go.kr/source/kr/assembly/bill.html"
TOTAL_PAGES = 615  # 마지막 페이지 (HTML에서 확인)

# 글로벌 변수로 크롤링 상태 관리
crawling_stopped = False

async def extract_detail_info(page):
    """상세 페이지에서 구조화된 정보 추출"""
    detail_info = {}

    try:
        # 의안명
        title_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(1) td[colspan="3"]')
        detail_info["상세_의안명"] = (await title_elem.inner_text()).strip() if title_elem else ""

        # 대수 / 회기
        daesu_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(2) td:nth-child(1)')
        detail_info["상세_대수"] = (await daesu_elem.inner_text()).strip() if daesu_elem else ""
        hoegi_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(2) td:nth-child(2)')
        detail_info["상세_회기"] = (await hoegi_elem.inner_text()).strip() if hoegi_elem else ""

        # 의안 번호 / 의안 종류
        bill_no_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(3) td:nth-child(1)')
        detail_info["상세_의안번호"] = (await bill_no_elem.inner_text()).strip() if bill_no_elem else ""
        bill_type_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(3) td:nth-child(2)')
        detail_info["상세_의안종류"] = (await bill_type_elem.inner_text()).strip() if bill_type_elem else ""

        # 발의일 / 대표 발의 의원
        proposal_date_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(4) td:nth-child(1)')
        detail_info["상세_발의일"] = (await proposal_date_elem.inner_text()).strip() if proposal_date_elem else ""
        main_proposer_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(4) td:nth-child(2)')
        detail_info["상세_대표발의의원"] = (await main_proposer_elem.inner_text()).strip() if main_proposer_elem else ""

        # 공동 발의 의원
        co_proposers_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(5) td[colspan="3"]')
        detail_info["상세_공동발의의원"] = (await co_proposers_elem.inner_text()).strip() if co_proposers_elem else ""

        # 위원회 정보
        committee_info = await page.query_selector('table.board_view.bill tbody tr:nth-child(6) td:nth-child(1)')
        detail_info["상세_위원회_소관위"] = (await committee_info.inner_text()).strip() if committee_info else ""
        committee_assign_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(6) td:nth-child(2)')
        detail_info["상세_위원회_회부일"] = (await committee_assign_date.inner_text()).strip() if committee_assign_date else ""
        committee_present_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(7) td:nth-child(1)')
        detail_info["상세_위원회_상정일"] = (await committee_present_date.inner_text()).strip() if committee_present_date else ""
        committee_process_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(7) td:nth-child(2)')
        detail_info["상세_위원회_처리일"] = (await committee_process_date.inner_text()).strip() if committee_process_date else ""
        committee_result = await page.query_selector('table.board_view.bill tbody tr:nth-child(8) td[colspan="3"]')
        detail_info["상세_위원회_처리결과"] = (await committee_result.inner_text()).strip() if committee_result else ""

        # 본회의 정보
        plenary_assign_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(9) td[colspan="3"]')
        detail_info["상세_본회의_회부일"] = (await plenary_assign_date.inner_text()).strip() if plenary_assign_date else ""
        plenary_present_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(10) td:nth-child(1)')
        detail_info["상세_본회의_상정일"] = (await plenary_present_date.inner_text()).strip() if plenary_present_date else ""
        plenary_process_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(10) td:nth-child(2)')
        detail_info["상세_본회의_처리일"] = (await plenary_process_date.inner_text()).strip() if plenary_process_date else ""
        plenary_result = await page.query_selector('table.board_view.bill tbody tr:nth-child(11) td:nth-child(1)')
        detail_info["상세_본회의_처리결과"] = (await plenary_result.inner_text()).strip() if plenary_result else ""

        # 집행부 이송일 / 공포일
        transfer_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(12) td:nth-child(1)')
        detail_info["상세_집행부이송일"] = (await transfer_date.inner_text()).strip() if transfer_date else ""
        promulgation_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(12) td:nth-child(2)')
        detail_info["상세_공포일"] = (await promulgation_date.inner_text()).strip() if promulgation_date else ""

        # 공포 번호
        promulgation_no = await page.query_selector('table.board_view.bill tbody tr:nth-child(13) td:nth-child(1)')
        detail_info["상세_공포번호"] = (await promulgation_no.inner_text()).strip() if promulgation_no else ""

        # 비고
        remarks = await page.query_selector('table.board_view.bill tbody tr:nth-child(14) td[colspan="4"]')
        detail_info["상세_비고"] = (await remarks.inner_text()).strip() if remarks else ""

        # 첨부파일
        attachment_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(15) td[colspan="3"]')
        if attachment_elem:
            attachment_text = await attachment_elem.inner_text()
            attachment_links = await attachment_elem.query_selector_all('a')
            attachment_info = []
            for link in attachment_links:
                link_text = await link.inner_text()
                link_href = await link.get_attribute('href')
                attachment_info.append(f"{link_text} ({link_href})")
            detail_info["상세_첨부파일"] = " | ".join(attachment_info) if attachment_info else attachment_text.strip()
        else:
            detail_info["상세_첨부파일"] = ""

    except Exception as e:
        detail_info["상세_오류"] = str(e)

    return detail_info


def render_detail_table(detail: dict) -> str:
    """상세 데이터를 HTML 테이블 형태로 렌더링"""
    def safe(v):
        return (v or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    return f"""
<table class=\"board_view bill\">
    <tbody>
        <tr>
            <th scope=\"row\" colspan=\"2\">의안명</th>
            <td colspan=\"3\">{safe(detail.get('상세_의안명'))}</td>
        </tr>
        <tr>
            <th scope=\"row\" colspan=\"2\">대수</th>
            <td>{safe(detail.get('상세_대수'))}</td>
            <th scope=\"row\">회기</th>
            <td>{safe(detail.get('상세_회기'))}</td>
        </tr>
        <tr>
            <th scope=\"row\" colspan=\"2\">의안 번호</th>
            <td>{safe(detail.get('상세_의안번호'))}</td>
            <th scope=\"row\">의안 종류</th>
            <td>{safe(detail.get('상세_의안종류'))}</td>
        </tr>
        <tr>
            <th scope=\"row\" colspan=\"2\">발의일</th>
            <td>{safe(detail.get('상세_발의일'))}</td>
            <th scope=\"row\">대표 발의 의원</th>
            <td>{safe(detail.get('상세_대표발의의원'))}</td>
        </tr>
        <tr>
            <th scope=\"row\" colspan=\"2\">공동 발의 의원</th>
            <td colspan=\"3\">{safe(detail.get('상세_공동발의의원'))}</td>
        </tr>
        <tr>
            <th scope=\"row\" rowspan=\"3\" class=\"borderR\">위원회</th>
            <th scope=\"row\">소관위</th>
            <td>{safe(detail.get('상세_위원회_소관위'))}</td>
            <th scope=\"row\">회부일</th>
            <td>{safe(detail.get('상세_위원회_회부일'))}</td>
        </tr>
        <tr>
            <th scope=\"row\">상정일</th>
            <td>{safe(detail.get('상세_위원회_상정일'))}</td>
            <th scope=\"row\">처리일</th>
            <td>{safe(detail.get('상세_위원회_처리일'))}</td>
        </tr>
        <tr>
            <th scope=\"row\">처리결과</th>
            <td colspan=\"3\">{safe(detail.get('상세_위원회_처리결과'))}</td>
        </tr>
        <tr>
            <th scope=\"row\" rowspan=\"3\" class=\"borderR\">본회의</th>
            <th scope=\"row\">회부일</th>
            <td colspan=\"3\">{safe(detail.get('상세_본회의_회부일'))}</td>
        </tr>
        <tr>
            <th scope=\"row\">상정일</th>
            <td>{safe(detail.get('상세_본회의_상정일'))}</td>
            <th scope=\"row\">처리일</th>
            <td>{safe(detail.get('상세_본회의_처리일'))}</td>
        </tr>
        <tr>
            <th scope=\"row\">처리 결과</th>
            <td>{safe(detail.get('상세_본회의_처리결과'))}</td>
            <td colspan=\"2\"></td>
        </tr>
        <tr>
            <th scope=\"row\" colspan=\"2\">집행부 이송일</th>
            <td>{safe(detail.get('상세_집행부이송일'))}</td>
            <th scope=\"row\">공포일</th>
            <td>{safe(detail.get('상세_공포일'))}</td>
        </tr>
        <tr>
            <th scope=\"row\" colspan=\"2\">공포 번호</th>
            <td>{safe(detail.get('상세_공포번호'))}</td>
            <td colspan=\"2\"></td>
        </tr>
        <tr>
            <th colspan=\"2\">비 고</th>
            <td colspan=\"4\">{safe(detail.get('상세_비고'))}</td>
        </tr>
        <tr>
            <th colspan=\"2\">첨부</th>
            <td colspan=\"3\">{safe(detail.get('상세_첨부파일'))}</td>
        </tr>
    </tbody>
</table>
<div class=\"board_btn\"> <a href=\"#\" class=\"btn\">목록보기</a> </div>
"""

        # 의안 번호
        bill_no_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(3) td:nth-child(1)')
        detail_info["상세_의안번호"] = (await bill_no_elem.inner_text()).strip() if bill_no_elem else ""

        # 의안 종류
        bill_type_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(3) td:nth-child(2)')
        detail_info["상세_의안종류"] = (await bill_type_elem.inner_text()).strip() if bill_type_elem else ""

        # 발의일
        proposal_date_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(4) td:nth-child(1)')
        detail_info["상세_발의일"] = (await proposal_date_elem.inner_text()).strip() if proposal_date_elem else ""

        # 대표 발의 의원
        main_proposer_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(4) td:nth-child(2)')
        detail_info["상세_대표발의의원"] = (await main_proposer_elem.inner_text()).strip() if main_proposer_elem else ""

        # 공동 발의 의원
        co_proposers_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(5) td[colspan="3"]')
        detail_info["상세_공동발의의원"] = (await co_proposers_elem.inner_text()).strip() if co_proposers_elem else ""

        # 위원회 정보
        committee_info = await page.query_selector('table.board_view.bill tbody tr:nth-child(6) td:nth-child(1)')
        detail_info["상세_위원회_소관위"] = (await committee_info.inner_text()).strip() if committee_info else ""

        committee_assign_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(6) td:nth-child(2)')
        detail_info["상세_위원회_회부일"] = (await committee_assign_date.inner_text()).strip() if committee_assign_date else ""

        committee_present_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(7) td:nth-child(1)')
        detail_info["상세_위원회_상정일"] = (await committee_present_date.inner_text()).strip() if committee_present_date else ""

        committee_process_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(7) td:nth-child(2)')
        detail_info["상세_위원회_처리일"] = (await committee_process_date.inner_text()).strip() if committee_process_date else ""

        committee_result = await page.query_selector('table.board_view.bill tbody tr:nth-child(8) td[colspan="3"]')
        detail_info["상세_위원회_처리결과"] = (await committee_result.inner_text()).strip() if committee_result else ""

        # 본회의 정보
        plenary_assign_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(9) td[colspan="3"]')
        detail_info["상세_본회의_회부일"] = (await plenary_assign_date.inner_text()).strip() if plenary_assign_date else ""

        plenary_present_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(10) td:nth-child(1)')
        detail_info["상세_본회의_상정일"] = (await plenary_present_date.inner_text()).strip() if plenary_present_date else ""

        plenary_process_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(10) td:nth-child(2)')
        detail_info["상세_본회의_처리일"] = (await plenary_process_date.inner_text()).strip() if plenary_process_date else ""

        plenary_result = await page.query_selector('table.board_view.bill tbody tr:nth-child(11) td:nth-child(1)')
        detail_info["상세_본회의_처리결과"] = (await plenary_result.inner_text()).strip() if plenary_result else ""

        # 집행부 이송일
        transfer_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(12) td:nth-child(1)')
        detail_info["상세_집행부이송일"] = (await transfer_date.inner_text()).strip() if transfer_date else ""

        # 공포일
        promulgation_date = await page.query_selector('table.board_view.bill tbody tr:nth-child(12) td:nth-child(2)')
        detail_info["상세_공포일"] = (await promulgation_date.inner_text()).strip() if promulgation_date else ""

        # 공포 번호
        promulgation_no = await page.query_selector('table.board_view.bill tbody tr:nth-child(13) td:nth-child(1)')
        detail_info["상세_공포번호"] = (await promulgation_no.inner_text()).strip() if promulgation_no else ""

        # 비고
        remarks = await page.query_selector('table.board_view.bill tbody tr:nth-child(14) td[colspan="4"]')
        detail_info["상세_비고"] = (await remarks.inner_text()).strip() if remarks else ""

        # 첨부파일
        attachment_elem = await page.query_selector('table.board_view.bill tbody tr:nth-child(15) td[colspan="3"]')
        if attachment_elem:
            attachment_text = await attachment_elem.inner_text()
            attachment_links = await attachment_elem.query_selector_all('a')
            attachment_info = []
            for link in attachment_links:
                link_text = await link.inner_text()
                link_href = await link.get_attribute('href')
                attachment_info.append(f"{link_text} ({link_href})")
            detail_info["상세_첨부파일"] = " | ".join(attachment_info) if attachment_info else attachment_text.strip()
        else:
            detail_info["상세_첨부파일"] = ""

    except Exception as e:
        detail_info["상세_오류"] = str(e)

    return detail_info

async def crawl_all_pages():
    all_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print(f"[시작] 전주시의회 의안 수집 - 총 {TOTAL_PAGES}페이지")
        print("=" * 60)

        # 첫 페이지 접속
        await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
        await page.wait_for_selector("table.normal_list tbody tr", timeout=10000)

        for current_page in range(1, TOTAL_PAGES + 1):
            try:
                if current_page > 1:
                    # totalrec, nowpage hidden input 설정 후 폼 submit
                    await page.evaluate(f"""
                        document.getElementById('totalrec').value = '6144';
                        document.getElementById('nowpage').value = '{current_page}';
                        document.getElementById('search_form').submit();
                    """)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    await page.wait_for_selector("table.normal_list tbody tr", timeout=10000)

                # 테이블 데이터 파싱
                rows = await page.query_selector_all("table.normal_list tbody tr")

                for row in rows:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 6:
                        continue

                    bill_no   = (await cells[0].inner_text()).strip()
                    year      = (await cells[1].inner_text()).strip()
                    title_el  = await cells[2].query_selector("a")
                    title     = (await title_el.inner_text()).strip() if title_el else (await cells[2].inner_text()).strip()

                    # 상세 링크
                    href = await title_el.get_attribute("href") if title_el else ""
                    detail_url = ""
                    if href and "thid=" in href:
                        thid = href.split("thid=")[-1]
                        detail_url = f"{BASE_URL}?type=view&thid={thid}"

                    proposer      = (await cells[3].inner_text()).strip()
                    committee_res = (await cells[4].inner_text()).strip()
                    plenary_res   = (await cells[5].inner_text()).strip()

                    all_data.append({
                        "의안번호": bill_no,
                        "년도": year,
                        "의안명": title,
                        "발의자": proposer,
                        "위원회처리결과": committee_res,
                        "본회의처리결과": plenary_res,
                        "상세URL": detail_url,
                    })

                print(f"[{current_page:>3}/{TOTAL_PAGES}] {len(rows)}건 수집 | 누계: {len(all_data)}건")

                # 서버 부하 방지
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"[오류] {current_page}페이지 오류: {e} → 3초 후 재시도")
                await asyncio.sleep(3)
                try:
                    await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
                    await page.evaluate(f"""
                        document.getElementById('totalrec').value = '6144';
                        document.getElementById('nowpage').value = '{current_page}';
                        document.getElementById('search_form').submit();
                    """)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    rows = await page.query_selector_all("table.normal_list tbody tr")
                    for row in rows:
                        cells = await row.query_selector_all("td")
                        if len(cells) < 6:
                            continue
                        bill_no   = (await cells[0].inner_text()).strip()
                        year      = (await cells[1].inner_text()).strip()
                        title_el  = await cells[2].query_selector("a")
                        title     = (await title_el.inner_text()).strip() if title_el else ""
                        href      = await title_el.get_attribute("href") if title_el else ""
                        detail_url = f"{BASE_URL}?type=view&thid={href.split('thid=')[-1]}" if href and "thid=" in href else ""
                        proposer      = (await cells[3].inner_text()).strip()
                        committee_res = (await cells[4].inner_text()).strip()
                        plenary_res   = (await cells[5].inner_text()).strip()
                        all_data.append({
                            "의안번호": bill_no, "년도": year, "의안명": title,
                            "발의자": proposer, "위원회처리결과": committee_res,
                            "본회의처리결과": plenary_res, "상세URL": detail_url,
                        })
                    print(f"  └ 재시도 성공: {len(rows)}건")
                except Exception as e2:
                    print(f"  └ 재시도 실패: {e2} → 해당 페이지 스킵")

            # 중단 체크
            if crawling_stopped:
                print("[중단] 사용자에 의해 크롤링이 중단되었습니다.")
                print(f"[중간 저장] {len(all_data)}건의 데이터를 저장합니다...")
                save_results(all_data)  # 중간에 저장
                break

        await browser.close()

    return all_data


async def crawl_all_pages_with_details():
    """전체 의안 리스트 + 상세 페이지까지 크롤링"""
    global crawling_stopped
    crawling_stopped = False
    all_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print(f"[시작] 전주시의회 의안 수집 (상세 포함) - 총 {TOTAL_PAGES}페이지")
        print("=" * 60)

        # 첫 페이지 접속
        await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
        await page.wait_for_selector("table.normal_list tbody tr", timeout=10000)

        for current_page in range(1, TOTAL_PAGES + 1):
            if crawling_stopped:
                print("[중단] 사용자에 의해 크롤링이 중단되었습니다.")
                print(f"[중간 저장] {len(all_data)}건의 데이터를 저장합니다...")
                save_results(all_data)  # 중간에 저장
                break

            try:
                if current_page > 1:
                    await page.evaluate(f"""
                        document.getElementById('totalrec').value = '6144';
                        document.getElementById('nowpage').value = '{current_page}';
                        document.getElementById('search_form').submit();
                    """)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    await page.wait_for_selector("table.normal_list tbody tr", timeout=10000)

                # 테이블 데이터 파싱
                rows = await page.query_selector_all("table.normal_list tbody tr")

                for row in rows:
                    if crawling_stopped:
                        break

                    cells = await row.query_selector_all("td")
                    if len(cells) < 6:
                        continue

                    bill_no   = (await cells[0].inner_text()).strip()
                    year      = (await cells[1].inner_text()).strip()
                    title_el  = await cells[2].query_selector("a")
                    title     = (await title_el.inner_text()).strip() if title_el else (await cells[2].inner_text()).strip()

                    # 상세 링크
                    href = await title_el.get_attribute("href") if title_el else ""
                    detail_url = ""
                    if href and "thid=" in href:
                        thid = href.split("thid=")[-1]
                        detail_url = f"{BASE_URL}?type=view&thid={thid}"

                    proposer      = (await cells[3].inner_text()).strip()
                    committee_res = (await cells[4].inner_text()).strip()
                    plenary_res   = (await cells[5].inner_text()).strip()

                    # 상세 페이지 크롤링
                    detail_info = {}
                    if detail_url:
                        try:
                            detail_page = await browser.new_page()
                            await detail_page.goto(detail_url, wait_until="networkidle", timeout=30000)
                            await detail_page.wait_for_timeout(1000)

                            # 상세 정보 추출
                            detail_info = await extract_detail_info(detail_page)
                            await detail_page.close()
                        except Exception as e:
                            print(f"  [상세 페이지 오류] {bill_no}: {e}")
                            detail_info = {"오류": str(e)}

                    all_data.append({
                        **detail_info,  # 상세 정보만 저장 (리스트 정보 제외)
                    })

                print(f"[{current_page:>3}/{TOTAL_PAGES}] {len(rows)}건 수집 (상세 포함) | 누계: {len(all_data)}건")

                # 서버 부하 방지
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"[오류] {current_page}페이지 오류: {e} → 3초 후 재시도")
                await asyncio.sleep(3)
                try:
                    await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
                    await page.evaluate(f"""
                        document.getElementById('totalrec').value = '6144';
                        document.getElementById('nowpage').value = '{current_page}';
                        document.getElementById('search_form').submit();
                    """)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    rows = await page.query_selector_all("table.normal_list tbody tr")
                    for row in rows:
                        if crawling_stopped:
                            break
                        cells = await row.query_selector_all("td")
                        if len(cells) < 6:
                            continue
                        bill_no   = (await cells[0].inner_text()).strip()
                        year      = (await cells[1].inner_text()).strip()
                        title_el  = await cells[2].query_selector("a")
                        title     = (await title_el.inner_text()).strip() if title_el else ""
                        href      = await title_el.get_attribute("href") if title_el else ""
                        detail_url = f"{BASE_URL}?type=view&thid={href.split('thid=')[-1]}" if href and "thid=" in href else ""
                        proposer      = (await cells[3].inner_text()).strip()
                        committee_res = (await cells[4].inner_text()).strip()
                        plenary_res   = (await cells[5].inner_text()).strip()
                        all_data.append({
                            **detail_info,  # 상세 정보만 저장 (리스트 정보 제외)
                        })
                    print(f"  └ 재시도 성공: {len(rows)}건")
                except Exception as e2:
                    print(f"  └ 재시도 실패: {e2} → 해당 페이지 스킵")

            # 중단 체크 (상세 크롤링용)
            if crawling_stopped:
                print("[중단] 사용자에 의해 크롤링이 중단되었습니다.")
                print(f"[중간 저장] {len(all_data)}건의 데이터를 저장합니다...")
                save_results(all_data)  # 중간에 저장
                break

        await browser.close()

    return all_data


def save_results(data):
    if not data:
        print("수집된 데이터가 없습니다.")
        return None

    df = pd.DataFrame(data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Excel 저장 (Excel만 저장)
    xlsx_path = f"jeonju_bills_{timestamp}.xlsx"
    df.to_excel(xlsx_path, index=False)
    print(f"[저장] Excel: {xlsx_path}")

    print(f"\n{'='*60}")
    print(f"총 수집 건수: {len(df)}건")
    print(f"{'='*60}")
    print(df.head(3).to_string())
    
    return {
        "status": "success",
        "total_count": len(df),
        "excel_file": xlsx_path,
        "data": data[:10]  # 상위 10개만 반환
    }


@app.get("/crawl")
async def crawl_endpoint():
    """전주시의회 의안 수집 API 엔드포인트"""
    try:
        start = time.time()
        data = await crawl_all_pages()
        result = save_results(data)
        elapsed = time.time() - start
        
        result["elapsed_time"] = f"{elapsed:.1f}초"
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@app.get("/crawlView")
async def crawl_view_endpoint():
    """전주시의회 의안 수집 + 상세 페이지 크롤링 API"""
    try:
        start = time.time()
        data = await crawl_all_pages_with_details()
        result = save_results(data)
        elapsed = time.time() - start
        
        result["elapsed_time"] = f"{elapsed:.1f}초"
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@app.get("/stop")
async def stop_crawl():
    """크롤링 중단"""
    global crawling_stopped
    crawling_stopped = True
    return {"status": "중단 명령 전송됨", "message": "실행 중인 크롤링이 중단되고 수집된 데이터가 저장됩니다."}


@app.get("/")
async def root():
    """API 정보"""
    return {
        "message": "전주시의회 의안 수집 API",
        "endpoints": {
            "GET /crawl": "의안 리스트만 수집 (의안번호, 년도, 의안명, 발의자, 위원회처리결과, 본회의처리결과, 상세URL)",
            "GET /crawlView": "의안 리스트 + 각 의안의 상세 페이지 정보 수집 (상세 페이지 값들만 저장)",
            "GET /stop": "실행 중인 크롤링 중단 (수집된 데이터 자동 저장)"
        }
    }


if __name__ == "__main__":
    # FastAPI 서버 실행
    uvicorn.run(app, host="0.0.0.0", port=8000)