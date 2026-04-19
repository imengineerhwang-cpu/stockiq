"""DART 사업보고서 원문 수집/파싱 유틸.

list.json → rcept_no 조회 → document.xml → ZIP 해제 → XML 파싱 → 섹션 추출.
원문 ZIP 해제본은 data/raw/doc_{rcept_no}.xml 로 캐시.
"""
import os
import json
import re
import io
import zipfile
import requests
from dotenv import load_dotenv

load_dotenv()
DART_KEY = os.getenv("DART_API_KEY")
os.makedirs("data/raw", exist_ok=True)


def _load_report_list(corp_code, year):
    """DART list.json 응답을 캐시와 함께 반환."""
    cache_path = f"data/raw/list_{corp_code}_{year}.json"
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    r = requests.get(
        "https://opendart.fss.or.kr/api/list.json",
        params={
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bgn_de": f"{year}0101",
            "end_de": f"{int(year)+1}0601",
            "pblntf_detail_ty": "A001",
            "page_count": 100,
        },
        timeout=30,
    )
    data = r.json()
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return data


def list_rcept_candidates(corp_code, year):
    """해당 연도 사업보고서 rcept_no 후보 리스트.

    [첨부정정]은 document.xml 본문이 없는 경우가 많아 최후순위로 배치.
    [기재정정]/원본은 최신 접수일을 우선.
    """
    year = str(year)
    data = _load_report_list(corp_code, year)
    if data.get("status") != "000":
        return []

    primary, fallback_year_miss = [], []
    for item in data.get("list", []):
        name = item.get("report_nm", "")
        if "사업보고서" not in name:
            continue
        entry = (item["rcept_no"], name, item.get("rcept_dt", ""))
        if f"({year}" in name:
            primary.append(entry)
        else:
            fallback_year_miss.append(entry)

    def sort_key(e):
        # [첨부정정]은 뒤로, 그다음 최신 접수일 우선
        return ("[첨부정정]" in e[1], -int(e[2] or 0))

    primary.sort(key=sort_key)
    fallback_year_miss.sort(key=sort_key)
    return [e[0] for e in primary + fallback_year_miss]


def fetch_rcept_no(corp_code, year):
    """첫 번째 후보 rcept_no 반환 (하위 호환)."""
    cands = list_rcept_candidates(corp_code, year)
    return cands[0] if cands else None


def _pick_main_xml(names, rcept_no):
    """ZIP 내 여러 XML 중 메인 사업보고서를 선택.

    1순위: '{rcept_no}.xml' 정확 매칭 (접미사 없는 원본)
    2순위: 크기가 가장 큰 XML (본문이 보통 가장 큼)
    """
    xml_files = [n for n in names if n.lower().endswith(".xml")]
    if not xml_files:
        return None
    exact = f"{rcept_no}.xml"
    for n in xml_files:
        if n.endswith(exact) and "_" not in n.rsplit("/", 1)[-1].rsplit(".", 1)[0].replace(rcept_no, ""):
            return n
    return None


def fetch_document_xml(rcept_no):
    """원문 XML 텍스트 반환. document.xml ZIP 응답을 해제해 캐시.

    ZIP에 여러 XML이 있는 경우 첨부(감사보고서 등)가 아닌 본 사업보고서를 선택.
    """
    cache_path = f"data/raw/doc_{rcept_no}.xml"
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

    r = requests.get(
        "https://opendart.fss.or.kr/api/document.xml",
        params={"crtfc_key": DART_KEY, "rcept_no": rcept_no},
        timeout=60,
    )
    try:
        zf = zipfile.ZipFile(io.BytesIO(r.content))
    except zipfile.BadZipFile:
        return None
    names = zf.namelist()
    # 1순위: 정확히 '{rcept_no}.xml' (원본, 접미사 없음)
    main = next((n for n in names if n == f"{rcept_no}.xml"), None)
    # 2순위: 가장 큰 XML
    if not main:
        xml_files = [n for n in names if n.lower().endswith(".xml")]
        if not xml_files:
            return None
        main = max(xml_files, key=lambda n: zf.getinfo(n).file_size)
    raw = zf.read(main)
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        content = raw.decode("euc-kr", errors="replace")
    with open(cache_path, "w", encoding="utf-8") as f:
        f.write(content)
    return content


def _strip_tags(text):
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z#0-9]+;", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def extract_sections(xml_content):
    """사업의 내용, 이사의 경영진단 및 분석의견 섹션 추출. 각 섹션 본문을 평문으로."""
    if not xml_content:
        return {}
    text = _strip_tags(xml_content)
    sections = {}

    m = re.search(
        r"(II\.\s*사업의\s*내용.*?)(?=III\.\s*재무에\s*관한\s*사항|III\.\s*재무)",
        text, re.DOTALL,
    )
    if m:
        sections["사업의 내용"] = m.group(1).strip()[:40000]

    m = re.search(
        r"(이사의\s*경영진단\s*및\s*분석의견.*?)(?=이사회\s*등\s*회사의\s*기관에\s*관한|주주에\s*관한\s*사항|VII\.|VIII\.|\Z)",
        text, re.DOTALL,
    )
    if m:
        sections["이사의 경영진단 및 분석의견"] = m.group(1).strip()[:30000]

    return sections


def get_report_sections(corp_code, year):
    """Top-level: (corp_code, year) → {'사업의 내용': ..., '이사의 경영진단 및 분석의견': ..., '_rcept_no': ...}

    후보 rcept_no 리스트를 순서대로 시도해 document.xml 본문이 성공적으로 파싱되는 첫 결과 반환.
    """
    for rcept_no in list_rcept_candidates(corp_code, year):
        xml = fetch_document_xml(rcept_no)
        if not xml:
            continue
        sections = extract_sections(xml)
        if not sections:
            continue
        sections["_rcept_no"] = rcept_no
        return sections
    return {}
