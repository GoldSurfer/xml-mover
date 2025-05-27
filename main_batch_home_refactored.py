import os
import xmltodict
import json
import re
import traceback
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import argparse
import time
from collections import defaultdict
import logging


#############################
# 1️⃣  공통 유틸리티 함수   #
#############################

def read_xml_with_encoding(file_path: str) -> str | None:  # 다양한 인코딩 시도
    """UTF‑8 → EUC‑KR 순으로 시도해서 XML 텍스트를 반환."""
    for enc in ("utf-8", "euc-kr", "cp949", "latin1"):
        try:
            with open(file_path, "r", encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            return None
    return None

# 📌 xmlns="…"   혹은   xmlns:xx="…"  전체 제거 (xml 네임스페이스 선언을 제거하여 파싱을 단순화)
def strip_xmlns(xml: str) -> str:
    return re.sub(r"\sxmlns(:\w+)?=\"[^\"]+\"", "", xml)

# 📌 태그 이름에 붙은 prefix("abc:") 제거  →  dict 파싱 후 재귀적으로 key 정규화
def strip_prefix(obj):
    if isinstance(obj, dict):
        return {k.split(":")[-1]: strip_prefix(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [strip_prefix(i) for i in obj]
    return obj

# 📌 안전하게 dictionary 값 가져오기
def safe_dict_get(d, key, default=None):
    """사전에서 안전하게 값을 가져오는 헬퍼 함수"""
    if not isinstance(d, dict):
        return default
    return d.get(key, default)

# 📌 dict/list 에서 안전하게 key 추출
def safe_get(obj, key):
    """dict / list 깊이를 가리지 않고 key 값을 탐색"""
    if obj is None:
        return None
    
    # 단순 dict 빠른 처리
    if isinstance(obj, dict) and key in obj:
        return obj[key]
    
    # 탐색 필요 시 시작
    queue = [obj]
    visited = set()
    results = []
    
    while queue:
        current = queue.pop(0)
        
        # 방문 확인 (순환 참조 방지)
        if isinstance(current, (dict, list)):
            current_id = id(current)
            if current_id in visited:
                continue
            visited.add(current_id)
        
        if isinstance(current, dict):
            # 현재 dict에서 key 찾기
            if key in current:
                value = current[key]
                if isinstance(value, dict):
                    # text 내용 우선 추출
                    if "#text" in value:
                        results.append(value["#text"])
                    elif "text" in value:
                        results.append(value["text"])
                    else:
                        results.append(value)
                elif isinstance(value, list) and value:
                    # 리스트의 경우 모두 추가
                    for v in value:
                        queue.append(v)
                else:
                    results.append(value)
            
            # 모든 값을 큐에 추가
            for v in current.values():
                if v is not None:
                    queue.append(v)
        
        elif isinstance(current, list):
            # 리스트의 모든 요소 큐에 추가
            for v in current:
                if v is not None:
                    queue.append(v)
    
    # 결과 반환
    if len(results) == 1:
        return results[0]
    elif results:
        return results
    return None




# 📌 모든 문자열 노드를 안전하게 문자열로 추출
def extract_text(node):
    """다양한 노드에서 텍스트를 추출하여 문자열로 반환"""
    if node is None:
        return None
    
    # 문자열이면 그대로 반환
    if isinstance(node, str):
        return node.strip()
    
    # 딕셔너리 처리
    if isinstance(node, dict):
        # 속성 키는 건너뜀 (번호 등)
        clean_dict = {k: v for k, v in node.items() if not (isinstance(k, str) and k.startswith('@'))}
        
        # #text 또는 text 태그 우선 처리
        for key in ["#text", "text"]:
            if key in node and node[key]:
                return extract_text(node[key])
        
        # 다른 모든 값에서 텍스트 추출
        texts = []
        for v in clean_dict.values():
            text = extract_text(v)
            if text:
                texts.append(text)
        
        # 결합하여 반환
        if texts:
            return "\n".join(texts)
        return None
    
    # 리스트 처리
    if isinstance(node, list):
        texts = []
        for item in node:
            text = extract_text(item)
            if text:
                texts.append(text)
        return "\n".join(texts) if texts else None
    
    # 기타 타입은 문자열로 변환
    return str(node).strip()

############################################
# 2️⃣  타입별 세부 파싱 로직 (CN, BUSINESS) #
############################################

def get_abstract_text(abstract):
    """추상/초록 텍스트 추출 함수"""
    if abstract is None:
        return None
    
    # Paragraphs 태그가 있는 경우 처리
    paragraphs = None
    if isinstance(abstract, dict):
        paragraphs = abstract.get("Paragraphs") or abstract.get("p")
    
    if paragraphs:
        return extract_text(paragraphs)
    
    # 직접 텍스트 추출
    return extract_text(abstract)

def extract_claims(root):
    """XML에서 청구항 추출"""
    # 1. Business(구버전) 포맷 청구항 (Claims/Claim) -> 정확히는 Claims/Claim/ClaimText 임!
    claims_section = None
    if isinstance(root, dict):
        if "Claims" in root:
            claims_section = root["Claims"]
    
    if claims_section:
        claim_list = safe_get(claims_section, "Claim")
        if claim_list:
            all_claims = []
            
            # 리스트 형태로 처리
            if isinstance(claim_list, list):
                for i, claim in enumerate(claim_list, 1):
                    if not isinstance(claim, dict):
                        continue
                        
                    claim_num = safe_dict_get(claim, "@num", str(i))
                    if "ClaimText" in claim:
                        claim_text = extract_text(claim["ClaimText"])
                    else:
                        claim_text = extract_text(claim)
                        
                    if claim_text:
                        all_claims.append(f"{claim_num}. {claim_text}")
            # 단일 청구항
            elif isinstance(claim_list, dict):
                claim_num = safe_dict_get(claim_list, "@num", "1")
                if "ClaimText" in claim_list:
                    claim_text = extract_text(claim_list["ClaimText"])
                else:
                    claim_text = extract_text(claim_list)
                    
                if claim_text:
                    all_claims.append(f"{claim_num}. {claim_text}")
            
            if all_claims:
                return "¶".join(all_claims)
    
    # # 2. Business 속성 기반 청구항 (단순 텍스트) -> [수정 고려사항] 현실적으로 있기 힘든 xml 구조
    # if isinstance(root, dict) and "Claims" in root and isinstance(root["Claims"], str):
    #     return root["Claims"]

    # # 3. Business 청구항 직접 접근 방식 -> [수정 고려사항] 1번에서 이미 커버함
    # if isinstance(root, dict) and "Claims" in root and not isinstance(root["Claims"], str):
    #     return extract_text(root["Claims"])
        
    # 4. CN 포맷 청구항
    if isinstance(root, dict) and "application-body" in root and isinstance(root["application-body"], dict):
        claims_section = safe_dict_get(root["application-body"], "claims")
        if claims_section and isinstance(claims_section, dict):
            claim_list = safe_get(claims_section, "claim")
            if claim_list:
                all_claims = []
                
                # 리스트 형태로 처리
                if isinstance(claim_list, list):
                    for i, claim in enumerate(claim_list, 1):
                        if not isinstance(claim, dict):
                            continue
                            
                        claim_num = safe_dict_get(claim, "@num", str(i))
                        if "claim-text" in claim:
                            claim_text = extract_text(claim["claim-text"])
                        else:
                            claim_text = extract_text(claim)
                            
                        if claim_text:
                            all_claims.append(f"{claim_text}")

                # 단일 청구항
                elif isinstance(claim_list, dict):
                    claim_num = safe_dict_get(claim_list, "@num", "1")
                    if "claim-text" in claim_list:
                        claim_text = extract_text(claim_list["claim-text"])
                    else:
                        claim_text = extract_text(claim_list)
                        
                    if claim_text:
                        all_claims.append(f"{claim_text}")
                
                if all_claims:
                    return "¶".join(all_claims)
    
    # # 5. KR 포맷 청구항 -> 현재 중국 특허에 대한 작업 중!
    # if isinstance(root, dict) and "claims" in root:
    #     return extract_text(root["claims"])
    
    # 6. 딥 서치
    deep_claims = safe_get(root, "claim-text") or safe_get(root, "ClaimText")
    if deep_claims:
        return extract_text(deep_claims)
    
    return None

# 📌 IPC 텍스트에서 괄호와 날짜 제거하는 함수 추가
def clean_ipc_text(ipc_text):
    """IPC 텍스트 정리: 공백 정리 및 괄호(날짜 포함) 제거"""
    if not ipc_text:
        return None
    # 괄호와 그 안의 내용 제거
    cleaned_text = re.sub(r'\([^)]*\)', '', str(ipc_text))
    # 공백 정리 (연속된 공백을 하나로)
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text.strip()

# 📌 기관 이름에서 괄호와 숫자 제거하는 함수 추가
def clean_organization_name(org_name):
    """기관 이름에서 괄호와 숫자 제거"""
    if not org_name:
        return None
    # 괄호와 그 안의 내용 제거
    cleaned_name = re.sub(r'\([^)]*\)', '', str(org_name))
    # 뒤에 붙은 숫자 제거
    cleaned_name = re.sub(r'\s*\d+$', '', cleaned_name)
    return cleaned_name.strip()

# 📌 구형 문서의 "도면의 간단한 설명"과 "발명을 실시하기 위한 구체적인 내용" 구분 함수 추가
def extract_description_sections(paragraphs):
    """단락 목록에서 도면 설명과 실시예 섹션만 추출하고 전체 설명도 함께 반환"""
    # 도면 설명 마커 (확장성 위해 키워드 추가)
    DRAWING_MARKERS = [
        "[도면의 간단한 설명]", "도면의 간단한 설명"
    ]
    
    # 실시예 마커 (확장성 위해 키워드 추가)
    EMBODIMENT_MARKERS = [
        "[발명을 실시하기 위한 구체적인 내용]", "더욱 상세하게 설명한다", "구체적인 실시방식:"
    ]
    
    # 결과 초기화
    brief_description_of_drawings = None
    description_of_embodiments = None
    
    # 전체 description 내용 추출 (모든 필터링 제거)
    full_description = []
    for p in paragraphs:
        text = extract_text(p)
        if text:  # 모든 텍스트 포함, 필터링 없음
            full_description.append(text)
    
    # 전체 내용 합치기
    full_description_text = "\n\n".join(full_description) if full_description else None
    
    # 인덱스 초기화
    drawings_start_idx = -1
    embodiment_start_idx = -1
    
    # 1. 마커로 섹션 시작점 찾기
    for i, p in enumerate(paragraphs):
        text = extract_text(p)
        if not text:
            continue
            
        # 텍스트 정규화
        lower_text = text.lower()
            
        # 도면 설명 시작점 확인 - 길이 조건 완화
        if any(marker.lower() in lower_text for marker in DRAWING_MARKERS) and len(text) < 100:
            drawings_start_idx = i
            continue
            
        # 실시예 시작점 확인 - 길이 조건 완화
        if any(marker.lower() in lower_text for marker in EMBODIMENT_MARKERS) and len(text) < 100:
            # 도면 설명의 끝점도 여기로 정의 가능
            if drawings_start_idx != -1 and drawings_start_idx < i:
                # 도면 설명 끝점이 실시예 시작점으로 간주
                drawing_paragraphs = []
                for j in range(drawings_start_idx + 1, i):
                    drawing_text = extract_text(paragraphs[j])
                    if drawing_text: # and not is_table_like(drawing_text): '테이블 필터링' 제거
                        drawing_paragraphs.append(drawing_text)
                
                if drawing_paragraphs:
                    brief_description_of_drawings = "\n\n".join(drawing_paragraphs)
            
            embodiment_start_idx = i
            continue
    
    # 2. 실시예 섹션은 description 태그가 끝나는 부분까지 (모든 남은 단락)
    if embodiment_start_idx != -1:
        embodiment_paragraphs = []
        for j in range(embodiment_start_idx + 1, len(paragraphs)):
            embodiment_text = extract_text(paragraphs[j])
            if embodiment_text: # and not is_table_like(embodiment_text): '테이블 필터링' 제거
                embodiment_paragraphs.append(embodiment_text)
        
        if embodiment_paragraphs:
            description_of_embodiments = "\n\n".join(embodiment_paragraphs)
    
    # 3. 도면 설명 마커만 있고 실시예 마커가 없는 경우
    # 도면 설명 부분을 별도로 추출하지 않고, description 전체를 반환
    if drawings_start_idx != -1 and embodiment_start_idx == -1:
        # brief_description_of_drawings는 None으로 유지
        # 도면 설명은 전체 description에 포함되어 있음
        # 도면 설명 구분 없이 description 전체를 참조하도록 함
        pass
    
    return brief_description_of_drawings, description_of_embodiments, full_description_text

def is_table_like(text):
    """테이블 형식의 텍스트인지 확인합니다"""
    if not text:
        return False
        
    # 1. 숫자와 특수문자 비율이 높은 경우
    numeric_ratio = sum(c.isdigit() or c in '-+*/,.()%' for c in text) / max(len(text), 1)
    if numeric_ratio > 0.4:  # 경험적 임계값
        return True
        
    # 2. 한글이 없고 중국어/영어만 있는 경우 (이 조건도 제거)
    # has_korean = any('\uAC00' <= c <= '\uD7A3' for c in text)
    # if not has_korean and any('\u4e00' <= c <= '\u9fff' for c in text):
    #     return True
        
    # # 3. 표 형식의 시작 패턴 (영어 패턴 추가)
    # text_start = text.strip().lower()
    # table_starts = [
    #     '序列', 'm序列', 'preferred gold', 'katsami', 'sequence', 'm sequence', 
    #     'qs(og-', 'gold', 'snr', '계산', '4-qs', '8-qs', '16-qs', 
    #     '32-qs', '64-qs', '128-qs', '256-qs', '512-qs', '1024-qs',
    #     'table', '표', '표:', '도표'
    # ]
    
    # if any(text_start.startswith(marker) for marker in table_starts):
    #     return True
        
    # # 4. 표에 자주 사용되는 영어 단어로 시작하는 줄
    # table_marker_words = ('sequence', 'length', 'number', 'ratio', 'level', 'signal', 'method')
    # if any(text_start.startswith(word) for word in table_marker_words):
    #     return True
        
    # # 5. 짧은 줄이 여러 개 반복되는 패턴 (테이블 행)
    # if len(text.strip()) < 30 and text.count('\n') > 2:
    #     return True
    
    # 6. 중국어 비율 조건 제거
    # chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    # chinese_ratio = chinese_chars / max(len(text), 1)
    # if chinese_ratio > 0.2:
    #     return True
        
    return False

def scrub_tables(raw_text):
    """표와 중국어 텍스트를 제거하고 깨끗한 내용만 반환"""
    if not raw_text:
        return None
        
    clean_lines = []
    for line in raw_text.splitlines():
        if not line.strip():
            continue
        if is_table_like(line):
            continue
        clean_lines.append(line)
    
    result = "\n\n".join(clean_lines).strip()
    return result if result else None

# 출원일자와 공개일자 사이의 기간을 계산하는 함수 추가
def calculate_date_diff(app_date, pub_date):
    """출원일자와 공개일자 사이의 기간(개월)을 계산"""
    if not app_date or not pub_date:
        return None
    
    try:
        # 날짜 형식은 YYYYMMDD로 가정
        app_date_obj = datetime.strptime(str(app_date), "%Y%m%d")
        pub_date_obj = datetime.strptime(str(pub_date), "%Y%m%d")
        
        # 날짜 차이 계산 (월 단위)
        diff_months = (pub_date_obj.year - app_date_obj.year) * 12 + (pub_date_obj.month - app_date_obj.month)
        return diff_months
    except (ValueError, TypeError):
        return None

def format_numbers(app_number, pub_number, pub_date, kind, app_date=None):
    """특허 번호 형식 변환 - 출원일자와 공개일자 간 기간을 고려하여 처리"""
    clean_app = clean_application_number(app_number)
    
    publication_number = None
    publication_date = None
    open_number = None
    open_date = None
    register_number = None
    register_date = None
    
    # months_diff = calculate_date_diff(app_date, pub_date) # Kind 'Y'는 공개제도 없으므로 이 계산 불필요

    if kind and kind.upper() in ['B', 'C']: # 등록 특허, 등록 실용신안(구 코드)
        publication_number = pub_number if pub_number else None # 공고번호
        publication_date = pub_date # 공고일자
        
        # 등록번호는 출원번호와 동일하게 설정
        register_number = clean_app if clean_app else None
        register_date = pub_date

        # 공개 정보 처리 (19개월 룰은 일반 특허에 주로 해당, 등록된 것은 공고 정보를 따름)
        # 만약 app_date와 pub_date가 있고, 그 차이가 19개월 이상이면 공개되었을 수 있음.
        # 하지만 등록된 경우, 공고 정보가 우선시 됨. 여기서는 일단 null로 두거나,
        # 더 명확한 규칙이 있다면 해당 규칙을 따라야 함.
        # 현재는 B, C의 경우 open_number/date를 별도로 설정하지 않고 있음 (기존 로직 유지).
        # 필요시 calculate_date_diff 및 관련 로직 여기에 적용 가능.
        months_diff_for_BC = calculate_date_diff(app_date, pub_date)
        if months_diff_for_BC is not None:
            if months_diff_for_BC > 18: # 19개월 초과 시 공개된 것으로 간주하나, 정보 없으면 공고번호 사용
                open_number = pub_number 
                open_date = None # 공개일자는 특정 불가
            # else: 19개월 미만이면 공개 없이 바로 등록, open_number/date는 None (기본값)
        # else: 날짜 정보 부족 시 open_number/date는 None (기본값)


    elif kind and kind.upper() in ['U', 'Y']: # 실용신안 (U: 구, Y: 신)
        # 실용신안은 공개 제도가 없으므로 OpenNumber, OpenDate는 항상 null
        open_number = None
        open_date = None
        
        # PublicationNumber/Date는 공고번호/일자로 설정 (등록 간주)
        publication_number = pub_number if pub_number else None
        publication_date = pub_date
        
        # RegisterNumber는 출원번호와 동일하게 설정
        register_number = clean_app if clean_app else None
        register_date = pub_date
        
    elif kind and kind.upper() == 'A': # 공개 특허
        open_number = pub_number if pub_number else None
        open_date = pub_date
        # PublicationNumber, PublicationDate 등은 null (아직 공고/등록 전)
        # register_number, register_date도 null

    # 최종 반환 시 출원번호는 clean_app 사용
    return clean_app, publication_number, publication_date, open_number, open_date, register_number, register_date

def parse_cn_patent(root: dict) -> dict:
    """<cn-patent-document> 전용 파서"""
    biblio = safe_dict_get(root, "cn-bibliographic-data", {})
    parties = safe_dict_get(biblio, "cn-parties", {})

    # 기본 문서 정보 추출
    pub_ref_container = safe_dict_get(biblio, "cn-publication-reference", {})
    pub_ref = safe_get(pub_ref_container, "document-id") or {}
    app_ref_container = safe_dict_get(biblio, "application-reference", {})
    app_ref = safe_get(app_ref_container, "document-id") or {}

    # 딕셔너리 타입 검증
    if not isinstance(pub_ref, dict): pub_ref = {}
    if not isinstance(app_ref, dict): app_ref = {}
        
    # 번호와 날짜 추출
    doc_number = safe_dict_get(pub_ref, "doc-number")
    app_number = safe_dict_get(app_ref, "doc-number")
    pub_date = safe_dict_get(pub_ref, "date")
    app_date = safe_dict_get(app_ref, "date")
    kind = safe_dict_get(pub_ref, "kind")
    
    # 번호 형식 변환
    app_number_clean, pub_number_formatted, pub_date_formatted, open_number, open_date, register_number, register_date = format_numbers(app_number, doc_number, pub_date, kind, app_date)
        
    # CPC/IPC 코드
    cpc_text = safe_get(safe_get(safe_dict_get(biblio, "classifications-ipcr", {}), "classification-ipcr"), "text")
    main_cpc = None
    if isinstance(cpc_text, str):
        main_cpc = clean_ipc_text(cpc_text)
    elif isinstance(cpc_text, list) and cpc_text:
        main_cpc = clean_ipc_text(cpc_text[0])

    # 출원인 정보
    applicants = safe_get(safe_get(safe_dict_get(parties, "cn-applicants", {}), "cn-applicant"), "name")
    applicant_name = None
    if isinstance(applicants, list) and applicants:
        applicant_name = clean_organization_name(applicants[0])
    elif applicants:
        applicant_name = clean_organization_name(applicants)
        
    # 발명자 정보
    inventors = safe_get(safe_get(safe_dict_get(parties, "cn-inventors", {}), "cn-inventor"), "name")
    inventor_name = None
    if isinstance(inventors, list):
        inventor_name = ", ".join([i for i in inventors if i])
    elif inventors:
        inventor_name = str(inventors)

    # 대리인 정보
    agents_block = safe_dict_get(parties, "cn-agents", {})
    agent_entries = safe_get(agents_block, "cn-agent") if isinstance(agents_block, dict) else None
    agent_name_combined = None
    
    if agent_entries:
        if not isinstance(agent_entries, list):
            agent_entries = [agent_entries]
        agents_formatted = []
        for ag in agent_entries:
            if not isinstance(ag, dict):
                continue
            ind_name = safe_dict_get(ag, "name", "")
            agency = safe_get(safe_dict_get(ag, "cn-agency", {}), "name") or ""
            agency = clean_organization_name(agency)
            agents_formatted.append(f"{ind_name} ({agency})".strip())
        agent_name_combined = "; ".join(agents_formatted)

    # 발명 제목
    title = safe_dict_get(biblio, "invention-title")
    if isinstance(title, dict):
        title = safe_dict_get(title, "#text") or safe_dict_get(title, "text")
    
    # 요약 정보
    abstract = safe_dict_get(biblio, "abstract")
    summary = get_abstract_text(abstract)
    
    # 청구항
    claims = extract_claims(root)
    
    # 상세 설명 추출
    description = find_description_in_document(root)
    all_paragraphs = extract_description_paragraphs(description)
    drawing_section, embodiment_section, full_description_text = extract_structured_description(description, all_paragraphs)
    
    # 결과 생성 및 반환
    return create_parsed_result(
        main_cpc, kind, open_number, open_date, register_number, register_date,
        pub_number_formatted, pub_date_formatted, app_number_clean, app_date,
        applicant_name, inventor_name, agent_name_combined, title, summary,
        drawing_section, embodiment_section, full_description_text, claims
    )

def parse_business(root: dict) -> dict:
    """<PatentDocumentAndRelated> 타입 파서"""
    biblio = safe_dict_get(root, "BibliographicData", {})
    
    # --- 공개·출원 정보 추출 ---
    pub_info, pub_doc = extract_publication_info(root, biblio)
    
    # 공개번호, 공개일자 추출
    pub_no, pub_dt = safe_dict_get(pub_doc, "DocNumber"), safe_dict_get(pub_doc, "Date")
    
    # 공개번호가 없을 경우 문서에서 직접 추출 시도
    if not pub_no:
        pub_no = safe_dict_get(pub_info, "DocNumber") or safe_dict_get(root, "@docNumber")
            
    # 공개일자가 없을 경우
    if not pub_dt:
        pub_dt = safe_dict_get(pub_info, "Date")

    # 출원번호, 출원일자 추출
    application_number, application_date = extract_application_info(biblio)
    
    # --- CPC / IPC 추출 ---
    main_cpc = extract_cpc_info(root, biblio)
    
    # --- 당사자 정보 추출 ---
    applicant_name = extract_applicant_info(biblio)
    inventor_name = extract_inventor_info(biblio)
    agent_name = extract_agent_info(biblio)
    
    # --- 발명 정보 추출 ---
    title = extract_text(safe_get(biblio, "InventionTitle"))
    abstract = safe_get(root, "Abstract") or safe_get(biblio, "Abstract")
    summary = get_abstract_text(abstract)
    claims = extract_claims(root)
    
    # --- 상세 설명 추출 ---
    description = find_description_in_document(root)
    all_paragraphs = extract_description_paragraphs(description)
    drawing_section, embodiment_section, full_description_text = extract_structured_description(description, all_paragraphs)
    
    # --- 번호 형식 변환 ---
    kind = safe_dict_get(pub_doc, "Kind") or safe_dict_get(root, "@kind")
    app_number_clean, pub_number_formatted, pub_date_formatted, open_number, open_date, register_number, register_date = format_numbers(application_number, pub_no, pub_dt, kind, application_date)
    
    # Kind가 B 또는 C인 경우 RegisterDate가 null이면 직접 설정
    if kind and kind.upper() in ['B', 'C'] and not register_date and pub_dt:
        register_date = pub_dt
    
    return create_parsed_result(
        main_cpc, kind, open_number, open_date, register_number, register_date,
        pub_number_formatted, pub_date_formatted, app_number_clean, application_date,
        applicant_name, inventor_name, agent_name, title, summary,
        drawing_section, embodiment_section, full_description_text, claims
    )

# 출원번호에서 소수점 및 소수점 이하의 숫자를 제거하는 함수
def clean_application_number(app_number):
    """출원번호에서 소수점 및 소수점 이하의 숫자를 제거"""
    if not app_number:
        return app_number
    # 문자열로 변환
    app_number_str = str(app_number)
    # 소수점이 있는 경우 소수점 이하 제거
    if '.' in app_number_str:
        return app_number_str.split('.')[0]
    return app_number_str

###################################
# 3️⃣  단일 파일 처리 진입 함수  #
###################################

def process_xml_file(file_path: str): # file_path는 이미 전체 경로를 가지고 있음
    """XML 파일을 파싱하여 JSON 결과를 반환하며, (파일경로, 결과데이터, 오류, 추출상태, 누락정보) 형식으로 반환합니다."""
    extraction_status = {}
    missing_details = None 
    try:
        xml_txt = read_xml_with_encoding(file_path)
        if xml_txt is None:
            return file_path, None, f"인코딩을 인식할 수 없음", extraction_status, missing_details

        xml_txt = strip_xmlns(xml_txt)
        data = xmltodict.parse(xml_txt)
        data = strip_prefix(data)

        root_key = next(iter(data))
        root = data[root_key]
        parsed = None
        
        if root_key.lower().startswith("cn-patent-document"):
            parsed = parse_cn_patent(root)
        elif root_key.lower().endswith("patentdocumentandrelated"):
            parsed = parse_business(root)
        else:
            parsed = process_fallback_xml(root)
            
        if not parsed:
            return file_path, None, "알 수 없는 XML 구조", extraction_status, missing_details

        parsed.update({
            "meta": {
                "convertedDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "originalFileName": os.path.basename(file_path)
            }
        })

        for key, value in parsed.items():
            if key != "meta":
                extraction_status[key] = value is not None and value != ""
        
        detected_missing_fields = [field for field, extracted in extraction_status.items() if not extracted]

        if detected_missing_fields:
            current_contextual_issues = [] # 여러 문맥적 이슈를 담을 리스트

            # --- 조건 1: 도면 또는 실시예 설명 누락 시 Description 누락 ---
            brief_drawings_present = bool(parsed.get("BriefDescriptionOfDrawings"))
            embodiments_present = bool(parsed.get("DescriptionOfEmbodiments"))
            if not brief_drawings_present or not embodiments_present:
                if "Description" in detected_missing_fields:
                    current_contextual_issues.append("Description_missing_when_drawings_or_embodiments_absent")
            
            # --- 조건 2: Kind 'A'일 때 OpenNumber 또는 OpenDate 누락 ---
            doc_kind = parsed.get("Kind")
            if doc_kind == 'A':
                missing_A_fields = []
                if "OpenNumber" in detected_missing_fields:
                    missing_A_fields.append("OpenNumber")
                if "OpenDate" in detected_missing_fields:
                    missing_A_fields.append("OpenDate")
                
                if missing_A_fields: # OpenNumber 또는 OpenDate 중 하나라도 누락된 경우
                    current_contextual_issues.append(f"Kind_A_missing_{'_and_'.join(missing_A_fields)}")

            # missing_details 생성
            missing_details = {
                "full_file_path": file_path,
                "missing_fields": detected_missing_fields,
                "contextual_issues": current_contextual_issues if current_contextual_issues else None 
                # contextual_issues 키로 변경하고, 리스트가 비어있으면 None
            }
        
        return file_path, parsed, None, extraction_status, missing_details

    except Exception as e:
        traceback_str = traceback.format_exc()
        error_msg = f"Error: {str(e)}\\nTraceback: {traceback_str}"
        # extraction_status는 빈 dict, missing_details는 None으로 반환
        return file_path, None, error_msg, {}, None # 수정된 반환값: (file_path, data, error, extraction_status, missing_details)

#################################
# 4️⃣  멀티‑프로세스 배치 실행 #
#################################

def collect_xmls_for_year(input_folder, year):
    """연도별 XML 파일 수집"""
    year_dir = os.path.join(input_folder, year)
    if not os.path.isdir(year_dir):
        print(f"   • {year} 폴더를 찾을 수 없습니다.")
        return []
        
    xml_files = []
    print(f"[스캔] {year} 폴더에서 XML 수집 중...")
    
    for root, _, files in os.walk(year_dir):
        for file in files:
            if file.lower().endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    
    print(f"[완료] {year}: {len(xml_files)}개 XML 파일 발견")
    return xml_files

def generate_stats_dict(items_count, field_success_counts, kind_A_stats, desc_fallback_stats):
    """통계 정보를 담은 딕셔너리를 생성하는 헬퍼 함수"""
    stats_output = {
        "summary": {
            "total_processed_items": items_count,
        },
        "field_extraction_success_rate": [],
        "kind_A_special_stats": {},
        "description_fallback_stats": {},
        # "missing_item_details": missing_details_list if missing_details_list is not None else [] # 이 부분 제거
    }

    if items_count > 0:
        for field, count in sorted(field_success_counts.items()):
            rate = (count / items_count * 100)
            stats_output["field_extraction_success_rate"].append({
                "field": field,
                "success_count": count,
                "total_items": items_count,
                "success_rate_percent": round(rate, 2)
            })

    if kind_A_stats.get("total_A_items", 0) > 0:
        total_A = kind_A_stats["total_A_items"]
        open_num_success = kind_A_stats["OpenNumber_success"]
        open_date_success = kind_A_stats["OpenDate_success"]
        stats_output["kind_A_special_stats"] = {
            "target_A_documents": total_A,
            "OpenNumber_success_count": open_num_success,
            "OpenNumber_success_rate_percent": round((open_num_success / total_A * 100), 2),
            "OpenDate_success_count": open_date_success,
            "OpenDate_success_rate_percent": round((open_date_success / total_A * 100), 2)
        }
    else:
        stats_output["kind_A_special_stats"] = {"message": "No Kind 'A' documents processed or found."}

    if desc_fallback_stats.get("total_partial_missing_items", 0) > 0:
        total_partial = desc_fallback_stats["total_partial_missing_items"]
        desc_success = desc_fallback_stats["Description_success_when_partial_missing"]
        stats_output["description_fallback_stats"] = {
            "target_documents_missing_drawings_or_embodiments": total_partial,
            "Description_extraction_success_count": desc_success,
            "Description_extraction_success_rate_percent": round((desc_success / total_partial * 100), 2)
        }
    else:
        stats_output["description_fallback_stats"] = {"message": "No cases of missing drawings/embodiments descriptions, or none processed."}
        
    return stats_output

def save_report_to_json(report_data, file_path, pbar_instance=None):
    """보고서 데이터를 JSON 파일로 저장하는 헬퍼 함수"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f_report:
            json.dump(report_data, f_report, ensure_ascii=False, indent=2)
        message = f"ℹ️ 통계 보고서 저장: {os.path.basename(file_path)}" # 메시지 수정

        if pbar_instance:
            pbar_instance.write(f"      {message}")
        else:
            print(f"   {message}")
    except Exception as e:
        error_message = f"❌ 통계 보고서 저장 중 오류 발생 ({os.path.basename(file_path)}): {str(e)}" # 메시지 수정
        if pbar_instance:
            pbar_instance.write(f"      {error_message}")
        else:
            print(f"   {error_message}")

def save_missing_items_report(missing_items_list, file_path, pbar_instance=None):
    """누락 항목 상세 정보를 JSON 파일로 저장하는 헬퍼 함수"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f_report:
            json.dump(missing_items_list, f_report, ensure_ascii=False, indent=2)
        message = f"ℹ️ 누락 항목 보고서 저장: {os.path.basename(file_path)} (누락 {len(missing_items_list)}건)"
        
        if pbar_instance:
            pbar_instance.write(f"      {message}")
        else:
            print(f"   {message}")
    except Exception as e:
        error_message = f"❌ 누락 항목 보고서 저장 중 오류 발생 ({os.path.basename(file_path)}): {str(e)}"
        if pbar_instance:
            pbar_instance.write(f"      {error_message}")
        else:
            print(f"   {error_message}")

def process_year(year, files, output_folder, max_items_per_file, max_file_size_gb, cpu_count_val):
    """연도별 XML 파일 처리 및 JSON 변환, 청크별/연도별 상세 보고서 생성"""
    if not files:
        print(f"⚠️ {year}년에는 처리할 파일이 없습니다.")
        return 0, 0, [], {} 
        
    print(f"\n📅 {year}년 데이터 처리 중... (총 {len(files)}개 파일)")
    
    year_output_dir = os.path.join(output_folder, year)
    os.makedirs(year_output_dir, exist_ok=True)
    
    chunk_count = 0
    current_chunk_data = [] 
    current_size_bytes = 0
    max_size_bytes = max_file_size_gb * 1024 * 1024 * 1024
    
    success_count = 0
    fail_count = 0
    failed_files = []
    
    # 연도 전체 통계 및 누락 보고
    total_processed_for_stats_year = 0
    overall_field_success_counts_year = defaultdict(int)
    kind_A_stats_year = {"OpenNumber_success": 0, "OpenDate_success": 0, "total_A_items": 0}
    desc_fallback_stats_year = {"Description_success_when_partial_missing": 0, "total_partial_missing_items": 0}
    yearly_total_missing_item_reports = [] 
    
    # 현재 청크 통계 및 누락 보고
    current_chunk_items_count = 0
    current_chunk_field_success_counts = defaultdict(int)
    current_chunk_kind_A_stats = {"OpenNumber_success": 0, "OpenDate_success": 0, "total_A_items": 0}
    current_chunk_desc_fallback_stats = {"Description_success_when_partial_missing": 0, "total_partial_missing_items": 0}
    current_chunk_missing_details_list = []

    start_time = time.time()
    try:
        with Pool(processes=cpu_count_val) as pool:
            with tqdm(total=len(files), desc=f"{year}년 변환 진행률", unit="파일", 
                     ncols=100, ascii=True, mininterval=0.1) as pbar:
                
                for result in pool.imap_unordered(process_xml_file, files, chunksize=8):
                    file_path, parsed_data, error, extraction_status, missing_details = result
                    
                    if error:
                        fail_count += 1
                        failed_files.append((file_path, error))
                    else:
                        success_count += 1
                        if parsed_data:
                            # --- 연도 전체 통계 업데이트 ---
                            total_processed_for_stats_year += 1
                            for field, extracted in extraction_status.items():
                                if extracted: overall_field_success_counts_year[field] += 1
                            if parsed_data.get("Kind") == 'A':
                                kind_A_stats_year["total_A_items"] += 1
                                if extraction_status.get("OpenNumber", False): kind_A_stats_year["OpenNumber_success"] += 1
                                if extraction_status.get("OpenDate", False): kind_A_stats_year["OpenDate_success"] += 1
                            brief_drawings_year = bool(parsed_data.get("BriefDescriptionOfDrawings"))
                            embodiments_year = bool(parsed_data.get("DescriptionOfEmbodiments"))
                            if not brief_drawings_year or not embodiments_year:
                                desc_fallback_stats_year["total_partial_missing_items"] += 1
                                if extraction_status.get("Description", False): desc_fallback_stats_year["Description_success_when_partial_missing"] += 1
                            if missing_details:
                                yearly_total_missing_item_reports.append(missing_details)
                                current_chunk_missing_details_list.append(missing_details)

                            # --- 현재 청크 통계 업데이트 ---
                            current_chunk_items_count +=1
                            for field, extracted in extraction_status.items():
                                if extracted: current_chunk_field_success_counts[field] += 1
                            if parsed_data.get("Kind") == 'A':
                                current_chunk_kind_A_stats["total_A_items"] += 1
                                if extraction_status.get("OpenNumber", False): current_chunk_kind_A_stats["OpenNumber_success"] += 1
                                if extraction_status.get("OpenDate", False): current_chunk_kind_A_stats["OpenDate_success"] += 1
                            brief_drawings_chunk = bool(parsed_data.get("BriefDescriptionOfDrawings"))
                            embodiments_chunk = bool(parsed_data.get("DescriptionOfEmbodiments"))
                            if not brief_drawings_chunk or not embodiments_chunk:
                                current_chunk_desc_fallback_stats["total_partial_missing_items"] += 1
                                if extraction_status.get("Description", False): current_chunk_desc_fallback_stats["Description_success_when_partial_missing"] += 1
                            
                            item_json = json.dumps(parsed_data, ensure_ascii=False)
                            item_size = len(item_json.encode('utf-8'))

                            if current_chunk_data and (len(current_chunk_data) >= max_items_per_file or current_size_bytes + item_size > max_size_bytes):
                                chunk_count += 1
                                chunk_file_name_base = f"{year}_chunk_{chunk_count}"
                                
                                # 데이터 청크 저장
                                chunk_data_file_path = os.path.join(year_output_dir, f"{chunk_file_name_base}.json")
                                save_report_to_json(current_chunk_data, chunk_data_file_path, pbar) # save_report_to_json 사용 (단, 메시지 커스텀 필요)
                                # 위 save_report_to_json은 일반 데이터용이므로, 기존 print 유지 또는 별도 함수
                                with open(chunk_data_file_path, 'w', encoding='utf-8') as f_data:
                                     json.dump(current_chunk_data, f_data, ensure_ascii=False, indent=2)
                                chunk_size_mb = os.path.getsize(chunk_data_file_path) / (1024 * 1024)
                                pbar.write(f"\n   ✅ 데이터 청크 저장: {os.path.basename(chunk_data_file_path)} (항목 {len(current_chunk_data)}개, {chunk_size_mb:.2f}MB)")


                                # 청크별 통계 보고서 저장
                                chunk_stats_report_data = generate_stats_dict(
                                    current_chunk_items_count,
                                    current_chunk_field_success_counts,
                                    current_chunk_kind_A_stats,
                                    current_chunk_desc_fallback_stats
                                    # current_chunk_missing_details_list # generate_stats_dict에서 제거됨
                                )
                                chunk_stats_report_file_path = os.path.join(year_output_dir, f"{chunk_file_name_base}_extraction_stats_report.json") # 파일명 변경
                                save_report_to_json(chunk_stats_report_data, chunk_stats_report_file_path, pbar)
                                
                                # 청크별 누락 항목 보고서 저장
                                if current_chunk_missing_details_list:
                                    chunk_missing_items_file_path = os.path.join(year_output_dir, f"{chunk_file_name_base}_missing_items_report.json") # 새 파일명
                                    save_missing_items_report(current_chunk_missing_details_list, chunk_missing_items_file_path, pbar)
                                
                                # 터미널에는 간략한 요약 또는 기존 통계 출력 유지 (선택 사항)
                                # pbar.write(f"      📊 청크 내 항목별 추출 성공률 ({current_chunk_items_count}개 문서 기준): ...") # 기존 상세 출력 대신 파일 저장 알림으로 대체 가능

                                # 현재 청크 변수 초기화
                                current_chunk_data = []
                                current_size_bytes = 0
                                current_chunk_items_count = 0
                                current_chunk_field_success_counts = defaultdict(int)
                                current_chunk_kind_A_stats = {"OpenNumber_success": 0, "OpenDate_success": 0, "total_A_items": 0}
                                current_chunk_desc_fallback_stats = {"Description_success_when_partial_missing": 0, "total_partial_missing_items": 0}
                                current_chunk_missing_details_list = []
                            
                            current_chunk_data.append(parsed_data)
                            current_size_bytes += item_size
                    
                    pbar.update(1)
                    pbar.set_postfix({'성공': success_count, '실패': fail_count})
        
        # 마지막 남은 청크 처리
        if current_chunk_data: 
            chunk_count += 1
            chunk_file_name_base = f"{year}_chunk_{chunk_count}"
            
            chunk_data_file_path = os.path.join(year_output_dir, f"{chunk_file_name_base}.json")
            # save_report_to_json(current_chunk_data, chunk_data_file_path) # 일반 데이터 저장
            with open(chunk_data_file_path, 'w', encoding='utf-8') as f_data:
                 json.dump(current_chunk_data, f_data, ensure_ascii=False, indent=2)
            chunk_size_mb = os.path.getsize(chunk_data_file_path) / (1024 * 1024)
            print(f"\n   ✅ 마지막 데이터 청크 저장: {os.path.basename(chunk_data_file_path)} (항목 {len(current_chunk_data)}개, {chunk_size_mb:.2f}MB)")


            chunk_stats_report_data = generate_stats_dict( # 변수명 변경 chunk_report_data -> chunk_stats_report_data
                current_chunk_items_count,
                current_chunk_field_success_counts,
                current_chunk_kind_A_stats,
                current_chunk_desc_fallback_stats
                # current_chunk_missing_details_list # generate_stats_dict에서 제거됨
            )
            chunk_stats_report_file_path = os.path.join(year_output_dir, f"{chunk_file_name_base}_extraction_stats_report.json") # 파일명 변경
            save_report_to_json(chunk_stats_report_data, chunk_stats_report_file_path) # pbar 없음

            # 마지막 청크의 누락 항목 보고서 저장
            if current_chunk_missing_details_list:
                chunk_missing_items_file_path = os.path.join(year_output_dir, f"{chunk_file_name_base}_missing_items_report.json") # 새 파일명
                save_missing_items_report(current_chunk_missing_details_list, chunk_missing_items_file_path) # pbar 없음

    except KeyboardInterrupt:
        # ... (이하 동일)
        print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
        # 중단 시에도 현재까지의 연도 전체 통계 및 누락 보고는 반환할 수 있도록 함
        year_final_stats_report_data_on_interrupt = generate_stats_dict( # 변수명 변경
            total_processed_for_stats_year,
            overall_field_success_counts_year,
            kind_A_stats_year,
            desc_fallback_stats_year
            # yearly_total_missing_item_reports # generate_stats_dict에서 제거됨
        )
        # 중단 시 누락 보고는 별도로 처리하지 않거나, 필요시 저장 로직 추가 가능 (현재는 통계만 반환)
        return success_count, fail_count, failed_files, {"full_year_stats_report_data": year_final_stats_report_data_on_interrupt, "yearly_missing_items": yearly_total_missing_item_reports}


    except Exception as e:
        print(f"\n❌ {year}년 데이터 처리 중 오류 발생: {str(e)}")
        print(traceback.format_exc())
        year_final_stats_report_data_on_error = generate_stats_dict( # 변수명 변경
            total_processed_for_stats_year,
            overall_field_success_counts_year,
            kind_A_stats_year,
            desc_fallback_stats_year
            # yearly_total_missing_item_reports # generate_stats_dict에서 제거됨
        )
        return success_count, fail_count, failed_files, {"full_year_stats_report_data": year_final_stats_report_data_on_error, "yearly_missing_items": yearly_total_missing_item_reports}

    # --- 연도별 최종 보고서 생성 및 저장 ---
    year_final_stats_report_data = generate_stats_dict( # 변수명 변경
        total_processed_for_stats_year,
        overall_field_success_counts_year,
        kind_A_stats_year,
        desc_fallback_stats_year
        # yearly_total_missing_item_reports # generate_stats_dict에서 제거됨
    )
    year_stats_report_file_path = os.path.join(year_output_dir, f"{year}_extraction_stats_report.json") # 파일명 변경
    save_report_to_json(year_final_stats_report_data, year_stats_report_file_path)
    
    # 연도별 누락 항목 보고서 저장
    if yearly_total_missing_item_reports:
        year_missing_items_file_path = os.path.join(year_output_dir, f"{year}_missing_items_report.json") # 새 파일명
        save_missing_items_report(yearly_total_missing_item_reports, year_missing_items_file_path)

    # 터미널에는 연도별 요약만 출력 (누락 건수 라인 제거)
    print(f"\n📊 {year}년 최종 추출 통계 요약 (상세 내용은 '{os.path.basename(year_stats_report_file_path)}' 및 관련 누락 보고서 참조):") # 메시지 수정
    print(f"   - 총 처리 문서 (유효): {total_processed_for_stats_year}")

    return success_count, fail_count, failed_files, {"full_year_stats_report_data": year_final_stats_report_data, "yearly_missing_items": yearly_total_missing_item_reports} # 반환값에 yearly_missing_items 추가

def batch_convert(input_folder: str, output_folder: str, target_folders: list = None, max_items_per_file: int = 50000, max_file_size_gb: int = 5):
    if not os.path.exists(input_folder):
        print(f"❌ 입력 폴더가 없습니다 → {input_folder}")
        return

    print(f"\n📂 입력 폴더: {input_folder}")
    print(f"📂 출력 폴더: {output_folder}")
    print(f"🎯 대상 폴더: {target_folders if target_folders else '전체'}")
    print(f"📊 파일당 최대 항목 수: {max_items_per_file}개")
    print(f"📊 파일당 최대 크기: {max_file_size_gb}GB")
    
    print("\n🔍 폴더 구조 확인 중...")
    xml_files_by_year = {}
    folder_stats = {} 

    cpu_count_val = 5 
    print(f"💿 사용할 CPU 코어 수: {cpu_count_val}")

    total_success_all_years = 0
    total_fail_all_years = 0
    all_failed_files_reports = [] # 전체 실패 보고서를 담을 리스트 초기화
    
    grand_total_processed_items = 0
    grand_overall_field_success_counts = defaultdict(int)
    grand_kind_A_stats = {"OpenNumber_success": 0, "OpenDate_success": 0, "total_A_items": 0}
    grand_desc_fallback_stats = {"Description_success_when_partial_missing": 0, "total_partial_missing_items": 0}
    grand_missing_item_reports_list = []

    if not target_folders: 
        target_folders = [d for d in os.listdir(input_folder) if os.path.isdir(os.path.join(input_folder, d))]
        print(f"ℹ️ 대상 폴더가 지정되지 않아 입력 폴더 내 모든 연도 폴더를 처리합니다: {target_folders}")

    for year in target_folders:
        try:
            xml_files_by_year[year] = collect_xmls_for_year(input_folder, year)
            folder_stats[year] = len(xml_files_by_year[year])
            if not os.path.exists(output_folder):
                os.makedirs(output_folder, exist_ok=True)
            
            success, fail, failed_files, year_result_data = process_year(
                year, xml_files_by_year[year], output_folder, max_items_per_file, max_file_size_gb, cpu_count_val
            )
                
            total_success_all_years += success
            total_fail_all_years += fail
            
            if year_result_data and "full_year_stats_report_data" in year_result_data: # 키 이름 변경
                year_stats_report = year_result_data["full_year_stats_report_data"] # 변수명 변경
                # print(f"DEBUG: Processing year_stats_report for {year}: {year_stats_report}") # 디버깅용

                current_year_processed_items = year_stats_report.get("summary", {}).get("total_processed_items", 0)
                grand_total_processed_items += current_year_processed_items
                
                # field_extraction_success_rate는 리스트, 각 요소는 dict {"field": name, "success_count": count, ...}
                for field_stat in year_stats_report.get("field_extraction_success_rate", []):
                    field_name = field_stat.get("field")
                    success_cnt = field_stat.get("success_count", 0)
                    if field_name: # 필드 이름이 있어야 합산 가능
                         grand_overall_field_success_counts[field_name] += success_cnt
                
                year_kind_A = year_stats_report.get("kind_A_special_stats", {})
                # generate_stats_dict에서 메시지가 아닌 실제 데이터일 때만 키들이 존재함
                if "target_A_documents" in year_kind_A:
                    grand_kind_A_stats["total_A_items"] += year_kind_A.get("target_A_documents", 0)
                    grand_kind_A_stats["OpenNumber_success"] += year_kind_A.get("OpenNumber_success_count", 0)
                    grand_kind_A_stats["OpenDate_success"] += year_kind_A.get("OpenDate_success_count", 0)

                year_desc_fallback = year_stats_report.get("description_fallback_stats", {})
                if "target_documents_missing_drawings_or_embodiments" in year_desc_fallback:
                    grand_desc_fallback_stats["total_partial_missing_items"] += year_desc_fallback.get("target_documents_missing_drawings_or_embodiments", 0)
                    grand_desc_fallback_stats["Description_success_when_partial_missing"] += year_desc_fallback.get("Description_extraction_success_count", 0)
                
            if year_result_data and "yearly_missing_items" in year_result_data: # 누락 아이템 리스트 합산
                 grand_missing_item_reports_list.extend(year_result_data.get("yearly_missing_items", []))
            # else:
                # print(f"DEBUG: No valid 'full_year_stats_report_data' or 'yearly_missing_items' for year {year}")

            # process_year로부터 받은 failed_files를 all_failed_files_reports에 추가
            if failed_files: # failed_files가 None이 아니고 내용이 있을 때
                all_failed_files_reports.extend(failed_files)

            print(f"\n✅ {year}년 처리 완료 – 성공 {success}건 / 실패 {fail}건")
            
        except Exception as e:
            print(f"❌ {year}년 처리 중 심각한 오류 발생: {str(e)}")
            print(traceback.format_exc())

    # print(f"DEBUG: 최종 합산된 grand_total_processed_items: {grand_total_processed_items}") # 디버깅
    # print(f"DEBUG: 최종 합산된 grand_overall_field_success_counts: {dict(grand_overall_field_success_counts)}") # 디버깅
    # print(f"DEBUG: 최종 합산된 grand_kind_A_stats: {grand_kind_A_stats}") # 디버깅
    # print(f"DEBUG: 최종 합산된 grand_desc_fallback_stats: {grand_desc_fallback_stats}") # 디버깅


    print(f"\n🏁🏁🏁 전체 변환 작업 완료 – 총 성공 {total_success_all_years}건 / 총 실패 {total_fail_all_years}건 🏁🏁🏁")

    overall_stats_report_data = generate_stats_dict( # 변수명 변경
        grand_total_processed_items,
        grand_overall_field_success_counts, 
        grand_kind_A_stats, 
        grand_desc_fallback_stats
        # grand_missing_item_reports_list # generate_stats_dict에서 제거됨
    )
    
    overall_stats_report_file_path = os.path.join(output_folder, "overall_extraction_stats_report.json") # 파일명 변경
    save_report_to_json(overall_stats_report_data, overall_stats_report_file_path)

    # 전체 누락 항목 보고서 저장
    if grand_missing_item_reports_list:
        overall_missing_items_file_path = os.path.join(output_folder, "overall_missing_items_report.json") # 새 파일명
        save_missing_items_report(grand_missing_item_reports_list, overall_missing_items_file_path)


    # 터미널에는 전체 요약만 간략히 (누락 건수 라인 제거)
    print(f"\n\n📊 전체 기간 최종 추출 통계 요약 (상세 내용은 '{os.path.basename(overall_stats_report_file_path)}' 및 '{os.path.basename(overall_missing_items_file_path)}' 참조):") # 메시지 수정
    print(f"   - 총 처리 문서 (유효): {grand_total_processed_items}")
    # 아래 라인 제거:
    # if grand_missing_item_reports_list:
    #     print(f"   - 누락 항목 발생 건수 (전체): {len(grand_missing_item_reports_list)}")
    # else:
    #     print(f"   - 전체 기간 동안 누락된 항목이 발견되지 않았습니다.")

    # 전체 실패 상세 보고서 저장 로직 추가
    if all_failed_files_reports:
        failures_log_path = os.path.join(output_folder, "overall_failures.log")
        try:
            with open(failures_log_path, 'w', encoding='utf-8') as f_log:
                for file_path, error_message in all_failed_files_reports:
                    f_log.write(f"File: {file_path}\nError: {error_message}\n--------------------\n") # <--- 여기에 닫는 따옴표 추가
            print(f"\n⚠️ 전체 실패 상세 보고서 ({len(all_failed_files_reports)}건)가 {failures_log_path} 에 저장되었습니다.")
        except Exception as e:
            print(f"\n❌ 전체 실패 상세 보고서 저장 중 오류 발생: {str(e)}")
    elif total_fail_all_years > 0: # 전체 실패 카운트는 있으나, 상세 보고 리스트가 비어있는 경우
        print(f"\n⚠️ 총 {total_fail_all_years}건의 실패가 기록되었으나, 상세 실패 보고 내용이 없습니다. 코드 점검이 필요할 수 있습니다.")
    # else: # 실패가 없는 경우 특별한 메시지 없음

########################################################
# 5️⃣  CLI 진입점 – 필요하면 main_batch.py 에서 호출 #
########################################################

if __name__ == "__main__":
    import traceback
    import argparse
    
    parser = argparse.ArgumentParser(description="XML to JSON 변환 도구")
    parser.add_argument("--single-file", help="단일 파일만 처리")
    parser.add_argument("--output-dir", default="./output", help="결과 파일이 저장될 디렉토리 (기본값: ./output)")
    parser.add_argument("--max-items", type=int, default=50000, help="파일당 최대 항목 수 (기본값: 50000)")
    parser.add_argument("--max-size", type=int, default=5, help="파일당 최대 크기(GB) (기본값: 5)")
    args = parser.parse_args()
    
    if args.single_file:
        # 단일 파일 처리 모드
        print(f"단일 파일 '{args.single_file}' 처리 중...")
        file_path, data, error, extraction_status, missing_details = process_xml_file(args.single_file)
        if error:
            print(f"오류 발생: {error}")
        else:
            # 출력 디렉토리 생성
            os.makedirs(args.output_dir, exist_ok=True)
            
            # 파일명만 추출
            file_name = os.path.basename(args.single_file)
            base_name = os.path.splitext(file_name)[0]
            
            # 출력 파일 경로 생성
            output_file = os.path.join(args.output_dir, base_name + '.json')
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump([data], f, ensure_ascii=False, indent=2)
            print(f"변환 완료: {output_file}")
    else:
        # 일반 배치 처리 모드
        # 기존 Windows 경로 (나중에 사용할 수 있도록 주석 처리)
        INPUT_DIR = r"C:\Users\kwoor\Python\folder_arange\CN_번역"
        # OUTPUT_DIR = r"D:\CN_json_result_2021_2"  # 기존 결과와 구분하기 위해 별도 폴더 사용
        OUTPUT_DIR = r"D:\CN_json_result\2021_datatypetest"
        # target_folders = ["2021"]  # 2021 폴더 전체(CN20, CN21 하위폴더 포함) 처리
        target_folders = [str(year) for year in range(2021, 2022)]
        
        # Mac 경로 (2023년 데이터 처리용)
        # INPUT_DIR = "/Users/macsh/Documents/Wand-project/cn/cn_번역"
        # OUTPUT_DIR = "/Users/macsh/Documents/Wand-project/cn/cn_json_results"
        # target_folders = ["2023"]
        
        print(f"\n🎯 처리할 연도 폴더: {target_folders}")
        
        print("\n🔍 입력 폴더 존재 여부 확인...")
        if not os.path.exists(INPUT_DIR):
            print(f"❌ 입력 폴더가 존재하지 않습니다: {INPUT_DIR}")
            exit(1)
            
        print("✅ 입력 폴더 확인 완료")
        batch_convert(INPUT_DIR, OUTPUT_DIR, target_folders, args.max_items, args.max_size)

# 문서에서 description 객체 찾기
def find_description_in_document(root):
    """문서에서 description 객체를 찾아 반환"""
    description = None
    # 1. application-body/description 경로 확인
    app_body = safe_dict_get(root, "application-body", {})
    if isinstance(app_body, dict) and "description" in app_body:
        description = app_body["description"]
    # 2. 직접 description 경로 확인
    elif "description" in root:
        description = root["description"]
    # 3. cn-description 경로 확인 (중국 특허 문서)
    elif "cn-description" in root:
        description = root["cn-description"]
    # 4. Description 경로 확인 (구 형식 문서)
    elif "Description" in root:
        description = root["Description"]
    
    return description

# 설명 단락들(paragraphs) 추출 함수
def extract_description_paragraphs(description):
    """description 객체에서 단락들을 추출"""
    if not isinstance(description, dict):
        return []
        
    all_paragraphs = []
    # p 태그 리스트 확인
    if "p" in description and isinstance(description["p"], list):
        all_paragraphs = description["p"]
    elif "p" in description:
        all_paragraphs = [description["p"]]
    # Paragraphs 태그 리스트 확인
    elif "Paragraphs" in description and isinstance(description["Paragraphs"], list):
        all_paragraphs = description["Paragraphs"]
    elif "Paragraphs" in description:
        all_paragraphs = [description["Paragraphs"]]
    
    return all_paragraphs

# 도면 설명과 실시예 추출하는 공통 함수
def extract_structured_description(description, all_paragraphs):
    """도면 설명과 실시예를 추출하는 공통 함수"""
    drawing_section = None
    embodiment_section = None
    full_description_text = None
    
    # 1. 태그 기반 추출 시도
    if isinstance(description, dict):
        # 도면 설명 관련 태그 찾기
        drawings_desc = None
        for key in ["drawings-description", "description-of-drawings", "DrawingsDescription", "drawings"]:
            if key in description:
                drawings_desc = description[key]
                break
        
        # 실시예 관련 태그 찾기
        embodiments = None
        for key in ["detailed-description", "invention-description", "embodiments", "mode-for-invention", "InventionMode"]:
            if key in description:
                embodiments = description[key]
                break
        
        # 태그 기반 결과 추출
        if drawings_desc:
            raw_drawing_text = extract_text(drawings_desc)
            drawing_section = scrub_tables(raw_drawing_text)
        
        if embodiments:
            raw_embodiment_text = extract_text(embodiments)
            embodiment_section = scrub_tables(raw_embodiment_text)
    
    # 2. 마커 기반 추출 시도 (태그 기반 결과가 없는 경우)
    if not drawing_section or not embodiment_section:
        if all_paragraphs:
            marker_drawing_section, marker_embodiment_section, _ = extract_description_sections(all_paragraphs)
            
            if not drawing_section and marker_drawing_section:
                drawing_section = marker_drawing_section
            
            if not embodiment_section and marker_embodiment_section:
                embodiment_section = marker_embodiment_section
    
    # 3. 전체 description 텍스트 추출 (섹션 구분 없이)
    if isinstance(description, dict):
        full_description_text = extract_text(description)
    
    return drawing_section, embodiment_section, full_description_text

# 파싱 결과 생성 함수
def create_parsed_result(main_cpc, kind, open_number, open_date, register_number, register_date,
                       pub_number, pub_date, app_number, app_date, applicant_name, inventor_name,
                       agent_name, title, summary, drawing_section, embodiment_section, full_description_text, claims):
    """파싱 결과를 표준화된 딕셔너리로 생성"""
    return {
        "MainCPC": main_cpc,
        "Kind": kind,
        "OpenNumber": open_number,
        "OpenDate": open_date,
        "RegisterNumber": register_number,
        "RegisterDate": register_date,
        "PublicationNumber": pub_number,
        "PublicationDate": pub_date,
        "ApplicationNumber": app_number,
        "ApplicationDate": app_date,
        "ApplicantName": applicant_name,
        "InventorName": inventor_name,
        "AgentName": agent_name,
        "Title": title,
        "Summary": summary,
        "SummaryOfInvention": None,  # 항상 None으로 설정
        "BriefDescriptionOfDrawings": drawing_section,
        "DescriptionOfEmbodiments": embodiment_section,
        "Description": full_description_text if (not bool(drawing_section) or not bool(embodiment_section)) else None,
        "Claims": claims,
    }

def extract_publication_info(root, biblio):
    """공개 정보 추출 함수"""
    pub_refs = safe_get(biblio, "PublicationReference")
    
    # standard 형식 우선 선택
    pub_info = {}
    if isinstance(pub_refs, list):
        for ref in pub_refs:
            if isinstance(ref, dict) and ref.get("@dataFormat") == "standard":
                pub_info = ref
                break
    elif isinstance(pub_refs, dict):
        pub_info = pub_refs
    
    # DocumentID 검색
    pub_doc = {}
    if "@dataFormat" in pub_info and pub_info["@dataFormat"] == "standard":
        pub_doc = safe_get(pub_info, "DocumentID") or {}
    else:
        all_doc_ids = safe_get(pub_info, "DocumentID")
        if isinstance(all_doc_ids, list):
            for doc in all_doc_ids:
                if isinstance(doc, dict) and "@dataFormat" in doc and doc["@dataFormat"] == "standard":
                    pub_doc = doc
                    break
    
    if not isinstance(pub_doc, dict):
        pub_doc = {}
        
    return pub_info, pub_doc

def extract_application_info(biblio):
    """출원 정보 추출 함수"""
    application_number = None
    application_date = None
    
    # 1. ApplicationReference 목록에서 추출
    app_refs = safe_get(biblio, "ApplicationReference")
    if isinstance(app_refs, list):
        for app_ref in app_refs:
            if not isinstance(app_ref, dict):
                continue
            
            if "@dataFormat" in app_ref and app_ref["@dataFormat"] == "standard":
                doc_id = safe_get(app_ref, "DocumentID")
                if isinstance(doc_id, dict):
                    doc_num = safe_dict_get(doc_id, "DocNumber")
                    doc_date = safe_dict_get(doc_id, "Date")
                    
                    if doc_num:
                        application_number = doc_num
                    if doc_date:
                        application_date = doc_date
                    break
    
    # 2. 단일 ApplicationReference에서 추출
    if not application_number or not application_date:
        app_info = safe_get(biblio, "ApplicationReference") or {}
        if isinstance(app_info, dict):
            if "@dataFormat" in app_info and app_info["@dataFormat"] == "standard":
                app_doc = safe_get(app_info, "DocumentID") or {}
                if isinstance(app_doc, dict):
                    if not application_number:
                        application_number = safe_dict_get(app_doc, "DocNumber")
                    if not application_date:
                        application_date = safe_dict_get(app_doc, "Date")
            else:
                all_doc_ids = safe_get(app_info, "DocumentID")
                if isinstance(all_doc_ids, list):
                    for doc in all_doc_ids:
                        if isinstance(doc, dict) and "@dataFormat" in doc and doc["@dataFormat"] == "standard":
                            if not application_number:
                                application_number = safe_dict_get(doc, "DocNumber")
                            if not application_date:
                                application_date = safe_dict_get(doc, "Date")
                            break
    
    return application_number, application_date

def extract_cpc_info(root, biblio):
    """CPC/IPC 정보 추출 함수"""
    # 루트 문서에서 직접 CPC 추출 시도
    main_cpc = safe_dict_get(root, "@docNumber") and extract_text(safe_get(root, "Text"))
    
    # ClassificationIPCRDetails에서 추출
    ipcs = safe_get(biblio, "ClassificationIPCRDetails")
    if ipcs:
        ipc_items = safe_get(ipcs, "ClassificationIPCR")
        if isinstance(ipc_items, list) and ipc_items:
            text = safe_get(ipc_items[0], "Text")
            if text:
                main_cpc = clean_ipc_text(extract_text(text))
        elif isinstance(ipc_items, dict):
            text = safe_get(ipc_items, "Text")
            if text:
                main_cpc = clean_ipc_text(extract_text(text))
    
    # 정리 처리
    if isinstance(main_cpc, list) and main_cpc:
        main_cpc = clean_ipc_text(main_cpc[0])
    elif main_cpc:
        main_cpc = clean_ipc_text(main_cpc)
    
    return main_cpc

def extract_applicant_info(biblio):
    """출원인 정보 추출 함수"""
    applicant_details = safe_get(biblio, "ApplicantDetails")
    applicant = safe_get(applicant_details, "Applicant")
    applicant_name = None
    
    if isinstance(applicant, list) and applicant:
        # 첫 번째 이름만 처리
        address_book = safe_get(applicant[0], "AddressBook")
        if address_book:
            if isinstance(address_book, list) and address_book:
                applicant_name = clean_organization_name(extract_text(safe_get(address_book[0], "Name")))
            elif isinstance(address_book, dict):
                applicant_name = clean_organization_name(extract_text(safe_get(address_book, "Name")))
    elif isinstance(applicant, dict):
        # 단일 지원자 처리
        address_book = safe_get(applicant, "AddressBook")
        if address_book:
            if isinstance(address_book, list) and address_book:
                applicant_name = clean_organization_name(extract_text(safe_get(address_book[0], "Name")))
            elif isinstance(address_book, dict):
                applicant_name = clean_organization_name(extract_text(safe_get(address_book, "Name")))
    
    return applicant_name

def extract_inventor_info(biblio):
    """발명자 정보 추출 함수"""
    inventor_block = safe_get(biblio, "InventorDetails")
    inventor_list = safe_get(inventor_block, "Inventor")
    inventor_name = None
    
    if isinstance(inventor_list, list):
        inventor_names = []
        for inv in inventor_list:
            if not isinstance(inv, dict):
                continue
            address_book = safe_get(inv, "AddressBook")
            if address_book:
                if isinstance(address_book, list) and address_book:
                    name = extract_text(safe_get(address_book[0], "Name"))
                elif isinstance(address_book, dict):
                    name = extract_text(safe_get(address_book, "Name"))
                else:
                    name = None
                    
                if name:
                    inventor_names.append(name)
        if inventor_names:
            inventor_name = ", ".join(inventor_names)
    elif isinstance(inventor_list, dict):
        address_book = safe_get(inventor_list, "AddressBook")
        if address_book:
            if isinstance(address_book, list) and address_book:
                inventor_name = extract_text(safe_get(address_book[0], "Name"))
            elif isinstance(address_book, dict):
                inventor_name = extract_text(safe_get(address_book, "Name"))
    
    return inventor_name

def extract_agent_info(biblio):
    """대리인 정보 추출 함수"""
    agent_block = safe_get(biblio, "AgentDetails")
    agent_name = None
    
    if isinstance(agent_block, dict):
        agents = safe_get(agent_block, "Agent")
        if isinstance(agents, list) and agents:
            agency_names = []
            for agent in agents:
                if not isinstance(agent, dict):
                    continue
                
                # 1. 에이전트 이름과 조직 추출    
                agent_person_name = extract_text(safe_get(agent, "Name"))
                agent_org_name = clean_organization_name(extract_text(safe_get(agent, "OrganizationName")))
                
                # 2. 주소록에서 이름 추출 시도
                address_book = safe_get(agent, "AddressBook")
                if isinstance(address_book, dict):
                    if not agent_person_name:
                        agent_person_name = extract_text(safe_get(address_book, "Name"))
                    if not agent_org_name:
                        agent_org_name = clean_organization_name(extract_text(safe_get(address_book, "OrganizationName")))
                
                # 3. Agency 섹션 검색
                agency = safe_get(agent, "Agency")
                if isinstance(agency, dict):
                    agency_address_book = safe_get(agency, "AddressBook")
                    if isinstance(agency_address_book, dict):
                        if not agent_org_name:
                            agent_org_name = clean_organization_name(extract_text(safe_get(agency_address_book, "OrganizationName")))
                
                # 4. 조합하여 에이전트 이름 생성
                if agent_person_name and agent_org_name:
                    agency_names.append(f"{agent_person_name} ({agent_org_name})")
                elif agent_person_name:
                    agency_names.append(agent_person_name)
                elif agent_org_name:
                    agency_names.append(agent_org_name)
            
            if agency_names:
                agent_name = "; ".join(agency_names)
        elif isinstance(agents, dict):
            # 단일 에이전트 처리 로직
            agent_person_name = extract_text(safe_get(agents, "Name"))
            agent_org_name = clean_organization_name(extract_text(safe_get(agents, "OrganizationName")))
            
            # 주소록에서 이름 추출 시도
            address_book = safe_get(agents, "AddressBook")
            if isinstance(address_book, dict):
                if not agent_person_name:
                    agent_person_name = extract_text(safe_get(address_book, "Name"))
                if not agent_org_name:
                    agent_org_name = clean_organization_name(extract_text(safe_get(address_book, "OrganizationName")))
            
            # Agency 섹션 검색
            agency = safe_get(agents, "Agency")
            if isinstance(agency, dict):
                agency_address_book = safe_get(agency, "AddressBook")
                if isinstance(agency_address_book, dict):
                    if not agent_org_name:
                        agent_org_name = clean_organization_name(extract_text(safe_get(agency_address_book, "OrganizationName")))
            
            # 조합하여 에이전트 이름 생성
            if agent_person_name and agent_org_name:
                agent_name = f"{agent_person_name} ({agent_org_name})"
            elif agent_person_name:
                agent_name = agent_person_name
            elif agent_org_name:
                agent_name = agent_org_name
    
    return agent_name

def process_fallback_xml(root):
    """알 수 없는 구조의 XML에 대한 fallback 파싱 처리"""
    # 기본값 초기화
    claims = None
    title = None
    summary = None
    drawing_section = None
    embodiment_section = None
    full_description_text = None
    
    # 기본 파싱 시도
    if 'application-body' in root and 'description' in root['application-body']:
        description = root['application-body']['description']
        
        # 텍스트 정보 추출
        claims = extract_claims(root)
        title = extract_text(safe_dict_get(root, 'invention-title'))
        abstract = safe_get(root, 'abstract')
        summary = get_abstract_text(abstract)
        
        # 설명 섹션 추출
        all_paragraphs = extract_description_paragraphs(description)
        drawing_section, embodiment_section, full_description_text = extract_structured_description(description, all_paragraphs)
    else:
        return None
    
    # 기본 정보 추출
    app_number = safe_get(root, '@applicationNumber')
    app_date = safe_get(root, '@applicationDate')
    pub_number = None  # fallback에서는 공개번호가 없을 수 있음
    pub_date = None
    kind = safe_get(root, '@kind') or safe_get(root, 'kind')

    # 번호 형식 변환
    app_number_clean, pub_number_formatted, pub_date_formatted, open_number, open_date, register_number, register_date = format_numbers(app_number, pub_number, pub_date, kind, app_date)

    # 결과 반환
    return create_parsed_result(
        None, kind, open_number, open_date, register_number, register_date,
        pub_number_formatted, pub_date_formatted, app_number_clean, app_date,
        None, None, None, title, summary,
        drawing_section, embodiment_section, full_description_text, claims
    )

# def convert_fixed_files_to_json(fixed_files_list, output_dir):  # json 오류로그 기반으로 xml 파일을 수정하는 것은 추후 대표님과 상의 후 진행할 예정.
#     """수정된 XML 파일을 JSON으로 변환"""
#     if not os.path.exists(fixed_files_list):
#         logging.error(f"수정된 파일 목록을 찾을 수 없음: {fixed_files_list}")
#         return False
    
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir, exist_ok=True)
    
#     # main_batch_home_refactored.py에서 필요한 함수들 가져오기
#     from main_batch_home_refactored import process_xml_file
    
#     success_count = 0
#     fail_count = 0
    
#     with open(fixed_files_list, 'r', encoding='utf-8') as f:
#         file_paths = f.read().splitlines()
    
#     total_files = len(file_paths)
#     logging.info(f"총 {total_files}개의 수정된 XML 파일을 JSON으로 변환합니다.")
    
#     results = []
    
#     for file_path in tqdm(file_paths, desc="JSON 변환 진행률"):
#         try:
#             file_path, parsed_data, error, extraction_status, missing_details = process_xml_file(file_path)
            
#             if error:
#                 fail_count += 1
#                 logging.error(f"JSON 변환 실패: {file_path} - {error}")
#             else:
#                 success_count += 1
#                 if parsed_data:
#                     results.append(parsed_data)
#         except Exception as e:
#             fail_count += 1
#             logging.error(f"JSON 변환 중 예외 발생: {file_path} - {str(e)}")
    
#     if results:
#         output_file = os.path.join(output_dir, "fixed_xml_converted.json")
#         with open(output_file, 'w', encoding='utf-8') as f:
#             json.dump(results, f, ensure_ascii=False, indent=2)
        
#         logging.info(f"✅ 변환 완료: {output_file} (성공: {success_count}, 실패: {fail_count})")
#         return True
#     else:
#         logging.error("변환할 결과가 없습니다.")
#         return False