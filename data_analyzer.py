import pandas as pd
import numpy as np
import os
from datetime import datetime
import re
import logging

logging.basicConfig(level=logging.DEBUG, # INFO -> DEBUG 로 변경
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def find_header_row(df):
    """
    1행부터 10행 사이에서 부서명, 물품코드, 청구량, 수령량이 모두 포함된 행을 찾아 헤더 행으로 반환합니다.
    """
    # 확인할 키워드들 ('물품코드' 추가)
    keywords = ['부서명', '물품코드', '청구량', '수령량']
    
    # 처음 10행만 검사 (더 적은 행을 가진 경우 모든 행 검사)
    max_rows = min(10, len(df))
    
    for i in range(max_rows):
        try:
            row = df.iloc[i].astype(str)
            row_values = [str(val).lower() for val in row.values]
            
            # 모든 키워드가 이 행에 포함되어 있는지 확인
            if all(any(keyword in val for val in row_values) for keyword in keywords):
                return i
        except IndexError:
            # 행 인덱스가 범위를 벗어나는 경우 중단
            break
    
    # 못 찾으면 기본값 0 반환
    logger.warning("헤더 행을 찾지 못했습니다. 기본값 0을 사용합니다.") # 헤더 못 찾을 경우 경고 로그 추가
    return 0

def load_excel_data(file_path, is_cumulative_flag: bool = False):
    """엑셀 파일을 로드하여 데이터프레임으로 반환합니다.
       시트명에서 YYYY-MM-DD 형식의 날짜를 추출/표준화하고,
       실패 시 해당 시트는 건너뛰도록 수정합니다.
    
    Args:
        file_path: 엑셀 파일 경로 또는 버퍼
        is_cumulative_flag (bool): 누적 엑셀 파일 여부 플래그. 기본값은 False.
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        all_data = []
        processed_dates = [] # 처리된 표준 날짜 저장

        # 누적 엑셀 여부를 is_cumulative_flag로 판단 (기존 로직 제거)
        # is_cumulative = 'cumulative_excel.xlsx' in file_path
        is_cumulative = is_cumulative_flag
        
        logger.info(f"엑셀 파일 로드 시작: {'누적 파일' if is_cumulative else file_path}, 시트 수: {len(excel_file.sheet_names)}")

        for sheet_name in excel_file.sheet_names:
            original_sheet_name = sheet_name.strip()
            
            if is_cumulative:
                logger.info(f"누적 엑셀 시트 처리 중: {original_sheet_name}")
                # 누적 파일: 헤더 없이 읽고 L번째 열(인덱스 11)을 날짜로 사용
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    # L번째 열(인덱스 11) 존재 확인
                    if df.shape[1] > 11:
                        df['날짜'] = df.iloc[:, 11].astype(str)
                        # 헤더 재설정 (첫 행을 헤더로 가정)
                        df.columns = df.iloc[0]
                        df = df[1:].reset_index(drop=True)
                        all_data.append(df)
                        processed_dates.extend(df['날짜'].unique().tolist())
                        logger.info(f"누적 시트 '{original_sheet_name}' 처리 완료 (L열 사용): {len(df)}행")
                    else:
                        logger.warning(f"누적 시트 '{original_sheet_name}'에 L열(12번째 열)이 없습니다. 컬럼 수: {df.shape[1]}. 건너뜁니다.")
                        continue
                except Exception as cumulative_load_err:
                    logger.error(f"누적 시트 '{original_sheet_name}' 처리 중 오류: {cumulative_load_err}")
                    continue
            else:
                # 일반 엑셀 파일의 경우 시트명에서 날짜 추출
                standardized_date = standardize_date(original_sheet_name)
                if standardized_date == original_sheet_name:
                    logger.warning(f"시트명 '{original_sheet_name}'에서 유효한 날짜를 인식할 수 없어 건너뜁니다.") # '건너뛰도록 수정합니다' -> '건너뜁니다'
                    continue

                logger.info(f"시트 '{original_sheet_name}' -> 표준 날짜: {standardized_date}")
                processed_dates.append(standardized_date)

                df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                header_row = find_header_row(df_raw)
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
                # 완전히 빈 행만 제거하고 부분 NaN 유지
                df = df.dropna(how='all')
                logger.info(f"시트 '{original_sheet_name}' 로드: {len(df)}행, 컬럼: {df.columns.tolist()}")
                df['날짜'] = standardized_date
                all_data.append(df)

        if not all_data:
            return {"status": "error", "message": "엑셀 파일에서 유효한 데이터를 찾을 수 없습니다."}

        combined_df = pd.concat(all_data, ignore_index=True)
        
        # --- 중복 컬럼 제거 로직 추가 ---
        original_columns_before_dedup = combined_df.columns.tolist()
        combined_df = combined_df.loc[:, ~combined_df.columns.duplicated(keep='first')]
        final_columns_after_dedup = combined_df.columns.tolist()
        if len(original_columns_before_dedup) != len(final_columns_after_dedup):
            logger.warning(f"데이터 병합 후 중복 컬럼 제거됨. 이전: {original_columns_before_dedup}, 이후: {final_columns_after_dedup}")
        # --------------------------------

        logger.info(f"로드된 데이터 컬럼 (중복 제거 후, 변경 전): {combined_df.columns.tolist()}")
        logger.info(f"모든 시트 통합 후 총 행 수: {len(combined_df)}")

        # --- 컬럼명 표준화 (정확 일치 기준) ---
        rename_map = {}
        current_columns_normalized = {str(col).lower().strip(): col for col in combined_df.columns}

        exact_match_required = {
            '물품코드': ['물품코드'],
            '물품명': ['물품명'],
            '부서명': ['부서명'],
            '청구량': ['청구량'],
            '수령량': ['수령량']
        }
        
        found_columns = {}

        for std_name, exact_original_names_normalized in exact_match_required.items():
            found = False
            for name_norm in exact_original_names_normalized:
                if name_norm in current_columns_normalized:
                    original_col_name = current_columns_normalized[name_norm]
                    if original_col_name not in rename_map:
                        rename_map[original_col_name] = std_name
                        found_columns[std_name] = original_col_name
                        found = True
                        logger.info(f"정확 일치 컬럼 매핑: 원본 '{original_col_name}' -> 표준 '{std_name}'")
                        break
            
            if not found and std_name in ['청구량', '수령량']:
                if std_name == '수령량' and '수령량' in found_columns:
                    continue
                
                possible_names_str = ', '.join(exact_original_names_normalized)
                logger.error(f"필수 컬럼 '{possible_names_str}' 중 하나를 정확히 찾을 수 없습니다.")
                return {"status": "error", "message": f"필수 컬럼 '{possible_names_str}' 중 하나를 정확히 찾을 수 없습니다."}

        if '날짜' in rename_map:
            del rename_map['날짜']

        if rename_map:
            combined_df.rename(columns=rename_map, inplace=True)
            logger.info(f"컬럼명 표준화 적용 (정확 일치): {rename_map}")
            logger.info(f"로드된 데이터 컬럼 (변경 후): {combined_df.columns.tolist()}")
            if '물품코드' in combined_df.columns:
                logger.info("'물품코드' 컬럼이 표준화 후 존재합니다.")
            else:
                logger.warning("'물품코드' 컬럼이 표준화 후 존재하지 않습니다!")
        else:
            logger.warning("표준화할 컬럼명을 찾지 못했습니다.")

        # --- 컬럼 표준화 후 다시 중복 컬럼 확인 (선택적이지만 안전) ---
        original_columns_before_std_dedup = combined_df.columns.tolist()
        combined_df = combined_df.loc[:, ~combined_df.columns.duplicated(keep='first')]
        final_columns_after_std_dedup = combined_df.columns.tolist()
        if len(original_columns_before_std_dedup) != len(final_columns_after_std_dedup):
             logger.warning(f"컬럼 표준화 후 중복 컬럼 제거됨. 이전: {original_columns_before_std_dedup}, 이후: {final_columns_after_std_dedup}")
        # ----------------------------------------------------

        required_std_cols = ['부서명', '물품코드', '물품명', '청구량', '수령량']
        missing_std_cols = [col for col in required_std_cols if col not in combined_df.columns]
        if missing_std_cols:
            logger.error(f"표준화 후 필수 컬럼 누락: {missing_std_cols}")
            return {"status": "error", "message": f"데이터 처리 중 필수 컬럼 누락: {', '.join(missing_std_cols)}"}

        # NaN 값 처리 (물품코드와 부서명은 필수값으로 유지, 청구량/수령량은 0으로 대체)
        # 물품명이 NaN인 경우 물품코드로 대체
        if '물품명' in combined_df.columns:
            combined_df['물품명'] = combined_df.apply(
                lambda row: str(row['물품코드']) if pd.isna(row['물품명']) else row['물품명'], axis=1
            )
        
        # 청구량/수령량이 NaN인 경우 0으로 대체
        if '청구량' in combined_df.columns:
            combined_df['청구량'] = combined_df['청구량'].fillna(0)
        
        if '수령량' in combined_df.columns:
            combined_df['수령량'] = combined_df['수령량'].fillna(0)
        
        # 부서별 품목 개수 로그
        if '부서명' in combined_df.columns and '물품코드' in combined_df.columns:
            dept_counts = combined_df.groupby('부서명')['물품코드'].nunique()
            for dept, count in dept_counts.items():
                logger.info(f"부서 '{dept}'의 고유 물품코드 수: {count}")

        logger.info(f"최종 데이터프레임 크기: {combined_df.shape}")
        return {"status": "success", "data": combined_df}

    except Exception as e:
        logger.error(f"엑셀 로드 중 오류: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

def find_mismatches(df):
    """청구량과 수령량 불일치 항목을 찾습니다.
       컬럼명이 표준화되지 않은 경우 자동으로 매핑을 시도합니다.
    """
    logger.debug(f"--- find_mismatches 시작 ---")
    logger.debug(f"입력 데이터프레임 (상위 5행):\n{df.head().to_string()}")
    logger.debug(f"입력 데이터 컬럼: {df.columns.tolist()}")
    try:
        # 컬럼명 매핑 (표준화되지 않은 경우 대비)
        column_mapping = {}
        available_cols = df.columns.tolist()
        
        # 필수 컬럼 매핑 시도
        mapping_rules = {
            '날짜': ['날짜'],
            '부서명': ['부서명'],
            '물품코드': ['물품코드'],
            '물품명': ['물품명'],
            '청구량': ['청구량'],
            '수령량': ['수령량']
        }
        
        for std_name, possible_names in mapping_rules.items():
            found = False
            for possible_name in possible_names:
                if possible_name in available_cols:
                    column_mapping[std_name] = possible_name
                    found = True
                    break
            if not found:
                logger.error(f"find_mismatches: 필수 컬럼 '{std_name}' (가능한 이름: {possible_names})을 찾을 수 없습니다.")
                return {"status": "error", "message": f"필수 컬럼 '{std_name}'을 찾을 수 없습니다."}
        
        logger.debug(f"컬럼 매핑: {column_mapping}")
        
        # 매핑된 컬럼으로 데이터 추출
        df_compare = df[[column_mapping[col] for col in ['날짜', '부서명', '물품코드', '물품명', '청구량', '수령량']]].copy()
        df_compare.columns = ['날짜', '부서명', '물품코드', '물품명', '청구량', '수령량']
        
        # 데이터 타입 변환 및 NaN 처리
        df_compare['청구량'] = pd.to_numeric(df_compare['청구량'], errors='coerce').fillna(0)
        df_compare['수령량'] = pd.to_numeric(df_compare['수령량'], errors='coerce').fillna(0)
        
        logger.debug(f"타입 변환 후 청구량 샘플: {df_compare['청구량'].head().tolist()}")
        logger.debug(f"타입 변환 후 수령량 샘플: {df_compare['수령량'].head().tolist()}")
        
        # 불일치 항목 찾기
        df_mismatch = df_compare[df_compare['청구량'] != df_compare['수령량']].copy()
        
        # 결과가 비어있지 않은지 확인
        if df_mismatch.empty:
            logger.debug("불일치 항목이 없습니다.")
            logger.debug(f"--- find_mismatches 종료 --- ")
            return {
                "status": "success", 
                "data": df_mismatch, # 빈 데이터프레임 반환
                "message": "불일치 항목이 없습니다."
            }
            
        # 차이 계산
        df_mismatch['차이'] = df_mismatch['수령량'] - df_mismatch['청구량']
        
        logger.debug(f"반환될 불일치 데이터프레임 (상위 5행):\n{df_mismatch.head().to_string()}")
        logger.debug(f"총 {len(df_mismatch)}개의 불일치 항목 발견됨.")
        logger.debug(f"--- find_mismatches 종료 --- ")
        return {"status": "success", "data": df_mismatch}
    
    except Exception as e:
        logger.error(f"불일치 항목 찾기 오류: {e}", exc_info=True)
        return {"status": "error", "message": f"불일치 항목 찾기 오류: {str(e)}"}

def filter_by_department(df, department):
    """부서명으로 데이터를 필터링합니다."""
    try:
        if '부서명' not in df.columns:
            return {"status": "error", "message": "'부서명' 컬럼을 찾을 수 없습니다."}
            
        # 부서명으로 필터링
        filtered_df = df[df['부서명'] == department]
        
        if len(filtered_df) == 0:
            return {
                "status": "success", 
                "data": filtered_df,
                "message": f"'{department}' 부서의 데이터가 없습니다."
            }
            
        return {"status": "success", "data": filtered_df}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_unique_departments(df):
    """데이터프레임에서 고유한 부서명 목록을 반환합니다."""
    try:
        if '부서명' not in df.columns:
            return {"status": "error", "message": "'부서명' 컬럼을 찾을 수 없습니다."}
            
        departments = sorted(list(set(df['부서명'].astype(str).tolist())))
        return {"status": "success", "data": departments}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

def merge_department_data(excel_df, ocr_departments):
    """엑셀 데이터와 OCR에서 추출한 부서 정보를 병합하여 비교 결과를 반환합니다."""
    try:
        # 엑셀에서 고유 부서 추출 (문자열로 변환 후 처리)
        if '부서명' not in excel_df.columns:
            return {"status": "error", "message": "엑셀 데이터에 '부서명' 컬럼이 없습니다."}
        excel_depts = set(excel_df['부서명'].astype(str).unique())
        
        # OCR에서 추출한 부서 정보 변환 (부서명, 페이지 리스트)
        ocr_dept_page_map = {}
        for dept, page in ocr_departments:
            ocr_dept_page_map.setdefault(dept, []).append(page)
        # 중복 제거 및 정렬
        for dept in ocr_dept_page_map:
            ocr_dept_page_map[dept] = sorted(set(ocr_dept_page_map[dept]))
        ocr_depts = set(ocr_dept_page_map.keys())
        
        # 공통 부서 찾기
        common_depts = excel_depts.intersection(ocr_depts)
        
        # OCR에만 있는 부서
        ocr_only = ocr_depts - excel_depts
        
        # 엑셀에만 있는 부서
        excel_only = excel_depts - ocr_depts
        
        # 병합 결과 생성
        comparison_result = {
            "common": sorted(list(common_depts)),
            "excel_only": sorted(list(excel_only)),
            "ocr_only": sorted([{"dept": dept, "page": ocr_dept_page_map[dept]} 
                              for dept in ocr_only], key=lambda x: x['dept'])
        }
        
        return {"status": "success", "data": comparison_result, "dept_page_map": ocr_dept_page_map}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

def generate_summary(mismatch_df):
    """불일치 데이터에 대한 요약 통계를 생성합니다."""
    try:
        if len(mismatch_df) == 0:
            return {
                "status": "success",
                "data": {
                    "total_mismatches": 0,
                    "departments": [],
                    "avg_difference": 0
                }
            }
        
        # 부서별 불일치 개수
        dept_counts = mismatch_df['부서명'].value_counts().to_dict()
        
        # 평균 차이
        avg_diff = mismatch_df['차이'].mean()
        
        # 불일치 총 개수
        total = len(mismatch_df)
        
        # 물품명별 불일치 개수
        item_counts = mismatch_df['물품명'].value_counts().to_dict()
        
        summary = {
            "total_mismatches": total,
            "departments": [{"name": dept, "count": count} for dept, count in dept_counts.items()],
            "items": [{"name": item, "count": count} for item, count in item_counts.items()],
            "avg_difference": float(avg_diff)
        }
        
        return {"status": "success", "data": summary}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

now = datetime.now()
logger = logging.getLogger(__name__) # 로거 정의 필요

def extract_items_from_ocr(ocr_text):
    """
    OCR 텍스트에서 물품코드(L+6자리 숫자)를 추출합니다.
    [부서명] 패턴이 나타나면 다음 [부서명] 패턴이 나타나기 전까지를 해당 부서의 블록으로 간주하고,
    블록 내에서 L+6자리 숫자 코드를 찾아 반환합니다.
    
    Args:
        ocr_text (str): OCR로 추출된 텍스트
        
    Returns:
        list: (물품코드, 부서명) 튜플의 리스트
    """
    items = []
    
    # "[부서명]" 정확한 단어를 찾는 패턴
    dept_pattern = r'\[부서명\]'
    
    # L+6자리 숫자 코드를 찾는 패턴 (더 유연한 패턴으로 수정)
    item_pattern = re.compile(r'L\d{6}')
    
    # 텍스트를 줄 단위로 분리
    lines = ocr_text.split('\n')
    
    # 부서 블록의 시작 위치 찾기
    dept_positions = []
    for i, line in enumerate(lines):
        if re.search(dept_pattern, line):
            # "[부서명]" 다음 행에 실제 부서 이름이 있음
            if i + 1 < len(lines):
                dept_name = lines[i + 1].strip()
                dept_positions.append((i + 1, dept_name))  # (부서명 위치, 부서명)
                logger.debug(f"부서 블록 발견: '{dept_name}' (라인 {i+1})")
    
    # 각 부서 블록에서 물품코드 추출
    for i in range(len(dept_positions)):
        start_idx = dept_positions[i][0] + 1  # 부서명 다음 행부터 시작
        end_idx = dept_positions[i+1][0] if i+1 < len(dept_positions) else len(lines)
        dept_name = dept_positions[i][1]
        
        # 해당 부서의 텍스트 블록
        dept_block = lines[start_idx:end_idx]
        logger.debug(f"부서 '{dept_name}' 블록 검사 중 (라인 {start_idx}부터 {end_idx}까지)")
        
        # 부서 블록 내에서 물품코드 추출
        for line_num, line in enumerate(dept_block, start=start_idx):
            match = item_pattern.search(line)
            if match:
                item_code = match.group(0).strip()
                items.append((item_code, dept_name))
                logger.debug(f"물품코드 발견: '{item_code}' (라인 {line_num}): '{line.strip()}'")
            else:
                # 디버깅을 위해 L로 시작하는 모든 텍스트 로깅
                if 'L' in line:
                    logger.debug(f"L 포함된 라인 발견 (물품코드 아님): '{line.strip()}' (라인 {line_num})")
    
    logger.info(f"OCR 텍스트에서 {len(items)}개의 물품코드 추출 완료.")
    return items

def standardize_date(date_str):
    """다양한 형식의 날짜 문자열을 YYYY-MM-DD로 표준화합니다."""
    year = now.year # 기본 연도는 현재 연도
    match_ymd = re.match(r'(\d{4})[.-]?(\d{1,2})[.-]?(\d{1,2})', str(date_str).strip())
    if match_ymd:
        y, m, d = map(int, match_ymd.groups())
        try: return datetime(y, m, d).strftime('%Y-%m-%d')
        except ValueError: pass
    match_md = re.match(r'(\d{1,2})[.-](\d{1,2})\.?$', str(date_str).strip())
    if match_md:
        m, d = map(int, match_md.groups())
        try: return datetime(year, m, d).strftime('%Y-%m-%d')
        except ValueError: pass
    logger.warning(f"날짜 형식 인식 불가: {date_str}")
    return str(date_str).strip()

def extract_departments_with_pages(ocr_text):
    """
    OCR 텍스트에서 '[부서명]' 패턴을 찾고, 그 다음 행에 있는 실제 부서명을 추출하여
    해당 부서명이 등장하는 페이지 번호와 함께 목록으로 반환합니다.
    
    각 페이지의 OCR 텍스트를 순회하며 부서명과 페이지 번호 쌍의 목록을 생성합니다.
    여러 페이지에 동일한 부서가 있는 경우, 해당 부서는 여러 쌍으로 반환됩니다.
    """
    departments = []
    departments_found = set()  # 추출된 부서 중복 체크
    
    # "[부서명]" 정확한 단어를 찾는 패턴
    dept_pattern = r'\[부서명\]'
    
    for page_idx, page_text in enumerate(ocr_text, 1):  # 1-based page numbering
        lines = page_text.split('\n')
        for i, line in enumerate(lines):
            if re.search(dept_pattern, line):
                # "[부서명]" 다음 행에 실제 부서 이름이 있음
                if i + 1 < len(lines):
                    dept_name = lines[i + 1].strip()
                    if dept_name and dept_name not in departments_found:
                        departments_found.add(dept_name)
                        departments.append((dept_name, page_idx))
                        logger.debug(f"부서명 추출 성공: 부서='{dept_name}', 페이지={page_idx}")
    
    # 부서별로 페이지 번호 그룹화 (하나의 부서가 여러 페이지에 걸쳐 있는 경우)
    dept_page_dict = {}
    for dept, page in departments:
        if dept not in dept_page_dict:
            dept_page_dict[dept] = []
        # 이미 리스트에 있는 페이지는 추가하지 않음 (중복 방지)
        if page not in dept_page_dict[dept]:
            dept_page_dict[dept].append(page)
    
    # (부서명, 페이지번호) 형태의 튜플 목록으로 변환
    # 페이지 번호가 여러 개인 경우 각각 별도 튜플로 반환
    result = []
    for dept, pages in dept_page_dict.items():
        # 페이지 오름차순 정렬
        pages.sort()
        for page in pages:
            result.append((dept, page))
    
    return result

def extract_items_for_department(ocr_text_list, page_nums):
    """
    특정 부서의 페이지에서 OCR 텍스트를 기반으로 품목 리스트를 추출합니다.
    부서명 레이블 이후부터 다음 부서명 레이블 전까지의 구간에서,
    물품 코드는 L로 시작하는 6자리 숫자, 바로 다음 라인이 품목명이라 가정합니다.
    """
    items = []
    code_pattern = re.compile(r'^L\d{6}$')
    
    # page_nums가 단일 정수인 경우 리스트로 변환
    if isinstance(page_nums, int):
        page_nums = [page_nums]
    
    for page_num in page_nums:
        if not (1 <= page_num <= len(ocr_text_list)):
            logging.warning(f"페이지 번호 {page_num}가 OCR 결과 범위를 벗어났습니다.")
            continue
        page_text = ocr_text_list[page_num - 1]
        lines = [line.strip() for line in page_text.split('\n')]
        # 부서명 레이블 위치 찾기
        dept_positions = [i for i, line in enumerate(lines) if line == '부서명']
        if not dept_positions:
            logging.debug(f"페이지 {page_num}에서 '부서명' 레이블을 찾지 못했습니다.")
            continue
        for start_idx in dept_positions:
            # 다음 '부서명' 위치 또는 페이지 끝까지
            next_positions = [pos for pos in dept_positions if pos > start_idx]
            end_idx = next_positions[0] if next_positions else len(lines)
            block = lines[start_idx + 1:end_idx]
            for idx, line in enumerate(block):
                if code_pattern.match(line):
                    code = line
                    name = block[idx + 1] if idx + 1 < len(block) else ''
                    if name:
                        items.append((code, name))
    return items

# 새로 추가: 물품코드-물품명 매핑 DB 로드 함수
def load_item_db(file_path):
    """
    물품코드-물품명 매핑 DB를 로드하여 dict로 반환합니다.
    DB 엑셀 파일의 첫 두 열을 코드와 이름 순으로 가정합니다.
    """
    try:
        df = pd.read_excel(file_path, header=None, usecols=[0,1], names=['code','name'])
        df['code'] = df['code'].astype(str).str.strip()
        df['name'] = df['name'].astype(str).str.strip()
        return dict(zip(df['code'], df['name']))
    except Exception as e:
        logger.error(f"물품 DB 로드 오류: {e}")
        return {}

def compare_items(excel_df, ocr_codes, item_db):
    """
    Excel DataFrame(부서별)와 OCR 코드 리스트, item_db(mapping code->name)으로
    common, excel_only, ocr_only 품목 튜플 리스트를 반환합니다.
    """
    # DB 역매핑: 이름->코드
    inv_db = {name: code for code, name in item_db.items()}
    # Excel에서 품목명 목록 추출
    if '물품명' not in excel_df.columns:
        return { 'status': 'error', 'message': "Excel 데이터에 '물품명' 컬럼이 없습니다." }
    excel_names = excel_df['물품명'].astype(str).str.strip().unique()
    excel_codes = set()
    for name in excel_names:
        code = inv_db.get(name)
        if code:
            excel_codes.add(code)
        else:
            logger.warning(f"Excel 품목명 매핑 실패: '{name}'")
    ocr_codes_set = set(ocr_codes)
    # 교집합 및 차집합
    common_codes = excel_codes & ocr_codes_set
    excel_only_codes = excel_codes - ocr_codes_set
    ocr_only_codes = ocr_codes_set - excel_codes
    # 코드-이름 튜플 생성
    common = [(code, item_db.get(code, '')) for code in sorted(common_codes)]
    excel_only = [(code, item_db.get(code, '')) for code in sorted(excel_only_codes)]
    ocr_only = [(code, item_db.get(code, '')) for code in sorted(ocr_only_codes)]
    return { 'status': 'success', 'data': { 'common': common, 'excel_only': excel_only, 'ocr_only': ocr_only } }

def aggregate_ocr_results_by_department(ocr_text, departments_with_pages):
    """
    OCR 텍스트에서 부서별 물품코드를 집계합니다.
    각 (부서명, 페이지번호) 쌍을 처리하여 페이지별 부서 블록에서 L+6자리 코드를 추출하고,
    동일 부서가 여러 페이지에 걸쳐 있는 경우 중복을 제거하여 하나의 리스트로 통합합니다.
    """
    try:
        dept_blocks = {}
        item_pattern = re.compile(r'L\d{6}')
        # 각 (부서명, 페이지번호) 쌍 처리
        for dept_info in departments_with_pages:
            if isinstance(dept_info, (tuple, list)) and len(dept_info) == 2:
                dept_name, page_num = dept_info[0], dept_info[1]
            elif isinstance(dept_info, dict) and 'department' in dept_info and 'page' in dept_info:
                dept_name, page_num = dept_info['department'], dept_info['page']
            else:
                logger.warning(f"지원하지 않는 부서 정보 형식: {dept_info}")
                continue

            if page_num < 1 or page_num > len(ocr_text):
                logger.warning(f"페이지 번호 {page_num}가 OCR 결과 범위를 벗어났습니다.")
                continue
            page_lines = [line.strip() for line in ocr_text[page_num - 1].split('\n')]

            # 동일 페이지에 등장하는 모든 부서명 목록 수집
            page_depts = []
            for di in departments_with_pages:
                if isinstance(di, (tuple, list)) and len(di) == 2 and di[1] == page_num:
                    page_depts.append(di[0])
                elif isinstance(di, dict) and di.get('page') == page_num:
                    page_depts.append(di.get('department'))
            page_depts = set(page_depts)
            # 페이지 내 부서명 위치들
            page_dept_positions = sorted([idx for idx, line in enumerate(page_lines) if line in page_depts])
            # 현재 부서명 위치만 필터링하여 블록 시작점 구하기
            block_starts = [pos for pos in page_dept_positions if page_lines[pos] == dept_name]
            if not block_starts:
                logger.debug(f"페이지 {page_num}에서 부서 '{dept_name}' 블록을 찾지 못했습니다.")
                continue
            # 각 블록별 라인 추출 (다음 부서명 등장 위치 전까지)
            for start in block_starts:
                # 다음 부서명 위치 찾기
                next_positions = [pos for pos in page_dept_positions if pos > start]
                end_idx = next_positions[0] if next_positions else len(page_lines)
                # 부서명 다음 라인부터 end_idx 전까지 블록으로 간주
                block = page_lines[start+1:end_idx]
                dept_blocks.setdefault(dept_name, []).extend(block)
                logger.debug(f"부서 '{dept_name}' 페이지 {page_num} 블록 라인 수: {len(block)}")

        aggregated = {}
        for dept_name, lines in dept_blocks.items():
            codes = set()
            for line in lines:
                for match in item_pattern.findall(line):
                    codes.add(match)
            aggregated[dept_name] = {
                'items': sorted(codes),
                'page_range': None,
                'total_items': len(codes)
            }
            logger.info(f"부서 '{dept_name}'에서 총 {len(codes)}개의 중복 제거된 물품코드 추출 완료.")

        return {
            'status': 'success',
            'data': aggregated
        }
    except Exception as e:
        logger.error(f"부서별 OCR 결과 집계 중 오류 발생: {e}")
        return {
            'status': 'error',
            'message': str(e)
        } 