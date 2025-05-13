import os
import glob
import shutil
import re
from concurrent.futures import ProcessPoolExecutor, as_completed

def collect_processed(dest_base):
    """
    주어진 dest_base(목적지 기본 경로)를 스캔하여 연도별로 이미 처리된 하위 폴더를 수집합니다.
    {연도: 하위폴더 이름 집합} 형태의 dict를 반환합니다.
    """
    processed = {}
    # dest_base 디렉터리가 없으면 빈 dict 반환
    if not os.path.isdir(dest_base):
        return processed
    # 연도별 디렉터리 순회
    for year in os.listdir(dest_base):
        year_dir = os.path.join(dest_base, year)
        if not os.path.isdir(year_dir):
            continue
        processed.setdefault(int(year), set())
        # 하위 날짜별 폴더 수집
        for sub in os.listdir(year_dir):
            processed[int(year)].add(sub)
    return processed


def process_folder(year, src_folder, dest_base, folder, processed_subs):
    """
    단일 폴더에서 XML 파일을 찾아 dest_base/year/YYYYMMDD 하위 폴더로 이동합니다.
    processed_subs에 포함된 날짜 키는 건너뜁니다.
    """
    folder_path = os.path.join(src_folder, folder)
    if not os.path.isdir(folder_path):
        return 0

    total_moved = 0
    # 재귀적으로 XML 파일 검색
    xml_files = glob.glob(os.path.join(folder_path, '**', '*.xml'), recursive=True)
    for src in xml_files:
        filename = os.path.basename(src)
        date_key = filename[:8]  # 파일명 앞 8자리(YYYYMMDD)
        # 이미 처리된 날짜면 건너뜀
        if date_key in processed_subs:
            continue
        # 연도 디렉터리와 날짜별 하위 디렉터리 생성
        year_dir = os.path.join(dest_base, str(year))
        out_dir = os.path.join(year_dir, date_key)
        os.makedirs(out_dir, exist_ok=True)
        dst = os.path.join(out_dir, filename)
        try:
            shutil.move(src, dst)
            total_moved += 1
        except Exception as e:
            print(f"{src} 이동 중 오류 발생: {e}")
    print(f"[{year}] {folder}: {total_moved}개의 파일 이동 완료")
    return total_moved


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="소스 폴더에서 연도와 날짜별로 XML 파일을 정리하여 목적지 기본 디렉터리에 이동합니다."
    )
    parser.add_argument('src_root', help="연도별 하위 폴더를 포함하는 루트 디렉터리, 예: D:/중국번역")
    parser.add_argument('dest_base', help="목적지 기본 디렉터리, 예: C:/.../cn_번역")
    parser.add_argument('--years', nargs='+', type=int, default=None,
                        help="처리할 연도 목록(예: 2021 2022). 지정하지 않으면 src_root 아래 모든 연도 폴더를 처리합니다.")
    parser.add_argument('--workers', type=int, default=os.cpu_count(),
                        help="병렬 프로세스 수를 지정합니다.")
    args = parser.parse_args()

    # 처리할 연도 결정
    if args.years:
        years = args.years
    else:
        years = [int(n) for n in os.listdir(args.src_root) if n.isdigit()]

    # 이미 처리된 날짜별 폴더 수집
    processed = collect_processed(args.dest_base)

    total_all = 0
    for year in years:
        src_folder = os.path.join(args.src_root, f"중국번역_{year}")
        if not os.path.isdir(src_folder):
            print(f"{year}년 건너뜀: 소스 폴더를 찾을 수 없음: {src_folder}")
            continue

        # 해당 연도 소스 폴더 내 하위 디렉터리 목록 가져오기
        folders = [d for d in os.listdir(src_folder)
                   if os.path.isdir(os.path.join(src_folder, d))]
        # 이미 처리된 날짜 키
        subs_done = processed.get(year, set())

        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(process_folder, year, src_folder,
                            args.dest_base, f, subs_done): f for f in folders
            }
            for fut in as_completed(futures):
                moved = fut.result()
                total_all += moved

        print(f"== {year}년: 총 이동된 파일 {total_all}개 ==")

if __name__ == '__main__':
    main()
