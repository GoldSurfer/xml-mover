import os
import shutil
import argparse
from multiprocessing import Pool, cpu_count
import datetime

def get_file_info_recursive(folder_path):
    """
    폴더 내의 모든 XML 파일에 대한 이름, 용량 정보를 딕셔너리로 반환합니다.
    {파일 이름: (전체 경로, 용량)} 형태입니다.
    """
    file_info = {}
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith('.xml'):
                file_path = os.path.join(root, file)
                try:
                    file_size = os.path.getsize(file_path)
                    file_info[file] = (file_path, file_size)
                except OSError as e:
                    print(f"Error accessing file {file_path}: {e}")
    return file_info

def find_unique_files(folder1_path, folder2_path):
    """
    두 폴더를 비교해서 두 번째 폴더에만 있는 파일들의 목록을 반환합니다.
    """
    print(f"'{folder1_path}' 폴더 스캔 중...")
    files_in_folder1 = get_file_info_recursive(folder1_path)
    print(f"'{folder2_path}' 폴더 스캔 중...")
    files_in_folder2 = get_file_info_recursive(folder2_path)
    
    print(f"첫 번째 폴더: {len(files_in_folder1)}개 XML 파일")
    print(f"두 번째 폴더: {len(files_in_folder2)}개 XML 파일")
    
    # 두 번째 폴더에만 있는 파일들 찾기
    unique_files = []
    for filename, (file_path, size2) in files_in_folder2.items():
        if filename not in files_in_folder1:
            unique_files.append(file_path)
        elif files_in_folder1[filename][1] != size2:  # 같은 이름이지만 크기가 다른 경우
            print(f"Warning: 같은 이름이지만 크기가 다른 파일 발견: {filename}")
            print(f"  첫 번째 폴더: {files_in_folder1[filename][1]} bytes")
            print(f"  두 번째 폴더: {size2} bytes")
            unique_files.append(file_path)
    
    print(f"두 번째 폴더에만 있는 파일: {len(unique_files)}개")
    return unique_files

def move_file_to_organized_structure(args):
    """
    단일 파일을 연도/날짜 구조로 이동하는 함수
    """
    src_file, dest_base = args
    
    try:
        file_name = os.path.basename(src_file)
        name_without_ext, _ = os.path.splitext(file_name)
        
        # 파일명에서 날짜 추출 (YYYYMMDD)
        if len(name_without_ext) >= 8:
            subfolder = name_without_ext[:8]  # 날짜(YYYYMMDD)
            year_folder = subfolder[:4]       # 연도(YYYY)
        else:
            # 날짜를 추출할 수 없는 경우 "unknown" 폴더에 저장
            year_folder = "unknown"
            subfolder = "unknown"
        
        # 대상 디렉토리 생성
        dest_dir = os.path.join(dest_base, year_folder, subfolder)
        os.makedirs(dest_dir, exist_ok=True)
        
        # 파일 이동
        dest_file = os.path.join(dest_dir, file_name)
        if os.path.exists(dest_file):
            print(f"이미 존재하는 파일 스킵: {file_name}")
            return 0
        
        shutil.move(src_file, dest_file)
        print(f"이동 완료: {file_name} -> {year_folder}/{subfolder}/")
        return 1
        
    except Exception as e:
        print(f"파일 이동 중 오류 발생 {src_file}: {str(e)}")
        return 0

def main():
    parser = argparse.ArgumentParser(description='두 번째 폴더에만 있는 XML 파일들을 선별적으로 정리')
    parser.add_argument('--folder1', type=str, required=True,
                      help='첫 번째 폴더 경로 (비교 기준)')
    parser.add_argument('--folder2', type=str, required=True,
                      help='두 번째 폴더 경로 (파일을 가져올 폴더)')
    parser.add_argument('--dest', type=str, required=True,
                      help='대상 폴더 경로 (정리된 파일들을 저장할 폴더)')
    args = parser.parse_args()

    # 폴더 존재 확인
    for folder_path in [args.folder1, args.folder2]:
        if not os.path.exists(folder_path):
            print(f"폴더가 존재하지 않습니다: {folder_path}")
            return

    # 대상 폴더 생성
    if not os.path.exists(args.dest):
        try:
            os.makedirs(args.dest, exist_ok=True)
            print(f"대상 폴더 생성됨: {args.dest}")
        except Exception as e:
            print(f"대상 폴더를 생성할 수 없습니다: {args.dest} - {str(e)}")
            return

    print("=== 두 폴더 비교 시작 ===")
    unique_files = find_unique_files(args.folder1, args.folder2)
    
    if not unique_files:
        print("이동할 파일이 없습니다.")
        return
    
    print(f"\n=== {len(unique_files)}개 파일 이동 시작 ===")
    
    # 멀티프로세싱으로 파일 이동
    num_processes = max(1, int(cpu_count() * 0.75))
    print(f"{num_processes}개 프로세스로 작업 시작")
    
    move_args = [(file_path, args.dest) for file_path in unique_files]
    
    with Pool(processes=num_processes) as pool:
        results = pool.map(move_file_to_organized_structure, move_args)
    
    total_moved = sum(results)
    print(f"\n=== 총 {total_moved}개 파일 이동 완료 ===")

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print(f"작업 시작 시간: {start_time}")
    main()
    end_time = datetime.datetime.now()
    print(f"총 소요 시간: {end_time - start_time}") 