import os
import glob
import shutil
import re
from multiprocessing import Pool, cpu_count
import time
import datetime
import zipfile
import argparse

def extract_zip(zip_path, extract_to):
    """zip 파일을 지정된 경로에 압축 해제"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        return True
    except Exception as e:
        print(f"압축 해제 실패 {zip_path}: {str(e)}")
        return False

def find_zip_files(folder_path):
    """폴더 내의 모든 zip 파일을 재귀적으로 찾는 함수"""
    zip_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith('.zip') and not file.startswith('._'):
                zip_files.append(os.path.join(root, file))
    return zip_files

def extract_folder(args):
    """압축 파일 해제만 담당하는 함수"""
    src_folder, dest_folder, folder = args
    folder_path = os.path.join(src_folder, folder)
    
    if not folder.endswith('.zip') or folder.startswith('._'):
        return 0

    try:
        # 압축 해제할 폴더명 생성 (원본 폴더명 사용)
        extract_to = os.path.join(dest_folder, os.path.splitext(folder)[0])
        if not os.path.exists(extract_to):
            os.makedirs(extract_to, exist_ok=True)
        if extract_zip(folder_path, extract_to):
            print(f"압축 해제 완료: {folder} -> {os.path.basename(extract_to)}")
            
            # 압축 해제된 폴더 내에서 추가 zip 파일 찾기
            nested_zips = find_zip_files(extract_to)
            if nested_zips:
                print(f"  - 추가 압축파일 {len(nested_zips)}개 발견")
                for nested_zip in nested_zips:
                    nested_folder = os.path.relpath(nested_zip, src_folder)
                    extract_folder((src_folder, dest_folder, nested_folder))
            
            return 1
    except Exception as e:
        print(f"압축 해제 중 오류 발생 {folder}: {str(e)}")
    return 0

def process_folder(args):
    """XML 파일 정리만 담당하는 함수"""
    src_folder, dest_base, folder = args
    folder_path = os.path.join(src_folder, folder)
    
    if not os.path.isdir(folder_path):
        return 0

    moved = 0
    # 재귀적으로 xml 파일 찾기 (하위 폴더까지 모두 검색, 대소문자 구분 없음)
    xml_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    
    print(f"{folder}: Found {len(xml_files)} XML files")
    
    for src_file in xml_files:
        try:
            file_name = os.path.basename(src_file)
            name_without_ext, _ = os.path.splitext(file_name)
            subfolder = name_without_ext[:8]  # 날짜(YYYYMMDD)

            dest_dir = os.path.join(dest_base, subfolder)
            os.makedirs(dest_dir, exist_ok=True)

            dest_file = os.path.join(dest_dir, file_name)
            shutil.move(src_file, dest_file)
            moved += 1
            if moved % 100 == 0:  # 100개마다 진행상황 출력
                print(f"{folder}: Moved {moved} files so far...")
        except Exception as e:
            print(f"Error moving {src_file}: {str(e)}")

    print(f"{folder}: Moved {moved} files")
    return moved

def main():
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description='XML 파일 정리 도구')
    parser.add_argument('--mode', choices=['extract', 'organize', 'both'], default='organize',
                      help='처리 모드 선택: extract(압축해제만), organize(파일정리만), both(모두 처리)')
    parser.add_argument('--src', type=str, required=True,
                      help='소스 폴더 경로 (압축파일이나 XML 파일이 있는 폴더)')
    parser.add_argument('--dest', type=str, required=False,
                      help='대상 폴더 경로 (XML 파일을 정리할 폴더, organize 모드에서만 사용)')
    args = parser.parse_args()

    # 모드별 필수 인자 체크
    if args.mode in ['organize', 'both'] and not args.dest:
        parser.error("organize 모드에서는 --dest 인자가 필요합니다.")

    # 소스 폴더 존재 확인
    if not os.path.exists(args.src):
        parser.error(f"소스 폴더가 존재하지 않습니다: {args.src}")

    # 대상 폴더가 지정된 경우 존재 확인
    if args.dest and not os.path.exists(args.dest):
        try:
            os.makedirs(args.dest, exist_ok=True)
            print(f"대상 폴더 생성됨: {args.dest}")
        except Exception as e:
            parser.error(f"대상 폴더를 생성할 수 없습니다: {args.dest} - {str(e)}")

    # 맥북 코어에 맞게 프로세스 수 자동 조정 (최대 코어 수의 75% 사용)
    num_processes = max(1, int(cpu_count() * 0.75))
    print(f"{num_processes}개 프로세스로 작업 시작")

    if args.mode in ['extract', 'both']:
        print("\n=== 압축 파일 해제 시작 ===")
        print(f"소스 폴더: {args.src}")
        if not args.dest:
            parser.error("extract 모드에서는 --dest 인자가 필요합니다.")
        # 최상위 압축 파일 찾기
        zip_files = [f for f in os.listdir(args.src) if f.endswith('.zip')]
        print(f"최상위 압축 파일 {len(zip_files)}개 발견")
        
        # 압축 해제 작업
        extract_args = [(args.src, args.dest, folder) for folder in zip_files]
        with Pool(processes=num_processes) as pool:
            extract_results = pool.map(extract_folder, extract_args)
        
        total_extracted = sum(extract_results)
        print(f"=== 총 {total_extracted}개 압축 파일 해제 완료 ===\n")

    if args.mode in ['organize', 'both']:
        print("\n=== XML 파일 정리 시작 ===")
        print(f"소스 폴더: {args.src}")
        print(f"대상 폴더: {args.dest}")
        # 압축 해제된 폴더만 찾기
        folders = [f for f in os.listdir(args.src) 
                  if os.path.isdir(os.path.join(args.src, f))]
        print(f"총 {len(folders)}개 폴더 정리 예정")
        
        # 파일 정리 작업
        organize_args = [(args.src, args.dest, folder) for folder in folders]
        with Pool(processes=num_processes) as pool:
            organize_results = pool.map(process_folder, organize_args)
        
        total_moved = sum(organize_results)
        print(f"=== 총 이동된 파일 {total_moved}개 ===\n")

    print(f"작업 완료 시간: {datetime.datetime.now()}")

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print(f"작업 시작 시간: {start_time}")
    main()
    end_time = datetime.datetime.now()
    print(f"총 소요 시간: {end_time - start_time}")