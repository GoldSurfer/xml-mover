import os
import shutil
from pathlib import Path
import threading
from multiprocessing import Pool, cpu_count, Manager
import multiprocessing as mp  # multiprocessing을 mp로 import
import time
from concurrent.futures import ThreadPoolExecutor
import hashlib
import argparse
import datetime
import queue
from tqdm import tqdm # tqdm 임포트

def get_file_hash(file_path):
    """파일의 첫 1KB만 해시하여 빠르게 비교"""
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read(1024)).hexdigest()
    except:
        return None

def process_folder(args):
    """XML 파일 정리만 담당하는 함수"""
    src_folder, dest_base, folder, existing_files, file_queue = args
    folder_path = os.path.join(src_folder, folder)
    
    if not os.path.isdir(folder_path):
        print(f"폴더가 존재하지 않음: {folder_path}")
        return 0, 0

    moved = 0
    skipped = 0
    
    # 디렉토리 생성 캐시
    created_dirs = set()
    
    try:
        print(f"폴더 처리 시작: {folder_path}")
        xml_files = []
        
        # 먼저 모든 XML 파일 목록을 수집
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.xml'):
                    xml_files.append((root, file))
        
        print(f"폴더 {folder_path}에서 {len(xml_files)}개의 XML 파일 발견")
        
        # 수집된 파일들을 처리
        for root, file in xml_files:
            try:
                src_file = os.path.join(root, file)
                if not os.path.exists(src_file):
                    print(f"파일이 존재하지 않음: {src_file}")
                    continue
                    
                name_without_ext, _ = os.path.splitext(file)
                subfolder = name_without_ext[:8]
                year_folder = subfolder[:4]
                dest_rel_path = os.path.join(year_folder, subfolder, file)
                
                # 빠른 해시 비교로 중복 체크
                src_hash = get_file_hash(src_file)
                if src_hash and src_hash in existing_files:
                    print(f"중복 파일 건너뜀: {src_file}")
                    skipped += 1
                    continue

                dest_dir = os.path.join(dest_base, year_folder, subfolder)
                dest_file = os.path.join(dest_dir, file)
                
                # 파일 이동 작업을 큐에 추가
                print(f"파일 이동 요청 추가: {src_file} -> {dest_file}")
                file_queue.put((src_file, dest_file, src_hash))
                moved += 1
                
                if moved % 1000 == 0:
                    print(f"폴더 {folder_path}에서 {moved}개 파일 이동 요청 추가됨")
                
            except Exception as e:
                print(f"파일 처리 중 오류 발생 {src_file}: {str(e)}")
                continue
                    
    except Exception as e:
        print(f"폴더 처리 중 오류 발생 {folder_path}: {str(e)}")

    print(f"폴더 {folder_path} 처리 완료: {moved}개 이동 요청, {skipped}개 건너뜀")
    return moved, skipped

def file_mover_worker(file_queue, stats, batch_size=100):
    """파일 이동 워커 스레드"""
    batch = []
    local_moved = 0 # 로컬 카운터
    local_errors = 0 # 로컬 카운터
    
    print(f"워커 스레드 시작: {threading.current_thread().name}")
    
    while True:
        try:
            # 배치 크기만큼 작업 수집
            while len(batch) < batch_size:
                try:
                    item = file_queue.get(timeout=1)
                    if item is None: # 종료 신호 확인
                        if not batch: # 배치가 비어있으면 바로 종료
                            print(f"워커 종료: {threading.current_thread().name}, 이동 {local_moved}개, 오류 {local_errors}개")
                            return
                        print(f"종료 신호 수신, 남은 배치 작업 {len(batch)}개 처리 후 종료: {threading.current_thread().name}")
                        break
                    batch.append(item)
                    if len(batch) % 1000 == 0:
                        print(f"워커 {threading.current_thread().name}: {len(batch)}개 작업 수집됨")
                except queue.Empty:
                    if batch:  # 배치에 작업이 있으면 처리
                        break
                    time.sleep(0.1)  # 큐가 비어있으면 잠시 대기
                    continue
            
            if not batch:
                if file_queue.empty():
                    print(f"워커 {threading.current_thread().name}: 큐가 비어있어 종료")
                    return
                continue
                
            # 배치 단위로 파일 이동
            for src_file, dest_file, file_hash in batch:
                try:
                    if not os.path.exists(src_file):
                        print(f"소스 파일이 존재하지 않음: {src_file}")
                        local_errors += 1
                        stats['errors'] += 1
                        continue
                        
                    if os.path.exists(dest_file):
                        print(f"대상 파일이 이미 존재함: {dest_file}")
                        stats['skipped'] += 1
                        continue
                    
                    # 대상 디렉토리 생성
                    dest_dir = os.path.dirname(dest_file)
                    os.makedirs(dest_dir, exist_ok=True)
                    
                    # 파일 이동 시도
                    print(f"파일 이동 시도: {src_file} -> {dest_file}")
                    shutil.move(src_file, dest_file)
                    
                    # 이동 후 검증
                    if os.path.exists(src_file):
                        raise OSError(f"소스 파일이 삭제되지 않음: {src_file}")
                    if not os.path.exists(dest_file):
                        raise OSError(f"대상 파일이 생성되지 않음: {dest_file}")
                    
                    local_moved += 1
                    stats['moved'] += 1
                    
                    if local_moved % 100 == 0:
                        print(f"워커 {threading.current_thread().name}: {local_moved}개 파일 이동 완료")
                    
                except Exception as e:
                    print(f"파일 이동 실패 {src_file}: {str(e)}")
                    local_errors += 1
                    stats['errors'] += 1
            
            batch.clear()
            
        except Exception as e:
            print(f"워커 오류 발생 {threading.current_thread().name}: {str(e)}")
            batch.clear()

def main():
    parser = argparse.ArgumentParser(description='XML 파일 정리 도구')
    parser.add_argument('--src', type=str, required=True,
                      help='소스 폴더 경로 (XML 파일이 있는 폴더)')
    parser.add_argument('--dest', type=str, required=True,
                      help='대상 폴더 경로 (XML 파일을 정리할 폴더)')
    args = parser.parse_args()

    if not os.path.exists(args.src):
        parser.error(f"소스 폴더가 존재하지 않습니다: {args.src}")

    if not os.path.exists(args.dest):
        try:
            os.makedirs(args.dest, exist_ok=True)
            print(f"대상 폴더 생성됨: {args.dest}")
        except Exception as e:
            parser.error(f"대상 폴더를 생성할 수 없습니다: {args.dest} - {str(e)}")

    # CPU 코어 100% 활용
    num_processes = cpu_count()
    print(f"{num_processes}개 프로세스로 작업 시작")

    print("\n=== XML 파일 정리 시작 ===")
    
    # 파일 해시 기반 중복 체크
    print("대상 폴더의 파일 해시를 계산하는 중...")
    with Manager() as manager:
        existing_files = manager.dict()
        file_queue = manager.Queue()
        stats = manager.dict({'moved': 0, 'skipped': 0, 'errors': 0})
        
        # 대상 폴더의 파일 해시를 병렬로 로드
        print("대상 폴더의 기존 파일 해시를 로드하는 중...")
        dest_xml_files = []
        for root, _, files in os.walk(args.dest):
            for file in files:
                if file.lower().endswith('.xml'):
                    dest_xml_files.append(os.path.join(root, file))

        # tqdm을 사용하여 대상 파일 해시 로드 진행 상황 표시
        with ThreadPoolExecutor(max_workers=num_processes * 2) as executor:
            pbar = tqdm(total=len(dest_xml_files), desc="대상 파일 해시 로드")
            
            def load_and_update_pbar(file_path):
                file_hash = get_file_hash(file_path)
                if file_hash:
                    existing_files[file_hash] = True
                pbar.update(1)
                return
            
            for file_path in dest_xml_files:
                executor.submit(load_and_update_pbar, file_path)
            
            # 모든 작업이 완료될 때까지 대기
            executor.shutdown(wait=True)
            pbar.close()
        
        print(f"대상 폴더에서 {len(existing_files)}개의 고유한 XML 파일 해시를 로드했습니다.")
        
        # 파일 이동을 위한 워커 스레드 시작 (tqdm 진행바를 위한 콜백 추가)
        num_workers = min(32, num_processes * 4)  # I/O 작업에 최적화된 워커 수
        workers = []
        
        # 워커 스레드 시작
        for _ in range(num_workers):
            t = threading.Thread(target=file_mover_worker, 
                               args=(file_queue, stats, 100))
            t.start()
            workers.append(t)

        # 폴더 처리
        processed_folders_in_current_run = set()  # 이미 처리한 폴더를 추적
        all_src_folders = [f for f in os.listdir(args.src)
                         if os.path.isdir(os.path.join(args.src, f))]
        total_folders_to_process = len(all_src_folders)
        
        # 전체 진행 상황을 표시할 tqdm 바 (폴더 기준)
        main_pbar = tqdm(total=total_folders_to_process, desc="전체 폴더 처리 진행", unit="폴더")
        
        # 폴더 처리를 위한 프로세스 풀 생성
        process_pool = Pool(processes=num_processes)
        
        try:
            while True:
                folders_to_process_this_batch = [f for f in os.listdir(args.src) 
                                             if os.path.isdir(os.path.join(args.src, f)) and 
                                                f not in processed_folders_in_current_run]
                
                if not folders_to_process_this_batch:
                    print("\n모든 폴더 분석 및 파일 이동 요청이 완료되었습니다. 워커 스레드 종료를 기다립니다...")
                    break
                    
                batch_size = min(20, len(folders_to_process_this_batch))  # 한 번에 처리할 폴더 수 제한
                folders_to_process_this_batch = folders_to_process_this_batch[:batch_size]
                    
                print(f"\n처리할 폴더 배치: {len(folders_to_process_this_batch)}개")
                
                organize_args = [(args.src, args.dest, folder, existing_files, file_queue) 
                               for folder in folders_to_process_this_batch]
                
                results_from_process_folder = []
                # imap_unordered 사용하여 폴더 분석 및 파일 이동 요청
                batch_pbar = tqdm(total=len(organize_args), desc="폴더 분석 및 파일 이동 요청", 
                                leave=False, unit="폴더")
                
                for res in process_pool.imap_unordered(process_folder, organize_args):
                    results_from_process_folder.append(res)
                    batch_pbar.update(1)
                
                batch_pbar.close()
                
                batch_requested_to_move = sum(moved for moved, _ in results_from_process_folder)
                batch_skipped_by_process_folder = sum(skipped for _, skipped in results_from_process_folder)

                print(f"이번 배치에서 {batch_requested_to_move}개 파일 이동 요청, {batch_skipped_by_process_folder}개 사전 건너뜀 (폴더 분석 단계)")

                for folder_name in folders_to_process_this_batch:
                    processed_folders_in_current_run.add(folder_name)
                    main_pbar.update(1)
                
                main_pbar.set_postfix_str(f"파일 이동 현황 - 이동: {stats['moved']}, 건너뜀: {stats['skipped']}, 오류: {stats['errors']}")
                
                # 각 배치 처리 후 워커 스레드에게 파일 이동 작업을 처리할 시간을 줌
                if batch_requested_to_move > 0:
                    print(f"워커 스레드가 파일 이동 작업을 처리할 시간을 줍니다... ({stats['moved']}/{batch_requested_to_move}개 처리됨)")
                    time.sleep(3)  # 3초 대기

        finally:
            # 프로세스 풀 정리
            process_pool.close()
            process_pool.join()
            # tqdm 진행바 닫기
            main_pbar.close()

        # 모든 폴더 분석이 끝나면 워커 종료 신호 전송
        print("\n파일 이동 워커 스레드에게 종료 신호 전송 중...")
        for _ in range(num_workers):
            file_queue.put(None)  # 워커에게 종료 신호 전송
        
        # 파일 이동이 완료될 때까지 충분한 시간을 기다림
        if stats['moved'] == 0 and sum(moved for moved, _ in results_from_process_folder) > 0:
            print("워커 스레드들이 파일 이동을 완료할 시간을 줍니다...")
            estimated_wait_time = min(30, sum(moved for moved, _ in results_from_process_folder) // 50000 + 1)
            for i in tqdm(range(estimated_wait_time), desc="대기 중", unit="초"):
                time.sleep(1)
                if stats['moved'] > 0:
                    print(f"파일 이동이 시작되었습니다! 현재 {stats['moved']}개 이동됨")
                    break
                # 추가: 파일 이동이 시작되지 않았을 때, 큐에 남아있는 작업 수를 확인
                if not file_queue.empty():
                    print(f"큐에 남아있는 작업 수: {file_queue.qsize()}")
        
        print("파일 이동 워커 스레드 종료 대기 중...")
        # 워커 스레드가 종료될 때까지 대기
        for t in tqdm(workers, desc="워커 스레드 종료", unit="스레드"):
            t.join()
            
        print(f"\n=== 최종 결과: 총 이동된 파일 {stats['moved']}개, 건너뛴 파일 (중복 등) {stats['skipped']}개, 오류 {stats['errors']}개 ===")

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print(f"작업 시작 시간: {start_time}")
    main()
    end_time = datetime.datetime.now()
    print(f"총 소요 시간: {end_time - start_time}")
