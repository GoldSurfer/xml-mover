import os
import glob
import shutil
import time

for year in range(2013,2016):
    # 소스 폴더 (XML 파일이 있는 폴더)
    src_folder = f"D:\중국번역\중국번역_{year}"
    folder_list = os.listdir(src_folder)

    count = 0
    for folder in folder_list:
        folder_path = os.path.join(src_folder, folder)
        if not os.path.isdir(folder_path):
            print(f"{folder_path} is not a directory")
            continue
        
        start_time = time.time()
        # 소스 폴더 내의 모든 XML 파일 찾기
        xml_files = glob.glob(os.path.join(folder_path, '**', '*.xml'), recursive=True)
        print(len(xml_files), time.time() - start_time)

        if not xml_files:
            print("소스 폴더에 XML 파일이 없습니다.")
        else:
            for src_file in xml_files:
                # 파일 이름 (예: 201010136102.xml)
                file_name = os.path.basename(src_file)
                # 파일명과 확장자를 분리 (예: "201010136102", ".xml")
                name_without_ext, ext = os.path.splitext(file_name)
                
                # 파일 이름에서 연도(앞 4자리)와 날짜(앞 8자리) 추출                
                subfolder = name_without_ext[:8]
                
                # 대상 디렉토리 경로 구성
                dest_dir = os.path.join(f"C:/Users/kwoor/Python/folder_arange/cn_번역", str(year), subfolder)
                
                # 대상 디렉토리 생성 (없으면)
                os.makedirs(dest_dir, exist_ok=True)
                
                # 대상 파일 경로: 파일명은 그대로 사용 (확장자는 .xml)
                dest_file = os.path.join(dest_dir, file_name)
                
                # 파일 이동
                shutil.move(src_file, dest_file)
                count += 1
                if count % 100 == 0:
                    print(f"Moved {src_file} -> {dest_file}", count)
