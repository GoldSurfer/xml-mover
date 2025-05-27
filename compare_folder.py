import os
import collections

def get_file_info_recursive(folder_path):
    """
    폴더 내의 모든 파일(하위 폴더 포함)에 대한 이름, 용량 정보를 딕셔너리로 반환합니다.
    {파일 이름: 용량 (바이트)} 형태입니다.
    만약 같은 이름의 파일이 여러 하위 폴더에 있다면, 마지막으로 스캔된 파일의 정보가 저장됩니다.
    """
    file_info = {}
    for root, _, files in os.walk(folder_path): # os.walk를 사용하여 하위 폴더까지 탐색
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_size = os.path.getsize(file_path)
                file_info[file] = file_size # 파일 이름만 키로 사용
            except OSError as e:
                print(f"Error accessing file {file_path}: {e}")
    return file_info

# 비교할 두 폴더 경로 (여기 경로가 정확한지 다시 한번 확인해주세요!)
folder1_path = r"C:\Users\kwoor\Python\folder_arange\CN_번역\2021"
folder2_path = r"C:\Users\kwoor\Python\folder_arange\CN_번역\2021_zip_extract_2"

print(f"'{folder1_path}' 폴더 스캔 중...")
files_in_folder1 = get_file_info_recursive(folder1_path)
print(f"'{folder2_path}' 폴더 스캔 중...")
files_in_folder2 = get_file_info_recursive(folder2_path)

print("\n--- 파일 개수 비교 ---")
print(f"'{folder1_path}' 파일 개수: {len(files_in_folder1)}개")
print(f"'{folder2_path}' 파일 개수: {len(files_in_folder2)}개")

if len(files_in_folder1) == len(files_in_folder2):
    print("-> 파일 개수가 동일합니다.")
else:
    print("-> 파일 개수가 다릅니다.")

print("\n--- 파일 이름 및 용량 비교 ---")

# 두 폴더 모두에 있는 파일 중 이름과 용량이 일치하는 파일
matching_files = {}
for filename, size1 in files_in_folder1.items():
    if filename in files_in_folder2 and files_in_folder2[filename] == size1:
        matching_files[filename] = size1

# 첫 번째 폴더에만 있거나 용량이 다른 파일
unique_to_folder1_or_diff_size = {}
for filename, size1 in files_in_folder1.items():
    if filename not in files_in_folder2:
        unique_to_folder1_or_diff_size[filename] = f"첫 번째 폴더에만 있음 (용량: {size1} bytes)"
    elif files_in_folder2[filename] != size1:
        unique_to_folder1_or_diff_size[filename] = f"용량 불일치 (첫 번째: {size1} bytes, 두 번째: {files_in_folder2[filename]} bytes)"

# 두 번째 폴더에만 있는 파일
unique_to_folder2 = {}
for filename, size2 in files_in_folder2.items():
    if filename not in files_in_folder1:
        unique_to_folder2[filename] = f"두 번째 폴더에만 있음 (용량: {size2} bytes)"

print(f"\n[이름과 용량이 모두 일치하는 파일: {len(matching_files)}개]")
if matching_files:
    count = 0
    for filename in sorted(matching_files.keys()):
        print(f"- {filename} ({matching_files[filename]} bytes)")
        count += 1
        if count >= 10 and len(matching_files) > 10:
            print(f"... 외 {len(matching_files) - 10}개")
            break
else:
    print("  없음")

print(f"\n[첫 번째 폴더에만 있거나, 두 번째 폴더와 용량이 다른 파일: {len(unique_to_folder1_or_diff_size)}개]")
if unique_to_folder1_or_diff_size:
    for filename in sorted(unique_to_folder1_or_diff_size.keys()):
        print(f"- {filename}: {unique_to_folder1_or_diff_size[filename]}")
else:
    print("  없음")

print(f"\n[두 번째 폴더에만 있는 파일: {len(unique_to_folder2)}개]")
if unique_to_folder2:
    for filename in sorted(unique_to_folder2.keys()):
        print(f"- {filename}: {unique_to_folder2[filename]}")
else:
    print("  없음")

print("\n--- 최종 결과 ---")
if len(unique_to_folder1_or_diff_size) == 0 and len(unique_to_folder2) == 0:
    print("두 폴더의 **모든 파일**이 이름과 용량 기준으로 완벽하게 일치합니다.")
else:
    print("두 폴더의 파일들이 이름 또는 용량 기준으로 **일치하지 않는 부분**이 있습니다.")
    print("위의 상세 비교 결과를 참고하세요.")