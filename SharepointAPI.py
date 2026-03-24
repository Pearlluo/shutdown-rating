import requests
import pandas as pd
from dotenv import load_dotenv
import os
import re

env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SHAREPOINT_HOST = os.getenv("SHAREPOINT_HOST")
SITE_NAME = os.getenv("SITE_NAME")
ROSTER_LIST_NAME = os.getenv("ROSTER_LIST_NAME")
RANKING_LIST_NAME = os.getenv("RANKING_LIST_NAME")

print("TENANT_ID =", TENANT_ID)
print("CLIENT_ID =", CLIENT_ID)
print("SITE_NAME =", SITE_NAME)
print("ROSTER_LIST_NAME =", ROSTER_LIST_NAME)
print("RANKING_LIST_NAME =", RANKING_LIST_NAME)


# ==============================
# 1. 获取 Token
# ==============================
def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }

    res = requests.post(url, data=data)
    res.raise_for_status()
    return res.json()["access_token"]


# ==============================
# 2. 获取 Site ID
# ==============================
def get_site_id(token):
    url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_HOST}:/sites/{SITE_NAME}"
    headers = {"Authorization": f"Bearer {token}"}

    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return res.json()["id"]


# ==============================
# 3. 获取 List ID
# ==============================
def get_list_id(token, site_id, list_name):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    headers = {"Authorization": f"Bearer {token}"}

    res = requests.get(url, headers=headers)
    res.raise_for_status()

    for item in res.json()["value"]:
        if item.get("name") == list_name:
            return item["id"]

    raise Exception(f"List not found: {list_name}")


# ==============================
# 4. 获取所有数据（分页）
# ==============================
def get_all_items(token, site_id, list_id):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$top=5000"

    all_items = []

    while url:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        all_items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return all_items


# ==============================
# 5. 展平所有字段
# ==============================
def flatten_items(items):
    rows = []

    for item in items:
        fields = item.get("fields", {}).copy()

        row = {
            "ItemID": item.get("id"),
            "CreatedDateTime": item.get("createdDateTime"),
            "LastModifiedDateTime": item.get("lastModifiedDateTime"),
            "WebUrl": item.get("webUrl")
        }

        for k, v in fields.items():
            row[k] = v

        rows.append(row)

    return pd.DataFrame(rows)


# ==============================
# 6. 读取 roster dataframe（调试用）
# ==============================
def get_roster_dataframe():
    token = get_token()
    site_id = get_site_id(token)
    list_id = get_list_id(token, site_id, ROSTER_LIST_NAME)

    items = get_all_items(token, site_id, list_id)
    df = flatten_items(items)

    return df


# ==============================
# 7. 判断是否有评分
# ==============================
def has_ranking_data(row):
    fields = [
        "Safety",
        "Punctuality",
        "Comms",
        "Conduct",
        "Teamwork",
        "Skills",
        "Drive"
    ]

    for field in fields:
        value = row.get(field, "")
        if value is not None and str(value).strip() != "":
            return True

    return False


# ==============================
# 8. 拼接 RankingData
# 保留空位，确保顺序固定
# ==============================
def build_ranking_data(row):
    fields = [
        "Safety",
        "Punctuality",
        "Comms",
        "Conduct",
        "Teamwork",
        "Skills",
        "Drive"
    ]

    values = []

    for field in fields:
        value = row.get(field, "")
        value_str = str(value).strip() if value is not None else ""
        values.append(value_str)

    return ",".join(values)


# ==============================
# 9. 从源 list 构建 lookup 映射
# 显示值 -> SharePoint item id
# ==============================
def build_lookup_map(token, site_id, source_list_name, source_field_name):
    source_list_id = get_list_id(token, site_id, source_list_name)
    items = get_all_items(token, site_id, source_list_id)

    mapping = {}
    duplicates = {}

    for item in items:
        item_id = int(item["id"])
        fields = item.get("fields", {})
        raw_value = fields.get(source_field_name)

        if raw_value is None:
            continue

        key = str(raw_value).strip()
        if not key:
            continue

        if key in mapping:
            duplicates.setdefault(key, []).append(item_id)
        else:
            mapping[key] = item_id

    if duplicates:
        print(f"\n⚠ {source_list_name} 有重复键，默认取第一条:")
        for k, ids in duplicates.items():
            print(f"{k} -> first={mapping[k]}, duplicates={ids}")

    return mapping


# ==============================
# 10. 提取 Job 编码
# 例如:
# SH-26043 - MARCH FPS -> SH-26043
# FB-26010 Tooling Container -> FB-26010
# PR-25004 xxxx -> PR-25004
# ==============================
def extract_job_code(job_text: str) -> str:
    text = str(job_text).strip().upper()

    m = re.match(r"^([A-Z]{2,3}-\d+)", text)
    if m:
        return m.group(1)

    return text


# ==============================
# 11. 从 JMSJobs 构建:
# JobID -> SharePoint item id
# 例如:
# SH-26043 -> 570
# ==============================
def build_jobid_lookup_map(token, site_id, source_list_name="JMSJobs"):
    source_list_id = get_list_id(token, site_id, source_list_name)
    items = get_all_items(token, site_id, source_list_id)

    mapping = {}
    duplicates = {}

    for item in items:
        item_id = int(item["id"])
        fields = item.get("fields", {})

        raw_jobid = fields.get("JobID")
        if raw_jobid is None:
            continue

        key = str(raw_jobid).strip().upper()
        if not key:
            continue

        if key in mapping:
            duplicates.setdefault(key, []).append(item_id)
        else:
            mapping[key] = item_id

    if duplicates:
        print(f"\n⚠ {source_list_name} 的 JobID 有重复，默认取第一条:")
        for k, ids in duplicates.items():
            print(f"{k} -> first={mapping[k]}, duplicates={ids}")

    return mapping


# ==============================
# 12. 单条写入 PPLRankingTX
# Person / Job 必须有
# Supervisor 可选，找不到就跳过，不丢数据
# ==============================
def add_ranking_item(row, token, site_id, list_id, person_map, supervisor_map, job_map):
    if not has_ranking_data(row):
        print(f"⏭ 跳过未评分: {row.get('Person', '')}")
        return None

    person_name = str(row.get("Person", "")).strip()
    supervisor_name = str(row.get("Supervisor", "")).strip()
    job_name = str(row.get("Job", "")).strip()
    comments = str(row.get("Comments", "")).strip()

    ranking_data = build_ranking_data(row)

    person_id = person_map.get(person_name)
    supervisor_id = supervisor_map.get(supervisor_name)

    # 关键：先提取 JobID，再查 JMSJobs.JobID
    job_code = extract_job_code(job_name)
    job_id = job_map.get(job_code)

    if not person_id:
        raise Exception(f"Person not found in PPLPeople.Title: {person_name}")

    if not job_id:
        raise Exception(f"Job not found in JMSJobs.JobID: {job_name} -> extracted [{job_code}]")

    fields_payload = {
        "Title": "Form Submit",
        "PersonLookupId": person_id,
        "JobLookupId": job_id,
        "RankingData": ranking_data,
        "Comments": comments
    }

    if supervisor_id:
        fields_payload["SupervisorLookupId"] = supervisor_id
    else:
        print(f"⚠ Supervisor not found, skipped: {supervisor_name}")

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "fields": fields_payload
    }

    print("\n=== INSERT PAYLOAD ===")
    print(payload)

    res = requests.post(url, headers=headers, json=payload)

    print("Status:", res.status_code)
    print("Response:", res.text)

    if res.status_code not in [200, 201]:
        raise Exception(f"Write failed: {res.text}")

    print(f"✅ 写入成功: {person_name} -> {ranking_data}")
    return res.json()


# ==============================
# 13. 批量写回 PPLRankingTX
# ==============================
def push_rows_to_ranking_list(rows):
    if not rows:
        print("没有可写入的数据")
        return []

    token = get_token()
    site_id = get_site_id(token)
    ranking_list_id = get_list_id(token, site_id, RANKING_LIST_NAME)

    # lookup 来源
    person_map = build_lookup_map(token, site_id, "PPLPeople", "Title")
    supervisor_map = build_lookup_map(token, site_id, "PPLPeople", "Title")
    job_map = build_jobid_lookup_map(token, site_id, "JMSJobs")

    print(f"PPLPeople mapping count: {len(person_map)}")
    print(f"JMSJobs JobID mapping count: {len(job_map)}")

    results = []

    for row in rows:
        result = add_ranking_item(
            row=row,
            token=token,
            site_id=site_id,
            list_id=ranking_list_id,
            person_map=person_map,
            supervisor_map=supervisor_map,
            job_map=job_map
        )
        if result is not None:
            results.append(result)

    return results


# ==============================
# 主程序（本地测试映射/写入）
# ==============================
if __name__ == "__main__":
    token = get_token()
    site_id = get_site_id(token)

    person_map = build_lookup_map(token, site_id, "PPLPeople", "Title")
    job_map = build_jobid_lookup_map(token, site_id, "JMSJobs")

    print("\n=== PERSON MAP SAMPLE ===")
    for i, (k, v) in enumerate(person_map.items()):
        print(k, "->", v)
        if i >= 9:
            break

    print("\n=== JOBID MAP SAMPLE ===")
    for i, (k, v) in enumerate(job_map.items()):
        print(k, "->", v)
        if i >= 9:
            break

