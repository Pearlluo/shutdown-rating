import pandas as pd
from SharepointAPI import get_roster_dataframe

# ==============================
# Leader 关键词
# ==============================
LEADER_KEYWORDS = [
    "SUPERVISOR",
    "MOBILISATION COORDINATOR",
    "SUPERINTENDENT",
    "MANAGER",
    "HSE",
    "ADVISER",
    "ADVISOR",
    "SYSTEM",
    "DATA",
    "OPERATIONS"
]


def is_leader(position: str) -> bool:
    pos = str(position).upper().strip()
    return any(k in pos for k in LEADER_KEYWORDS)


def get_cleaned_roster_dataframe() -> pd.DataFrame:
    """
    直接从 SharePoint 读取 PPLRosters，
    按原有逻辑清洗后返回 DataFrame
    """
    df = get_roster_dataframe()
    df = df.copy()

    # 去掉列名前后空格
    df.columns = df.columns.str.strip()

    # 确保关键列存在
    required_cols = ["Title", "Position", "WorkType", "Project"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    # 只保留需要的列
    keep_cols = ["Title", "Position", "WorkType", "Project"]
    df = df[keep_cols].copy()

    # 所有保留列先转字符串并去空格
    for col in keep_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    # 只保留 SH- 开头的项目
    if "Project" in df.columns:
        df = df[df["Project"].str.upper().str.match(r"^SH-\d+")].copy()

    # 删除车辆（Z. 开头）
    if "Position" in df.columns:
        df = df[~df["Position"].str.upper().str.startswith("Z.")].copy()

    # 删除空数据
    if "Title" in df.columns:
        df = df[df["Title"] != ""].copy()

    if "Project" in df.columns:
        df = df[df["Project"] != ""].copy()

    # 去重：同一个人 + 同一个项目 + 同岗位 + 同工种 只保留一条
    dedupe_cols = [c for c in ["Title", "Project", "Position", "WorkType"] if c in df.columns]
    if dedupe_cols:
        df = df.drop_duplicates(subset=dedupe_cols).reset_index(drop=True)

    return df


def split_leaders_workers(df: pd.DataFrame):
    """
    按 Position 判断 leader / worker
    """
    df = df.copy()

    if "Position" not in df.columns:
        df["Position"] = ""

    df["IsLeader"] = df["Position"].apply(is_leader)

    df_leader = df[df["IsLeader"]].drop(columns=["IsLeader"]).reset_index(drop=True)
    df_worker = df[~df["IsLeader"]].drop(columns=["IsLeader"]).reset_index(drop=True)

    return df_worker, df_leader


def get_all_cleaned_data():
    """
    返回：
    - 总清洗数据
    - worker 数据
    - leader 数据
    """
    df = get_cleaned_roster_dataframe()
    df_worker, df_leader = split_leaders_workers(df)
    return df, df_worker, df_leader


if __name__ == "__main__":
    df, df_worker, df_leader = get_all_cleaned_data()

    print("Done!")
    print(f"Total Cleaned: {len(df)}")
    print(f"Workers: {len(df_worker)}")
    print(f"Leaders: {len(df_leader)}")

    print("\n=== Sample Leaders ===")
    if not df_leader.empty:
        print(df_leader.head(20).to_string(index=False))
    else:
        print("No leaders found.")