from flask import Flask, render_template, request, redirect, url_for, jsonify
from Datacleaning import get_cleaned_roster_dataframe
from SharepointAPI import push_rows_to_ranking_list
from datetime import datetime
import time
import threading

app = Flask(__name__)

LEADER_KEYWORDS = [
    "SUPERVISOR",
    "MOBILISATION COORDINATOR",
    "SUPERINTENDENT",
    "MANAGER",
    "HSE",
    "ADVISER",
    "ADVISOR",
    "OPERATIONS MANAGER",
    "SYSTEM & DATA ANALYST",
    "SYSTEM AND DATA ANALYST",
    "SYSTEM",
    "DATA ANALYST"
]

# ==============================
# 缓存配置
# ==============================
CACHE_SECONDS = 60

ROSTER_CACHE = {
    "df": None,
    "ts": 0
}

CACHE_LOCK = threading.Lock()


# ==============================
# 判断是否为 leader
# ==============================
def is_leader(position: str) -> bool:
    pos = str(position).upper().strip()
    return any(keyword in pos for keyword in LEADER_KEYWORDS)


# ==============================
# 清洗 roster
# Datacleaning 已经做过一轮清洗
# 这里保留网页展示所需字段
# ==============================
def prepare_roster(df):
    df = df.copy()

    if "Title" not in df.columns:
        df["Title"] = ""
    if "Position" not in df.columns:
        df["Position"] = ""
    if "Project" not in df.columns:
        df["Project"] = ""

    df["NameClean"] = df["Title"].fillna("").astype(str).str.strip()
    df["PositionClean"] = df["Position"].fillna("").astype(str).str.strip()
    df["ProjectClean"] = df["Project"].fillna("").astype(str).str.strip()

    df = df[(df["NameClean"] != "") & (df["ProjectClean"] != "")].copy()

    return df


# ==============================
# 获取缓存 roster
# 60秒内不重复拉 SharePoint
# ==============================
def get_cached_prepared_roster(force_refresh: bool = False):
    now = time.time()

    with CACHE_LOCK:
        cache_expired = (now - ROSTER_CACHE["ts"]) > CACHE_SECONDS
        cache_missing = ROSTER_CACHE["df"] is None

        if force_refresh or cache_missing or cache_expired:
            print("🔄 Refreshing roster cache from SharePoint...")
            df = prepare_roster(get_cleaned_roster_dataframe())

            ROSTER_CACHE["df"] = df
            ROSTER_CACHE["ts"] = now
        else:
            print("⚡ Using cached roster data")

        return ROSTER_CACHE["df"].copy()


# ==============================
# 清空缓存
# 提交后可强制刷新
# ==============================
def clear_roster_cache():
    with CACHE_LOCK:
        ROSTER_CACHE["df"] = None
        ROSTER_CACHE["ts"] = 0
        print("🧹 Roster cache cleared")


# ==============================
# 从 roster 按项目分 leader / worker
# ==============================
def split_project_people(df, selected_project: str):
    df = df.copy()

    if selected_project:
        df = df[df["ProjectClean"] == selected_project].copy()

    if "PositionClean" not in df.columns:
        df["PositionClean"] = ""

    df["IsLeader"] = df["PositionClean"].apply(is_leader)

    df_leaders = (
        df[df["IsLeader"]][["NameClean", "PositionClean", "ProjectClean"]]
        .drop_duplicates(subset=["NameClean"])
        .sort_values(by=["NameClean"])
        .rename(columns={
            "NameClean": "Name",
            "PositionClean": "Position",
            "ProjectClean": "Project"
        })
    )

    df_workers = (
        df[~df["IsLeader"]][["NameClean", "PositionClean", "ProjectClean"]]
        .drop_duplicates(subset=["NameClean"])
        .sort_values(by=["NameClean"])
        .rename(columns={
            "NameClean": "Name",
            "PositionClean": "Position",
            "ProjectClean": "Project"
        })
    )

    return df_leaders, df_workers


# ==============================
# 安全取值
# ==============================
def get_value(lst, i):
    if i < len(lst) and lst[i] is not None:
        return str(lst[i]).strip()
    return ""


# ==============================
# 首页
# ==============================
@app.route("/")
def home():
    df = get_cached_prepared_roster()
    projects = sorted(df["ProjectClean"].dropna().unique().tolist())

    return render_template(
        "rating.html",
        contracts=projects,
        selected_contract="",
        selected_job="",
        leaders=[],
        workers=[],
        excel_leaders=[]
    )


# ==============================
# 获取 jobs
# 当前 contract/job 实际都来自 ProjectClean
# 为兼容前端，先保留这个接口
# ==============================
@app.route("/get_jobs")
def get_jobs():
    contract = request.args.get("contract", "").strip()
    df = get_cached_prepared_roster()

    if contract:
        jobs = sorted(
            df[df["ProjectClean"] == contract]["ProjectClean"]
            .dropna()
            .unique()
            .tolist()
        )
    else:
        jobs = sorted(df["ProjectClean"].dropna().unique().tolist())

    return jsonify(jobs)


# ==============================
# 获取项目人员
# 只返回：
# 1. roster leaders
# 2. roster workers
# excel_leaders 不再从 Excel 读取，返回空列表兼容前端
# ==============================
@app.route("/get_project_data")
def get_project_data():
    project = request.args.get("project", "").strip()
    df_roster = get_cached_prepared_roster()
    df_leaders, df_workers = split_project_people(df_roster, project)

    return jsonify({
        "leaders": df_leaders.to_dict(orient="records"),
        "workers": df_workers.to_dict(orient="records"),
        "excel_leaders": []
    })


# ==============================
# 手动刷新缓存（可选）
# 以后你前端要加 refresh 按钮可以直接调这个
# ==============================
@app.route("/refresh_cache", methods=["POST"])
def refresh_cache():
    try:
        get_cached_prepared_roster(force_refresh=True)
        return jsonify({"status": "success", "message": "Roster cache refreshed"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ==============================
# 提交评分
# 只提交有评分的数据行
# ==============================
@app.route("/submit", methods=["POST"])
def submit():
    try:
        supervisor = request.form.get("supervisor", "").strip()
        contract = request.form.get("contract", "").strip()
        job = request.form.get("job", "").strip()

        names = request.form.getlist("name")
        positions = request.form.getlist("position")
        comments = request.form.getlist("comments")
        safety = request.form.getlist("safety")
        punctuality = request.form.getlist("punctuality")
        comms = request.form.getlist("comms")
        conduct = request.form.getlist("conduct")
        teamwork = request.form.getlist("teamwork")
        skills = request.form.getlist("skills")
        drive = request.form.getlist("drive")

        submitted_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        rows = []

        max_len = max(
            len(names),
            len(positions),
            len(comments),
            len(safety),
            len(punctuality),
            len(comms),
            len(conduct),
            len(teamwork),
            len(skills),
            len(drive),
            1
        )

        seen_keys = set()

        for i in range(max_len):
            person = get_value(names, i)
            position = get_value(positions, i)
            comment = get_value(comments, i)

            s1 = get_value(safety, i)
            s2 = get_value(punctuality, i)
            s3 = get_value(comms, i)
            s4 = get_value(conduct, i)
            s5 = get_value(teamwork, i)
            s6 = get_value(skills, i)
            s7 = get_value(drive, i)

            has_rating_data = any([s1, s2, s3, s4, s5, s6, s7])

            if not has_rating_data:
                continue

            if not person:
                continue

            # 本次提交内去重，避免重复写入同一行
            dedupe_key = (
                person.upper(),
                (job if job else contract).upper(),
                supervisor.upper(),
                s1, s2, s3, s4, s5, s6, s7,
                comment.strip()
            )

            if dedupe_key in seen_keys:
                continue

            seen_keys.add(dedupe_key)

            rows.append({
                "Person": person,
                "Job": job if job else contract,
                "Supervisor": supervisor,
                "Comments": comment,
                "Safety": s1,
                "Punctuality": s2,
                "Comms": s3,
                "Conduct": s4,
                "Teamwork": s5,
                "Skills": s6,
                "Drive": s7,

                # 这些字段不会存本地 Excel，只保留给调试/追踪
                "Contract": contract,
                "Position": position,
                "SubmittedAt": submitted_at
            })

        if not rows:
            print("⚠ 没有有效评分数据，未提交")
            return redirect(url_for("home"))

        print("提交 rows 数量 =", len(rows))
        for idx, r in enumerate(rows, 1):
            print(f"row {idx} = {r}")

        try:
            result = push_rows_to_ranking_list(rows)
            print("✅ 写回 PPLRankingTX 成功:", result)

            # 提交成功后清缓存，下次页面读取最新数据
            clear_roster_cache()

        except Exception as e:
            print("❌ 写回 PPLRankingTX 失败:", e)
            return f"Submit failed: {e}", 500

        return redirect(url_for("home"))

    except Exception as e:
        print("❌ submit() 失败:", e)
        return f"Submit failed: {e}", 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)