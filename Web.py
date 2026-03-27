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

RECENT_SUBMISSIONS = {}
RECENT_SUBMISSION_SECONDS = 120
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


# ⭐ 只新增这一段
@app.route("/processing")
def processing():
    return render_template("processing.html")
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
# 只提交有 overall_rating 的数据行
# ==============================
@app.route("/submit", methods=["POST"])
def submit():
    try:
        supervisor = request.form.get("supervisor", "").strip()
        contract = request.form.get("contract", "").strip()
        job = request.form.get("job", "").strip()
        submission_token = request.form.get("submission_token", "").strip()

        names = request.form.getlist("name")
        positions = request.form.getlist("position")
        comments = request.form.getlist("comments")
        overall_ratings = request.form.getlist("overall_rating")

        submitted_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        rows = []

        max_len = max(
            len(names),
            len(positions),
            len(comments),
            len(overall_ratings),
            1
        )

        selected_job = job if job else contract
        if not selected_job:
            print("⚠ 没有 job / contract，未提交")
            return jsonify({
                "status": "error",
                "message": "No job/contract selected"
            }), 400

        # 防止短时间内重复提交同一个 token
        now_ts = time.time()

        # 清理过期 token
        expired_tokens = [
            token for token, ts in RECENT_SUBMISSIONS.items()
            if (now_ts - ts) > RECENT_SUBMISSION_SECONDS
        ]
        for token in expired_tokens:
            RECENT_SUBMISSIONS.pop(token, None)

        # 如果同一个 token 已经提交过，直接拦截
        if submission_token and submission_token in RECENT_SUBMISSIONS:
            print("⚠ 重复提交已拦截:", submission_token)
            return jsonify({
                "status": "duplicate",
                "message": "This submission has already been processed."
            }), 200

        seen_keys = set()

        for i in range(max_len):
            person = get_value(names, i)
            position = get_value(positions, i)
            comment = get_value(comments, i)
            overall_rating = get_value(overall_ratings, i)

            if not person:
                continue

            if not overall_rating:
                continue

            dedupe_key = (
                person.upper(),
                selected_job.upper(),
                supervisor.upper(),
                overall_rating,
                comment.upper()
            )

            if dedupe_key in seen_keys:
                continue

            seen_keys.add(dedupe_key)

            rows.append({
                "Person": person,
                "Job": selected_job,
                "Supervisor": supervisor,
                "Comments": comment,
                "OverallRating": overall_rating,
                "Contract": contract,
                "Position": position,
                "SubmittedAt": submitted_at
            })

        if not rows:
            print("⚠ 没有有效评分数据，未提交")
            return jsonify({
                "status": "error",
                "message": "No valid rating data"
            }), 400

        print("提交 rows 数量 =", len(rows))
        for idx, r in enumerate(rows, 1):
            print(f"row {idx} = {r}")

        try:
            result = push_rows_to_ranking_list(rows)
            print("✅ 写回 PPLRankingTX 成功:", result)

            if submission_token:
                RECENT_SUBMISSIONS[submission_token] = now_ts

            clear_roster_cache()

        except Exception as e:
            print("❌ 写回 PPLRankingTX 失败:", e)
            return jsonify({
                "status": "error",
                "message": f"Submit failed: {e}"
            }), 500

        return jsonify({
            "status": "success",
            "message": "Ratings submitted successfully",
            "count": len(rows)
        })

    except Exception as e:
        print("❌ submit() 失败:", e)
        return jsonify({
            "status": "error",
            "message": f"Submit failed: {e}"
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)