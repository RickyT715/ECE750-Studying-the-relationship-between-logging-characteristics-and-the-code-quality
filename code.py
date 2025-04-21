#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
logging_metrics_gitpython.py
一次分析单仓库（指定分支），统计（均基于每个 .java 文件）：
  • 产品度量  : LOC, NUM_LOGS, LOGD, LEVELD, AVG_LOG_LEN, LOG_SPREAD（基于 --end 日期快照）
  • 过程度量  : LOGADD, LOGDEL, FCOC, LOG_CODE_OWNERSHIP, CODE_CHURN（基于 --since…--end 提交）
  • 缺陷检测  :
      – pre-release: 在 --since…--end 范围内检测 HADOOP-????，关联到文件
      – post-release: 在 --defectstart…--defectend 范围内检测 HADOOP-????，关联到文件
并将每个文件的详情（包括 pre/post 缺陷列表及计数、TPC）输出到 JSON，同时生成对应 CSV（文件名自动基于仓库名和分支）
依赖: gitpython, tqdm
"""

import argparse
import os
import re
import json
import statistics
import datetime
import csv
import ast
from collections import Counter, defaultdict
from tqdm import tqdm
from git import Repo
from urllib.parse import urlparse

LOG_CALL_RE = re.compile(r'\bLOG\.(trace|debug|info|warn|error|fatal)\s*\(', re.I)
LEVEL_NUM   = {'trace':1,'debug':2,'info':3,'warn':4,'error':5,'fatal':6}
BUGFIX_RE   = re.compile(r'\b(fix(e[sd])?|bug|defect|issue|patch)\b', re.I)
DEFECT_RE   = re.compile(r'HADOOP-\d{4}')


def is_log(line):
    return bool(LOG_CALL_RE.search(line))


def get_level(line):
    m = LOG_CALL_RE.search(line)
    return LEVEL_NUM[m.group(1).lower()] if m else None


def parse_head_files(repo, commit):
    total_files = files_with_log = 0
    spread_num = spread_den = 0
    file_metrics = {}
    method_pat = re.compile(r'\b(public|protected|private)\s+\w+\s+\w+\s*\([^)]*\)\s*\{')

    for blob in commit.tree.traverse():
        if blob.type != 'blob' or not blob.path.endswith('.java'):
            continue
        src = blob.data_stream.read().decode(errors='ignore')
        lines = src.splitlines()
        loc = len(lines)
        num_logs = sum(is_log(ln) for ln in lines)
        total_files += 1
        if num_logs:
            files_with_log += 1
        levels = [get_level(ln) for ln in lines if get_level(ln)]
        msg_lens = [
            len(m.group(1))
            for ln in lines if is_log(ln)
            for m in [re.search(r'"(.*?)"', ln)] if m
        ]
        starts = [m.start() for m in method_pat.finditer(src)]
        total_meth = len(starts)
        spread_den += total_meth
        file_spread = 0
        if total_meth:
            boundaries = starts + [len(src)]
            meth_with_log = sum(1 for i in range(len(starts)) if LOG_CALL_RE.search(src[boundaries[i]:boundaries[i+1]]))
            file_spread = meth_with_log / total_meth
            spread_num += meth_with_log
        file_metrics[blob.path] = {
            "LOC": loc,
            "NUM_LOGS": num_logs,
            "LOGD": num_logs/loc if loc else 0,
            "LEVELD": statistics.mean(levels) if levels else 0,
            "AVG_LOG_LEN": statistics.mean(msg_lens) if msg_lens else 0,
            "LOG_SPREAD": file_spread,
        }
    proj_prod_summary = {
        "LOGD_mean": statistics.mean(f["LOGD"] for f in file_metrics.values()) if file_metrics else 0,
        "LEVELD_mean": statistics.mean(f["LEVELD"] for f in file_metrics.values()) if file_metrics else 0,
        "AVG_LOG_LEN": statistics.mean(f["AVG_LOG_LEN"] for f in file_metrics.values()) if file_metrics else 0,
        "LOG_SPREAD": (spread_num/spread_den) if spread_den else 0,
        "TOTAL_FILES": total_files,
        "FILES_WITH_LOG": files_with_log,
    }
    return file_metrics, proj_prod_summary


def analyse_process(repo, branch, since, until=None):
    added = Counter(); deleted = Counter()
    code_add = Counter(); code_del = Counter()
    tpc = Counter(); authors = set(); churn_auth = set()
    authors_file = defaultdict(set); churn_auth_file = defaultdict(set)
    bugfix_churn_file = Counter(); bugfix_count = 0; commit_count = 0
    kwargs = {"since": since.isoformat(), "no_merges": True}
    if until:
        kwargs["until"] = until.isoformat()
    for c in tqdm(repo.iter_commits(branch, **kwargs), desc="processing commits"):
        commit_count += 1
        author = c.author.email; authors.add(author)
        is_bug = bool(BUGFIX_RE.search(c.message))
        for diff in c.diff(c.parents[0] if c.parents else None, create_patch=True):
            path = diff.b_path or diff.a_path
            if not path or not path.endswith('.java'): continue
            tpc[path] += 1; authors_file[path].add(author)
            dl = diff.diff.decode(errors='ignore').splitlines()
            adds = [l[1:] for l in dl if l.startswith('+') and not l.startswith('+++')]
            dels = [l[1:] for l in dl if l.startswith('-') and not l.startswith('---')]
            code_add[path] += len(adds); code_del[path] += len(dels)
            al = sum(is_log(l) for l in adds); dl = sum(is_log(l) for l in dels)
            added[path] += al; deleted[path] += dl
            if al or dl:
                churn_auth_file[path].add(author)
                if is_bug: bugfix_churn_file[path] += 1
        if any(added[p] or deleted[p] for p in tpc): churn_auth.add(author)
        if is_bug: bugfix_count += 1
    proj_proc_summary = {
        "LOGADD": statistics.mean(added[p]/tpc[p] for p in tpc) if tpc else 0,
        "LOGDEL": statistics.mean(deleted[p]/tpc[p] for p in tpc) if tpc else 0,
        "FCOC": bugfix_count/commit_count if commit_count else 0,
        "LOG_CODE_OWNERSHIP": len(churn_auth)/len(authors) if authors else 0,
        "log_churn_total": sum(added.values()) + sum(deleted.values()),
        "code_churn_total": sum(code_add.values()) + sum(code_del.values()),
    }
    proc_file_metrics = {}
    for path in tpc:
        commits_on = tpc[path]
        proc_file_metrics[path] = {
            "LOGADD": added[path]/commits_on if commits_on else 0,
            "LOGDEL": deleted[path]/commits_on if commits_on else 0,
            "FCOC": bugfix_churn_file[path]/commits_on if commits_on else 0,
            "LOG_CODE_OWNERSHIP": len(churn_auth_file[path])/len(authors_file[path]) if authors_file[path] else 0,
            "CODE_CHURN": code_add[path] + code_del[path],
        }
    return proc_file_metrics, proj_proc_summary, tpc


def detect_defects(repo, branch, dsince, duntil):
    file_def = defaultdict(set)
    kwargs = {"since": dsince.isoformat(), "no_merges": True}
    if duntil: kwargs["until"] = duntil.isoformat()
    for c in tqdm(repo.iter_commits(branch, **kwargs), desc="detecting defects"):
        defects = set(DEFECT_RE.findall(c.message))
        if not defects: continue
        for diff in c.diff(c.parents[0] if c.parents else None, create_patch=True):
            p = diff.b_path or diff.a_path
            if p and p.endswith('.java'): file_def[p].update(defects)
    return {p: sorted(v) for p, v in file_def.items()}


def make_filenames(repo_url, branch):
    if repo_url.startswith('http'):
        name = os.path.basename(urlparse(repo_url).path.rstrip('/'))
    else:
        name = os.path.basename(repo_url.rstrip('/'))
    ver = branch.replace('branch-', '')
    base = f"{name}-{ver}"
    return f"metric-{base}.json", f"{base}.csv"


def compute_summary_from_csv(csv_path):
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    total_files = len(rows)
    sum_LOGD = sum(float(r["LOGD"]) for r in rows)
    sum_LEVELD = sum(float(r["LEVELD"]) for r in rows)
    sum_AVG_LOG_LEN = sum(float(r["AVG_LOG_LEN"]) for r in rows)
    sum_LOG_SPREAD = sum(float(r["LOG_SPREAD"]) for r in rows)
    files_with_log = sum(1 for r in rows if int(r["NUM_LOGS"]) > 0)
    sum_LOGADD = sum(float(r["LOGADD"]) for r in rows)
    sum_LOGDEL = sum(float(r["LOGDEL"]) for r in rows)
    sum_FCOC = sum(float(r["FCOC"]) for r in rows)
    sum_LOG_CODE_OWNERSHIP = sum(float(r["LOG_CODE_OWNERSHIP"]) for r in rows)
    total_code_churn = sum(int(r["CODE_CHURN"]) for r in rows)
    total_log_churn = sum((float(r["LOGADD"]) + float(r["LOGDEL"])) * int(r["TPC"]) for r in rows)
    pre_sets = set(); post_sets = set(); files_with_pre = files_with_post = 0
    for r in rows:
        pre_list = ast.literal_eval(r["pre_defects"])
        post_list = ast.literal_eval(r["post_defects"])
        if pre_list: files_with_pre += 1
        if post_list: files_with_post += 1
        pre_sets.update(pre_list)
        post_sets.update(post_list)
    return {
        "LOGD_mean": (sum_LOGD / total_files) if total_files else 0,
        "LEVELD_mean": (sum_LEVELD / total_files) if total_files else 0,
        "AVG_LOG_LEN": (sum_AVG_LOG_LEN / total_files) if total_files else 0,
        "LOG_SPREAD": (sum_LOG_SPREAD / total_files) if total_files else 0,
        "TOTAL_FILES": total_files,
        "FILES_WITH_LOG": files_with_log,
        "LOGADD": (sum_LOGADD / total_files) if total_files else 0,
        "LOGDEL": (sum_LOGDEL / total_files) if total_files else 0,
        "FCOC": (sum_FCOC / total_files) if total_files else 0,
        "LOG_CODE_OWNERSHIP": (sum_LOG_CODE_OWNERSHIP / total_files) if total_files else 0,
        "log_churn_total": total_log_churn,
        "code_churn_total": total_code_churn,
        "TOTAL_PRE_DEFECTS": len(pre_sets),
        "TOTAL_POST_DEFECTS": len(post_sets),
        "FILES_WITH_PRE": files_with_pre,
        "FILES_WITH_POST": files_with_post,
    }


def main(repo_url, branch, since, until, dstart, dend):
    repo_path = "./tmp_repo" if repo_url.startswith("http") else repo_url
    if repo_url.startswith("http") and not os.path.exists(repo_path):
        print(f"Cloning {repo_url} …")
        Repo.clone_from(repo_url, repo_path, branch=branch)
    repo = Repo(repo_path)
    if until:
        head = next(repo.iter_commits(branch, until=until.isoformat(), max_count=1), None)
        prod_commit = head or repo.commit(branch)
    else:
        prod_commit = repo.commit(branch)

    prod_file, prod_sum = parse_head_files(repo, prod_commit)
    proc_file, proc_sum, tpc = analyse_process(repo, branch, since, until)
    pre_map = detect_defects(repo, branch, since, until)
    post_map = detect_defects(repo, branch, dstart, dend)
    json_name, csv_name = make_filenames(repo_url, branch)

    per_file = {}
    for p, pm in prod_file.items():
        qm = proc_file.get(p, {"LOGADD":0,"LOGDEL":0,"FCOC":0,
                               "LOG_CODE_OWNERSHIP":0,"CODE_CHURN":0})
        pre_list = pre_map.get(p, [])
        post_list = post_map.get(p, [])
        per_file[p] = {**pm, **qm,
                        "TPC": tpc.get(p, 0),
                        "pre_defects": pre_list,
                        "pre_defect_count": len(pre_list),
                        "post_defects": post_list,
                        "post_defect_count": len(post_list)}

    if per_file:
        headers = ["file"] + sorted(next(iter(per_file.values())).keys())
        with open(csv_name, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for path, metrics in per_file.items():
                writer.writerow([path] + [metrics[h] for h in headers[1:]])
        print(f"Wrote CSV:  {csv_name}")

    project_summary = compute_summary_from_csv(csv_name)

    with open(json_name, 'w', encoding='utf-8') as f:
        json.dump({"per_file_metrics": per_file, "project_summary": project_summary},
                  f, indent=2, ensure_ascii=False)
    print(f"Wrote JSON: {json_name}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", help="GitHub URL or local path")
    ap.add_argument("--branch", required=True, help="branch name")
    ap.add_argument("--since", required=True, help="process start YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="process end YYYY-MM-DD")
    ap.add_argument("--defectstart", required=True, help="post-release start YYYY-MM-DD")
    ap.add_argument("--defectend", default=None, help="post-release end YYYY-MM-DD")
    args = ap.parse_args()

    since_dt = datetime.datetime.fromisoformat(args.since)
    end_dt = datetime.datetime.fromisoformat(args.end) if args.end else None
    defectstart_dt = datetime.datetime.fromisoformat(args.defectstart)
    defectend_dt = datetime.datetime.fromisoformat(args.defectend) if args.defectend else None

    main(args.repo, args.branch, since_dt, end_dt, defectstart_dt, defectend_dt)
