#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenWRT专用EPG生成脚本 v4.1（GitHub Actions适配版）
适配特性：
1. 路径适配GitHub Actions运行环境
2. 日志输出到工作目录，便于Action上传
3. 自动创建必要目录，无需手动创建
4. 增加文件权限处理，适配Linux环境
"""

import os
import sys
import json
import datetime
import xml.etree.ElementTree as ET
import gzip
import re
import urllib.request
import traceback
import hashlib
import shutil
import urllib.parse

# ===================== 核心配置区（适配GitHub Actions） =====================
# 基础开关配置
ENABLE_OFFICIAL_EPG = False  # 是否开启官方EPG下载（True/False）

# EPG服务器和文件路径（适配GitHub工作目录）
EPG_SERVER_URL = "http://210.13.21.3"
BJcul_PATH = "./bjcul.txt"
EPG_SAVE_PATH = "./epg.xml"                 # 生成的EPG文件（简化路径）
EPG_GZ_PATH = "./epg.xml.gz"                # 压缩的gz文件
LOG_PATH = "./epg_run.log"                  # 日志文件（简化路径）
EPG_OFFSET_START = -1                       # EPG时间偏移（前1天）
EPG_OFFSET_END = 3                          # EPG时间偏移（后3天）

# 缓存配置（GitHub Actions每次运行都是全新环境，缓存目录简化）
CACHE_DIR = "./epg_cache"                   # 缓存目录
CACHE_TIMEOUT = 30                          # 网络请求超时时间（秒）
CACHE_RETRY_TIMES = 2                       # 缓存文件下载重试次数

# 需保留的特殊4K名称
KEEP_4K_NAMES = ["CCTV4K", "CCTV4k", "爱上4K", "4K超清"]
# 需清理的后缀
CLEAN_SUFFIX = ["4k", "4K", "SDR", "HDR", "超高清", "英语", "英文"]

# 其他网络请求配置
TIMEOUT = CACHE_TIMEOUT
RETRY_TIMES = CACHE_RETRY_TIMES

# 多外部EPG源配置（按优先级排序）
EXTERNAL_EPG_SOURCES = [                              
    {
        "url": "https://raw.githubusercontent.com/zzzz0317/beijing-unicom-iptv-playlist/main/epg.xml.gz",
        "name": "主EPG源-zzzz0317",
        "is_official": False,
        "clean_name": True
    },
    {
        "url": "https://epg.zsdc.eu.org/t.xml.gz",
        "name": "备用EPG源1-zsdc",
        "is_official": False,
        "clean_name": True
    },
    {
        "url": "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
        "name": "备用EPG源2-e.erw.cc",
        "is_official": False,
        "clean_name": True
    },
    {
        "url": "https://gitee.com/taksssss/tv/raw/main/epg/112114.xml.gz",
        "name": "备用EPG源3-112114",
        "is_official": False,
        "clean_name": True
    },
    {
        "url": "https://gitee.com/taksssss/tv/raw/main/epg/51zmt.xml.gz",
        "name": "备用EPG源4-51zmt",
        "is_official": False,
        "clean_name": True
    }
]

# JSON格式配置
PLAYLIST_FILE_PATH = "https://raw.githubusercontent.com/zzzz0317/beijing-unicom-iptv-playlist/main/playlist-zz.json"
PLAYLIST_FORMAT = "zz"

# ===================== JSON格式字段映射配置 =====================
FORMAT_MAPPING = {
    "zz": {
        "channel_id_field": "id_sys",          
        "user_channel_id_field": "tvg_id",     
        "channel_url_path": ["live", "bjunicom-multicast", "addr"],
        "is_dict_format": True,                
        "url_replace_rule": None               
    },
    "raw": {
        "channel_id_field": "channelID",
        "user_channel_id_field": "userChannelID",
        "channel_url_path": ["channelURL"],
        "is_dict_format": False,               
        "url_replace_rule": ("igmp://", "rtp://")
    }
}

# ===================== 工具函数 =====================
def write_log(content, section="INFO"):
    """写入日志文件（适配GitHub环境）"""
    try:
        # 确保日志目录存在
        log_dir = os.path.dirname(LOG_PATH)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] [{section}] {content}\n")
        # 同时输出到控制台，便于GitHub Actions日志查看
        print(f"[{timestamp}] [{section}] {content}")
    except Exception as e:
        print(f"日志写入失败：{str(e)}")

def get_nested_value(data, path_list):
    """获取嵌套字典的字段值"""
    if not isinstance(data, dict) or not path_list:
        return None
    current = data
    for key in path_list:
        if key not in current:
            return None
        current = current[key]
    return current

def compress_xml_to_gz(xml_path, gz_path):
    """将XML文件压缩为gz格式"""
    try:
        write_log(f"开始压缩XML文件：{xml_path} → {gz_path}", "GZ_COMPRESS")
        with open(xml_path, 'rb') as f_in:
            with gzip.open(gz_path, 'wb', compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        if os.path.exists(gz_path):
            gz_size = os.path.getsize(gz_path)
            xml_size = os.path.getsize(xml_path)
            compression_ratio = round((1 - gz_size/xml_size) * 100, 2)
            write_log(f"压缩成功！原大小：{xml_size}字节，压缩后：{gz_size}字节，压缩率：{compression_ratio}%", "GZ_SUCCESS")
            print(f"  → 压缩完成：{gz_path}（压缩率{compression_ratio}%）")
            return True
        else:
            write_log("压缩文件生成失败", "GZ_FAIL")
            return False
    except Exception as e:
        write_log(f"压缩失败：{str(e)}", "GZ_ERROR")
        print(f"  ❌ 压缩失败：{str(e)}")
        return False

def get_url_md5(url):
    """生成URL的MD5哈希值作为缓存文件名"""
    encoded_url = urllib.parse.quote_plus(url).encode('utf-8')
    md5_hash = hashlib.md5(encoded_url).hexdigest()
    return md5_hash

def download_with_cache(url, cache_dir, timeout=30, retry=2):
    """下载文件并缓存（适配GitHub环境）"""
    # 创建缓存目录
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)
    
    # 生成缓存文件名
    url_md5 = get_url_md5(url)
    cache_file = os.path.join(cache_dir, f"{url_md5}.txt")
    old_cache_file = os.path.join(cache_dir, f"{url_md5}_old.txt")
    
    # 备份旧缓存
    if os.path.exists(cache_file):
        try:
            if os.path.exists(old_cache_file):
                os.remove(old_cache_file)
            os.rename(cache_file, old_cache_file)
            write_log(f"已备份旧缓存文件：{old_cache_file}", "CACHE")
        except Exception as e:
            write_log(f"备份旧缓存失败：{e}", "CACHE_ERROR")
    
    # 下载新文件
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    download_success = False
    for i in range(retry + 1):
        try:
            write_log(f"下载远程文件（重试{i}/{retry}）：{url}", "DOWNLOAD")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as res:
                if res.status == 200:
                    # 写入缓存文件
                    with open(cache_file, 'wb') as f:
                        f.write(res.read())
                    download_success = True
                    write_log(f"下载成功，缓存到：{cache_file}", "DOWNLOAD_SUCCESS")
                    break
                else:
                    write_log(f"下载失败，状态码：{res.status}", "DOWNLOAD_ERROR")
        except Exception as e:
            write_log(f"下载重试{i}失败：{e}", "DOWNLOAD_ERROR")
            continue
    
    # 处理下载结果
    if download_success:
        if os.path.exists(old_cache_file):
            try:
                os.remove(old_cache_file)
                write_log(f"删除旧缓存文件：{old_cache_file}", "CACHE")
            except Exception as e:
                write_log(f"删除旧缓存失败：{e}", "CACHE_ERROR")
        return cache_file
    else:
        if os.path.exists(old_cache_file):
            try:
                os.rename(old_cache_file, cache_file)
                write_log(f"下载失败，使用旧缓存：{cache_file}", "CACHE_FALLBACK")
                return cache_file
            except Exception as e:
                write_log(f"恢复旧缓存失败：{e}", "CACHE_ERROR")
        else:
            write_log(f"下载失败且无缓存文件：{url}", "CACHE_FATAL")
            return None

def get_local_path(path):
    """处理路径（支持本地路径和远程URL）"""
    if path.startswith(('http://', 'https://')):
        # 远程URL，下载并缓存
        local_file = download_with_cache(path, CACHE_DIR, CACHE_TIMEOUT, CACHE_RETRY_TIMES)
        if not local_file or not os.path.exists(local_file):
            raise Exception(f"远程文件处理失败：{path}")
        return local_file
    else:
        # 本地路径
        if not os.path.exists(path):
            raise Exception(f"本地文件不存在：{path}")
        return path

def clean_channel_name(raw_name):
    """统一的频道名清理规则"""
    if not raw_name:
        return ""
    raw_name = str(raw_name)
    if raw_name in KEEP_4K_NAMES:
        return raw_name
    
    # 去除横杠和空格
    raw_name = raw_name.replace("-", "").replace(" ", "")
    
    suffix_pattern = r"(\s*[-_()]?\s*(" + "|".join(CLEAN_SUFFIX) + r"))+$"
    clean_name = re.sub(suffix_pattern, "", raw_name, flags=re.IGNORECASE).strip()
    clean_name = re.sub(r"\s+", "", clean_name)
    return clean_name



def fuzzy_match(local_clean_name, ext_names, clean_ext_name=True):
    """
    统一的模糊匹配规则（彻底修复CCTV5/CCTV5+互配、4K误配问题）
    :param local_clean_name: 本地清理后的频道名
    :param ext_names: 外部EPG频道名列表
    :param clean_ext_name: 是否对外部名称执行清理规则
    :return: 匹配到的外部频道名
    """
    if not local_clean_name:
        return None
    
    # ========== 核心修复：提取CCTV频道的「数字+特殊标识」（如5、5+、16+） ==========
    # 正则匹配：CCTV+数字+可选的+号（覆盖CCTV5、CCTV5+、CCTV16+等）
    cctv_pattern = re.compile(r'CCTV(\d+\+?)')
    local_cctv_tag = None  # 存储本地CCTV标识（如5、5+、15）
    local_is_4k = local_clean_name in KEEP_4K_NAMES  # 判断本地是否是4K频道
    
    # 提取本地CCTV的核心标识
    local_cctv_match = cctv_pattern.search(local_clean_name)
    if local_cctv_match:
        local_cctv_tag = local_cctv_match.group(1)  # 如CCTV5→5，CCTV5+→5+，CCTV4欧洲→4
    
    # ========== 预处理外部名称（过滤4K+提取标识） ==========
    ext_candidate = []  # 存储(外部清理名, 外部CCTV标识, 原始外部名)
    for ext_name in ext_names:
        if clean_ext_name:
            ext_clean = clean_channel_name(ext_name)
        else:
            ext_clean = ext_name.strip().replace(" ", "")
        
        # 规则1：非4K本地频道，直接跳过含4K的外部频道
        if not local_is_4k and "4K" in ext_clean:
            continue
        
        # 提取外部CCTV的核心标识
        ext_cctv_tag = None
        ext_cctv_match = cctv_pattern.search(ext_clean)
        if ext_cctv_match:
            ext_cctv_tag = ext_cctv_match.group(1)
        
        ext_candidate.append((ext_clean, ext_cctv_tag, ext_name))
    
    # ========== 第一步：精准名称匹配 ==========
    for ext_clean, _, ext_name in ext_candidate:
        if local_clean_name == ext_clean:
            return ext_name
    
    # ========== 第二步：CCTV核心标识精准匹配（区分5和5+） ==========
    if local_cctv_tag:
        # 筛选：外部标识与本地完全一致（如5→5，5+→5+）
        matched_cctv = []
        for ext_clean, ext_cctv_tag, ext_name in ext_candidate:
            if ext_cctv_tag == local_cctv_tag:
                # 记录（外部名，长度）：优先匹配最短的（如CCTV5→CCTV5高清，而非CCTV5体育高清）
                matched_cctv.append((ext_clean, len(ext_clean), ext_name))
        
        if matched_cctv:
            # 按名称长度升序，选最接近本地名的
            matched_cctv.sort(key=lambda x: x[1])
            return matched_cctv[0][2]
    
    # ========== 第三步：正向包含匹配（仅本地名在外部名中，且长度接近） ==========
    match_candidates = []
    for ext_clean, _, ext_name in ext_candidate:
        if local_clean_name in ext_clean and len(ext_clean) <= len(local_clean_name) + 6:
            match_candidates.append((ext_clean, len(ext_clean), ext_name))
    
    if match_candidates:
        match_candidates.sort(key=lambda x: x[1])
        return match_candidates[0][2]
    
    # ========== 第四步：移除+号兜底匹配（仅作为最后备选） ==========
    local_no_plus = local_clean_name.replace("+", "")
    for ext_clean, _, ext_name in ext_candidate:
        ext_no_plus = ext_clean.replace("+", "")
        if local_no_plus == ext_no_plus:
            return ext_name
    
    return None

def extract_program_title(prog_elem):
    """精准提取节目标题"""
    title_zh = prog_elem.find(".//title[@lang='zh']")
    if title_zh is not None and title_zh.text is not None:
        title = title_zh.text.strip()
        if title:
            return title
    
    title_any = prog_elem.find(".//title")
    if title_any is not None and title_any.text is not None:
        title = title_any.text.strip()
        if title:
            return title
    
    return "未知节目"

def download_url(url, headers=None):
    """轻量化网络下载"""
    headers = headers or {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for i in range(RETRY_TIMES):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as res:
                if res.status == 200:
                    return res.read()
            write_log(f"下载失败：{url} 状态码：{res.status}", "ERROR")
        except Exception as e:
            write_log(f"下载重试{i+1}失败：{url} {str(e)}", "ERROR")
    return None

def parse_external_epg(epg_data, is_official=False):
    """解析外部EPG数据"""
    external_epg_map = {}
    ext_channel_identifiers = []
    id_to_name_map = {}
    
    try:
        # 解压处理
        try:
            epg_data = gzip.decompress(epg_data)
        except Exception as e:
            write_log(f"EPG解压失败（非gzip格式）：{str(e)}", "EPG_PARSE_WARN")
        
        # 解析XML
        ext_xml = ET.fromstring(epg_data.decode("utf-8", errors="ignore"))
        
        # 提取频道信息
        ext_channel_info = {}
        for channel in ext_xml.findall(".//channel"):
            cid = channel.get("id", "")
            if not cid:
                continue
            aliases = [elem.text.strip() for elem in channel.findall("display-name") if elem.text and elem.text.strip()]
            main_name = aliases[0] if aliases else cid
            ext_channel_info[cid] = {
                "main_name": main_name,
                "id": cid
            }
            id_to_name_map[cid] = main_name
        
        # 提取节目信息
        for prog in ext_xml.findall(".//programme"):
            cid = prog.get("channel", "")
            start = prog.get("start")
            stop = prog.get("stop")
            if not cid or not start or not stop:
                continue
            if cid not in ext_channel_info:
                continue
            
            title = extract_program_title(prog)
            channel_info = ext_channel_info[cid]
            
            # 根据是否官方选择匹配键
            key = cid if is_official else channel_info["main_name"]
            
            if key not in external_epg_map:
                external_epg_map[key] = []
            external_epg_map[key].append({
                "start": start,
                "stop": stop,
                "title": title
            })
        
        ext_channel_identifiers = list(external_epg_map.keys())
        write_log(f"EPG解析完成 - 频道{len(ext_channel_identifiers)}个，节目{sum(len(v) for v in external_epg_map.values())}条", "EPG_PARSE_DETAIL")
    
    except Exception as e:
        error_info = f"EPG解析失败：{str(e)}\n{traceback.format_exc()}"
        write_log(error_info, "EPG_PARSE_ERROR")
    
    return external_epg_map, ext_channel_identifiers, id_to_name_map

# ===================== 主运行逻辑 =====================
def main():
    # 初始化日志（清空旧日志）
    if os.path.exists(LOG_PATH):
        os.remove(LOG_PATH)
    write_log("="*60 + " EPG生成脚本开始运行 " + "="*60, "START")
    start_time = datetime.datetime.now()
    
    # 验证格式配置
    if PLAYLIST_FORMAT not in FORMAT_MAPPING:
        error_msg = f"不支持的JSON格式：{PLAYLIST_FORMAT}，支持的格式：{list(FORMAT_MAPPING.keys())}"
        write_log(error_msg, "FATAL")
        print(f"❌ {error_msg}")
        return
    format_config = FORMAT_MAPPING[PLAYLIST_FORMAT]
    write_log(f"使用JSON格式配置：{PLAYLIST_FORMAT} → {format_config}", "CONFIG")
    write_log(f"官方EPG下载功能：{'开启' if ENABLE_OFFICIAL_EPG else '关闭'}", "CONFIG")
    
    try:
        # ===================== 步骤1：读取bjcul.txt =====================
        write_log("开始读取bjcul.txt，过滤分组行和无效行", "STEP1")
        bjcul_channel_map = {}
        all_bjcul_rtp_urls = []
        
        # 处理本地/远程路径
        try:
            bjcul_local_path = get_local_path(BJcul_PATH)
        except Exception as e:
            error_msg = f"处理bjcul文件失败：{e}"
            write_log(error_msg, "FATAL")
            print(f"❌ {error_msg}")
            return
        
        valid_line_count = 0
        filtered_line_count = 0
        with open(bjcul_local_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or "#genre#" in line or ',' not in line:
                    filtered_line_count += 1
                    continue
                
                raw_name, rtp_url = line.split(',', 1)
                raw_name = raw_name.strip()
                rtp_url = rtp_url.strip()
                clean_name = clean_channel_name(raw_name)
                bjcul_channel_map[rtp_url] = {
                    "raw_name": raw_name,
                    "clean_name": clean_name
                }
                all_bjcul_rtp_urls.append(rtp_url)
                valid_line_count += 1
        
        all_bjcul_rtp_urls = list(set(all_bjcul_rtp_urls))
        total_valid_channels = len(all_bjcul_rtp_urls)
        
        print(f"[1/7] 读取bjcul.txt：{total_valid_channels} 个有效频道")
        write_log(f"读取完成 - 过滤{filtered_line_count}行无效数据，有效频道{total_valid_channels}个", "STEP1")

        # ===================== 步骤2：匹配频道ID =====================
        write_log(f"开始匹配频道ID（{PLAYLIST_FILE_PATH}）", "STEP2")
        matched_channels = {}
        local_num_map = {}
        matched_rtp_urls = []
        unmatched_bjcul_channels = []
        
        # 处理本地/远程路径
        try:
            playlist_local_path = get_local_path(PLAYLIST_FILE_PATH)
        except Exception as e:
            error_msg = f"处理playlist文件失败：{e}"
            write_log(error_msg, "FATAL")
            print(f"❌ {error_msg}")
            return
        
        # 读取JSON并匹配
        match_success_count = 0
        with open(playlist_local_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        # 处理不同格式的JSON
        channel_items = []
        if format_config["is_dict_format"]:
            channel_items = [(name, info) for name, info in raw_data.items()]
        else:
            channel_items = [(f"channel_{idx}", item) for idx, item in enumerate(raw_data)]
        
        # 遍历频道匹配
        for channel_name, channel_info in channel_items:
            channel_url = get_nested_value(channel_info, format_config["channel_url_path"])
            if not channel_url:
                write_log(f"频道{channel_name}未找到URL字段", "STEP2_WARN")
                continue
            
            # 处理URL替换
            rtp_url = channel_url
            if format_config["url_replace_rule"]:
                old_str, new_str = format_config["url_replace_rule"]
                if channel_url.startswith(old_str):
                    rtp_url = channel_url.replace(old_str, new_str, 1)
            
            # 匹配bjcul
            if rtp_url in bjcul_channel_map:
                channel_id = channel_info.get(format_config["channel_id_field"])
                user_channel_id = channel_info.get(format_config["user_channel_id_field"])
                
                if not channel_id:
                    write_log(f"频道{channel_name}未找到{format_config['channel_id_field']}字段", "STEP2_WARN")
                    continue
                if not user_channel_id:
                    user_channel_id = f"UN_{str(channel_id)[:8]}"
                
                # 保存匹配结果
                bjcul_info = bjcul_channel_map[rtp_url]
                matched_channels[channel_id] = {
                    "raw_name": bjcul_info["raw_name"],
                    "clean_name": bjcul_info["clean_name"],
                    "local_num": str(user_channel_id),
                    "rtp_url": rtp_url,
                    "channel_name": channel_name
                }
                local_num_map[str(user_channel_id)] = matched_channels[channel_id]
                matched_rtp_urls.append(rtp_url)
                match_success_count += 1
        
        # 收集未匹配频道
        matched_rtp_urls = list(set(matched_rtp_urls))
        for rtp_url in all_bjcul_rtp_urls:
            if rtp_url not in matched_rtp_urls and rtp_url in bjcul_channel_map:
                channel_info = bjcul_channel_map[rtp_url]
                unmatched_bjcul_channels.append({
                    "type": "unmatched_id",
                    "raw_name": channel_info["raw_name"],
                    "clean_name": channel_info["clean_name"],
                    "rtp_url": rtp_url,
                    "local_num": None
                })
        
        unmatched_count = len(unmatched_bjcul_channels)
        print(f"[2/7] 匹配频道ID：{match_success_count} 个成功，{unmatched_count} 个未匹配")
        write_log(f"匹配完成 - 成功{match_success_count}个，未匹配{unmatched_count}个", "STEP2")
        if unmatched_count > 0:
            write_log("未匹配频道详细列表：", "STEP2_UNMATCHED")
            for idx, channel in enumerate(unmatched_bjcul_channels):
                write_log(f"  {idx+1}. 原始名：{channel['raw_name']} | RTP_URL：{channel['rtp_url']}", "STEP2_UNMATCHED")

        # ===================== 步骤3：下载官方EPG =====================
        write_log("开始处理官方EPG下载", "STEP3")
        programme_list = []
        fail_channels_detail = []
        total_official_progs = 0
        official_fail_count = 0
        
        if ENABLE_OFFICIAL_EPG:
            # 开启官方EPG下载
            datetime_now = datetime.datetime.now()
            for channel_code in matched_channels.keys():
                channel_info = matched_channels[channel_code]
                raw_name = channel_info["raw_name"]
                clean_name = channel_info["clean_name"]
                local_num = channel_info["local_num"]
                download_fail = True
                channel_prog_count = 0
                
                for day_offset in range(EPG_OFFSET_START, EPG_OFFSET_END):
                    datestr = (datetime_now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
                    url = f"{EPG_SERVER_URL}/schedules/{channel_code}_{datestr}.json"
                    data = download_url(url)
                    if not data:
                        continue
                    
                    try:
                        epg_data = json.loads(data.decode("utf-8"))
                        for schedule in epg_data.get("schedules", []):
                            start_str = schedule.get("starttime", schedule.get("showStarttime", ""))
                            end_str = schedule.get("endtime", start_str)
                            if not start_str or not end_str:
                                continue
                            try:
                                start_time = datetime.datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
                                end_time = datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
                            except:
                                continue
                            title = schedule.get("title", "").strip() or "未知节目"
                            time_format = "%Y%m%d%H%M%S +0800"
                            programme_list.append({
                                "channel": local_num,
                                "start": start_time.strftime(time_format),
                                "stop": end_time.strftime(time_format),
                                "title": title
                            })
                            channel_prog_count += 1
                            total_official_progs += 1
                        download_fail = False
                    except Exception as e:
                        write_log(f"解析频道{raw_name}({channel_code})EPG失败：{str(e)}", "STEP3_ERROR")
                        continue
                
                if download_fail:
                    # 添加到待匹配列表
                    unmatched_bjcul_channels.append({
                        "type": "official_fail",
                        "raw_name": raw_name,
                        "clean_name": clean_name,
                        "rtp_url": channel_info["rtp_url"],
                        "local_num": local_num
                    })
                    fail_channels_detail.append({
                        "channel_id": channel_code,
                        "raw_name": raw_name,
                        "local_num": local_num
                    })
                else:
                    write_log(f"频道{raw_name}({channel_code})下载成功，获取{channel_prog_count}条节目", "STEP3_DETAIL")
            
            official_fail_count = len(fail_channels_detail)
        else:
            # 关闭官方EPG下载
            write_log("官方EPG下载已关闭，所有匹配到ID的频道将使用外部EPG源", "STEP3_SKIP")
            for channel_code in matched_channels.keys():
                channel_info = matched_channels[channel_code]
                unmatched_bjcul_channels.append({
                    "type": "official_skip",
                    "raw_name": channel_info["raw_name"],
                    "clean_name": channel_info["clean_name"],
                    "rtp_url": channel_info["rtp_url"],
                    "local_num": channel_info["local_num"]
                })
            official_fail_count = len(matched_channels)
        
        total_pending_channels = len(unmatched_bjcul_channels)
        print(f"[3/7] 处理官方EPG：{total_official_progs} 条节目，{official_fail_count} 个频道需匹配外部源")
        write_log(f"官方EPG处理完成 - 总节目{total_official_progs}条，需外部匹配{official_fail_count}个", "STEP3")
        write_log(f"待匹配频道总数：{total_pending_channels}（未匹配ID：{unmatched_count} + 官方处理：{official_fail_count}）", "STEP3_PENDING")

        # ===================== 步骤4：多EPG源优先级匹配 =====================
        write_log("开始多EPG源优先级匹配", "STEP4")
        # 临时ID前缀
        temp_local_num_prefix = "unm_"
        temp_num_counter = 1
        total_matched_by_external = 0
        pending_channels = unmatched_bjcul_channels
        
        # 遍历所有EPG源
        for source_idx, epg_source in enumerate(EXTERNAL_EPG_SOURCES):
                source_name = epg_source["name"]
                source_url = epg_source["url"]
                is_official = epg_source.get("is_official", False)
                clean_name = epg_source.get("clean_name", True)
                
                # 终止条件
                if len(pending_channels) == 0:
                        write_log(f"无待匹配频道，终止后续EPG源匹配", "STEP4_TERMINATE")
                        break
                
                write_log(f"开始处理第{source_idx+1}个EPG源：{source_name} ({source_url})", "STEP4_SOURCE")
                print(f"[4/{7}] 匹配外部EPG源{source_idx+1}：{source_name}（待匹配{len(pending_channels)}个频道）")
                
                # 下载当前EPG源
                epg_data = download_url(source_url)
                if not epg_data:
                        write_log(f"第{source_idx+1}个EPG源{source_name}下载失败，尝试下一个源", "STEP4_SOURCE_FAIL")
                        continue
                
                # 解析EPG源
                epg_map, epg_identifiers, id_to_name_map = parse_external_epg(epg_data, is_official)
                if not epg_map or len(epg_identifiers) == 0:
                        write_log(f"第{source_idx+1}个EPG源{source_name}解析失败，尝试下一个源", "STEP4_SOURCE_PARSE_FAIL")
                        continue
                
                # 匹配待匹配频道
                matched_in_this_source = 0
                new_pending_channels = []
                
                for channel in pending_channels:
                        clean_name_local = channel["clean_name"]
                        raw_name = channel["raw_name"]
                        local_num = channel["local_num"]
                        channel_id = None
                        
                        # 官方EPG源：ID匹配
                        if is_official and local_num and not local_num.startswith(temp_local_num_prefix):
                                channel_id = local_num
                                if channel_id in epg_identifiers and epg_map.get(channel_id):
                                        ext_channel_name = id_to_name_map.get(channel_id, f"ID_{channel_id}")
                                        ext_progs = epg_map[channel_id]
                                        added_count = len(ext_progs)
                                        for prog in ext_progs:
                                                programme_list.append({
                                                        "channel": local_num,
                                                        "start": prog["start"],
                                                        "stop": prog["stop"],
                                                        "title": prog["title"]
                                                })
                                        matched_in_this_source += 1
                                        total_matched_by_external += 1
                                        write_log(f"频道{raw_name}({channel_id})通过官方ID匹配到{source_name}（{ext_channel_name}），新增{added_count}条节目", "STEP4_MATCH_SUCCESS")
                                        continue
                                else:
                                        new_pending_channels.append(channel)
                                        write_log(f"频道{raw_name}({channel_id})官方ID匹配失败", "STEP4_MATCH_FAIL")
                                        continue
                        else:
                                # 非官方EPG源：名称匹配
                                match_ext_name = fuzzy_match(clean_name_local, epg_identifiers, clean_name)
                                if not match_ext_name or match_ext_name not in epg_map:
                                        new_pending_channels.append(channel)
                                        write_log(f"频道{raw_name}在{source_name}中未匹配到外部频道", "STEP4_MATCH_FAIL")
                                        continue
                                
                                ext_progs = epg_map[match_ext_name]
                                if not ext_progs:
                                        new_pending_channels.append(channel)
                                        write_log(f"频道{raw_name}匹配到{match_ext_name}但无节目数据", "STEP4_MATCH_NO_PROG")
                                        continue
                                
                                if not local_num:
                                        local_num = f"{temp_local_num_prefix}{temp_num_counter}"
                                        temp_num_counter += 1
                                        channel["local_num"] = local_num
                                
                                added_count = len(ext_progs)
                                for prog in ext_progs:
                                        programme_list.append({
                                                "channel": local_num,
                                                "start": prog["start"],
                                                "stop": prog["stop"],
                                                "title": prog["title"]
                                        })
                                
                                matched_in_this_source += 1
                                total_matched_by_external += 1
                                write_log(f"频道{raw_name}({local_num})在{source_name}中匹配到{match_ext_name}，新增{added_count}条节目", "STEP4_MATCH_SUCCESS")
                
                # 更新待匹配列表
                pending_channels = new_pending_channels
                write_log(f"{source_name}匹配完成 - 成功{matched_in_this_source}个，剩余待匹配{len(pending_channels)}个", "STEP4_SOURCE_SUMMARY")
                print(f"  → 源{source_idx+1}匹配成功{matched_in_this_source}个，剩余{len(pending_channels)}个未匹配")

        # 最终匹配统计
        total_unmatched_final = len(pending_channels)
        print(f"[5/7] 多EPG源匹配完成：总计匹配{total_matched_by_external}个，剩余{total_unmatched_final}个未匹配")
        write_log(f"多EPG源匹配汇总 - 总匹配{total_matched_by_external}个，最终未匹配{total_unmatched_final}个", "STEP4_FINAL_SUMMARY")
        
        # 记录未匹配频道
        if total_unmatched_final > 0:
            write_log("最终未匹配的频道列表：", "STEP4_UNMATCHED_FINAL")
            for idx, channel in enumerate(pending_channels):
                write_log(f"  {idx+1}. 类型：{channel['type']} | 原始名：{channel['raw_name']} | RTP：{channel['rtp_url']}", "STEP4_UNMATCHED_FINAL")

        # ===================== 步骤5：生成EPG XML =====================
        write_log("开始生成最终EPG XML文件", "STEP5")
        root = ET.Element("tv", {
            "generator-info-name": "OpenWRT EPG Generator v4.1 (GitHub Actions)",
            "generator-info-url": "custom"
        })
        
        # 添加已匹配频道
        channel_add_count = 0
        # 添加官方EPG成功的频道
        for channel_code in matched_channels.keys():
            channel_info = matched_channels[channel_code]
            local_num = channel_info["local_num"]
            raw_name = channel_info["raw_name"]
            channel_elem = ET.SubElement(root, "channel", {"id": local_num})
            ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = raw_name
            channel_add_count += 1
        
        # 添加外部EPG匹配成功的临时频道
        temp_channel_add_count = 0
        for channel in unmatched_bjcul_channels:
            if channel["local_num"] and channel["local_num"].startswith(temp_local_num_prefix):
                local_num = channel["local_num"]
                raw_name = channel["raw_name"]
                channel_elem = ET.SubElement(root, "channel", {"id": local_num})
                ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = f"{raw_name}"
                temp_channel_add_count += 1
        
        # 添加节目（去重）
        seen_progs = set()
        sorted_progs = sorted(programme_list, key=lambda x: (x["channel"], x["start"]))
        prog_add_count = 0
        non_unknown_count = 0
        
        for prog in sorted_progs:
            if not prog.get("channel") or not prog.get("start") or not prog.get("title"):
                continue
            
            key = (prog["channel"], prog["start"], prog["title"])
            if key in seen_progs:
                continue
            
            seen_progs.add(key)
            prog_elem = ET.SubElement(root, "programme", {
                "start": prog["start"],
                "stop": prog["stop"],
                "channel": prog["channel"]
            })
            ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
            prog_add_count += 1
            
            if prog["title"] != "未知节目":
                non_unknown_count += 1
        
        # 保存XML文件
        try:
            ET.ElementTree(root).write(
                EPG_SAVE_PATH,
                encoding="UTF-8",
                xml_declaration=True,
                short_empty_elements=False
            )
            # 修改文件权限，便于后续上传
            os.chmod(EPG_SAVE_PATH, 0o644)
            print(f"[6/7] 生成EPG文件：{EPG_SAVE_PATH}（去重后{prog_add_count}条节目）")
            write_log(f"EPG XML生成成功 - 保存路径：{EPG_SAVE_PATH}", "STEP5")
            write_log(f"频道统计：已匹配频道{channel_add_count}个，临时频道{temp_channel_add_count}个", "STEP5_DETAIL")
            write_log(f"节目统计：总节目{prog_add_count}条（去重后），非未知节目{non_unknown_count}条", "STEP5_DETAIL")
        except Exception as e:
            error_msg = f"EPG XML生成失败：{str(e)}\n{traceback.format_exc()}"
            write_log(error_msg, "STEP5_FATAL")
            print(f"❌ EPG文件生成失败：{str(e)}")
            return

        # 压缩XML文件
        print("[6/7] 压缩EPG文件为epg.xml.gz...")
        compress_success = compress_xml_to_gz(EPG_SAVE_PATH, EPG_GZ_PATH)
        if compress_success:
            os.chmod(EPG_GZ_PATH, 0o644)
        else:
            print("  ❌ 压缩失败，但不影响主程序运行")

        # ===================== 步骤6：统计结果 =====================
        write_log("开始统计最终运行结果", "STEP6")
        end_time = datetime.datetime.now()
        run_duration = (end_time - start_time).total_seconds()
        
        # 汇总统计
        summary = {
            "总运行时间(秒)": round(run_duration, 2),
            "bjcul有效频道数": total_valid_channels,
            "匹配到ID的频道数": match_success_count,
            "官方EPG处理频道数": official_fail_count,
            "外部EPG匹配成功数": total_matched_by_external,
            "最终未匹配频道数": total_unmatched_final,
            "最终EPG频道数": channel_add_count + temp_channel_add_count,
            "最终EPG节目数(去重)": prog_add_count,
            "非未知节目数": non_unknown_count
        }
        
        # 输出统计
        print(f"[7/7] 运行完成：耗时{summary['总运行时间(秒)']}秒，非未知节目{summary['非未知节目数']}条")
        print(f"✅ EPG文件已生成：{EPG_SAVE_PATH}, {EPG_GZ_PATH}")
        # 日志记录
        write_log("="*30 + " 运行结果汇总 " + "="*30, "STEP6_SUMMARY")
        for key, value in summary.items():
            write_log(f"{key}：{value}", "STEP6_SUMMARY")
        write_log("="*60 + " EPG生成脚本运行结束 " + "="*60 + "\n\n", "END")
        
    except Exception as e:
        error_info = f"脚本运行异常：{str(e)}\n{traceback.format_exc()}"
        write_log(error_info, "FATAL")
        print(f"❌ 脚本运行异常：{str(e)}")
        print(f"详细错误日志：{LOG_PATH}")
        sys.exit(1)

if __name__ == "__main__":
    main()
