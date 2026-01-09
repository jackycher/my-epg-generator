#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG生成脚本（简化去重：仅按时间区间重合去重）
核心规则：同一频道下，新节目时间区间与已有节目时间区间重合则跳过
修复点1：外部源ID与本地频道ID冲突，外部频道强制生成独立ID，仅按名称去重
修复点2：完整版XML外部频道漏加问题，区分本地/外部名称集合，仅过滤本地同名
新增功能：外部EPG源支持频道重命名配置
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


# ===================== EPG配置区 =====================
EPG_CONFIG = {
    'ENABLE_OFFICIAL_EPG': False,
    'ENABLE_EXTERNAL_EPG': True,
    'ENABLE_KEEP_OTHER_CHANNELS': True,
    'EPG_SERVER_URL': "http://210.13.21.3",
    'BJcul_PATH': "./bjcul.txt",
    'EXCLUDE_MULTI_SOURCE_CHANNELS': [""],
    'EXCLUDE_MULTI_SOURCE_CATEGORIES': ["TS频道", "体验频道"],
    'EPG_SAVE_PATH': "./epg.xml",
    'EPG_GZ_PATH': "./epg.xml.gz",
    'EPG_FULL_SAVE_PATH': "./epg_full.xml",
    'EPG_FULL_GZ_PATH': "./epg_full.xml.gz",
    'LOG_PATH': "./epg_run.log",
    'EPG_OFFSET_START': -1,
    'EPG_OFFSET_END': 3,
    'CACHE_DIR': "./epg_cache",
    'CACHE_TIMEOUT': 30,
    'CACHE_RETRY_TIMES': 2,
    'KEEP_4K_NAMES': ["CCTV4K", "爱上4K","4K超清"],
    'CLEAN_SUFFIX': ["4k", "4K", "SDR", "HDR", "超高清", "英语", "英文"],
    'TIMEOUT': 30,
    'RETRY_TIMES': 2,
    'EXTERNAL_EPG_SOURCES': [                              
        {
            "url": "https://raw.githubusercontent.com/zzzz0317/beijing-unicom-iptv-playlist/main/epg.xml.gz",
            "name": "主EPG源-zzzz0317",
            "is_official": False,
            "clean_name": True,
            "enabled": True,
            # 新增：频道重命名规则 [["原名称", "新名称"], ...]，默认空列表
            "channel_rename": [["重温经典", "北京重温经典"], ["影视剧场", "北京影视剧场"]]
        },
        {
            "url": "https://raw.githubusercontent.com/taksssss/tv/main/epg/erw.xml.gz",
            "name": "备用EPG源4-erw",
            "is_official": False,
            "clean_name": True,
            "enabled": True,
            "channel_rename": []
        },
        {
            "url": "https://epg.zsdc.eu.org/t.xml.gz",
            "name": "备用EPG源1-zsdc",
            "is_official": False,
            "clean_name": True,
            "enabled": False,
            "channel_rename": []
        },
        {
            "url": "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
            "name": "备用EPG源2-e.erw.cc",
            "is_official": False,
            "clean_name": True,
            "enabled": False,
            "channel_rename": []
        },
        {
            "url": "https://gitee.com/taksssss/tv/raw/main/epg/112114.xml.gz",
            "name": "备用EPG源3-112114",
            "is_official": False,
            "clean_name": True,
            "enabled": False,
            "channel_rename": []
        },
        {
            "url": "https://gitee.com/taksssss/tv/raw/main/epg/51zmt.xml.gz",
            "name": "备用EPG源5-51zmt",
            "is_official": False,
            "clean_name": True,
            "enabled": False,
            "channel_rename": []
        },
        {
            "url": "https://gitee.com/taksssss/tv/raw/main/epg/epgpw_cn.xml.gz",
            "name": "备用EPG源6-epgpw_cn",
            "is_official": False,
            "clean_name": True,
            "enabled": False,
            "channel_rename": []
        }
    ],
    'PLAYLIST_FILE_PATH': "https://raw.githubusercontent.com/zzzz0317/beijing-unicom-iptv-playlist/main/playlist-zz.json",
    'PLAYLIST_FORMAT': "zz",
    'FORMAT_MAPPING': {
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
}


# ===================== 核心去重函数（仅时间判断） =====================
def parse_time_str_to_timestamp(time_str):
    """解析EPG时间字符串为时间戳（格式：YYYYMMDDHHMMSS +0800）"""
    try:
        time_part = time_str.split(' ')[0]
        dt = datetime.datetime.strptime(time_part, "%Y%m%d%H%M%S")
        return dt.timestamp()
    except Exception:
        return None

def is_time_overlap(new_start_ts, new_end_ts, exist_start_ts, exist_end_ts):
    """判断两个时间区间是否重合"""
    return new_start_ts < exist_end_ts and exist_start_ts < new_end_ts

def add_program_if_no_time_overlap(programme_list, channel_time_ranges, new_prog):
    """仅当新节目与已有节目无时间重合时，才添加到列表"""
    channel = new_prog.get("channel")
    start_str = new_prog.get("start")
    stop_str = new_prog.get("stop")
    
    if not channel or not start_str or not stop_str:
        return False
    
    new_start_ts = parse_time_str_to_timestamp(start_str)
    new_end_ts = parse_time_str_to_timestamp(stop_str)
    if new_start_ts is None or new_end_ts is None:
        return False
    
    if channel not in channel_time_ranges:
        channel_time_ranges[channel] = []
    
    for (exist_start_ts, exist_end_ts) in channel_time_ranges[channel]:
        if is_time_overlap(new_start_ts, new_end_ts, exist_start_ts, exist_end_ts):
            return False
    
    programme_list.append(new_prog)
    channel_time_ranges[channel].append((new_start_ts, new_end_ts))
    return True

# ===================== 工具函数 =====================
def write_log(content, section="INFO"):
    log_path = EPG_CONFIG['LOG_PATH']
    try:
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] [{section}] {content}\n")
        print(f"[{timestamp}] [{section}] {content}")
    except Exception as e:
        print(f"日志写入失败：{str(e)}")

def get_nested_value(data, path_list):
    if not isinstance(data, dict) or not path_list:
        return None
    current = data
    for key in path_list:
        if key not in current:
            return None
        current = current[key]
    return current

def compress_xml_to_gz(xml_path, gz_path):
    try:
        write_log(f"开始压缩：{xml_path} → {gz_path}", "GZ_COMPRESS")
        with open(xml_path, 'rb') as f_in:
            with gzip.open(gz_path, 'wb', compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        if os.path.exists(gz_path):
            gz_size = os.path.getsize(gz_path)
            xml_size = os.path.getsize(xml_path)
            ratio = round((1 - gz_size/xml_size) * 100, 2)
            write_log(f"压缩成功！原{xml_size}字节 → 压缩后{gz_size}字节（{ratio}%）", "GZ_SUCCESS")
            print(f"  → 压缩完成：{gz_path}（{ratio}%）")
            return True
        else:
            write_log("压缩文件生成失败", "GZ_FAIL")
            return False
    except Exception as e:
        write_log(f"压缩失败：{str(e)}", "GZ_ERROR")
        print(f"  ❌ 压缩失败：{str(e)}")
        return False

def get_url_md5(url):
    encoded_url = urllib.parse.quote_plus(url).encode('utf-8')
    return hashlib.md5(encoded_url).hexdigest()

def download_with_cache(url, cache_dir, timeout=30, retry=2):
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)
    
    url_md5 = get_url_md5(url)
    cache_file = os.path.join(cache_dir, f"{url_md5}.txt")
    old_cache_file = os.path.join(cache_dir, f"{url_md5}_old.txt")
    
    if os.path.exists(cache_file):
        try:
            if os.path.exists(old_cache_file):
                os.remove(old_cache_file)
            os.rename(cache_file, old_cache_file)
            write_log(f"备份旧缓存：{old_cache_file}", "CACHE")
        except Exception as e:
            write_log(f"备份缓存失败：{e}", "CACHE_ERROR")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    download_success = False
    for i in range(retry + 1):
        try:
            write_log(f"下载（重试{i}/{retry}）：{url}", "DOWNLOAD")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as res:
                if res.status == 200:
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
    
    if download_success:
        if os.path.exists(old_cache_file):
            os.remove(old_cache_file)
        return cache_file
    else:
        if os.path.exists(old_cache_file):
            os.rename(old_cache_file, cache_file)
            write_log(f"下载失败，使用旧缓存：{cache_file}", "CACHE_FALLBACK")
            return cache_file
        else:
            write_log(f"下载失败且无缓存：{url}", "CACHE_FATAL")
            return None

def get_local_path(path):
    if path.startswith(('http://', 'https://')):
        local_file = download_with_cache(
            path, 
            EPG_CONFIG['CACHE_DIR'],
            EPG_CONFIG['CACHE_TIMEOUT'],
            EPG_CONFIG['CACHE_RETRY_TIMES']
        )
        if not local_file or not os.path.exists(local_file):
            raise Exception(f"远程文件处理失败：{path}")
        return local_file
    else:
        if not os.path.exists(path):
            raise Exception(f"本地文件不存在：{path}")
        return path

def clean_channel_name(raw_name):
    if not raw_name:
        return ""
    raw_name = str(raw_name)
    
    if "4K" in raw_name and any(key in raw_name for key in ["CCTV4K", "4K超高清", "爱上4K"]):
        return raw_name.replace("-", "").replace(" ", "")
    
    if raw_name in EPG_CONFIG['KEEP_4K_NAMES']:
        return raw_name
    
    raw_name = raw_name.replace("-", "").replace(" ", "")
    suffix_pattern = r"(\s*[-_()]?\s*(4K|SDR|HDR|超清))+$"
    clean_name = re.sub(suffix_pattern, "", raw_name, flags=re.IGNORECASE).strip()
    return re.sub(r"\s+", "", clean_name)

def fuzzy_match(local_clean_name, ext_names, clean_ext_name=True):
    if not local_clean_name:
        return None
    
    if "CGTN" in local_clean_name and "纪录" in local_clean_name:
        for ext_name in ext_names:
            ext_clean = clean_channel_name(ext_name) if clean_ext_name else ext_name.strip().replace(" ", "")
            if "CGTN" in ext_clean and "纪录" in ext_clean and "英文" in ext_clean:
                return ext_name
            elif "CGTN" in ext_clean and "纪录" in ext_clean:
                return ext_name
    
    is_cctv4_europe = "CCTV4" in local_clean_name and "欧洲" in local_clean_name
    is_cctv4_america = "CCTV4" in local_clean_name and "美洲" in local_clean_name
    is_cctv4k = "CCTV4K" in local_clean_name
    local_is_4k = is_cctv4k or local_clean_name in EPG_CONFIG['KEEP_4K_NAMES']

    cctv_pattern = re.compile(r'CCTV(4K|\d+\+?)')
    local_cctv_tag = cctv_pattern.search(local_clean_name).group(1) if cctv_pattern.search(local_clean_name) else None
    
    ext_candidate = []
    for ext_name in ext_names:
        ext_clean = clean_channel_name(ext_name) if clean_ext_name else ext_name.strip().replace(" ", "")
        
        if not local_is_4k and "4K" in ext_clean:
            continue
        
        ext_cctv_tag = cctv_pattern.search(ext_clean).group(1) if cctv_pattern.search(ext_clean) else None
        ext_candidate.append({
            "clean": ext_clean,
            "tag": ext_cctv_tag,
            "original": ext_name,
            "len": len(ext_clean)
        })
    
    for ext in ext_candidate:
        if local_clean_name == ext["clean"]:
            return ext["original"]
    
    if is_cctv4_europe or is_cctv4_america:
        region_key = "欧洲" if is_cctv4_europe else "美洲"
        region_matched = [ext for ext in ext_candidate if ext["tag"] == "4" and region_key in ext["clean"]]
        if region_matched:
            region_matched.sort(key=lambda x: x["len"])
            return region_matched[0]["original"]
    
    if is_cctv4k:
        cctv4k_matched = [ext for ext in ext_candidate if "CCTV4K" in ext["clean"]]
        if cctv4k_matched:
            cctv4k_matched.sort(key=lambda x: x["len"])
            return cctv4k_matched[0]["original"]
    
    if local_cctv_tag:
        tag_matched = [ext for ext in ext_candidate if ext["tag"] == local_cctv_tag]
        if tag_matched:
            tag_matched.sort(key=lambda x: x["len"])
            return tag_matched[0]["original"]
    
    include_matched = [
        ext for ext in ext_candidate
        if local_clean_name in ext["clean"] and ext["len"] <= len(local_clean_name) + 10
    ]
    if include_matched:
        include_matched.sort(key=lambda x: x["len"])
        return include_matched[0]["original"]
    
    local_no_plus = local_clean_name.replace("+", "")
    for ext in ext_candidate:
        ext_no_plus = ext["clean"].replace("+", "")
        if local_no_plus == ext_no_plus:
            return ext["original"]
    
    return None

def extract_program_title(prog_elem):
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

def download_url(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for i in range(EPG_CONFIG['RETRY_TIMES']):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=EPG_CONFIG['TIMEOUT']) as res:
                if res.status == 200:
                    return res.read()
            write_log(f"下载失败：{url} 状态码：{res.status}", "ERROR")
        except Exception as e:
            write_log(f"下载重试{i+1}失败：{url} {str(e)}", "ERROR")
    return None

def parse_external_epg(epg_data, is_official=False):
    external_epg_map = {}
    ext_channel_identifiers = []
    id_to_name_map = {}
    full_channel_info = {}
    full_program_info = []
    
    try:
        try:
            epg_data = gzip.decompress(epg_data)
        except Exception as e:
            write_log(f"解压失败（非GZ）：{str(e)}", "EPG_PARSE_WARN")
        
        ext_xml = ET.fromstring(epg_data.decode("utf-8", errors="ignore"))
        
        for channel in ext_xml.findall(".//channel"):
            cid = channel.get("id", "")
            if not cid:
                continue
            aliases = [elem.text.strip() for elem in channel.findall("display-name") if elem.text and elem.text.strip()]
            main_name = aliases[0] if aliases else cid
            full_channel_info[cid] = {
                "id": cid,
                "main_name": main_name,
                "aliases": aliases
            }
            id_to_name_map[cid] = main_name
            ext_channel_identifiers.append(main_name if not is_official else cid)
        
        for prog in ext_xml.findall(".//programme"):
            cid = prog.get("channel", "")
            start = prog.get("start")
            stop = prog.get("stop")
            if not cid or not start or not stop:
                continue
            if cid not in full_channel_info:
                continue
            
            title = extract_program_title(prog)
            full_program_info.append({
                "channel_id": cid,
                "start": start,
                "stop": stop,
                "title": title
            })
            
            key = cid if is_official else full_channel_info[cid]["main_name"]
            if key not in external_epg_map:
                external_epg_map[key] = []
            external_epg_map[key].append({
                "start": start,
                "stop": stop,
                "title": title
            })
        
        ext_channel_identifiers = list(external_epg_map.keys())
        write_log(f"EPG解析完成 - 频道{len(full_channel_info)}个（总），匹配用{len(ext_channel_identifiers)}个，节目{len(full_program_info)}条（总）", "EPG_PARSE_DETAIL")
    
    except Exception as e:
        error_info = f"解析失败：{str(e)}\n{traceback.format_exc()}"
        write_log(error_info, "EPG_PARSE_ERROR")
    
    return external_epg_map, ext_channel_identifiers, id_to_name_map, full_channel_info, full_program_info

def generate_unique_ext_channel_id(existing_ids, prefix="ext_"):
    """生成外部频道的唯一ID（确保不与本地频道ID冲突）"""
    counter = 1
    while True:
        new_id = f"{prefix}{counter}"
        if new_id not in existing_ids:
            return new_id
        counter += 1

# ===================== 主函数 =====================
def epg_main():
    config = EPG_CONFIG
    if os.path.exists(config['LOG_PATH']):
        os.remove(config['LOG_PATH'])
    write_log("="*60 + " EPG生成脚本开始运行 " + "="*60, "START")
    start_time = datetime.datetime.now()
    
    if config['PLAYLIST_FORMAT'] not in config['FORMAT_MAPPING']:
        error_msg = f"不支持的格式：{config['PLAYLIST_FORMAT']}，支持：{list(config['FORMAT_MAPPING'].keys())}"
        write_log(error_msg, "FATAL")
        print(f"❌ {error_msg}")
        return
    format_config = config['FORMAT_MAPPING'][config['PLAYLIST_FORMAT']]
    write_log(f"使用格式配置：{config['PLAYLIST_FORMAT']}", "CONFIG")
    write_log(f"官方EPG：{'开启' if config['ENABLE_OFFICIAL_EPG'] else '关闭'}", "CONFIG")
    write_log(f"外部EPG：{'开启' if config['ENABLE_EXTERNAL_EPG'] else '关闭'}", "CONFIG")
    write_log(f"保留其他频道：{'开启' if config['ENABLE_KEEP_OTHER_CHANNELS'] else '关闭'}", "CONFIG")
    
    all_external_channels = {}  # 存储外部频道信息（原始ID→名称/别名）
    all_external_programs = []  # 存储外部节目（原始ID关联）
    ext_id_mapping = {}  # 外部原始ID → 最终频道ID（本地或新生成）
    ext_channel_name_to_final_id = {}  # 外部频道名称 → 最终频道ID（用于名称去重）
    # 修复：新增外部最终ID→频道信息映射，方便完整版查找
    ext_final_id_to_info = {}
    
    try:
        # 步骤1：读取bjcul.txt
        write_log("开始读取bjcul.txt", "STEP1")
        bjcul_channel_map = {}
        all_bjcul_rtp_urls = []
        current_category = ""
        # 修复：收集本地txt所有频道名称（用于后续过滤外部同名）
        local_channel_names = set()
        
        bjcul_local_path = get_local_path(config['BJcul_PATH'])
        valid_line_count = 0
        filtered_line_count = 0
        with open(bjcul_local_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    filtered_line_count += 1
                    continue
                
                if "#genre#" in line:
                    current_category = line.replace("#genre#", "").strip().rstrip(',').strip()
                    write_log(f"识别到分类：{current_category}", "STEP1_CATEGORY")
                    filtered_line_count += 1
                    continue
                
                if ',' not in line:
                    filtered_line_count += 1
                    continue
                
                raw_name, rtp_url = line.split(',', 1)
                raw_name = raw_name.strip()
                rtp_url = rtp_url.strip()
                clean_name = clean_channel_name(raw_name)
                bjcul_channel_map[rtp_url] = {
                    "raw_name": raw_name,
                    "clean_name": clean_name,
                    "category": current_category
                }
                all_bjcul_rtp_urls.append(rtp_url)
                # 修复：添加到本地频道名称集合
                local_channel_names.add(raw_name)
                valid_line_count += 1
        
        all_bjcul_rtp_urls = list(set(all_bjcul_rtp_urls))
        total_valid_channels = len(all_bjcul_rtp_urls)
        # 修复：打印本地频道名称数量
        write_log(f"收集本地频道名称：{len(local_channel_names)}个", "STEP1_LOCAL_NAMES")
        print(f"[1/7] 读取bjcul.txt：{total_valid_channels} 个有效频道（{len(local_channel_names)}个唯一名称）")
        write_log(f"读取完成 - 过滤{filtered_line_count}行，有效{total_valid_channels}个", "STEP1")

        # 步骤2：匹配频道ID
        write_log(f"开始匹配频道ID：{config['PLAYLIST_FILE_PATH']}", "STEP2")
        matched_channels = {}
        unmatched_bjcul_channels = []
        playlist_local_path = get_local_path(config['PLAYLIST_FILE_PATH'])
        
        with open(playlist_local_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        channel_items = [(name, info) for name, info in raw_data.items()] if format_config["is_dict_format"] else [(f"channel_{idx}", item) for idx, item in enumerate(raw_data)]
        match_success_count = 0
        
        for channel_name, channel_info in channel_items:
            channel_url = get_nested_value(channel_info, format_config["channel_url_path"])
            if not channel_url:
                write_log(f"频道{channel_name}无URL", "STEP2_WARN")
                continue
            
            rtp_url = channel_url
            if format_config["url_replace_rule"]:
                old_str, new_str = format_config["url_replace_rule"]
                if channel_url.startswith(old_str):
                    rtp_url = channel_url.replace(old_str, new_str, 1)
            
            if rtp_url in bjcul_channel_map:
                channel_id = channel_info.get(format_config["channel_id_field"])
                user_channel_id = channel_info.get(format_config["user_channel_id_field"])
                
                if not channel_id:
                    write_log(f"频道{channel_name}无{format_config['channel_id_field']}", "STEP2_WARN")
                    continue
                if not user_channel_id:
                    user_channel_id = f"UN_{str(channel_id)[:8]}"
                
                bjcul_info = bjcul_channel_map[rtp_url]
                matched_channels[channel_id] = {
                    "raw_name": bjcul_info["raw_name"],
                    "clean_name": bjcul_info["clean_name"],
                    "category": bjcul_info["category"],
                    "local_num": str(user_channel_id),
                    "rtp_url": rtp_url,
                    "channel_name": channel_name
                }
                match_success_count += 1
        
        matched_rtp_urls = [v['rtp_url'] for v in matched_channels.values()]
        for rtp_url in all_bjcul_rtp_urls:
            if rtp_url not in matched_rtp_urls and rtp_url in bjcul_channel_map:
                bjcul_info = bjcul_channel_map[rtp_url]
                unmatched_bjcul_channels.append({
                    "type": "unmatched_id",
                    "raw_name": bjcul_info["raw_name"],
                    "clean_name": bjcul_info["clean_name"],
                    "category": bjcul_info["category"],
                    "rtp_url": rtp_url,
                    "local_num": None
                })
        
        unmatched_count = len(unmatched_bjcul_channels)
        print(f"[2/7] 匹配频道ID：{match_success_count} 成功，{unmatched_count} 未匹配")
        write_log(f"匹配完成 - 成功{match_success_count}个，未匹配{unmatched_count}个", "STEP2")

        # 步骤3：处理官方EPG
        write_log("开始处理官方EPG", "STEP3")
        programme_list = []
        channel_time_ranges = {}
        official_fail_count = 0
        channel_has_official_prog = set()
        
        if config['ENABLE_OFFICIAL_EPG']:
            datetime_now = datetime.datetime.now()
            for channel_code in matched_channels.keys():
                channel_info = matched_channels[channel_code]
                raw_name = channel_info["raw_name"]
                local_num = channel_info["local_num"]
                download_fail = True
                channel_prog_count = 0
                
                for day_offset in range(config['EPG_OFFSET_START'], config['EPG_OFFSET_END']):
                    datestr = (datetime_now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
                    url = f"{config['EPG_SERVER_URL']}/schedules/{channel_code}_{datestr}.json"
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
                            new_prog = {
                                "channel": local_num,
                                "start": start_time.strftime(time_format),
                                "stop": end_time.strftime(time_format),
                                "title": title
                            }
                            if add_program_if_no_time_overlap(programme_list, channel_time_ranges, new_prog):
                                channel_prog_count += 1
                            download_fail = False
                            channel_has_official_prog.add(local_num)
                    except Exception as e:
                        write_log(f"解析{raw_name}({channel_code})失败：{str(e)}", "STEP3_ERROR")
                        continue
                
                if download_fail:
                    unmatched_bjcul_channels.append({
                        "type": "official_fail",
                        "raw_name": raw_name,
                        "clean_name": channel_info["clean_name"],
                        "category": channel_info["category"],
                        "rtp_url": channel_info["rtp_url"],
                        "local_num": local_num
                    })
                    official_fail_count += 1
                else:
                    write_log(f"{raw_name}({channel_code})下载{channel_prog_count}条节目（去重后）", "STEP3_DETAIL")
        else:
            write_log("官方EPG关闭，所有匹配ID的频道使用外部源", "STEP3_SKIP")
            for channel_code in matched_channels.keys():
                channel_info = matched_channels[channel_code]
                unmatched_bjcul_channels.append({
                    "type": "official_skip",
                    "raw_name": channel_info["raw_name"],
                    "clean_name": channel_info["clean_name"],
                    "category": channel_info["category"],
                    "rtp_url": channel_info["rtp_url"],
                    "local_num": channel_info["local_num"]
                })
            official_fail_count = len(matched_channels)
        
        total_pending_channels = len(unmatched_bjcul_channels)
        print(f"[3/7] 官方EPG处理：{len(programme_list)} 条节目（去重后），{official_fail_count} 个需匹配外部源")
        write_log(f"官方EPG完成 - 节目{len(programme_list)}条（去重后），需外部匹配{official_fail_count}个", "STEP3")

        # 步骤4：多EPG源匹配（修复外部ID冲突）
        write_log("开始多EPG源匹配", "STEP4")
        temp_local_num_prefix = "unm_"
        temp_num_counter = 1
        total_matched_by_external = 0
        pending_channels = unmatched_bjcul_channels.copy()
        
        channel_has_external_single = set()
        channel_has_external_multi = set()
        
        if not config['ENABLE_EXTERNAL_EPG']:
            write_log("外部EPG总开关关闭，跳过所有外部源匹配", "STEP4_SKIP_ALL")
            print(f"[4/7] 外部EPG总开关关闭，跳过所有外部源匹配")
        else:
            enabled_sources = [s for s in config['EXTERNAL_EPG_SOURCES'] if s.get("enabled", True)]
            write_log(f"外部EPG总开关开启，有效源数量：{len(enabled_sources)}（总源数：{len(config['EXTERNAL_EPG_SOURCES'])}）", "STEP4_SOURCE_COUNT")

            # ========== 新增：初始化全局最终未匹配频道列表 ==========
            # 初始值为所有需要外部匹配的频道（深拷贝，避免修改原列表）
            global_final_unmatched_channels = [channel.copy() for channel in pending_channels]
            # 用于临时存储每个源匹配成功的频道（后续从全局列表移除）
            global_matched_channels = []
            
            for source_idx, epg_source in enumerate(enabled_sources):
                if len(pending_channels) == 0:
                    write_log("无待匹配频道，终止匹配", "STEP4_TERMINATE")
                    break
                
                source_name = epg_source["name"]
                source_url = epg_source["url"]
                is_official = epg_source.get("is_official", False)
                clean_name = epg_source.get("clean_name", True)
                
                write_log(f"处理第{source_idx+1}个源：{source_name} ({source_url})", "STEP4_SOURCE")
                print(f"[4/7] 匹配外部源{source_idx+1}：{source_name}（待匹配{len(pending_channels)}个）")
                
                epg_data = download_url(source_url)
                if not epg_data:
                    write_log(f"源{source_name}下载失败", "STEP4_SOURCE_FAIL")
                    continue
                
                epg_map, epg_identifiers, id_to_name_map, full_channel_info, full_program_info = parse_external_epg(epg_data, is_official)
                if not epg_map or len(epg_identifiers) == 0:
                    write_log(f"源{source_name}解析失败", "STEP4_SOURCE_PARSE_FAIL")
                    continue

                # ===================== 新增：频道重命名逻辑 =====================
                # 读取当前源的重命名规则
                channel_rename_rules = epg_source.get("channel_rename", [])
                if channel_rename_rules and len(channel_rename_rules) > 0:
                    # 构建重命名映射（原名称→新名称）
                    rename_map = {}
                    for old_name, new_name in channel_rename_rules:
                        if old_name and new_name:  # 跳过空规则
                            rename_map[old_name.strip()] = new_name.strip()
                    
                    if rename_map:
                        # 1. 处理epg_map（节目映射的频道名）
                        new_epg_map = {}
                        for old_key, progs in epg_map.items():
                            new_key = rename_map.get(old_key, old_key)
                            if new_key in new_epg_map:
                                new_epg_map[new_key].extend(progs)
                            else:
                                new_epg_map[new_key] = progs
                        epg_map = new_epg_map
                        
                        # 2. 处理频道标识符列表（去重）
                        new_epg_identifiers = []
                        for ident in epg_identifiers:
                            new_ident = rename_map.get(ident, ident)
                            if new_ident not in new_epg_identifiers:
                                new_epg_identifiers.append(new_ident)
                        epg_identifiers = new_epg_identifiers
                        
                        # 3. 处理full_channel_info（频道详情）
                        for cid, info in full_channel_info.items():
                            # 重命名主名称
                            info["main_name"] = rename_map.get(info["main_name"], info["main_name"])
                            # 重命名别名（去重）
                            new_aliases = []
                            for alias in info["aliases"]:
                                new_alias = rename_map.get(alias, alias)
                                if new_alias not in new_aliases:
                                    new_aliases.append(new_alias)
                            info["aliases"] = new_aliases
                        
                        # 4. 处理id_to_name_map（ID→名称映射）
                        new_id_to_name_map = {}
                        for cid, old_name in id_to_name_map.items():
                            new_id_to_name_map[cid] = rename_map.get(old_name, old_name)
                        id_to_name_map = new_id_to_name_map
                        
                        write_log(f"{source_name}完成频道重命名，生效规则数：{len(rename_map)}", "STEP4_RENAME")
                # ===================== 新增结束 =====================
                
                if config['ENABLE_KEEP_OTHER_CHANNELS']:
                    # 收集所有已存在的频道ID（本地+临时+已生成的外部ID）
                    matched_local_nums = [v['local_num'] for v in matched_channels.values()]
                    temp_local_nums = [c['local_num'] for c in unmatched_bjcul_channels if c['local_num'] and c['local_num'].startswith(temp_local_num_prefix)]
                    existing_ids = set(matched_local_nums + temp_local_nums + list(ext_id_mapping.values()))
                
                    # 处理外部频道：强制生成独立ID，仅按名称去重
                    for ext_raw_cid, channel_info in full_channel_info.items():
                        ext_main_name = channel_info["main_name"].strip()
                        ext_aliases = channel_info["aliases"]
                        
                        # 1. 先检查名称是否已存在（外部源之间的同名）
                        if ext_main_name in ext_channel_name_to_final_id:
                            # 名称已存在，关联到已有ID
                            final_id = ext_channel_name_to_final_id[ext_main_name]
                            ext_id_mapping[ext_raw_cid] = final_id
                            write_log(f"外部频道名称[{ext_main_name}]已存在（跨源），关联到ID[{final_id}]（外部原始ID：{ext_raw_cid}）", "STEP4_NAME_DUP")
                            continue
                        
                        # 2. 名称不存在，生成新的唯一ID（避免与本地冲突）
                        new_ext_id = generate_unique_ext_channel_id(existing_ids)
                        ext_id_mapping[ext_raw_cid] = new_ext_id
                        ext_channel_name_to_final_id[ext_main_name] = new_ext_id
                        existing_ids.add(new_ext_id)
                        
                        # 3. 存储外部频道信息
                        all_external_channels[ext_raw_cid] = {
                            "original_id": ext_raw_cid,
                            "final_id": new_ext_id,
                            "main_name": ext_main_name,
                            "aliases": ext_aliases
                        }
                        # 修复：同步更新外部最终ID→信息映射
                        ext_final_id_to_info[new_ext_id] = all_external_channels[ext_raw_cid]
                        write_log(f"新增外部频道：名称[{ext_main_name}]，生成独立ID[{new_ext_id}]（外部原始ID：{ext_raw_cid}）", "STEP4_NEW_EXT_CHANNEL")
                
                    # 处理外部节目：关联到最终ID（本地或新生成的外部ID）
                    for prog in full_program_info:
                        ext_raw_cid = prog["channel_id"]
                        final_cid = ext_id_mapping.get(ext_raw_cid, None)
                        if not final_cid:
                            continue  # 未找到有效ID，跳过
                        all_external_programs.append({
                            "channel": final_cid,
                            "start": prog["start"],
                            "stop": prog["stop"],
                            "title": prog["title"]
                        })
                
                matched_in_this_source = 0
                # ========== 新增：初始化当前源未匹配频道列表 ==========
                source_unmatched_channels = []  # 存储当前源完全未匹配的频道
                next_pending_channels = []
                
                for channel in pending_channels:
                    clean_name_local = channel["clean_name"]
                    raw_name = channel["raw_name"]
                    local_num = channel["local_num"]
                    channel_category = channel.get("category", "")
                    channel_matched = False
                    
                    skip_current_source = False
                    if local_num in channel_has_official_prog:
                        write_log(f"{raw_name}已获取官方节目，跳过当前源补充", "STEP4_SKIP_OFFICIAL")
                        skip_current_source = True
                    elif (
                        (raw_name in config['EXCLUDE_MULTI_SOURCE_CHANNELS'] or channel_category in config['EXCLUDE_MULTI_SOURCE_CATEGORIES'])
                        and local_num in channel_has_external_single
                    ):
                        write_log(f"{raw_name}（分类：{channel_category}）为排除多源频道且已获取外部节目，跳过当前源补充", "STEP4_SKIP_SINGLE")
                        skip_current_source = True
                    
                    if skip_current_source:
                        if not (raw_name in config['EXCLUDE_MULTI_SOURCE_CHANNELS'] or channel_category in config['EXCLUDE_MULTI_SOURCE_CATEGORIES']):
                            next_pending_channels.append(channel)
                        continue
                    
                    is_exclude_multi = (
                        raw_name in config['EXCLUDE_MULTI_SOURCE_CHANNELS'] 
                        or channel_category in config['EXCLUDE_MULTI_SOURCE_CATEGORIES']
                    )
                    
                    if is_official and local_num and not local_num.startswith(temp_local_num_prefix):
                        if local_num in epg_identifiers and epg_map.get(local_num):
                            ext_channel_name = id_to_name_map.get(local_num, f"ID_{local_num}")
                            ext_progs = epg_map[local_num]
                            new_prog_count = 0
                            for prog in ext_progs:
                                new_prog = {
                                    "channel": local_num,
                                    "start": prog["start"],
                                    "stop": prog["stop"],
                                    "title": prog["title"]
                                }
                                if add_program_if_no_time_overlap(programme_list, channel_time_ranges, new_prog):
                                    new_prog_count += 1
                            if new_prog_count > 0:
                                matched_in_this_source += 1
                                total_matched_by_external += 1
                                if is_exclude_multi:
                                    channel_has_external_single.add(local_num)
                                else:
                                    channel_has_external_multi.add(local_num)
                                write_log(f"{raw_name}({local_num})从{source_name}补充{new_prog_count}条节目（去重后）", "STEP4_MATCH_SUCCESS")
                                channel_matched = True
                                # ========== 新增：收集全局匹配成功的频道 ==========
                                global_matched_channels.append(channel.copy())
                    else:
                        match_ext_name = fuzzy_match(clean_name_local, epg_identifiers, clean_name)
                        if match_ext_name and match_ext_name in epg_map:
                            ext_progs = epg_map[match_ext_name]
                            if ext_progs:
                                if not local_num:
                                    local_num = f"{temp_local_num_prefix}{temp_num_counter}"
                                    temp_num_counter += 1
                                    channel["local_num"] = local_num
                                
                                new_prog_count = 0
                                for prog in ext_progs:
                                    new_prog = {
                                        "channel": local_num,
                                        "start": prog["start"],
                                        "stop": prog["stop"],
                                        "title": prog["title"]
                                    }
                                    if add_program_if_no_time_overlap(programme_list, channel_time_ranges, new_prog):
                                        new_prog_count += 1
                                if new_prog_count > 0:
                                    matched_in_this_source += 1
                                    total_matched_by_external += 1
                                    if is_exclude_multi:
                                        channel_has_external_single.add(local_num)
                                    else:
                                        channel_has_external_multi.add(local_num)
                                    write_log(f"{raw_name}({local_num})从{source_name}补充{new_prog_count}条节目（去重后）", "STEP4_MATCH_SUCCESS")
                                    channel_matched = True
                                # ========== 新增：收集全局匹配成功的频道 ==========
                                global_matched_channels.append(channel.copy())
                        else:
                            # ========== 新增：当前源未匹配，加入未匹配列表 ==========
                            source_unmatched_channels.append(channel)
                    
                    if not is_exclude_multi:
                        next_pending_channels.append(channel)
                    else:
                        if not channel_matched:
                            next_pending_channels.append(channel)
                        else:
                            write_log(f"{raw_name}（分类：{channel_category}）为排除多源频道，匹配成功后不再参与后续源", "STEP4_EXCLUDE_MULTI")

                pending_channels = next_pending_channels
                write_log(f"{source_name}匹配完成 - 补充{matched_in_this_source}个频道的节目，剩余{len(pending_channels)}个待补充", "STEP4_SOURCE_SUMMARY")
                print(f"  → 源{source_idx+1}补充{matched_in_this_source}个频道的节目，剩余{len(pending_channels)}个待补充")

                # ========== 新增：输出当前源未匹配频道列表 ==========
                if len(source_unmatched_channels) > 0:
                    write_log(f"=== 源{source_idx+1}完全未匹配频道明细（共{len(source_unmatched_channels)}个）===", "STEP4_SOURCE_UNMATCHED")
                    for idx, unmatch_channel in enumerate(source_unmatched_channels, 1):
                        raw_name = unmatch_channel.get("raw_name", "未知名称")
                        category = unmatch_channel.get("category", "未知分类")
                        local_num = unmatch_channel.get("local_num", "无本地ID")
                        log_content = f"[{idx}] 名称：{raw_name} | 分类：{category} | 本地ID：{local_num}"
                        write_log(log_content, "STEP4_UNMATCHED_ITEM")
                    write_log("=== 未匹配频道明细结束 ===", "STEP4_SOURCE_UNMATCHED")
    
                    # 控制台输出简洁版（前20个）
                    unmatch_names = [c.get("raw_name", "未知") for c in source_unmatched_channels]
                    print(f"  → 源{source_idx+1}完全未匹配频道：{unmatch_names[:20]}{'...' if len(unmatch_names) > 20 else ''}")
                else:
                    print(f"  → 源{source_idx+1}无完全未匹配频道")

                # ========== 新增：从全局最终未匹配列表中，移除当前源匹配成功的频道 ==========
                # 定义频道唯一标识（避免重名冲突，用raw_name+rtp_url）
                def get_channel_unique_key(chan):
                    return f"{chan.get('raw_name', '')}_{chan.get('rtp_url', '')}"

                # 提取当前源匹配成功的频道唯一标识
                matched_keys = [get_channel_unique_key(chan) for chan in global_matched_channels]
                # 筛选全局未匹配列表，移除已匹配的频道
                global_final_unmatched_channels = [
                    chan for chan in global_final_unmatched_channels
                    if get_channel_unique_key(chan) not in matched_keys
                ]
                # 清空当前源匹配列表，为下一个源做准备
                global_matched_channels.clear()



        total_unmatched_final = len(pending_channels)
        print(f"[5/7] 多源匹配完成：总计{total_matched_by_external}个，剩余{total_unmatched_final}个未匹配")
        write_log(f"多源匹配汇总 - 成功{total_matched_by_external}个，未匹配{total_unmatched_final}个", "STEP4_FINAL_SUMMARY")

        # ========== 新增：输出全局最终未匹配频道详细日志（所有源都没匹配） ==========
        global_unmatched_count = len(global_final_unmatched_channels)
        if global_unmatched_count > 0:
            write_log("="*50 + " 全局最终未匹配频道明细（所有源都未补充节目） " + "="*50, "STEP4_GLOBAL_FINAL_UNMATCHED")
            write_log(f"全局最终未匹配频道总数：{global_unmatched_count}个", "STEP4_GLOBAL_FINAL_UNMATCHED")
            # 输出完整明细到日志
            for idx, unmatch_channel in enumerate(global_final_unmatched_channels, 1):
                raw_name = unmatch_channel.get("raw_name", "未知名称")
                category = unmatch_channel.get("category", "未知分类")
                local_num = unmatch_channel.get("local_num", "无本地ID")
                rtp_url = unmatch_channel.get("rtp_url", "无RTP地址")
                channel_type = unmatch_channel.get("type", "未知类型")
                log_content = f"[{idx}] 名称：{raw_name} | 分类：{category} | 本地ID：{local_num} | 类型：{channel_type} | RTP：{rtp_url[:50]}..."
                write_log(log_content, "STEP4_GLOBAL_UNMATCHED_ITEM")
            write_log("="*110, "STEP4_GLOBAL_FINAL_UNMATCHED")
    
            # 控制台输出简洁版（方便快速查看）
            global_unmatch_names = [c.get("raw_name", "未知") for c in global_final_unmatched_channels]
            print(f"  → 全局最终未匹配（所有源都没匹配）：共{global_unmatched_count}个，名称：{global_unmatch_names[:30]}{'...' if len(global_unmatch_names) > 30 else ''}")
        else:
            write_log("全局无最终未匹配频道，所有需要补充的频道都已被至少一个源匹配成功", "STEP4_GLOBAL_FINAL_MATCH_ALL")
            print(f"  → 全局无最终未匹配频道，所有频道都已被补充节目")

        # 步骤5：生成XML（修复外部ID冲突+漏加问题）
        write_log("开始生成精简版EPG XML", "STEP5_LITE")
        root_lite = ET.Element("tv", {
            "generator-info-name": "MY EPG Generator v4.1 (Lite)",
            "generator-info-url": "https://github.com/jackycher/my-epg-generator",
            "generated-time": "UTC" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        channel_add_count = 0
        # 收集本地频道名称→ID映射（用于完整版名称去重）
        local_channel_name_to_id = {}
        for channel_code in matched_channels.keys():
            channel_info = matched_channels[channel_code]
            local_num = channel_info["local_num"]
            raw_name = channel_info["raw_name"].strip()
            channel_elem = ET.SubElement(root_lite, "channel", {"id": local_num})
            ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = raw_name
            channel_add_count += 1
            local_channel_name_to_id[raw_name] = local_num  # 本地名称→ID映射
        
        temp_channel_add_count = 0
        for channel in unmatched_bjcul_channels:
            if channel["local_num"] and channel["local_num"].startswith(temp_local_num_prefix):
                local_num = channel["local_num"]
                raw_name = channel["raw_name"].strip()
                channel_elem = ET.SubElement(root_lite, "channel", {"id": local_num})
                ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = raw_name
                temp_channel_add_count += 1
                local_channel_name_to_id[raw_name] = local_num  # 临时频道名称→ID映射
        
        seen_progs_lite = set()
        sorted_progs_lite = sorted(programme_list, key=lambda x: (x["channel"], x["start"]))
        prog_add_count_lite = 0
        non_unknown_count_lite = 0
        
        for prog in sorted_progs_lite:
            if not prog.get("channel") or not prog.get("start") or not prog.get("title"):
                continue
            key = (prog["channel"], prog["start"], prog["title"])
            if key in seen_progs_lite:
                continue
            seen_progs_lite.add(key)
            
            prog_elem = ET.SubElement(root_lite, "programme", {
                "start": prog["start"],
                "stop": prog["stop"],
                "channel": prog["channel"]
            })
            ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
            prog_add_count_lite += 1
            if prog["title"] != "未知节目":
                non_unknown_count_lite += 1
        
        ET.ElementTree(root_lite).write(
            config['EPG_SAVE_PATH'],
            encoding="UTF-8",
            xml_declaration=True,
            short_empty_elements=False
        )
        os.chmod(config['EPG_SAVE_PATH'], 0o644)
        print(f"[6/7] 生成精简版EPG：{config['EPG_SAVE_PATH']}（{prog_add_count_lite}条节目）")
        write_log(f"精简版XML生成成功：{config['EPG_SAVE_PATH']}，总频道{channel_add_count + temp_channel_add_count}个（txt{channel_add_count} + 临时{temp_channel_add_count}）", "STEP5_LITE")
        
        print("[6/7] 压缩精简版为epg.xml.gz...")
        compress_xml_to_gz(config['EPG_SAVE_PATH'], config['EPG_GZ_PATH'])

        other_channel_add_count = 0
        prog_add_count_full = 0
        non_unknown_count_full = 0
        if config['ENABLE_KEEP_OTHER_CHANNELS'] and ext_channel_name_to_final_id:  # 修复：判断是否有外部频道
            write_log("开始生成完整版EPG XML", "STEP5_FULL")
            root_full = ET.Element("tv", {
                "generator-info-name": "MY EPG Generator v4.1 (Full)",
                "generator-info-url": "https://github.com/jackycher/my-epg-generator",
                "generated-time": "UTC" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
            # 复制精简版的频道到完整版
            for channel_elem in root_lite.findall(".//channel"):
                new_channel = ET.SubElement(root_full, "channel", {"id": channel_elem.get("id")})
                for dn_elem in channel_elem.findall(".//display-name"):
                    dn_text = dn_elem.text.strip()
                    ET.SubElement(new_channel, "display-name", {"lang": "zh"}).text = dn_text
            
            # 合并本地+外部的名称→ID映射（用于最终名称去重）
            final_channel_name_to_id = local_channel_name_to_id.copy()
            existing_channel_ids = set([c.get("id") for c in root_full.findall(".//channel")])
            
            # 修复：遍历去重后的外部频道名称→ID，仅过滤本地同名频道
            for ext_main_name, ext_final_id in ext_channel_name_to_final_id.items():
                # 核心修复：仅过滤本地txt中已有的频道名称
                if ext_main_name in local_channel_names:
                    write_log(f"外部频道名称[{ext_main_name}]已存在于本地txt，跳过添加（外部最终ID：{ext_final_id}）", "STEP5_FULL_NAME_DUP")
                    continue
                
                # 确保ID不冲突（双重保障）
                if ext_final_id in existing_channel_ids:
                    write_log(f"外部频道最终ID[{ext_final_id}]冲突，重新生成ID（名称：{ext_main_name}）", "STEP5_FULL_ID_CONFLICT")
                    ext_final_id = generate_unique_ext_channel_id(existing_channel_ids)
                
                # 获取频道信息
                channel_info = ext_final_id_to_info.get(ext_final_id)
                if not channel_info:
                    write_log(f"未找到外部频道ID[{ext_final_id}]的信息，跳过", "STEP5_FULL_NO_INFO")
                    continue
                
                # 添加外部频道
                channel_elem = ET.SubElement(root_full, "channel", {"id": ext_final_id})
                ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = ext_main_name
                for alias in channel_info["aliases"][1:]:
                    alias_text = alias.strip()
                    # 别名也过滤本地同名
                    if alias_text not in local_channel_names and alias_text not in final_channel_name_to_id:
                        ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = alias_text
                
                # 更新映射和ID集合
                final_channel_name_to_id[ext_main_name] = ext_final_id
                existing_channel_ids.add(ext_final_id)
                other_channel_add_count += 1
                write_log(f"添加外部频道：名称[{ext_main_name}]，ID[{ext_final_id}]（外部原始ID：{channel_info['original_id']}）", "STEP5_FULL_ADD_EXT")
            
            write_log(f"添加外部源其他频道：{other_channel_add_count}个（过滤{len(ext_channel_name_to_final_id)-other_channel_add_count}个本地同名频道）", "STEP5_FULL_CHANNELS")
            
            # 收集所有节目（本地+外部）
            all_programs_full = []
            all_programs_full.extend(programme_list)
            all_programs_full.extend(all_external_programs)
            
            # 过滤有效节目并排序
            valid_progs_full = []
            for prog in all_programs_full:
                if isinstance(prog, dict) and "channel" in prog and "start" in prog and "title" in prog and prog["channel"] in existing_channel_ids:
                    valid_progs_full.append(prog)
            
            sorted_progs_full = sorted(valid_progs_full, key=lambda x: (x["channel"], x["start"]))
            
            # 去重并添加节目
            seen_progs_full = set()
            for prog in sorted_progs_full:
                if not prog.get("channel") or not prog.get("start") or not prog.get("title"):
                    continue
                key = (prog["channel"], prog["start"], prog["title"])
                if key in seen_progs_full:
                    continue
                seen_progs_full.add(key)
                
                prog_elem = ET.SubElement(root_full, "programme", {
                    "start": prog["start"],
                    "stop": prog["stop"],
                    "channel": prog["channel"]
                })
                ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
                prog_add_count_full += 1
                if prog["title"] != "未知节目":
                    non_unknown_count_full += 1
            
            ET.ElementTree(root_full).write(
                config['EPG_FULL_SAVE_PATH'],
                encoding="UTF-8",
                xml_declaration=True,
                short_empty_elements=False
            )
            os.chmod(config['EPG_FULL_SAVE_PATH'], 0o644)
            print(f"[6/7] 生成完整版EPG：{config['EPG_FULL_SAVE_PATH']}（去重后{prog_add_count_full}条，新增外部频道{other_channel_add_count}个）")
            write_log(f"完整版XML生成成功：{config['EPG_FULL_SAVE_PATH']}，总频道{channel_add_count + temp_channel_add_count + other_channel_add_count}个", "STEP5_FULL")
            
            print("[6/7] 压缩完整版为epg_full.xml.gz...")
            compress_xml_to_gz(config['EPG_FULL_SAVE_PATH'], config['EPG_FULL_GZ_PATH'])
        else:
            write_log("未开启保留其他频道或无外部频道，跳过完整版生成", "STEP5_FULL_SKIP")

        write_log("统计运行结果", "STEP6")
        end_time = datetime.datetime.now()
        run_duration = (end_time - start_time).total_seconds()
        
        summary = {
            "总耗时(秒)": round(run_duration, 2),
            "bjcul有效频道": total_valid_channels,
            "匹配ID频道": match_success_count,
            "外部匹配成功": total_matched_by_external,
            "最终未匹配": total_unmatched_final,
            "精简版EPG频道数": channel_add_count + temp_channel_add_count,
            "精简版EPG节目数(去重)": prog_add_count_lite,
            "精简版非未知节目": non_unknown_count_lite
        }
        if config['ENABLE_KEEP_OTHER_CHANNELS'] and ext_channel_name_to_final_id:
            summary["完整版EPG频道数"] = channel_add_count + temp_channel_add_count + other_channel_add_count
            summary["完整版EPG节目数(去重)"] = prog_add_count_full
            summary["完整版非未知节目"] = non_unknown_count_full
        
        print(f"[7/7] 运行完成：耗时{summary['总耗时(秒)']}秒，精简版非未知节目{summary['精简版非未知节目']}条")
        write_log("="*30 + " 运行结果 " + "="*30, "STEP6_SUMMARY")
        for key, value in summary.items():
            write_log(f"{key}：{value}", "STEP6_SUMMARY")
        write_log("="*60 + " EPG生成完成 " + "="*60 + "\n\n", "END")
        
    except Exception as e:
        error_info = f"运行异常：{str(e)}\n{traceback.format_exc()}"
        write_log(error_info, "FATAL")
        print(f"❌ EPG运行异常：{str(e)}")
        print(f"详细日志：{config['LOG_PATH']}")
        sys.exit(1)

if __name__ == "__main__":
    print("="*60)
    print("独立运行EPG生成脚本（修复外部源ID冲突+完整版漏加问题，仅按名称去重）")
    print("="*60)
    epg_main()
