import os
import xml.etree.ElementTree as ET
import json
from flask import Flask, request, jsonify

# 初始化Flask应用（适配Vercel的WSGI规范）
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # 强制JSON输出中文（非ASCII）

# ===================== 核心函数：解析XMLTV为DIYP格式 =====================
def parse_xml_to_diyp(xml_path="epg.xml"):
    """解析epg.xml（XMLTV格式）为DIYP EPG JSON结构"""
    # 检查epg.xml是否存在
    if not os.path.exists(xml_path):
        return {"error": "epg.xml文件不存在", "epg": []}
    
    try:
        # 解析XML（指定UTF-8编码，避免乱码）
        tree = ET.parse(xml_path, parser=ET.XMLParser(encoding='utf-8'))
        root = tree.getroot()

        # 第一步：提取所有频道信息
        channel_map = {}  # 频道ID → 频道信息
        for channel in root.findall("channel"):
            channel_id = channel.get("id")
            if not channel_id:
                continue
            
            # 提取频道名称（优先中文）
            channel_name = "未知频道"
            for name_elem in channel.findall("display-name"):
                lang = name_elem.get("lang", "")
                if lang in ["zh", "zh-CN", ""]:  # 优先中文/无语言标识的名称
                    channel_name = name_elem.text.strip() if name_elem.text else "未知频道"
                    break
            
            # 提取频道LOGO
            logo = ""
            icon_elem = channel.find("icon")
            if icon_elem is not None:
                logo = icon_elem.get("src", "").strip()
            
            # 初始化频道节目列表
            channel_map[channel_id] = {
                "name": channel_name,
                "tvgid": channel_id,
                "logo": logo,
                "program": []
            }

        # 第二步：提取节目信息并关联到频道
        for programme in root.findall("programme"):
            channel_id = programme.get("channel")
            if channel_id not in channel_map:
                continue  # 跳过无对应频道的节目
            
            # 处理节目时间（XMLTV格式：20250101080000 +0800 → 保留纯时间）
            start_time = programme.get("start", "").split(" ")[0]
            stop_time = programme.get("stop", "").split(" ")[0]
            if not start_time or not stop_time:
                continue
            
            # 提取节目名称（优先中文）
            program_title = "未知节目"
            for title_elem in programme.findall("title"):
                lang = title_elem.get("lang", "")
                if lang in ["zh", "zh-CN", ""]:
                    program_title = title_elem.text.strip() if title_elem.text else "未知节目"
                    break
            
            # 添加节目到对应频道
            channel_map[channel_id]["program"].append({
                "start": start_time,
                "end": stop_time,
                "title": program_title
            })

        # 转换为DIYP最终格式（列表）
        diyp_epg = [v for v in channel_map.values()]
        return {"epg": diyp_epg}
    
    except Exception as e:
        return {"error": f"解析XML失败：{str(e)}", "epg": []}

# ===================== Flask接口：带ch参数的DIYP EPG =====================
@app.route("/diyp_epg.json", methods=["GET"])
def diyp_epg_api():
    """
    DIYP EPG接口：
    - 无参数：返回全量EPG
    - ?ch=XXX：筛选频道（模糊匹配名称/TVGID，忽略大小写）
    """
    # 1. 解析URL参数ch
    ch_param = request.args.get("ch", "").strip().lower()
    
    # 2. 解析epg.xml为DIYP格式
    epg_result = parse_xml_to_diyp()
    if "error" in epg_result:
        return jsonify(epg_result), 500  # 解析失败返回500错误
    
    # 3. 筛选频道（带ch参数时）
    epg_list = epg_result["epg"]
    if ch_param:
        filtered_epg = []
        for channel in epg_list:
            # 模糊匹配：频道名称/TVGID包含ch参数（忽略大小写）
            if ch_param in channel["name"].lower() or ch_param in channel["tvgid"].lower():
                filtered_epg.append(channel)
        epg_result["epg"] = filtered_epg
    
    # 4. 返回JSON响应（UTF-8编码）
    return jsonify(epg_result)

# ===================== 适配Vercel的WSGI入口 =====================
# Vercel要求暴露WSGI应用实例，名称必须为app
application = app.wsgi_app

# 本地测试用（部署到Vercel时不会执行）
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
