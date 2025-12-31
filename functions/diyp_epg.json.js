/**
 * Cloudflare Pages Functions EPG 转换脚本
 * 路径：/functions/diyp_epg.js（建议重命名文件为 diyp_epg.js，避免 .json.js 后缀解析问题）
 * 访问：https://your-pages-domain/diyp_epg?ch=央视综合&date=2025-12-31
 */

// 配置项（根据实际情况修改）
const CONFIG = {
  // EPG.xml 源地址（替换为你的仓库地址）
  EPG_XML_URL: "https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml",
  // 是否开启繁体转简体
  CHT_TO_CHS: true,
  // 未找到数据时是否返回默认24小时节目
  RET_DEFAULT: true,
  // 默认URL（与PHP脚本保持一致）
  DEFAULT_URL: "https://github.com/taksssss/iptv-tool",
  // 台标基础URL（可根据实际配置修改）
  ICON_BASE_URL: "https://your-pages-domain/data/icon/"
};

// 简繁转换核心映射（基础版）
const CHT_TO_CHS_MAP = new Map([
  ["體", "体"], ["華", "华"], ["臺", "台"], ["灣", "湾"], ["語", "语"],
  ["鍵", "键"], ["裡", "里"], ["後", "后"], ["麼", "么"], ["倆", "俩"],
  ["請", "请"], ["進", "进"], ["岀", "出"], ["並", "并"], ["發", "发"],
  ["電", "电"], ["視", "视"], ["訊", "讯"], ["數", "数"], ["據", "据"],
  ["網", "网"], ["軟", "软"], ["硬", "硬"], ["機", "机"], ["構", "构"]
]);

/**
 * 清理频道名（去空格、特殊字符、简繁转换）
 * @param {string} channelName 原始频道名
 * @returns {string} 清理后的频道名
 */
function cleanChannelName(channelName) {
  if (!channelName) return "";
  // 去除空格、特殊字符
  let cleanName = channelName.trim().replace(/[^\u4e00-\u9fa5a-zA-Z0-9]/g, "");
  // 繁体转简体
  if (CONFIG.CHT_TO_CHS) {
    cleanName = Array.from(cleanName).map(char => 
      CHT_TO_CHS_MAP.get(char) || char
    ).join("");
  }
  return cleanName;
}

/**
 * 获取格式化日期（兼容PHP的getFormatTime逻辑）
 * @param {string} dateStr 原始日期字符串（如20251231、2025-12-31）
 * @returns {string} 格式化后的日期（YYYY-MM-DD）
 */
function getFormatDate(dateStr) {
  if (!dateStr) return new Date().toISOString().split("T")[0];
  
  // 去除非数字字符
  const numDate = dateStr.replace(/\D+/g, "");
  if (numDate.length < 8) return new Date().toISOString().split("T")[0];
  
  // 解析YYYYMMDD格式
  const year = numDate.slice(0, 4);
  const month = numDate.slice(4, 6);
  const day = numDate.slice(6, 8);
  return `${year}-${month}-${day}`;
}

/**
 * 匹配台标URL（模拟PHP的iconUrlMatch）
 * @param {string} channelName 频道名
 * @returns {string} 台标URL
 */
function getIconUrl(channelName) {
  // 简化实现，可根据实际台标命名规则扩展
  const cleanName = cleanChannelName(channelName).replace(/\s+/g, "");
  return `${CONFIG.ICON_BASE_URL}${encodeURIComponent(cleanName)}.png`;
}

/**
 * 解析EPG XML并提取指定频道+日期的节目单
 * @param {string} xmlStr XML字符串
 * @param {string} targetChannel 目标频道名（已清理）
 * @param {string} targetDate 目标日期（YYYY-MM-DD）
 * @returns {Array|null} 节目单数组（null表示未找到）
 */
function parseEpgXml(xmlStr, targetChannel, targetDate) {
  try {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xmlStr, "application/xml");
    
    // 查找匹配的频道（优先精准匹配，其次模糊匹配）
    let channelNode = null;
    const allChannels = doc.querySelectorAll("channel");
    
    // 1. 精准匹配
    channelNode = Array.from(allChannels).find(channel => {
      const displayName = channel.querySelector("display-name")?.textContent || "";
      return cleanChannelName(displayName) === targetChannel;
    });
    
    // 2. 模糊匹配（频道名包含目标关键词）
    if (!channelNode) {
      channelNode = Array.from(allChannels).find(channel => {
        const displayName = channel.querySelector("display-name")?.textContent || "";
        return cleanChannelName(displayName).includes(targetChannel);
      });
    }
    
    if (!channelNode) return null;
    
    const channelId = channelNode.getAttribute("id");
    const targetDateTs = new Date(targetDate).getTime();
    const nextDateTs = targetDateTs + 24 * 60 * 60 * 1000;
    
    // 提取该频道指定日期的节目
    const programmes = Array.from(doc.querySelectorAll(`programme[channel="${channelId}"]`))
      .map(prog => {
        // 兼容EPG XML的start/stop格式（如 20251231021500 +0800）
        const startStr = prog.getAttribute("start")?.replace(/\s+|\+\d+/g, "") || "";
        const stopStr = prog.getAttribute("stop")?.replace(/\s+|\+\d+/g, "") || "";
        
        // 解析时间（YYYYMMDDHHMMSS）
        const parseTime = (timeStr) => {
          if (timeStr.length < 12) return null;
          const year = timeStr.slice(0,4), month = timeStr.slice(4,6), day = timeStr.slice(6,8);
          const hour = timeStr.slice(8,10), min = timeStr.slice(10,12);
          return new Date(`${year}-${month}-${day}T${hour}:${min}:00`);
        };
        
        const start = parseTime(startStr);
        const end = parseTime(stopStr);
        
        // 过滤目标日期的节目
        if (!start || !end || start.getTime() < targetDateTs || start.getTime() >= nextDateTs) {
          return null;
        }
        
        return {
          start: start.toTimeString().slice(0, 5), // HH:MM
          end: end.toTimeString().slice(0, 5),
          title: prog.querySelector("title")?.textContent || "",
          desc: prog.querySelector("desc")?.textContent || ""
        };
      })
      .filter(Boolean)
      .sort((a, b) => a.start.localeCompare(b.start));
    
    return programmes.length > 0 ? programmes : null;
  } catch (e) {
    console.error("解析XML失败:", e);
    return null;
  }
}

/**
 * 构建默认EPG数据（24小时精彩节目）
 * @param {string} channelName 频道名
 * @param {string} date 日期
 * @returns {Object} 默认数据
 */
function getDefaultEpgData(channelName, date) {
  // 修复核心语法错误：先算 (hour+1)%24，再转字符串补零
  const epgData = Array.from({ length: 24 }, (_, hour) => {
    const nextHour = (hour + 1) % 24; // 先计算取模
    return {
      start: `${hour.toString().padStart(2, "0")}:00`,
      end: `${nextHour.toString().padStart(2, "0")}:00`, // 修正后的写法
      title: "精彩节目",
      desc: ""
    };
  });
  
  return {
    channel_name: cleanChannelName(channelName),
    date: date,
    url: CONFIG.DEFAULT_URL,
    icon: getIconUrl(channelName),
    epg_data: CONFIG.RET_DEFAULT ? epgData : ""
  };
}

/**
 * 主请求处理函数（Cloudflare Pages Functions 入口）
 */
export async function onRequest(context) {
  try {
    const { request } = context;
    const url = new URL(request.url);
    const queryParams = Object.fromEntries(url.searchParams.entries());
    
    // 解析参数（兼容ch/channel参数，date参数）
    const oriChannelName = queryParams.ch || queryParams.channel || "";
    const cleanChannel = cleanChannelName(oriChannelName);
    const targetDate = getFormatDate(queryParams.date);
    
    // 频道名为空时返回404
    if (!cleanChannel) {
      return new Response("404 Not Found. <br>未指定频道参数", {
        status: 404,
        headers: { "Content-Type": "text/html; charset=utf-8" }
      });
    }
    
    // 1. 从Cache API获取缓存的EPG XML（缓存24小时）
    const cache = caches.default;
    let cachedResponse = await cache.match(CONFIG.EPG_XML_URL);
    let xmlStr;
    
    // 2. 缓存未命中则下载XML
    if (!cachedResponse) {
      const xmlResponse = await fetch(CONFIG.EPG_XML_URL, {
        headers: { "User-Agent": "Cloudflare Pages EPG Fetcher" },
        cf: { cacheTtl: 86400 } // Cloudflare边缘缓存
      });
      
      if (!xmlResponse.ok) throw new Error(`EPG XML下载失败: ${xmlResponse.status}`);
      xmlStr = await xmlResponse.text();
      
      // 缓存XML（24小时）
      await cache.put(CONFIG.EPG_XML_URL, new Response(xmlStr, {
        headers: {
          "Cache-Control": "max-age=86400",
          "Content-Type": "application/xml; charset=utf-8"
        }
      }));
    } else {
      xmlStr = await cachedResponse.text();
    }
    
    // 3. 解析XML获取节目单
    const epgProgrammes = parseEpgXml(xmlStr, cleanChannel, targetDate);
    
    // 4. 构建响应数据
    let responseData;
    if (epgProgrammes) {
      responseData = {
        channel_name: cleanChannelName(oriChannelName),
        date: targetDate,
        url: CONFIG.DEFAULT_URL,
        icon: getIconUrl(oriChannelName),
        epg_data: epgProgrammes
      };
    } else {
      // 未找到数据返回默认值
      responseData = getDefaultEpgData(oriChannelName, targetDate);
    }
    
    // 5. 返回JSON响应（兼容PHP的响应头）
    return new Response(JSON.stringify(responseData, null, 2), {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "max-age=3600" // 响应缓存1小时
      }
    });
    
  } catch (error) {
    // 异常处理
    console.error("请求处理失败:", error);
    return new Response(JSON.stringify({
      error: "服务器错误",
      message: error.message
    }), {
      status: 500,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*"
      }
    });
  }
}
