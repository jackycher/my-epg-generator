/**
 * Cloudflare Pages EPG 转 diyp 格式脚本
 * 路径：/functions/diyp_epg.js
 * 访问：https://你的域名/diyp_epg?ch=CCTV13&date=20251224
 */

const CONFIG = {
  // EPG源地址（替换为你的真实地址）
  EPG_XML_URL: "https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml",
  CHT_TO_CHS: false, // 你的XML已是简体，无需转换
  RET_DEFAULT: true,
  DEFAULT_URL: "https://github.com/taksssss/iptv-tool",
  ICON_BASE_URL: "https://你的域名/data/icon/",
  DEBUG: true // 调试模式：加 ?debug=1 可查看详细匹配日志
};

/**
 * 1. 修复频道名清理（统一大小写+保留核心标识）
 */
function cleanChannelName(channelName) {
  if (!channelName) return "";
  // 统一转大写，解决 CCTV13 vs cctv13 匹配问题
  let cleanName = channelName.trim().toUpperCase();
  // 移除非核心字符（保留字母、数字、中文）
  cleanName = cleanName.replace(/[^\u4e00-\u9fa5A-Z0-9+]/g, "");
  
  // 处理特殊频道（如 CCTV5+）
  if (cleanName.includes("5+")) cleanName = cleanName.replace("5+", "5PLUS");
  
  return cleanName;
}

/**
 * 2. 修复日期格式化（兼容多种输入格式）
 */
function getFormatDate(dateStr) {
  if (!dateStr) return new Date().toISOString().split("T")[0];
  
  // 清理非数字字符
  const numDate = dateStr.replace(/\D+/g, "");
  if (numDate.length < 8) return new Date().toISOString().split("T")[0];
  
  // 统一转为 YYYY-MM-DD 格式
  const year = numDate.slice(0, 4);
  const month = numDate.slice(4, 6);
  const day = numDate.slice(6, 8);
  return `${year}-${month}-${day}`;
}

/**
 * 3. 修复时间解析（兼容 EPG 标准格式：YYYYMMDDHHMMSS +0800）
 */
function parseEpgTime(timeStr) {
  if (!timeStr) return null;
  
  // 提取核心时间部分（去除时区和空格）
  const cleanTime = timeStr.split(" ")[0];
  if (cleanTime.length !== 14) return null; // 必须是 YYYYMMDDHHMMSS 格式
  
  const year = cleanTime.slice(0, 4);
  const month = cleanTime.slice(4, 6);
  const day = cleanTime.slice(6, 8);
  const hour = cleanTime.slice(8, 10);
  const min = cleanTime.slice(10, 12);
  
  // 返回本地时间（考虑时区偏移）
  return new Date(`${year}-${month}-${day}T${hour}:${min}:00`);
}

/**
 * 4. 优化 XML 解析逻辑（支持 lang 属性+精准匹配）
 */
function parseEpgXml(xmlStr, targetChannel, targetDate, debug = false) {
  const debugInfo = {
    target: { channel: targetChannel, date: targetDate },
    xmlChannels: [],
    matchedChannel: null,
    matchedProgrammes: [],
    error: null
  };

  try {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xmlStr, "application/xml");

    // 收集所有频道信息（调试用）
    const allChannels = doc.querySelectorAll("channel");
    debugInfo.xmlChannels = Array.from(allChannels).map(channel => {
      const id = channel.getAttribute("id");
      const displayName = channel.querySelector('display-name[lang="zh"]')?.textContent || 
                          channel.querySelector("display-name")?.textContent || "";
      return {
        id: id,
        displayName: displayName,
        cleanName: cleanChannelName(displayName)
      };
    });

    // 1. 精准匹配（统一大小写后完全一致）
    let channelNode = Array.from(allChannels).find(channel => {
      const displayName = channel.querySelector('display-name[lang="zh"]')?.textContent || 
                          channel.querySelector("display-name")?.textContent || "";
      return cleanChannelName(displayName) === targetChannel;
    });

    // 2. 模糊匹配（备选方案）
    if (!channelNode) {
      channelNode = Array.from(allChannels).find(channel => {
        const displayName = channel.querySelector('display-name[lang="zh"]')?.textContent || 
                            channel.querySelector("display-name")?.textContent || "";
        return cleanChannelName(displayName).includes(targetChannel);
      });
    }

    if (!channelNode) {
      debugInfo.error = "未找到匹配的频道";
      return debug; // 返回调试信息
    }

    debugInfo.matchedChannel = {
      id: channelNode.getAttribute("id"),
      displayName: channelNode.querySelector('display-name[lang="zh"]')?.textContent || ""
    };

    // 过滤目标日期的节目
    const channelId = channelNode.getAttribute("id");
    const targetDateObj = new Date(targetDate);
    const nextDateObj = new Date(targetDateObj);
    nextDateObj.setDate(nextDateObj.getDate()+1); // 次日 00:00:00

    const allProgrammes = doc.querySelectorAll(`programme[channel="${channelId}"]`);
    debugInfo.matchedProgrammes = Array.from(allProgrammes).map(prog => {
      const start = parseEpgTime(prog.getAttribute("start"));
      const stop = parseEpgTime(prog.getAttribute("stop"));
      const title = prog.querySelector('title[lang="zh"]')?.textContent || 
                    prog.querySelector("title")?.textContent || "";
      const desc = prog.querySelector('desc[lang="zh"]')?.textContent || 
                   prog.querySelector("desc")?.textContent || "";

      return {
        start: start,
        stop: stop,
        title: title,
        desc: desc,
        isTargetDate: start && start >= targetDateObj && start < nextDateObj
      };
    }).filter(prog => prog.isTargetDate); // 只保留目标日期的节目

    return debugInfo;

  } catch (e) {
    debugInfo.error = `XML解析失败: ${e.message}`;
    return debugInfo;
  }
}

/**
 * 5. 生成默认节目数据（兜底方案）
 */
function getDefaultEpgData(channelName, date) {
  return {
    channel_name: cleanChannelName(channelName),
    date: date,
    url: CONFIG.DEFAULT_URL,
    icon: `${CONFIG.ICON_BASE_URL}${encodeURIComponent(cleanChannelName(channelName))}.png`,
    epg_data: CONFIG.RET_DEFAULT ? Array.from({ length: 24 }, (_, hour) => {
      const nextHour = (hour + 1) % 24;
      return {
        start: `${hour.toString().padStart(2, "0")}:00`,
        end: `${nextHour.toString().padStart(2, "0")}:00`,
        title: "精彩节目",
        desc: ""
      };
    }) : []
  };
}

/**
 * 主处理函数（Cloudflare Pages Functions 入口）
 */
export async function onRequest(context) {
  try {
    const { request } = context;
    const url = new URL(request.url);
    const queryParams = Object.fromEntries(url.searchParams.entries());
    const isDebug = queryParams.debug === "1" || CONFIG.DEBUG;

    // 1. 解析核心参数
    const oriChannelName = queryParams.ch || queryParams.channel || "";
    const cleanChannel = cleanChannelName(oriChannelName);
    const targetDate = getFormatDate(queryParams.date);

    // 2. 频道为空时返回 404
    if (!cleanChannel) {
      return new Response("404 Not Found. <br>未指定频道参数", {
        status: 404,
        headers: { "Content-Type": "text/html; charset=utf-8" }
      });
    }

    // 3. 获取 EPG XML（优先缓存，缓存 24 小时）
    const cache = caches.default;
    let cachedResponse = await cache.match(CONFIG.EPG_XML_URL);
    let xmlStr;

    // 缓存未命中则重新下载
    if (!cachedResponse) {
      const xmlResponse = await fetch(CONFIG.EPG_XML_URL, {
        headers: { "User-Agent": "Cloudflare EPG Fetcher" },
        cf: { cacheTtl: 86400 } // 边缘缓存 24 小时
      });

      if (!xmlResponse.ok) {
        throw new Error(`EPG XML 下载失败: ${xmlResponse.status}`);
      }

      xmlStr = await xmlResponse.text();
      // 存入缓存
      await cache.put(CONFIG.EPG_XML_URL, new Response(xmlStr, {
        headers: {
          "Cache-Control": "max-age=86400",
          "Content-Type": "application/xml; charset=utf-8"
        }
      }));
    } else {
      xmlStr = await cachedResponse.text();
    }

    // 4. 解析 XML 并提取节目数据
    const parseResult = parseEpgXml(xmlStr, cleanChannel, targetDate, isDebug);
    let epgData;

    if (parseResult.matchedProgrammes && parseResult.matchedProgrammes.length > 0) {
      // 格式化节目数据（适配 diyp 格式）
      epgData = {
        channel_name: cleanChannel,
        date: targetDate,
        url: CONFIG.DEFAULT_URL,
        icon: `${CONFIG.ICON_BASE_URL}${encodeURIComponent(cleanChannel)}.png`,
        epg_data: parseResult.matchedProgrammes.map(prog => ({
          start: prog.start.toTimeString().slice(0, 5),
          end: prog.stop.toTimeString().slice(0, 5),
          title: prog.title,
          desc: prog.desc
        }))
      };
    } else {
      // 未找到数据时返回默认值（并添加调试信息）
      epgData = getDefaultEpgData(oriChannelName, targetDate);
      if (isDebug) {
        epgData.debug = {
          reason: parseResult.error || "未找到目标日期的节目",
          xmlChannels: parseResult.xmlChannels.slice(0, 5), // 只显示前 5 个频道用于调试
          matchedChannel: parseResult.matchedChannel,
          targetDate: targetDate,
          requestChannel: cleanChannel
        };
      }
    }

    // 5. 返回响应
    return new Response(JSON.stringify(epgData, null, 2), {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "max-age=3600"
      }
    });

  } catch (error) {
    return new Response(JSON.stringify({
      error: "服务器错误",
      message: error.message,
      stack: CONFIG.DEBUG ? error.stack : undefined
    }), {
      status: 500,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*"
      }
    });
  }
}
