/**
 * Cloudflare Pages EPG 转 diyp 格式脚本（无 DOM 依赖）
 * 路径：/functions/diyp_epg.js
 * 访问：https://你的域名/diyp_epg?ch=CCTV1&date=20251224&debug=1
 */

const CONFIG = {
  EPG_XML_URL: "https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml",
  CHT_TO_CHS: false,
  RET_DEFAULT: true,
  DEFAULT_URL: "https://github.com/jackycher/my-epg-generator",
  ICON_BASE_URL: "https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/logo/",
  DEBUG: true
};

/**
 * 1. 清理频道名（统一大写+去特殊字符）
 */
function cleanChannelName(channelName) {
  if (!channelName) return "";
  return channelName.trim().toUpperCase().replace(/[^\u4e00-\u9fa5A-Z0-9+]/g, "");
}

/**
 * 2. 格式化日期（YYYY-MM-DD）
 */
function getFormatDate(dateStr) {
  if (!dateStr) return new Date().toISOString().split("T")[0];
  const numDate = dateStr.replace(/\D+/g, "");
  if (numDate.length < 8) return new Date().toISOString().split("T")[0];
  return `${numDate.slice(0,4)}-${numDate.slice(4,6)}-${numDate.slice(6,8)}`;
}

/**
 * 3. 解析 EPG 时间（YYYYMMDDHHMMSS +0800 → Date 对象）
 */
function parseEpgTime(timeStr) {
  if (!timeStr) return null;
  const cleanTime = timeStr.split(" ")[0]; // 提取 YYYYMMDDHHMMSS
  if (cleanTime.length !== 14) return null;
  const year = cleanTime.slice(0,4), month = cleanTime.slice(4,6), day = cleanTime.slice(6,8);
  const hour = cleanTime.slice(8,10), min = cleanTime.slice(10,12);
  return new Date(`${year}-${month}-${day}T${hour}:${min}:00`);
}

/**
 * 4. 纯正则解析 XML（替代 DOMParser，适配 Cloudflare 环境）
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
    // ========== 步骤1：提取所有频道（channel 标签） ==========
    const channelRegex = /<channel id="(\d+)">(.*?)<\/channel>/gs;
    let channelMatch;
    const channels = [];

    while ((channelMatch = channelRegex.exec(xmlStr)) !== null) {
      const channelId = channelMatch[1];
      const channelContent = channelMatch[2];
      
      // 提取 display-name（优先 lang="zh"）
      const nameRegex = /<display-name lang="zh">(.*?)<\/display-name>/;
      let displayName = nameRegex.exec(channelContent)?.[1] || "";
      if (!displayName) {
        displayName = /<display-name>(.*?)<\/display-name>/.exec(channelContent)?.[1] || "";
      }
      
      const cleanName = cleanChannelName(displayName);
      channels.push({ id: channelId, displayName, cleanName });
    }

    debugInfo.xmlChannels = channels;

    // ========== 步骤2：匹配目标频道 ==========
    let matchedChannel = channels.find(chan => chan.cleanName === targetChannel);
    // 模糊匹配（备选）
    if (!matchedChannel) {
      matchedChannel = channels.find(chan => chan.cleanName.includes(targetChannel));
    }

    if (!matchedChannel) {
      debugInfo.error = "未找到匹配的频道";
      return debugInfo;
    }
    debugInfo.matchedChannel = matchedChannel;

    // ========== 步骤3：提取该频道的所有节目 ==========
    const targetDateObj = new Date(targetDate);
    const nextDateObj = new Date(targetDateObj);
    nextDateObj.setDate(nextDateObj.getDate() + 1);

    // 匹配指定 channel id 的 programme 标签
    const programmeRegex = new RegExp(
      `<programme start="([^"]+)" stop="([^"]+)" channel="${matchedChannel.id}">(.*?)<\/programme>`,
      "gs"
    );
    let programmeMatch;
    const programmes = [];

    while ((programmeMatch = programmeRegex.exec(xmlStr)) !== null) {
      const startStr = programmeMatch[1];
      const stopStr = programmeMatch[2];
      const progContent = programmeMatch[3];
      
      // 提取 title（优先 lang="zh"）
      const titleRegex = /<title lang="zh">(.*?)<\/title>/;
      let title = titleRegex.exec(progContent)?.[1] || "";
      if (!title) {
        title = /<title>(.*?)<\/title>/.exec(progContent)?.[1] || "";
      }
      
      // 提取 desc（可选）
      const descRegex = /<desc lang="zh">(.*?)<\/desc>/;
      let desc = descRegex.exec(progContent)?.[1] || "";
      if (!desc) {
        desc = /<desc>(.*?)<\/desc>/.exec(progContent)?.[1] || "";
      }

      // 解析时间并过滤目标日期
      const start = parseEpgTime(startStr);
      const stop = parseEpgTime(stopStr);
      const isTargetDate = start && start >= targetDateObj && start < nextDateObj;

      if (isTargetDate) {
        programmes.push({
          start,
          stop,
          title,
          desc,
          isTargetDate
        });
      }
    }

    debugInfo.matchedProgrammes = programmes;
    return debugInfo;

  } catch (e) {
    debugInfo.error = `XML解析失败: ${e.message}`;
    return debugInfo;
  }
}

/**
 * 5. 生成默认节目数据
 */
function getDefaultEpgData(channelName, date) {
  const cleanName = cleanChannelName(channelName);
  return {
    channel_name: cleanName,
    date: date,
    url: CONFIG.DEFAULT_URL,
    icon: `${CONFIG.ICON_BASE_URL}${encodeURIComponent(cleanName)}.png`,
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
 * 主处理函数（Cloudflare Pages 入口）
 */
export async function onRequest(context) {
  try {
    const { request } = context;
    const url = new URL(request.url);
    const queryParams = Object.fromEntries(url.searchParams.entries());
    const isDebug = queryParams.debug === "1" || CONFIG.DEBUG;

    // 解析参数
    const oriChannelName = queryParams.ch || queryParams.channel || "";
    const cleanChannel = cleanChannelName(oriChannelName);
    const targetDate = getFormatDate(queryParams.date);

    // 频道为空返回404
    if (!cleanChannel) {
      return new Response("404 Not Found. <br>未指定频道参数", {
        status: 404,
        headers: { "Content-Type": "text/html; charset=utf-8" }
      });
    }

    // 获取 EPG XML（带缓存）
    const cache = caches.default;
    let cachedResponse = await cache.match(CONFIG.EPG_XML_URL);
    let xmlStr;

    if (!cachedResponse) {
      const xmlResponse = await fetch(CONFIG.EPG_XML_URL, {
        headers: { "User-Agent": "Cloudflare EPG Fetcher" },
        cf: { cacheTtl: 86400 }
      });

      if (!xmlResponse.ok) {
        throw new Error(`EPG XML 下载失败: ${xmlResponse.status}`);
      }

      xmlStr = await xmlResponse.text();
      await cache.put(CONFIG.EPG_XML_URL, new Response(xmlStr, {
        headers: {
          "Cache-Control": "max-age=86400",
          "Content-Type": "application/xml; charset=utf-8"
        }
      }));
    } else {
      xmlStr = await cachedResponse.text();
    }

    // 解析 XML 提取节目
    const parseResult = parseEpgXml(xmlStr, cleanChannel, targetDate, isDebug);
    let epgData;

    if (parseResult.matchedProgrammes.length > 0) {
      // 有匹配的节目数据
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
      // 调试模式添加额外信息
      if (isDebug) epgData.debug = parseResult;
    } else {
      // 无匹配数据返回默认值
      epgData = getDefaultEpgData(oriChannelName, targetDate);
      if (isDebug) epgData.debug = parseResult;
    }

    // 返回响应
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
