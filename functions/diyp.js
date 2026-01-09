/**
 * Cloudflare Pages EPG 转 diyp 格式脚本（无 DOM 依赖）
 * 路径：/functions/diyp_epg.js
 * 访问：https://你的域名/diyp_epg?ch=CHC影迷电影&date=20260105&debug=1
 * 修复点：1. 兼容XML标签闭合/空格 2. 增强display-name提取 3. 完善调试信息
 * 新增：debug信息中加入服务器时间、时区信息
 */

const CONFIG = {
  EPG_XML_URL: "https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg_full.xml",
  CHT_TO_CHS: false,
  RET_DEFAULT: true,
  DEFAULT_URL: "https://github.com/jackycher/my-epg-generator",
  ICON_BASE_URL: "https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/logo/",
  DEBUG: false
};

/**
 * 1. 清理频道名（放宽规则：仅移除HTML特殊字符+多余空格，不强制大写）
 */
function cleanChannelName(channelName) {
  if (!channelName) return "";
  // 仅移除HTML特殊字符、多余空格、不可见字符，保留核心特征
  return channelName.trim()
    .toUpperCase() // 新增：全部转为大写
    .replace(/[<>&"']/g, "") // 移除HTML特殊字符
    .replace(/\s+/g, "")    // 移除连续空格
    .replace(/[\u200B-\u200D\uFEFF]/g, ""); // 移除零宽空格等不可见字符
}

/**
 * 新增：计算两个字符串的相似度（简化版编辑距离）
 * 用于近似匹配（比如4K纪实专区 → 4K超清）
 */
function getStringSimilarity(str1, str2) {
  if (!str1 || !str2) return 0;
  const set1 = new Set(str1.split(""));
  const set2 = new Set(str2.split(""));
  const intersection = [...set1].filter(char => set2.has(char)).length;
  const union = new Set([...set1, ...set2]).size;
  return union === 0 ? 0 : intersection / union;
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
/*
function parseEpgTime(timeStr) {
  if (!timeStr) return null;
  const cleanTime = timeStr.split(" ")[0]; // 提取 YYYYMMDDHHMMSS
  if (cleanTime.length !== 14) return null;
  const year = cleanTime.slice(0,4), month = cleanTime.slice(4,6), day = cleanTime.slice(6,8);
  const hour = cleanTime.slice(8,10), min = cleanTime.slice(10,12);
  return new Date(`${year}-${month}-${day}T${hour}:${min}:00`);
}
 */
/**
 * 修正版：精准解析 EPG 时间（YYYYMMDDHHMMSS +0800 → Date 对象）
 * 核心：不重复计算UTC+8偏移，直接解析带时区标识的时间，避免时间错乱
 */
function parseEpgTime(timeStr) {
  if (!timeStr) return null;
  
  // 步骤1：拆分时间部分（YYYYMMDDHHMMSS）和时区部分（+0800，默认值兜底）
  const [cleanTime, timezone = "+0800"] = timeStr.split(" ");
  if (cleanTime.length !== 14) return null;
  
  // 步骤2：提取年月日时分秒，拼接成 JS 可解析的时间格式
  const year = cleanTime.slice(0, 4);
  const month = cleanTime.slice(4, 6); // 月份（1-12，无需减1，后续拼接后JS会自动解析）
  const day = cleanTime.slice(6, 8);
  const hour = cleanTime.slice(8, 10);
  const min = cleanTime.slice(10, 12);
  const sec = "00"; // EPG无秒数，默认补0
  
  // 步骤3：关键！拼接带标准时区格式的字符串（+0800 → +08:00，提升JS解析兼容性）
  const timezoneFormatted = `${timezone.slice(0, 3)}:${timezone.slice(3)}`; // +0800 → +08:00
  const parsableTimeStr = `${year}-${month}-${day}T${hour}:${min}:${sec}${timezoneFormatted}`;
  
  // 步骤4：直接解析带时区的字符串，不额外偏移，得到准确的Date对象
  const epgDate = new Date(parsableTimeStr);
  
  // 步骤5：验证解析结果（避免无效时间）
  return isNaN(epgDate.getTime()) ? null : epgDate;
}

// 修正后：强制获取UTC+8对应的时间（适配EPG时区，避免格式化偏差）
function formatTimeToUTC8(dateObj) {
  if (!dateObj) return "00:00";
  // 获取UTC时间，再手动加上8小时偏移，得到UTC+8的准确时间
  const utcHour = dateObj.getUTCHours();
  const utcMin = dateObj.getUTCMinutes();
  const targetHour = (utcHour + 8) % 24; // 处理跨天情况（如UTC 23:00 → UTC+8 07:00）
  return `${targetHour.toString().padStart(2, "0")}:${utcMin.toString().padStart(2, "0")}`;
}

/**
 * 4. 纯正则解析 XML（替代 DOMParser，适配 Cloudflare 环境）
 * 优化：1. 兼容XML标签空格/闭合 2. 增强display-name提取 3. 完善调试信息
 * 新增：debugInfo中加入服务器时间、时区信息
 */
function parseEpgXml(xmlStr, targetChannel, targetDate, debug = false) {
  // ========== 关键修改：新增服务器时间/时区信息 ==========
  const now = new Date();
  const debugInfo = {
    target: { channel: targetChannel, date: targetDate },
    // 新增服务器时间相关字段
    serverTime: {
      iso: now.toISOString(), // ISO标准时间（UTC）
      local: now.toLocaleString('zh-CN'), // 服务器本地时间（中文格式）
      timezoneOffset: now.getTimezoneOffset(), // 时区偏移（分钟），负数表示UTC+N
      timezoneDesc: `UTC${-now.getTimezoneOffset()/60}` // 易读的时区描述，如UTC+8
    },
    xmlChannels: [],
    allChannelNames: [], // 新增：所有频道的cleanName列表（方便排查）
    matchedChannel: null,
    matchedProgrammes: [],
    error: null,
    similarityMatch: null
  };

  try {
    // ========== 步骤1：提取所有频道（channel 标签） ==========
    const channelRegex = /<channel id="([^"]+)">(.*?)<\/channel>/gs;
    let channelMatch;
    const channels = [];

    while ((channelMatch = channelRegex.exec(xmlStr)) !== null) {
      const channelId = channelMatch[1];
      const channelContent = channelMatch[2];
      
      // 优化：兼容 display-name 标签的空格、额外属性（lang="zh"、lang="zh-CN"等）
      const zhNameRegex = /<display-name\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/display-name>/is;
      let displayName = zhNameRegex.exec(channelContent)?.[1] || "";
      
      // 备选：匹配任意 display-name（无lang属性或其他lang）
      if (!displayName) {
        const anyNameRegex = /<display-name\s*>(.*?)<\/display-name>/is;
        displayName = anyNameRegex.exec(channelContent)?.[1] || "";
      }
      
      const cleanName = cleanChannelName(displayName);
      channels.push({ id: channelId, displayName, cleanName });
    }

    debugInfo.xmlChannels = channels;
    debugInfo.allChannelNames = channels.map(chan => chan.cleanName); // 保存所有频道名

    // ========== 步骤2：匹配目标频道（优化容错性） ==========
    const cleanTarget = cleanChannelName(targetChannel); // 二次清理目标值
    let matchedChannel = null;

    // 步骤2.1：精确匹配（去除不可见字符后）
    matchedChannel = channels.find(chan => chan.cleanName === cleanTarget);

    // 步骤2.2：双向模糊匹配
    if (!matchedChannel) {
      matchedChannel = channels.find(chan => 
        chan.cleanName.includes(cleanTarget) || cleanTarget.includes(chan.cleanName)
      );
    }

    // 步骤2.3：相似度匹配
    if (!matchedChannel) {
      const channelWithSimilarity = channels.map(chan => ({
        ...chan,
        similarity: getStringSimilarity(chan.cleanName, cleanTarget)
      })).filter(chan => chan.similarity >= 0.3);
      
      if (channelWithSimilarity.length > 0) {
        channelWithSimilarity.sort((a, b) => b.similarity - a.similarity);
        matchedChannel = channelWithSimilarity[0];
        debugInfo.similarityMatch = {
          matchedChannel: matchedChannel,
          similarity: matchedChannel.similarity,
          candidateChannels: channelWithSimilarity.slice(0, 3)
        };
      }
    }

    // 步骤2.4：前缀匹配
    if (!matchedChannel && cleanTarget.length >= 2) {
      const prefix = cleanTarget.slice(0, 3);
      matchedChannel = channels.find(chan => chan.cleanName.startsWith(prefix));
    }

    if (!matchedChannel) {
      debugInfo.error = `未找到匹配的频道（目标：${cleanTarget}，所有频道数：${channels.length}）`;
      return debugInfo;
    }
    debugInfo.matchedChannel = matchedChannel;

    // ========== 步骤3：提取该频道的所有节目 ==========
    const targetDateObj = new Date(targetDate);
    const nextDateObj = new Date(targetDateObj);
    nextDateObj.setDate(nextDateObj.getDate() + 1);

    // 匹配指定 channel id 的 programme 标签（转义特殊字符）
    const escapedChannelId = matchedChannel.id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const programmeRegex = new RegExp(
      `<programme start="([^"]+)" stop="([^"]+)" channel="${escapedChannelId}">(.*?)<\/programme>`,
      "gs"
    );
    let programmeMatch;
    const programmes = [];

    while ((programmeMatch = programmeRegex.exec(xmlStr)) !== null) {
      const startStr = programmeMatch[1];
      const stopStr = programmeMatch[2];
      const progContent = programmeMatch[3];
      
      // 优化：提取 title（兼容lang属性差异）
      const titleRegex = /<title\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/title>/is;
      let title = titleRegex.exec(progContent)?.[1] || "";
      if (!title) {
        title = /<title\s*>(.*?)<\/title>/is.exec(progContent)?.[1] || "";
      }
      
      // 优化：提取 desc（兼容lang属性差异）
      const descRegex = /<desc\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/desc>/is;
      let desc = descRegex.exec(progContent)?.[1] || "";
      if (!desc) {
        desc = /<desc\s*>(.*?)<\/desc>/is.exec(progContent)?.[1] || "";
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

    // 获取 EPG XML（带缓存，强制刷新一次缓存避免旧数据影响）
    const cache = caches.default;
    let cachedResponse = await cache.match(CONFIG.EPG_XML_URL);
    let xmlStr;

    // 新增：如果是调试模式，跳过缓存（避免旧XML影响排查）
    if (isDebug || !cachedResponse) {
      const xmlResponse = await fetch(CONFIG.EPG_XML_URL, {
        headers: { "User-Agent": "Cloudflare EPG Fetcher" },
        cf: { cacheTtl: isDebug ? 60 : 3600 } // 调试模式缓存1分钟，正常模式1小时
      });

      if (!xmlResponse.ok) {
        throw new Error(`EPG XML 下载失败: ${xmlResponse.status}`);
      }

      xmlStr = await xmlResponse.text();
      await cache.put(CONFIG.EPG_XML_URL, new Response(xmlStr, {
        headers: {
          "Cache-Control": `max-age=${isDebug ? 60 : 86400}`,
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
/*
        epg_data: parseResult.matchedProgrammes.map(prog => ({
          start: prog.start.toTimeString().slice(0, 5),
          end: prog.stop.toTimeString().slice(0, 5),
          title: prog.title,
          desc: prog.desc
        }))
*/
        // 调用修正后的格式化函数
        epg_data: parseResult.matchedProgrammes.map(prog => ({
          start: formatTimeToUTC8(prog.start),
          end: formatTimeToUTC8(prog.stop),
          title: prog.title,
          desc: prog.desc
        }))
      };
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
