/**
 * Cloudflare Pages EPG 转 diyp 格式脚本（无 DOM 依赖）
 * 路径：/functions/diyp_epg.js
 * 访问：https://你的域名/diyp_epg?ch=CHC影迷电影&date=20260105&debug=1
 * 新增：支持 gzip 压缩包解压，解决 25MiB 大小限制问题
 */

const CONFIG = {
  // 关键修改1：替换为 gz 压缩包地址
  EPG_XML_URL: "https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml.gz",
  CHT_TO_CHS: false,
  RET_DEFAULT: true,
  DEFAULT_URL: "https://github.com/jackycher/my-epg-generator",
  ICON_BASE_URL: "https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/logo/",
  DEBUG: false
};

/**
 * 1. 清理频道名（统一转大写+移除特殊字符）
 */
function cleanChannelName(channelName) {
  if (!channelName) return "";
  return channelName.trim()
    .toUpperCase() 
    .replace(/[<>&"']/g, "") 
    .replace(/\s+/g, "")    
    .replace(/[\u200B-\u200D\uFEFF]/g, ""); 
}

/**
 * 新增：计算字符串相似度
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
function parseEpgTime(timeStr) {
  if (!timeStr) return null;
  const cleanTime = timeStr.split(" ")[0]; 
  if (cleanTime.length !== 14) return null;
  const year = cleanTime.slice(0,4), month = cleanTime.slice(4,6), day = cleanTime.slice(6,8);
  const hour = cleanTime.slice(8,10), min = cleanTime.slice(10,12);
  return new Date(`${year}-${month}-${day}T${hour}:${min}:00`);
}

/**
 * 新增：解压 gzip 二进制数据为文本（适配 Cloudflare Workers 环境）
 */
async function decompressGzip(buffer) {
  const stream = new Blob([buffer]).stream();
  const decompressedStream = stream.pipeThrough(new DecompressionStream('gzip'));
  const decompressedBlob = await new Response(decompressedStream).blob();
  return await decompressedBlob.text();
}

/**
 * 4. 纯正则解析 XML（逻辑不变）
 */
function parseEpgXml(xmlStr, targetChannel, targetDate, debug = false) {
  const debugInfo = {
    target: { channel: targetChannel, date: targetDate },
    xmlChannels: [],
    allChannelNames: [], 
    matchedChannel: null,
    matchedProgrammes: [],
    error: null,
    similarityMatch: null
  };

  try {
    const channelRegex = /<channel id="([^"]+)">(.*?)<\/channel>/gs;
    let channelMatch;
    const channels = [];

    while ((channelMatch = channelRegex.exec(xmlStr)) !== null) {
      const channelId = channelMatch[1];
      const channelContent = channelMatch[2];
      
      const zhNameRegex = /<display-name\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/display-name>/is;
      let displayName = zhNameRegex.exec(channelContent)?.[1] || "";
      
      if (!displayName) {
        const anyNameRegex = /<display-name\s*>(.*?)<\/display-name>/is;
        displayName = anyNameRegex.exec(channelContent)?.[1] || "";
      }
      
      const cleanName = cleanChannelName(displayName);
      channels.push({ id: channelId, displayName, cleanName });
    }

    debugInfo.xmlChannels = channels;
    debugInfo.allChannelNames = channels.map(chan => chan.cleanName);

    const cleanTarget = cleanChannelName(targetChannel);
    let matchedChannel = null;

    // 精确匹配
    matchedChannel = channels.find(chan => chan.cleanName === cleanTarget);

    // 双向模糊匹配
    if (!matchedChannel) {
      matchedChannel = channels.find(chan => 
        chan.cleanName.includes(cleanTarget) || cleanTarget.includes(chan.cleanName)
      );
    }

    // 相似度匹配
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

    // 前缀匹配
    if (!matchedChannel && cleanTarget.length >= 2) {
      const prefix = cleanTarget.slice(0, 3);
      matchedChannel = channels.find(chan => chan.cleanName.startsWith(prefix));
    }

    if (!matchedChannel) {
      debugInfo.error = `未找到匹配的频道（目标：${cleanTarget}，所有频道数：${channels.length}）`;
      return debugInfo;
    }
    debugInfo.matchedChannel = matchedChannel;

    // 提取节目数据
    const targetDateObj = new Date(targetDate);
    const nextDateObj = new Date(targetDateObj);
    nextDateObj.setDate(nextDateObj.getDate() + 1);

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
      
      const titleRegex = /<title\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/title>/is;
      let title = titleRegex.exec(progContent)?.[1] || "";
      if (!title) {
        title = /<title\s*>(.*?)<\/title>/is.exec(progContent)?.[1] || "";
      }
      
      const descRegex = /<desc\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/desc>/is;
      let desc = descRegex.exec(progContent)?.[1] || "";
      if (!desc) {
        desc = /<desc\s*>(.*?)<\/desc>/is.exec(progContent)?.[1] || "";
      }

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
 * 主处理函数（关键修改2：添加 gzip 解压逻辑）
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

    // 获取 gzip 压缩包（带缓存）
    const cache = caches.default;
    let cachedResponse = await cache.match(CONFIG.EPG_XML_URL);
    let xmlStr;

    if (isDebug || !cachedResponse) {
      const gzipResponse = await fetch(CONFIG.EPG_XML_URL, {
        headers: { "User-Agent": "Cloudflare EPG Fetcher" },
        cf: { cacheTtl: isDebug ? 60 : 86400 } 
      });

      if (!gzipResponse.ok) {
        throw new Error(`EPG gz 压缩包下载失败: ${gzipResponse.status}`);
      }

      // 关键修改3：读取二进制数据并解压为 XML 文本
      const gzipBuffer = await gzipResponse.arrayBuffer();
      xmlStr = await decompressGzip(gzipBuffer);

      // 缓存解压后的 XML（避免重复解压）
      await cache.put(CONFIG.EPG_XML_URL, new Response(xmlStr, {
        headers: {
          "Cache-Control": `max-age=${isDebug ? 60 : 86400}`,
          "Content-Type": "application/xml; charset=utf-8"
        }
      }));
    } else {
      // 从缓存读取已解压的 XML 文本
      xmlStr = await cachedResponse.text();
    }

    // 原有解析逻辑不变
    const parseResult = parseEpgXml(xmlStr, cleanChannel, targetDate, isDebug);
    let epgData;

    if (parseResult.matchedProgrammes.length > 0) {
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
      if (isDebug) epgData.debug = parseResult;
    } else {
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
