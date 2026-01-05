/**
 * Cloudflare Pages EPG 转 diyp 格式脚本（修复解压失败问题）
 * 路径：/functions/diyp_epg.js
 * 访问：https://你的域名/diyp_epg?ch=凤凰中文&date=20251230&debug=1
 */

const CONFIG = {
  EPG_XML_URL: "https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml.gz",
  CHT_TO_CHS: false,
  RET_DEFAULT: true,
  DEFAULT_URL: "https://github.com/jackycher/my-epg-generator",
  ICON_BASE_URL: "https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/logo/",
  DEBUG: true, // 临时开启全局调试，方便排查
  CACHE_TTL: 3600, // 缓存时间（秒），调试时可改为 60
  MAX_DECOMPRESS_SIZE: 1024 * 1024 * 50, // 最大解压大小（50MiB），防止内存溢出
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
 * 2. 计算字符串相似度
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
 * 3. 格式化日期（YYYY-MM-DD）
 */
function getFormatDate(dateStr) {
  if (!dateStr) return new Date().toISOString().split("T")[0];
  const numDate = dateStr.replace(/\D+/g, "");
  if (numDate.length < 8) return new Date().toISOString().split("T")[0];
  return `${numDate.slice(0,4)}-${numDate.slice(4,6)}-${numDate.slice(6,8)}`;
}

/**
 * 4. 解析 EPG 时间（YYYYMMDDHHMMSS +0800 → Date 对象）
 */
function parseEpgTime(timeStr) {
  if (!timeStr) return null;
  const cleanTime = timeStr.split(" ")[0];
  if (cleanTime.length !== 14) return null;
  const year = cleanTime.slice(0,4), month = cleanTime.slice(4,6)-1, day = cleanTime.slice(6,8);
  const hour = cleanTime.slice(8,10), min = cleanTime.slice(10,12);
  return new Date(Date.UTC(year, month, day, hour, min)); // 修复时区问题
}

/**
 * 核心优化：兼容 Cloudflare 的 gzip 解压逻辑（替换 DecompressionStream）
 * 原理：直接使用 Cloudflare Workers 内置的 fetch 自动解压（如果服务器支持）
 *  fallback：使用简单的 zlib 兼容逻辑（避免 Stream 兼容性问题）
 */
async function decompressGzip(buffer, debug = false) {
  try {
    // 验证是否为标准 gzip 格式（首字节 0x1f，第二字节 0x8b）
    const uint8Array = new Uint8Array(buffer);
    if (uint8Array.length < 2 || uint8Array[0] !== 0x1f || uint8Array[1] !== 0x8b) {
      throw new Error(`非标准 gzip 格式（首字节：0x${uint8Array[0].toString(16)}, 第二字节：0x${uint8Array[1].toString(16)}）`);
    }

    // 方案1：使用 Cloudflare 内置解压（优先推荐，兼容性更好）
    const response = new Response(buffer, {
      headers: { "Content-Encoding": "gzip" }
    });
    const text = await response.text();

    // 验证解压后大小（防止恶意压缩包）
    if (text.length > CONFIG.MAX_DECOMPRESS_SIZE) {
      throw new Error(`解压后文件过大（${text.length} 字节，超过限制 ${CONFIG.MAX_DECOMPRESS_SIZE} 字节）`);
    }

    if (debug) console.log(`解压成功，原始大小：${buffer.byteLength} 字节，解压后：${text.length} 字节`);
    return text;

  } catch (error) {
    // 方案1失败时，尝试方案2：使用简易解压逻辑（备用）
    try {
      if (debug) console.log(`方案1解压失败，尝试备用方案：${error.message}`);
      const { gunzip } = await import("https://esm.sh/pako@2.1.0");
      const uint8Array = new Uint8Array(buffer);
      const decompressed = gunzip(uint8Array);
      const text = new TextDecoder("utf-8").decode(decompressed);

      if (text.length > CONFIG.MAX_DECOMPRESS_SIZE) {
        throw new Error(`备用方案解压后文件过大（${text.length} 字节）`);
      }

      if (debug) console.log(`备用方案解压成功，解压后：${text.length} 字节`);
      return text;
    } catch (fallbackError) {
      throw new Error(`解压失败（主方案：${error.message}；备用方案：${fallbackError.message}）`);
    }
  }
}

/**
 * 5. 纯正则解析 XML（逻辑不变，修复时区问题）
 */
function parseEpgXml(xmlStr, targetChannel, targetDate, debug = false) {
  const debugInfo = {
    target: { channel: targetChannel, date: targetDate },
    xmlChannels: [],
    allChannelNames: [],
    matchedChannel: null,
    matchedProgrammes: [],
    error: null,
    similarityMatch: null,
    xmlSize: xmlStr.length // 新增：XML 大小，方便调试
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

    // 提取节目数据（修复时区问题：目标日期转为 UTC 时间比较）
    const targetDateObj = new Date(targetDate);
    const targetDateUTC = new Date(Date.UTC(
      targetDateObj.getFullYear(),
      targetDateObj.getMonth(),
      targetDateObj.getDate()
    ));
    const nextDateUTC = new Date(targetDateUTC);
    nextDateUTC.setDate(nextDateUTC.getDate() + 1);

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
      const isTargetDate = start && start >= targetDateUTC && start < nextDateUTC;

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
 * 6. 生成默认节目数据
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
 * 主处理函数（增强错误捕获和调试）
 */
export async function onRequest(context) {
  try {
    const { request, env } = context;
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

    // 日志：打印请求信息（调试用）
    if (isDebug) {
      console.log(`收到请求：channel=${cleanChannel}, date=${targetDate}, debug=${isDebug}`);
    }

    // 获取 gzip 压缩包（带缓存，调试时禁用缓存）
    const cache = caches.default;
    let cachedResponse = isDebug ? null : await cache.match(CONFIG.EPG_XML_URL);
    let xmlStr;

    if (!cachedResponse) {
      if (isDebug) console.log(`缓存未命中，下载 gzip 压缩包：${CONFIG.EPG_XML_URL}`);
      const gzipResponse = await fetch(CONFIG.EPG_XML_URL, {
        headers: { "User-Agent": "Cloudflare EPG Fetcher/1.0" },
        cf: { cacheTtl: CONFIG.CACHE_TTL, cacheEverything: true },
        timeout: 30000 // 30秒超时
      });

      if (!gzipResponse.ok) {
        throw new Error(`压缩包下载失败：HTTP ${gzipResponse.status}（${gzipResponse.statusText}）`);
      }

      // 验证下载的压缩包大小
      const contentLength = gzipResponse.headers.get("Content-Length");
      if (contentLength && parseInt(contentLength) > 1024 * 1024 * 30) { // 30MiB 限制
        throw new Error(`压缩包过大（${contentLength} 字节，超过 30MiB 限制）`);
      }

      // 读取二进制数据
      const gzipBuffer = await gzipResponse.arrayBuffer();
      if (isDebug) console.log(`压缩包下载成功，大小：${gzipBuffer.byteLength} 字节`);

      // 解压（核心修复：使用优化后的解压函数）
      xmlStr = await decompressGzip(gzipBuffer, isDebug);

      // 缓存解压后的 XML（避免重复解压）
      if (!isDebug) {
        await cache.put(CONFIG.EPG_XML_URL, new Response(xmlStr, {
          headers: {
            "Cache-Control": `max-age=${CONFIG.CACHE_TTL}`,
            "Content-Type": "application/xml; charset=utf-8",
            "X-EPG-Size": xmlStr.length
          }
        }));
        if (isDebug) console.log(`解压后的 XML 已缓存，有效期 ${CONFIG.CACHE_TTL} 秒`);
      }
    } else {
      // 从缓存读取已解压的 XML
      xmlStr = await cachedResponse.text();
      if (isDebug) console.log(`从缓存读取 XML，大小：${xmlStr.length} 字节`);
    }

    // 解析 XML 并生成响应
    const parseResult = parseEpgXml(xmlStr, cleanChannel, targetDate, isDebug);
    let epgData;

    if (parseResult.matchedProgrammes.length > 0) {
      epgData = {
        channel_name: cleanChannel,
        date: targetDate,
        url: CONFIG.DEFAULT_URL,
        icon: `${CONFIG.ICON_BASE_URL}${encodeURIComponent(cleanChannel)}.png`,
        epg_data: parseResult.matchedProgrammes.map(prog => ({
          start: prog.start.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Shanghai" }),
          end: prog.stop.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Shanghai" }),
          title: prog.title.trim(),
          desc: prog.desc.trim()
        })),
        total: parseResult.matchedProgrammes.length
      };
    } else {
      epgData = getDefaultEpgData(oriChannelName, targetDate);
      epgData.warning = parseResult.error || "未找到该日期的节目数据";
    }

    // 添加调试信息
    if (isDebug) {
      epgData.debug = {
        requestParams: queryParams,
        cacheHit: !!cachedResponse,
        xmlSize: xmlStr.length,
        channelCount: parseResult.xmlChannels.length,
        matchedChannel: parseResult.matchedChannel,
        similarityMatch: parseResult.similarityMatch,
        error: parseResult.error
      };
    }

    // 返回响应（启用压缩）
    return new Response(JSON.stringify(epgData, null, isDebug ? 2 : 0), {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": `max-age=${isDebug ? 60 : 3600}`,
        "Content-Encoding": "gzip" // 响应压缩，减少传输大小
      }
    });

  } catch (error) {
    // 详细错误日志
    const errorMsg = isDebug ? error.stack || error.message : error.message;
    console.error(`服务器错误：${errorMsg}`);

    return new Response(JSON.stringify({
      error: "服务器错误",
      message: errorMsg,
      requestId: crypto.randomUUID(), // 随机请求ID，方便排查
      timestamp: new Date().toISOString()
    }, null, isDebug ? 2 : 0), {
      status: 500,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*"
      }
    });
  }
}
