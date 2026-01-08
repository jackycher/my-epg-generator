<?php
/**
 * EPG 转 diyp 格式 PHP 脚本（OpenWrt适配版）
 * 适配：mips架构+PHP8.3+无HTTPS stream+权限限制
 * 访问：https://你的域名/diyp_epg.php?ch=CHC影迷电影&date=20260105&debug=1
 */

// ========== 新增：设置默认时区为北京时间 ==========
date_default_timezone_set('Asia/Shanghai');

// 开启输出缓冲（解决头信息已发送问题）
ob_start();

// ===== 配置项（适配OpenWrt）=====
define('EPG_XML_URL', 'https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/epg_full.xml');
define('RET_DEFAULT', true);
define('DEFAULT_URL', 'https://github.com/jackycher/my-epg-generator');
define('ICON_BASE_URL', 'https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/logo/');
define('DEBUG', false);
// OpenWrt下/tmp可写，改用此目录
define('CACHE_DIR', '/tmp/epg_cache/');
define('CACHE_TTL', 3600); // 缓存1小时（适配低资源）

/**
 * 1. 清理频道名（适配中文+零宽空格）
 */
function cleanChannelName($channelName) {
    if (empty($channelName)) return '';
    
    $cleaned = trim($channelName);
    $cleaned = strtoupper($cleaned);
    // 移除HTML特殊字符（无正则）
    $cleaned = str_replace(['<', '>', '&', '"', "'"], '', $cleaned);
    // 移除连续空格
    $cleaned = preg_replace('/\s+/', '', $cleaned);
    // 移除零宽空格（字节串写法，适配mips）
    $zeroWidthChars = [
        "\xe2\x80\x8b", "\xe2\x80\x8c", "\xe2\x80\x8d", "\xef\xbb\xbf"
    ];
    $cleaned = str_replace($zeroWidthChars, '', $cleaned);
    
    return $cleaned;
}

/**
 * 2. 字符串相似度计算（精简版）
 */
function getStringSimilarity($str1, $str2) {
    if (empty($str1) || empty($str2)) return 0;
    $set1 = array_unique(mb_str_split($str1));
    $set2 = array_unique(mb_str_split($str2));
    $intersection = count(array_intersect($set1, $set2));
    $union = count(array_unique(array_merge($set1, $set2)));
    return $union === 0 ? 0 : $intersection / $union;
}

/**
 * 3. 格式化日期（YYYY-MM-DD）
 */
function getFormatDate($dateStr) {
    if (empty($dateStr)) return date('Y-m-d');
    $numDate = preg_replace('/\D+/', '', $dateStr);
    if (strlen($numDate) < 8) return date('Y-m-d');
    return substr($numDate, 0, 4) . '-' . substr($numDate, 4, 2) . '-' . substr($numDate, 6, 2);
}

/**
 * 4. 解析EPG时间（修正substr参数+时区）
 */
function parseEpgTime($timeStr) {
    if (empty($timeStr)) return null;
    $cleanTime = explode(' ', $timeStr)[0];
    if (strlen($cleanTime) !== 14) return null;
    // 正确的substr参数：起始位置+长度
    $year = substr($cleanTime, 0, 4);
    $month = substr($cleanTime, 4, 2);
    $day = substr($cleanTime, 6, 2);
    $hour = substr($cleanTime, 8, 2);
    $min = substr($cleanTime, 10, 2);
    try {
        // ========== 修改：创建DateTime时指定北京时间时区 ==========
        $datetime = new DateTime("$year-$month-$day $hour:$min:00", new DateTimeZone('Asia/Shanghai'));
        return $datetime;
    } catch (Exception $e) {
        return null;
    }
}

/**
 * 5. 解析XML（纯正则，适配低内存）
 */
function parseEpgXml($xmlStr, $targetChannel, $targetDate, $debug = false) {
    $debugInfo = [
        'target' => ['channel' => $targetChannel, 'date' => $targetDate],
        // ========== 新增：调试信息中添加服务器当前时间 ==========
        'server_time' => date('Y-m-d H:i:s'),
        'matchedChannel' => null,
        'matchedProgrammes' => [],
        'error' => null
    ];

    try {
        // 提取所有频道
        preg_match_all('/<channel id="([^"]+)">(.*?)<\/channel>/s', $xmlStr, $channelMatches, PREG_SET_ORDER);
        $channels = [];
        foreach ($channelMatches as $match) {
            $channelId = $match[1];
            $content = $match[2];
            
            // 提取display-name（兼容lang属性）
            preg_match('/<display-name\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/display-name>/is', $content, $nameMatch);
            $displayName = $nameMatch[1] ?? '';
            if (empty($displayName)) {
                preg_match('/<display-name\s*>(.*?)<\/display-name>/is', $content, $nameMatch);
                $displayName = $nameMatch[1] ?? '';
            }
            
            $channels[] = [
                'id' => $channelId,
                'cleanName' => cleanChannelName($displayName)
            ];
        }

        // 匹配目标频道
        $cleanTarget = cleanChannelName($targetChannel);
        $matchedChannel = null;

        // 1. 精确匹配
        foreach ($channels as $chan) {
            if ($chan['cleanName'] === $cleanTarget) {
                $matchedChannel = $chan;
                break;
            }
        }

        // 2. 模糊匹配
        if (!$matchedChannel) {
            foreach ($channels as $chan) {
                if (str_contains($chan['cleanName'], $cleanTarget) || str_contains($cleanTarget, $chan['cleanName'])) {
                    $matchedChannel = $chan;
                    break;
                }
            }
        }

        // 3. 相似度匹配
        if (!$matchedChannel) {
            $similarChannels = [];
            foreach ($channels as $chan) {
                $sim = getStringSimilarity($chan['cleanName'], $cleanTarget);
                if ($sim >= 0.3) $similarChannels[] = [$chan, $sim];
            }
            if (!empty($similarChannels)) {
                usort($similarChannels, function($a, $b) { return $b[1] - $a[1]; });
                $matchedChannel = $similarChannels[0][0];
            }
        }

        if (!$matchedChannel) {
            $debugInfo['error'] = "未匹配频道：$cleanTarget";
            return $debugInfo;
        }
        $debugInfo['matchedChannel'] = $matchedChannel;

        // 提取节目
        // ========== 修改：创建日期对象时指定北京时间时区 ==========
        $targetDateObj = new DateTime($targetDate, new DateTimeZone('Asia/Shanghai'));
        $nextDateObj = clone $targetDateObj;
        $nextDateObj->modify('+1 day');

        // 匹配指定频道的节目
        $escapedId = preg_quote($matchedChannel['id'], '/');
        preg_match_all("/<programme start=\"([^\"]+)\" stop=\"([^\"]+)\" channel=\"$escapedId\">(.*?)<\/programme>/s", $xmlStr, $progMatches, PREG_SET_ORDER);
        
        $programmes = [];
        foreach ($progMatches as $match) {
            $start = parseEpgTime($match[1]);
            $stop = parseEpgTime($match[2]);
            if (!$start || !$stop) continue;

            // 过滤目标日期
            if ($start >= $targetDateObj && $start < $nextDateObj) {
                // 提取标题和描述
                preg_match('/<title\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/title>/is', $match[3], $titleMatch);
                $title = $titleMatch[1] ?? '';
                if (empty($title)) preg_match('/<title\s*>(.*?)<\/title>/is', $match[3], $titleMatch);
                $title = $titleMatch[1] ?? '';

                preg_match('/<desc\s+lang="zh(?:-[A-Za-z]+)?"\s*>(.*?)<\/desc>/is', $match[3], $descMatch);
                $desc = $descMatch[1] ?? '';
                if (empty($desc)) preg_match('/<desc\s*>(.*?)<\/desc>/is', $match[3], $descMatch);
                $desc = $descMatch[1] ?? '';

                $programmes[] = [
                    'start' => $start,
                    'stop' => $stop,
                    'title' => $title,
                    'desc' => $desc
                ];
            }
        }

        $debugInfo['matchedProgrammes'] = $programmes;
        return $debugInfo;

    } catch (Exception $e) {
        $debugInfo['error'] = "XML解析失败：" . $e->getMessage();
        return $debugInfo;
    }
}

/**
 * 6. 默认EPG数据（适配OpenWrt）
 */
function getDefaultEpgData($channelName, $date) {
    $cleanName = cleanChannelName($channelName);
    $epgData = [];
    if (RET_DEFAULT) {
        for ($h = 0; $h < 24; $h++) {
            $epgData[] = [
                'start' => str_pad($h, 2, '0', STR_PAD_LEFT) . ':00',
                'end' => str_pad(($h+1)%24, 2, '0', STR_PAD_LEFT) . ':00',
                'title' => '精彩节目',
                'desc' => ''
            ];
        }
    }
    return [
        'channel_name' => $cleanName,
        'date' => $date,
        'url' => DEFAULT_URL,
        'icon' => ICON_BASE_URL . urlencode($cleanName) . '.png',
        'epg_data' => $epgData
    ];
}

/**
 * 7. 用CURL获取XML（解决HTTPS访问问题）
 */
function getEpgXmlByCurl() {
    // 创建CURL资源（适配OpenWrt的curl）
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => EPG_XML_URL,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_USERAGENT => 'OpenWrt EPG Fetcher',
        CURLOPT_TIMEOUT => 30,
        // 跳过SSL验证（OpenWrt根证书可能不全）
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => false,
        // 压缩传输（节省流量）
        CURLOPT_ENCODING => 'gzip, deflate'
    ]);

    $xmlStr = curl_exec($ch);
    $error = curl_error($ch);
    curl_close($ch);

    if ($error || $xmlStr === false) {
        throw new Exception("CURL获取XML失败：$error");
    }
    return $xmlStr;
}

/**
 * 8. 缓存处理（适配/tmp目录）
 */
function getCachedXml() {
    // 创建缓存目录（/tmp可写）
    if (!is_dir(CACHE_DIR)) {
        mkdir(CACHE_DIR, 0700, true);
    }
    $cacheFile = CACHE_DIR . 'epg_full.xml';

    // 检查缓存是否有效
    if (file_exists($cacheFile) && (time() - filemtime($cacheFile)) < CACHE_TTL) {
        return file_get_contents($cacheFile);
    }

    // 重新获取XML并缓存
    $xmlStr = getEpgXmlByCurl();
    file_put_contents($cacheFile, $xmlStr);
    return $xmlStr;
}

// ===== 主逻辑 =====
try {
    ob_clean(); // 清空缓冲，确保头信息可设置

    // 获取请求参数
    $oriChannel = $_GET['ch'] ?? $_GET['channel'] ?? '';
    $cleanChannel = cleanChannelName($oriChannel);
    $targetDate = getFormatDate($_GET['date'] ?? '');
    $isDebug = isset($_GET['debug']) && $_GET['debug'] === '1';

    // 频道为空返回404
    if (empty($cleanChannel)) {
        http_response_code(404);
        header('Content-Type: text/html; charset=utf-8');
        echo "404 Not Found - 未指定频道参数";
        exit;
    }

    // 获取XML（带缓存）
    $xmlStr = getCachedXml();

    // 解析XML
    $parseResult = parseEpgXml($xmlStr, $cleanChannel, $targetDate, $isDebug);

    // 生成响应数据
    if (!empty($parseResult['matchedProgrammes'])) {
        $epgData = [
            'channel_name' => $cleanChannel,
            'date' => $targetDate,
            'url' => DEFAULT_URL,
            'icon' => ICON_BASE_URL . urlencode($cleanChannel) . '.png',
            'epg_data' => []
        ];
        foreach ($parseResult['matchedProgrammes'] as $prog) {
            $epgData['epg_data'][] = [
                'start' => $prog['start']->format('H:i'),
                'end' => $prog['stop']->format('H:i'),
                'title' => $prog['title'],
                'desc' => $prog['desc']
            ];
        }
        if ($isDebug) $epgData['debug'] = $parseResult;
    } else {
        $epgData = getDefaultEpgData($oriChannel, $targetDate);
        if ($isDebug) $epgData['debug'] = $parseResult;
    }

    // 返回JSON响应
    http_response_code(200);
    header('Content-Type: application/json; charset=utf-8');
    header('Access-Control-Allow-Origin: *');
    header('Cache-Control: max-age=3600');
    echo json_encode($epgData, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);

} catch (Exception $e) {
    // 错误处理
    ob_clean();
    http_response_code(500);
    header('Content-Type: application/json; charset=utf-8');
    header('Access-Control-Allow-Origin: *');
    $errorData = [
        'error' => '服务器错误',
        'message' => $e->getMessage()
    ];
    if (DEBUG) $errorData['stack'] = $e->getTraceAsString();
    echo json_encode($errorData, JSON_UNESCAPED_UNICODE);
}

// 关闭输出缓冲
ob_end_flush();
?>
