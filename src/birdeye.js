'use strict';
// src/birdeye.js — Birdeye API 封装 (V4 — 合并缓存，大幅降低API消耗)
//
// 架构：
//   1. 主力：Birdeye WebSocket SUBSCRIBE_PRICE（1s OHLCV 推送，延迟 50-150ms）
//   2. 兜底：HTTP /defi/price（WS 断开时自动切换）
//   3. FDV + LP：/defi/token_overview 合并请求，共享缓存（默认5分钟TTL）
//      ★ 修复V3：getFdv 和 getLiquidity 各自独立请求 → 合并为单次请求
//      ★ 缓存从5秒提升到5分钟，30个币×1/min vs 30个币×12/min，减少约90%消耗
//
// B-05 级别支持 WebSocket + 100 token 并发订阅

const WebSocket = require('ws');
const fetch     = require('node-fetch');
const logger    = require('./logger');

const BIRDEYE_KEY  = process.env.BIRDEYE_API_KEY || '';
const BASE         = 'https://public-api.birdeye.so';
// FDV/LP 缓存时间，默认30分钟（可通过 FDV_CACHE_MS 环境变量调整）
// ★ V5: 从5分钟提升到30分钟，FDV变化不敏感，买入前会强制刷新
const FDV_CACHE_MS = parseInt(process.env.FDV_CACHE_MS || String(30 * 60 * 1000), 10);

// ── WebSocket 价格流 ──────────────────────────────────────────────

const WS_URL = `wss://public-api.birdeye.so/socket/solana?x-api-key=${BIRDEYE_KEY}`;
const PING_INTERVAL_MS  = 25000;
const RECONNECT_BASE_MS = 1000;
const MAX_RECONNECT_MS  = 30000;

class BirdeyePriceStream {
  constructor() {
    this._ws         = null;
    this._connected  = false;
    this._pingTimer  = null;
    this._retryCount = 0;
    this._stopping   = false;

    // address → { price, ts, callbacks: Set<fn> }
    this._subscriptions = new Map();
  }

  start() {
    if (!BIRDEYE_KEY) {
      logger.warn('[BirdeyeWS] ⚠️ BIRDEYE_API_KEY 未设置，仅用 HTTP 轮询');
      return;
    }
    this._stopping = false;
    this._connect();
  }

  stop() {
    this._stopping = true;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._ws = null;
    }
    this._connected = false;
  }

  subscribe(address, onPrice) {
    if (!this._subscriptions.has(address)) {
      this._subscriptions.set(address, { price: null, ts: 0, callbacks: new Set() });
    }
    this._subscriptions.get(address).callbacks.add(onPrice);

    if (this._connected && this._ws?.readyState === WebSocket.OPEN) {
      this._sendSubscribe(address);
    }

    logger.info('[BirdeyeWS] 📌 订阅价格 %s，当前 %d 个', address.slice(0, 8) + '...', this._subscriptions.size);
  }

  unsubscribe(address) {
    if (this._connected && this._ws?.readyState === WebSocket.OPEN) {
      this._sendUnsubscribe(address);
    }
    this._subscriptions.delete(address);
    logger.info('[BirdeyeWS] 🔕 取消价格订阅 %s，剩余 %d 个', address.slice(0, 8) + '...', this._subscriptions.size);
  }

  getCachedPrice(address) {
    const sub = this._subscriptions.get(address);
    if (!sub || !sub.price || Date.now() - sub.ts > 10000) return null;
    return sub.price;
  }

  isConnected() { return this._connected; }

  _connect() {
    if (this._stopping) return;

    const safeUrl = WS_URL.replace(/x-api-key=[^&]+/, 'x-api-key=***');
    logger.info('[BirdeyeWS] 连接 %s ...', safeUrl);

    this._ws = new WebSocket(WS_URL);

    this._ws.on('open', () => {
      logger.info('[BirdeyeWS] ✅ WebSocket 已连接');
      this._connected  = true;
      this._retryCount = 0;

      this._pingTimer = setInterval(() => {
        if (this._ws?.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_INTERVAL_MS);

      for (const address of this._subscriptions.keys()) {
        this._sendSubscribe(address);
      }
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});

    this._ws.on('error', (err) => {
      logger.error('[BirdeyeWS] 错误: %s', err.message);
    });

    this._ws.on('close', () => {
      logger.warn('[BirdeyeWS] 连接关闭');
      this._connected = false;
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      if (!this._stopping) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_BASE_MS * Math.pow(1.5, this._retryCount - 1), MAX_RECONNECT_MS);
        logger.info('[BirdeyeWS] %ds 后重连 (第%d次)', (delay / 1000).toFixed(0), this._retryCount);
        setTimeout(() => this._connect(), delay);
      }
    });
  }

  _sendSubscribe(address) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    this._ws.send(JSON.stringify({
      type: 'SUBSCRIBE_PRICE',
      data: { queryType: 'simple', chartType: '1s', address, currency: 'usd' },
    }));
    logger.debug('[BirdeyeWS] 📡 SUBSCRIBE_PRICE %s', address.slice(0, 8) + '...');
  }

  _sendUnsubscribe(address) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    this._ws.send(JSON.stringify({
      type: 'UNSUBSCRIBE_PRICE',
      data: { queryType: 'simple', chartType: '1s', address, currency: 'usd' },
    }));
  }

  _handleMessage(rawData) {
    let msg;
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    if (msg.type === 'PRICE_DATA' && msg.data) {
      const { address } = msg.data;
      const close = parseFloat(msg.data.c);
      if (!address || !Number.isFinite(close) || close <= 0) return;

      const sub = this._subscriptions.get(address);
      if (!sub) return;

      const now = Date.now();
      sub.price = close;
      sub.ts    = now;

      const ohlcv = {
        open:   parseFloat(msg.data.o || close),
        high:   parseFloat(msg.data.h || close),
        low:    parseFloat(msg.data.l || close),
        volume: parseFloat(msg.data.v || 0),
      };

      for (const cb of sub.callbacks) {
        try { cb(close, now, ohlcv); } catch (err) {
          logger.error('[BirdeyeWS] 价格回调错误: %s', err.message);
        }
      }
    }
  }
}

// 单例
const priceStream = new BirdeyePriceStream();

// ── FDV + Liquidity 合并缓存 ─────────────────────────────────────
// ★ getFdv 和 getLiquidity 共享同一个 token_overview 请求 + 缓存
// ★ 默认缓存5分钟，30个币每分钟只请求1次（原来每5秒1次，减少60倍消耗）

const _overviewCache = new Map(); // address → { fdv, liquidity, ts }

async function _fetchOverview(address) {
  const cached = _overviewCache.get(address);
  if (cached && Date.now() - cached.ts < FDV_CACHE_MS) return cached;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const url = `${BASE}/defi/token_overview?address=${address}`;
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    if (!res.ok) {
      logger.warn('[Birdeye] token_overview %s 返回 %d', address, res.status);
      return cached || null;
    }
    const json = await res.json();
    const data = json?.data || {};
    // Birdeye token_overview 的 24h volume 字段可能有多种命名，全都兼容
    const v24h =
      data.v24hUSD ?? data.v24h ?? data.volume24hUSD ?? data.volume24h ??
      data.volume?.volume24hUSD ?? data.volume?.v24hUSD ?? null;
    const entry = {
      fdv:       data.fdv ?? data.mc ?? null,
      liquidity: data.liquidity ?? data.lp ?? null,
      // ★ V5: 代币创建时间（秒级时间戳 → 毫秒）
      createdAt: data.createdAt ? data.createdAt * 1000 : (data.createAt ? data.createAt * 1000 : null),
      // ★ V5-12: 24h 交易量(USD)，用于淘汰判断（比本地 ticks 累计更准确）
      v24hUSD:   typeof v24h === 'number' ? v24h : (v24h ? parseFloat(v24h) : null),
      ts:        Date.now(),
    };
    // 保留旧缓存中的 createdAt（不会变）
    if (!entry.createdAt && cached?.createdAt) entry.createdAt = cached.createdAt;
    _overviewCache.set(address, entry);
    return entry;
  } catch (err) {
    logger.warn('[Birdeye] _fetchOverview %s 失败: %s', address, err.message);
    return cached || null;
  } finally {
    clearTimeout(timeout);
  }
}

// ── HTTP 兜底价格查询 ─────────────────────────────────────────────
// ★ 双层缓存：WS缓存（10s）→ HTTP本地缓存（30s）→ 真实HTTP请求
// 避免BirdeyeWS推送不活跃的币（低流动性）每秒发HTTP

const _priceHttpCache = new Map(); // address → { price, ts }
const PRICE_HTTP_CACHE_MS = parseInt(process.env.PRICE_HTTP_CACHE_MS || '60000', 10); // ★ V5: 默认60秒

// ★ V5-13: 失败抑制 —— Birdeye 永久/临时错误时不再每秒打 HTTP
//   - 永久错误(400/404): Birdeye 没有此代币数据；TTL 长（默认 5 分钟）
//   - 临时错误(429/5xx/timeout): 可能是限流或瞬时故障；TTL 短（默认 30s）
const _priceFailCache = new Map(); // address → { status, ts, ttl, count, lastLogTs }
const PRICE_FAIL_PERM_TTL_MS = parseInt(process.env.PRICE_FAIL_PERM_TTL_MS || '300000', 10); // 5 min
const PRICE_FAIL_TEMP_TTL_MS = parseInt(process.env.PRICE_FAIL_TEMP_TTL_MS || '30000', 10);  // 30 s
const PRICE_FAIL_LOG_GAP_MS  = parseInt(process.env.PRICE_FAIL_LOG_GAP_MS  || '60000', 10);  // 1 min

function _recordFailure(address, status) {
  const perm = status === 400 || status === 404;
  const ttl = perm ? PRICE_FAIL_PERM_TTL_MS : PRICE_FAIL_TEMP_TTL_MS;
  const prev = _priceFailCache.get(address);
  const now = Date.now();
  _priceFailCache.set(address, {
    status,
    ts: now,
    ttl,
    count: (prev?.count || 0) + 1,
    lastLogTs: prev?.lastLogTs || 0,
  });
  // 日志去重：同一 address 同一状态码，1 分钟内只打一次
  const entry = _priceFailCache.get(address);
  if (now - entry.lastLogTs > PRICE_FAIL_LOG_GAP_MS) {
    entry.lastLogTs = now;
    if (perm) {
      logger.warn('[Birdeye] price %s 永久错误 HTTP %d (累计 %d 次)，已静默 %ds',
        address.slice(0, 8) + '...', status, entry.count, Math.round(ttl / 1000));
    } else {
      logger.warn('[Birdeye] price %s 临时错误 HTTP %d (累计 %d 次)，已静默 %ds',
        address.slice(0, 8) + '...', status, entry.count, Math.round(ttl / 1000));
    }
  }
}

/** 外部查询某 address 的失败状态 */
function getPriceFailStatus(address) {
  const f = _priceFailCache.get(address);
  if (!f) return null;
  if (Date.now() - f.ts >= f.ttl) {
    _priceFailCache.delete(address);
    return null;
  }
  return { status: f.status, count: f.count, remainingMs: f.ttl - (Date.now() - f.ts) };
}

async function getPrice(address) {
  // 1. 优先用 WS 缓存（最新，10秒有效）
  const wsCached = priceStream.getCachedPrice(address);
  if (wsCached !== null) return wsCached;

  // 2. HTTP 本地缓存（60秒，防止低流动性币每秒发请求）
  const httpCached = _priceHttpCache.get(address);
  if (httpCached && Date.now() - httpCached.ts < PRICE_HTTP_CACHE_MS) {
    return httpCached.price;
  }

  // ★ V5-13: 失败抑制 —— 最近失败过的直接返回 null，不发 HTTP
  const failed = _priceFailCache.get(address);
  if (failed && Date.now() - failed.ts < failed.ttl) {
    return null;
  }

  // 3. 真实 HTTP 请求
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const url = `${BASE}/defi/price?address=${address}`;
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    if (!res.ok) {
      _recordFailure(address, res.status);
      return null;
    }
    const json = await res.json();
    if (!json.success || !json.data) {
      _recordFailure(address, 200); // HTTP 200 但业务失败也算一次
      return null;
    }
    const price = json.data.value;
    if (!Number.isFinite(price) || price <= 0) {
      _recordFailure(address, 200);
      return null;
    }
    _priceHttpCache.set(address, { price, ts: Date.now() });
    // 成功后清除失败记录
    if (_priceFailCache.has(address)) _priceFailCache.delete(address);
    return price;
  } catch (err) {
    // 超时 / 网络错误
    _recordFailure(address, err.name === 'AbortError' ? 'TIMEOUT' : 'NET_ERR');
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function getFdv(address) {
  const entry = await _fetchOverview(address);
  return entry?.fdv ?? null;
}

/** 只读内存缓存，不发 HTTP 请求。缓存未命中时返回 null。
 *  用于主轮询的 FDV 监控：命中则检查，未命中则跳过（等买入前再刷）。*/
function getCachedFdv(address) {
  const cached = _overviewCache.get(address);
  if (!cached) return null;
  // 缓存已过期也返回旧值（宁可用旧值检查，也不跳过）
  return cached.fdv ?? null;
}

/** 强制绕过缓存，发 HTTP 请求获取最新 FDV。仅在买入前调用一次。*/
async function getFdvFresh(address) {
  _overviewCache.delete(address);          // 清除旧缓存，强制重新拉取
  const entry = await _fetchOverview(address);
  return entry?.fdv ?? null;
}

async function getLiquidity(address) {
  const entry = await _fetchOverview(address);
  return entry?.liquidity ?? null;
}

// ★ V5-12: 获取代币 24h 交易量(USD)，用于淘汰判断
//   复用 token_overview 缓存（默认 30 分钟），不产生额外请求
async function getV24hUSD(address) {
  const entry = await _fetchOverview(address);
  return entry?.v24hUSD ?? null;
}

function clearCache(address) {
  _overviewCache.delete(address);
  _priceHttpCache.delete(address);
}

/** 获取完整 overview（fdv + lp + createdAt），走缓存 */
async function getOverview(address) {
  return await _fetchOverview(address);
}

// ── 历史 OHLCV K 线拉取 ──────────────────────────────────────────
// Birdeye /defi/ohlcv 接口，返回历史 K 线数据
// type 映射：秒数 → Birdeye type 字符串
const KLINE_TYPE_MAP = {
  60:    '1m',
  180:   '3m',
  300:   '5m',
  900:   '15m',
  1800:  '30m',
  3600:  '1H',
  7200:  '2H',
  14400: '4H',
  21600: '6H',
  28800: '8H',
  43200: '12H',
  86400: '1D',
};

/**
 * 拉取历史 K 线，返回 candle 数组（可直接合并进 closedCandles）
 * @param {string} address - 代币地址
 * @param {number} intervalSec - K 线宽度（秒），如 300 = 5分钟
 * @param {number} bars - 需要拉取的 K 线根数，如 150
 * @returns {Array} candles - [{ openTime, closeTime, open, high, low, close, volume, buyVolume, sellVolume }]
 */
async function getOHLCV(address, intervalSec, bars = 150) {
  const type = KLINE_TYPE_MAP[intervalSec];
  if (!type) {
    // 没有精确匹配，找最接近的
    const keys = Object.keys(KLINE_TYPE_MAP).map(Number).sort((a, b) => a - b);
    const closest = keys.reduce((prev, curr) =>
      Math.abs(curr - intervalSec) < Math.abs(prev - intervalSec) ? curr : prev
    );
    logger.warn('[Birdeye] getOHLCV: %ds 无对应类型，使用 %ds (%s)', intervalSec, closest, KLINE_TYPE_MAP[closest]);
    return getOHLCV(address, closest, bars);
  }

  const now       = Math.floor(Date.now() / 1000);
  const time_from = now - intervalSec * (bars + 5); // 多拉5根，确保有足够数据
  const time_to   = now;

  const url = `${BASE}/defi/ohlcv?address=${address}&type=${type}&time_from=${time_from}&time_to=${time_to}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  // ★ 诊断 meta：附加到返回数组上，不破坏原调用者用法
  const meta = {
    requestedBars: bars,
    returnedItems: 0,
    closedBars:    0,
    httpStatus:    0,
    error:         null,
    fetchedAt:     Date.now(),
  };

  try {
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    meta.httpStatus = res.status;
    if (!res.ok) {
      meta.error = `HTTP_${res.status}`;
      logger.warn('[Birdeye] getOHLCV %s 返回 %d', address.slice(0, 8), res.status);
      const empty = []; empty._meta = meta; return empty;
    }
    const json = await res.json();
    const items = json?.data?.items || [];
    meta.returnedItems = items.length;
    if (items.length === 0) {
      meta.error = 'NO_DATA';
      logger.warn('[Birdeye] getOHLCV %s 无数据', address.slice(0, 8));
      const empty = []; empty._meta = meta; return empty;
    }

    // 转换为系统 candle 格式
    const candles = items.map(item => {
      const openTime  = item.unixTime * 1000;          // 秒 → 毫秒
      const closeTime = openTime + intervalSec * 1000;
      return {
        openTime,
        closeTime,
        open:       item.o,
        high:       item.h,
        low:        item.l,
        close:      item.c,
        volume:     item.v || 0,     // Birdeye OHLCV 的 v 是总量
        buyVolume:  0,               // 历史数据无买卖方向分离，置0
        sellVolume: 0,
        tickCount:  1,
        priceTickCount: 1,
        fromHistory: true,           // 标记为历史数据
      };
    });

    // 按时间升序排列，去掉最后一根（可能未收盘）
    candles.sort((a, b) => a.openTime - b.openTime);
    const closed = candles.slice(0, -1); // 去掉最后一根未收盘K线
    meta.closedBars = closed.length;

    logger.info('[Birdeye] getOHLCV %s type=%s 拉取 %d 根历史K线 (请求%d根, Birdeye返回%d条)',
      address.slice(0, 8) + '...', type, closed.length, bars, items.length);
    closed._meta = meta;
    return closed;
  } catch (err) {
    meta.error = err.name === 'AbortError' ? 'TIMEOUT' : (err.message || 'UNKNOWN');
    logger.warn('[Birdeye] getOHLCV %s 失败: %s', address.slice(0, 8), err.message);
    const empty = []; empty._meta = meta; return empty;
  } finally {
    clearTimeout(timeout);
  }
}

module.exports = { getPrice, getPriceFailStatus, getFdv, getCachedFdv, getFdvFresh, getLiquidity, getV24hUSD, getOverview, clearCache, priceStream, getOHLCV };
