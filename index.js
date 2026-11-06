/**
 * MCP Zing MP3 Server cho Xiaozhi AI ESP32
 * Chuẩn kết nối WebSocket Client tới Xiaozhi Cloud
 * Hỗ trợ bộ giải mã Esp32Music trực tiếp trên ESP32
 */

const express = require("express");
const fetch = require("node-fetch");
const cors = require("cors");
const path = require("path");
const fs = require("fs");
const WebSocket = require("ws");
const zing = require("./zingmp3");
const extraTools = require("./tools");
const { spawn } = require("child_process");
let ffmpegPath = null;
try { ffmpegPath = require("ffmpeg-static"); } catch (e) { console.warn("[ffmpeg] ffmpeg-static not available, serving original audio:", e.message); }

const app = express();
app.use(cors());
app.use(express.json());

// ─── Tự động phát hiện vị trí từ IP Client (GeoIP) ───────────────────────
let lastResolvedIp = "";
let lastIpCheckTime = 0;

async function detectCityFromIp(ip) {
  if (!ip) return;
  if (ip.includes("127.0.0.1") || ip === "::1" || ip.startsWith("10.") || ip.startsWith("192.168.")) return;
  if (ip === lastResolvedIp && Date.now() - lastIpCheckTime < 3600000) return;

  try {
    const res = await fetch(`http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,country,regionName,city`, {
      timeout: 5000,
    });
    const data = await res.json();
    if (data && data.status === "success" && data.city) {
      lastResolvedIp = ip;
      lastIpCheckTime = Date.now();
      extraTools.setAutoDetectedLocation(data.city);
      console.log(`[GeoIP] 📍 Đã nhận diện vị trí từ IP ${ip}: ${data.city}, ${data.regionName || ""}, ${data.country || ""}`);
    }
  } catch (err) {
    // ignore
  }
}

app.use((req, res, next) => {
  const forwarded = req.headers["x-forwarded-for"];
  const clientIp = forwarded ? forwarded.split(",")[0].trim() : req.socket.remoteAddress;
  if (clientIp) {
    detectCityFromIp(clientIp).catch(() => {});
  }
  next();
});

app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 5005;
const DEVICES_FILE = path.join(__dirname, "devices.json");
const connectedDevices = new Map();

// ─── Quản lý lưu trữ thiết bị (devices.json) ──────────────────────────────
function loadSavedDevices() {
  try {
    if (fs.existsSync(DEVICES_FILE)) {
      const data = fs.readFileSync(DEVICES_FILE, "utf-8");
      const parsed = JSON.parse(data);
      if (Array.isArray(parsed) && parsed.length > 0) return parsed;
    }
  } catch (e) {
    console.error(`[storage] ❌ Lỗi đọc devices.json: ${e.message}`);
  }
  return [];
}

function saveDevicesToFile(devices) {
  try {
    fs.writeFileSync(DEVICES_FILE, JSON.stringify(devices, null, 2), "utf-8");
  } catch (e) {
    console.error(`[storage] ❌ Lỗi ghi devices.json: ${e.message}`);
  }
}

function addSavedDevice(deviceName, wssUrl) {
  const devices = loadSavedDevices();
  const idx = devices.findIndex((d) => d.deviceName === deviceName);
  const entry = {
    deviceName,
    wssUrl,
    savedAt: new Date().toISOString(),
    autoConnect: true,
  };
  if (idx >= 0) {
    devices[idx] = { ...devices[idx], ...entry };
  } else {
    devices.push(entry);
  }
  saveDevicesToFile(devices);
}

function removeSavedDevice(deviceName) {
  const devices = loadSavedDevices();
  const filtered = devices.filter((d) => d.deviceName !== deviceName);
  saveDevicesToFile(filtered);
  return filtered.length < devices.length;
}

function updateAutoConnect(deviceName, autoConnect) {
  const devices = loadSavedDevices();
  const dev = devices.find((d) => d.deviceName === deviceName);
  if (dev) {
    dev.autoConnect = autoConnect;
    saveDevicesToFile(devices);
    return true;
  }
  return false;
}

// ─── Định nghĩa MCP Tools chuẩn có đầy đủ inputSchema ─────────────────────
const MCP_TOOLS = [
  {
    name: "search_music",
    description: "Tìm kiếm bài hát, ca sĩ trên Zing MP3 Việt Nam để tra cứu thông tin. LƯU Ý QUAN TRỌNG: Để phát nhạc trực tiếp ra loa của thiết bị, bạn KHÔNG gọi công cụ này mà BẮT BUỘC phải gọi công cụ phần cứng thiết bị 'self.music.play_song'.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Tên bài hát hoặc ca sĩ cần tìm (ví dụ: 'sơn tùng', 'lạc trôi')",
        },
        limit: {
          type: "number",
          description: "Số lượng bài cần tìm (mặc định 5)",
          default: 5,
        },
      },
      required: ["query"],
    },
  },
  {
    name: "get_song_info",
    description: "Lấy thông tin chi tiết bài hát: tên, ca sĩ, album, thể loại, thời lượng.",
    inputSchema: {
      type: "object",
      properties: {
        encodeId: {
          type: "string",
          description: "Mã bài hát encodeId",
        },
      },
      required: ["encodeId"],
    },
  },
  {
    name: "get_lyrics",
    description: "Lấy lời bài hát (lời đồng bộ thời gian và lời văn bản).",
    inputSchema: {
      type: "object",
      properties: {
        encodeId: {
          type: "string",
          description: "Mã bài hát encodeId",
        },
      },
      required: ["encodeId"],
    },
  },
  {
    name: "get_top_charts",
    description: "Lấy danh sách các bài hát đang thịnh hành (Top Charts) trên Zing MP3.",
    inputSchema: {
      type: "object",
      properties: {
        limit: {
          type: "number",
          description: "Số lượng bài hát (mặc định 10)",
          default: 10,
        },
      },
    },
  },
  {
    name: "get_weather",
    description: "Tra cứu thời tiết (nhiệt độ, mưa/nắng, độ ẩm, gió, UV). Nếu người dùng không nói rõ địa điểm hoặc chỉ hỏi 'hôm nay thế nào', 'thời tiết ở đây', hệ thống sẽ tự động tra cứu theo vị trí hiện tại.",
    inputSchema: {
      type: "object",
      properties: {
        location: {
          type: "string",
          description: "Tên địa điểm cần xem thời tiết (ví dụ: 'Hà Nội', 'TP.HCM', 'Đà Nẵng'). Có thể bỏ trống nếu hỏi thời tiết tại chỗ.",
        },
      },
    },
  },
  {
    name: "get_lunar_calendar",
    description: "Tra cứu âm lịch hôm nay, can chi ngày/tháng/năm, năm con gì theo lịch vạn niên Việt Nam.",
    inputSchema: {
      type: "object",
      properties: {
        date: {
          type: "string",
          description: "Ngày dương lịch cần tra dạng YYYY-MM-DD (để trống nếu muốn xem ngày hôm nay)",
        },
      },
    },
  },
  {
    name: "get_latest_news",
    description: "Điểm tin tức báo chí mới nhất trong ngày từ VnExpress.",
    inputSchema: {
      type: "object",
      properties: {
        category: {
          type: "string",
          description: "Chuyên mục tin tức: 'tin-moi-nhat', 'thoi-su', 'the-gioi', 'kinh-doanh', 'giai-tri', 'the-thao'",
          default: "tin-moi-nhat",
        },
      },
    },
  },
  {
    name: "web_search",
    description: "Tra cứu thông tin trên Internet, Google, Wikipedia và Bách khoa toàn thư. BẮT BUỘC gọi công cụ này khi người dùng hỏi các câu hỏi kiến thức, hỏi 'ai là', 'ở đâu', 'như thế nào', 'hiện tại', 'chủ tịch nước', 'thời sự', 'nhân vật', 'tin tức'.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Từ khóa hoặc câu hỏi cần tra cứu (ví dụ: 'Chủ tịch nước Việt Nam', 'Sơn Tùng M-TP', 'Thời sự')",
        },
      },
      required: ["query"],
    },
  },
];

// ─── Xử lý các lệnh gọi MCP Tool ─────────────────────────────────────────
async function handleToolCall(name, args = {}) {
  console.log(`[tool_call] 🛠️ Thực thi: ${name} với args:`, JSON.stringify(args));

  if (name === "search_music") {
    const q = args.query || args.q || args.song || "";
    const limit = args.limit || 5;
    const songs = await zing.searchSongs(q, limit);
    return {
      message: songs.length ? `Tìm thấy ${songs.length} bài hát cho "${q}"` : `Không tìm thấy bài hát cho "${q}"`,
      songs,
    };
  }

  if (name === "play_music") {
    const song = args.song || args.query || args.name || "bài hát";
    return {
      message: `Để phát bài hát ra loa thiết bị, bạn hãy gọi công cụ phần cứng 'self.music.play_song' với tham số song_name='${song}'.`,
    };
  }

  if (name === "get_stream_url") {
    const streamUrl = await zing.getStreamUrl(args.encodeId);
    return { encodeId: args.encodeId, streamUrl };
  }

  if (name === "get_song_info") {
    const info = await zing.getSongInfo(args.encodeId);
    return info || { error: "Không tìm thấy bài hát" };
  }

  if (name === "get_lyrics") {
    const lyrics = await zing.getLyrics(args.encodeId);
    return { encodeId: args.encodeId, lyrics };
  }

  if (name === "get_top_charts") {
    const limit = args.limit || 10;
    const charts = await zing.getTopCharts(limit);
    return { charts, message: `🔥 Top ${charts.length} bài hot` };
  }

  if (name === "get_weather") {
    const loc = args.location || args.city || "";
    return await extraTools.getWeather(loc);
  }

  if (name === "get_lunar_calendar") {
    return extraTools.getLunarCalendar(args.date);
  }

  if (name === "get_latest_news") {
    const cat = args.category || "tin-moi-nhat";
    return await extraTools.getLatestNews(cat);
  }

  if (name === "web_search") {
    const q = args.query || args.q || "";
    return await extraTools.searchWeb(q);
  }

  throw new Error(`Tool không hỗ trợ: ${name}`);
}

// ─── WebSocket Client tới Xiaozhi Cloud ──────────────────────────────────
function handleXiaozhiMessage(ws, raw, deviceName) {
  let msg;
  try {
    msg = JSON.parse(raw.toString());
  } catch {
    return;
  }

  const { id, method, params } = msg;
  if (!method || method.startsWith("notifications/")) return;

  console.log(`[${deviceName}] ← ${method}`);

  if (method === "initialize") {
    const response = {
      jsonrpc: "2.0",
      id,
      result: {
        protocolVersion: "2024-11-05",
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: "xiaozhi-vn-music", version: "1.0.0" },
      },
    };
    ws.send(JSON.stringify(response));
    console.log(`[${deviceName}] → initialize OK`);
    return;
  }

  if (method === "tools/list") {
    const response = {
      jsonrpc: "2.0",
      id,
      result: { tools: MCP_TOOLS },
    };
    ws.send(JSON.stringify(response));
    console.log(`[${deviceName}] → tools/list OK (${MCP_TOOLS.length} tools)`);
    return;
  }

  if (method === "tools/call") {
    const { name, arguments: args } = params || {};
    handleToolCall(name, args)
      .then((toolResult) => {
        const response = {
          jsonrpc: "2.0",
          id,
          result: {
            content: [{ type: "text", text: JSON.stringify(toolResult, null, 2) }],
          },
        };
        ws.send(JSON.stringify(response));
        console.log(`[${deviceName}] → tools/call ${name} OK`);
      })
      .catch((err) => {
        console.error(`[${deviceName}] ❌ Lỗi tools/call ${name}:`, err.message);
        const response = {
          jsonrpc: "2.0",
          id,
          error: { code: -32603, message: err.message },
        };
        ws.send(JSON.stringify(response));
      });
    return;
  }

  if (method === "ping") {
    ws.send(JSON.stringify({ jsonrpc: "2.0", id, result: {} }));
    return;
  }
}

function connectToXiaozhi(deviceName, wssUrl) {
  const device = connectedDevices.get(deviceName);
  if (device?.ws && device.ws.readyState === WebSocket.OPEN) {
    device.ws.close();
  }
  if (device?.reconnectTimer) {
    clearTimeout(device.reconnectTimer);
  }

  console.log(`[WSS] Đang kết nối tới Xiaozhi: ${deviceName}`);

  const ws = new WebSocket(wssUrl, {
    headers: {
      "User-Agent": "Xiaozhi-VN-Music/1.0",
      Accept: "application/json",
    },
  });

  connectedDevices.set(deviceName, {
    ws,
    url: wssUrl,
    connectedAt: null,
    status: "connecting",
    reconnectTimer: null,
  });

  ws.on("open", () => {
    const dev = connectedDevices.get(deviceName);
    if (dev) {
      dev.status = "connected";
      dev.connectedAt = new Date().toISOString();
    }
    console.log(`[WSS] ✅ Đã kết nối: ${deviceName}`);
  });

  ws.on("message", (data) => handleXiaozhiMessage(ws, data, deviceName));

  ws.on("close", (code) => {
    const dev = connectedDevices.get(deviceName);
    if (dev) dev.status = "disconnected";
    console.log(`[WSS] ❌ Ngắt kết nối: ${deviceName} (code ${code})`);

    if (code === 4004) {
      console.error(`[WSS] ⛔ Token hết hạn hoặc không hợp lệ: ${deviceName}`);
      return;
    }

    const timer = setTimeout(() => {
      if (connectedDevices.has(deviceName)) {
        console.log(`[WSS] 🔄 Reconnecting: ${deviceName}`);
        connectToXiaozhi(deviceName, wssUrl);
      }
    }, 10000);

    if (connectedDevices.has(deviceName)) {
      connectedDevices.get(deviceName).reconnectTimer = timer;
    }
  });

  ws.on("error", (err) => {
    console.error(`[WSS] ⚠️ Lỗi socket ${deviceName}: ${err.message}`);
  });
}

// ─── Debug API ───────────────────────────────────────────────────────────
app.get("/debug_stream", async (req, res) => {
  const id = req.query.id || "qYD31k5TVTCQ";
  try {
    const info = await zing.getSongInfo(id);
    const stream = await zing.getStreamUrl(id);
    res.json({ id, info, stream });
  } catch (err) {
    res.json({ error: err.message });
  }
});

// ─── API cho Firmware ESP32 (Esp32Music) ───────────────────────────────────
app.get("/stream_pcm", async (req, res) => {
  const song = req.query.song || "";
  const artist = req.query.artist || "";
  const searchQuery = `${song} ${artist}`.trim();

  console.log(`[ESP32] 📻 Yêu cầu bài hát: "${searchQuery}"`);
  if (!searchQuery) {
    return res.status(400).json({ error: "Missing song parameter" });
  }

  try {
    const results = await zing.searchSongs(searchQuery, 1);
    if (!results || results.length === 0) {
      return res.status(404).json({ error: "Song not found" });
    }

    const firstSong = results[0];
    const responsePayload = {
      title: firstSong.title,
      artist: firstSong.artist,
      audio_url: `/proxy_audio?id=${firstSong.encodeId}`,
      lyric_url: `/proxy_lyric?id=${firstSong.encodeId}`,
      language: "vietnamese",
    };

    console.log(`[ESP32] ✅ Trả về cho thiết bị:`, responsePayload);
    res.json(responsePayload);
  } catch (err) {
    console.error(`[ESP32] ❌ Lỗi stream_pcm:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/proxy_audio", async (req, res) => {
  const id = req.query.id;
  if (!id) return res.status(400).send("Missing id");

  try {
    const streamUrl = await zing.getStreamUrl(id);
    if (!streamUrl) {
      return res.status(404).send("Stream URL not found");
    }

    console.log("[proxy_audio] stream MP3 id=" + id);
    const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    // Raw passthrough (original behaviour) when ffmpeg is not available or ?raw=1 is given.
    if (!ffmpegPath || req.query.raw === "1") {
      const rawRes = await fetch(streamUrl, { headers: { "User-Agent": UA, Range: req.headers.range || "bytes=0-" } });
      res.set({
        "Content-Type": rawRes.headers.get("content-type") || "audio/mpeg",
        "Content-Length": rawRes.headers.get("content-length") || "",
        "Accept-Ranges": "bytes",
      });
      rawRes.body.pipe(res);
      return;
    }

    // Low-bandwidth stream for small devices: mono, 16 kHz, 48 kbps MP3 without ID3 tag or cover art.
    // A Range request resumes by skipping the first N bytes of the (deterministic) transcoded output.
    const m = /bytes=(\d+)-/.exec(req.headers.range || "");
    const outOffset = m ? parseInt(m[1], 10) : 0;
    let skip = 0;
    let srcRange = "bytes=0-";
    if (outOffset > 0) {
      // Resume after a dropped connection: the output is a constant 6000 B/s, so jump straight to the matching
      // time in the 128 kbps source (after its ID3 tag) instead of transcoding and discarding everything before it.
      try {
        const head = await fetch(streamUrl, { headers: { "User-Agent": UA, Range: "bytes=0-9" } });
        const hb = await head.buffer();
        let id3 = 0;
        if (hb.length >= 10 && hb.toString("latin1", 0, 3) === "ID3") {
          id3 = (((hb[6] & 0x7f) << 21) | ((hb[7] & 0x7f) << 14) | ((hb[8] & 0x7f) << 7) | (hb[9] & 0x7f)) + 10;
        }
        srcRange = "bytes=" + (id3 + Math.floor((outOffset / 6000) * 16000)) + "-";
      } catch (e) {
        skip = outOffset;
      }
    }
    const srcRes = await fetch(streamUrl, { headers: { "User-Agent": UA, Range: srcRange } });
    const ff = spawn(ffmpegPath, ["-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-vn", "-map_metadata", "-1",
      "-ac", "1", "-ar", "16000", "-b:a", "48k", "-id3v2_version", "0", "-write_xing", "0", "-f", "mp3", "pipe:1"]);
    res.set({ "Content-Type": "audio/mpeg", "Accept-Ranges": "bytes", "Cache-Control": "no-store" });
    srcRes.body.pipe(ff.stdin);
    ff.stdin.on("error", () => {});
    // Pace the output slightly faster than real time (about 7 KB/s for a 6 KB/s stream) so the TLS records stay a few
    // hundred bytes long. A small device cannot allocate the 16 KB buffer that coalesced records would need.
    const queue = [];
    let queued = 0;
    let ended = false;
    let ticks = 0;
    const timer = setInterval(() => {
      if (res.writableEnded || res.destroyed) { clearInterval(timer); return; }
      ticks++;
      let allowance = ticks <= 10 ? 1400 : 350; // quick start, then steady pace
      while (allowance > 0 && queue.length > 0) {
        const c = queue[0];
        const n = Math.min(c.length, allowance);
        res.write(c.subarray(0, n));
        allowance -= n;
        queued -= n;
        if (n === c.length) queue.shift(); else queue[0] = c.subarray(n);
      }
      if (queued < 65536) ff.stdout.resume();
      if (ended && queue.length === 0) { clearInterval(timer); res.end(); }
    }, 50);
    ff.stdout.on("data", (chunk) => {
      if (skip > 0) {
        if (chunk.length <= skip) { skip -= chunk.length; return; }
        chunk = chunk.subarray(skip);
        skip = 0;
      }
      queue.push(chunk);
      queued += chunk.length;
      if (queued >= 65536) ff.stdout.pause();
    });
    ff.stdout.on("end", () => { ended = true; });
    ff.stderr.on("data", (d) => console.error("[ffmpeg]", d.toString().trim()));
    ff.on("error", (err) => {
      console.error("[proxy_audio] ffmpeg error:", err.message);
      if (!res.headersSent) res.status(500).send(err.message); else res.end();
    });
    res.on("close", () => {
      clearInterval(timer);
      try { ff.kill("SIGKILL"); } catch (e) {}
      try { srcRes.body.destroy(); } catch (e) {}
    });
  } catch (err) {
    console.error("[proxy_audio] error:", err.message);
    if (!res.headersSent) res.status(500).send(err.message);
  }
});

app.get("/proxy_lyric", async (req, res) => {
  const id = req.query.id;
  if (!id) return res.status(400).send("Missing id");

  try {
    const lyrics = await zing.getLyrics(id);
    res.type("text/plain").send(lyrics || "");
  } catch (err) {
    console.error(`[proxy_lyric] ❌ Lỗi:`, err.message);
    res.status(500).send(err.message);
  }
});

// ─── API cho Giao diện Quản lý Web Portal ──────────────────────────────────
app.get("/api/devices", (req, res) => {
  const saved = loadSavedDevices();
  const devices = [];

  for (const [name, dev] of connectedDevices.entries()) {
    devices.push({
      name: name,
      deviceName: name,
      url: dev.url,
      wssUrl: dev.url,
      status: dev.status,
      connectedAt: dev.connectedAt,
    });
  }

  res.json({ devices, connected: devices, saved });
});

app.get("/api/saved-devices", (req, res) => {
  const saved = loadSavedDevices();
  const savedDevices = saved.map(d => ({
    ...d,
    isConnected: connectedDevices.has(d.deviceName) && connectedDevices.get(d.deviceName).status === "connected",
  }));
  res.json({ savedDevices });
});

app.get("/api/location", (req, res) => {
  res.json({ location: extraTools.getAutoDetectedLocation() });
});

app.post("/api/location", (req, res) => {
  const { city } = req.body;
  if (city) {
    extraTools.setAutoDetectedLocation(city);
    return res.json({ success: true, location: city });
  }
  res.status(400).json({ error: "Thiếu tên thành phố" });
});

app.post("/api/connect", (req, res) => {
  const { deviceName, wssUrl } = req.body;
  if (!deviceName || !wssUrl) {
    return res.status(400).json({ error: "Thiếu deviceName hoặc wssUrl" });
  }

  addSavedDevice(deviceName, wssUrl);
  connectToXiaozhi(deviceName, wssUrl);
  res.json({ success: true, message: `Đang kết nối tới ${deviceName}` });
});

app.post("/api/disconnect", (req, res) => {
  const { deviceName } = req.body;
  const dev = connectedDevices.get(deviceName);
  if (dev) {
    if (dev.reconnectTimer) clearTimeout(dev.reconnectTimer);
    if (dev.ws) dev.ws.close();
    connectedDevices.delete(deviceName);
    res.json({ success: true, message: `Đã ngắt kết nối ${deviceName}` });
  } else {
    res.status(404).json({ error: "Không tìm thấy thiết bị" });
  }
});

app.delete("/api/devices/:name", (req, res) => {
  const deviceName = req.params.name;
  const dev = connectedDevices.get(deviceName);
  if (dev) {
    if (dev.reconnectTimer) clearTimeout(dev.reconnectTimer);
    if (dev.ws) dev.ws.close();
    connectedDevices.delete(deviceName);
  }
  removeSavedDevice(deviceName);
  res.json({ success: true });
});

app.patch("/api/devices/:name/auto-connect", (req, res) => {
  const { autoConnect } = req.body;
  updateAutoConnect(req.params.name, autoConnect);
  res.json({ success: true });
});

// ─── Khởi động Server ────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`===============================================`);
  console.log(`🎵 Xiaozhi Zing MP3 MCP Server đang chạy tại port ${PORT}`);
  console.log(`🌐 Web UI: http://localhost:${PORT}`);
  console.log(`===============================================`);

  // Tự động kết nối lại các thiết bị đã lưu
  const saved = loadSavedDevices();
  console.log(`[storage] 🔄 Tự động kết nối ${saved.length} thiết bị...`);
  for (const dev of saved) {
    if (dev.autoConnect !== false && dev.wssUrl) {
      connectToXiaozhi(dev.deviceName, dev.wssUrl);
    }
  }
});
