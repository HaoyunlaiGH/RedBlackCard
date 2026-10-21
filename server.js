// Please Choose — zero-dependency Node.js server (Node 18+)
// Run: node server.js   then open http://localhost:3000
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT) || 3000;
const DATA_FILE = path.join(__dirname, "data.json");
// index.html can sit in a "public" folder or next to server.js
const INDEX = [path.join(__dirname, "public", "index.html"), path.join(__dirname, "index.html")]
  .find((f) => fs.existsSync(f)) || path.join(__dirname, "index.html");
// If the host's page has been closed this long, the Host seat in that group frees up.
const HOST_TIMEOUT_MS = (Number(process.env.HOST_TIMEOUT_MIN) || 10) * 60 * 1000;
// A group nobody has used for this long is deleted.
const ROOM_IDLE_MS = (Number(process.env.ROOM_IDLE_MIN) || 60) * 60 * 1000;
const MAX_ROOMS = 10000;

// rooms: group number -> {
//   round, votes: {cid: 0|1}, hostToken, hostSeen, hostCid, lastActive,
//   lives: null | 1..10, users: {cid: {pid, name, lives}}
// }
let rooms = {};
try { rooms = JSON.parse(fs.readFileSync(DATA_FILE, "utf8")).rooms || {}; } catch (_) {}
const clients = new Map(); // group -> Set of { res, isHost }

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => fs.writeFile(DATA_FILE, JSON.stringify({ rooms }), () => {}), 300);
}

const validGroup = (g) => typeof g === "string" && /^[0-9]{1,12}$/.test(g);
const validCid = (c) => typeof c === "string" && /^[A-Za-z0-9-]{8,64}$/.test(c);

// Names: Chinese or English letters (single spaces between words allowed), max 20 characters.
const NAME_RE = /^[A-Za-z\u3400-\u4dbf\u4e00-\u9fff]+( [A-Za-z\u3400-\u4dbf\u4e00-\u9fff]+)*$/;
function cleanName(raw) {
  const name = typeof raw === "string" ? raw.trim().replace(/\s+/g, " ") : "";
  if (!name) return { error: "Name can't be empty." };
  if (Array.from(name).length > 20) return { error: "Name must be 20 characters or fewer." };
  if (!NAME_RE.test(name)) return { error: "Names can only use Chinese or English letters, with no symbols or punctuation." };
  return { name };
}

// The first time a group number appears, it becomes a new room.
function getRoom(g) {
  if (!validGroup(g)) return null;
  if (!rooms[g]) {
    if (Object.keys(rooms).length >= MAX_ROOMS) return null;
    rooms[g] = { round: 1, votes: {}, hostToken: null, hostSeen: 0, hostCid: null, lives: null, users: {}, lastActive: Date.now() };
    save();
  }
  const r = rooms[g];
  if (!r.users) r.users = {};
  if (r.lives === undefined) r.lives = null;
  r.lastActive = Date.now();
  return r;
}
function roomClients(g) {
  if (!clients.has(g)) clients.set(g, new Set());
  return clients.get(g);
}
function hostActive(g, room) {
  if (!room.hostToken) return false;
  for (const c of roomClients(g)) if (c.isHost) return true;
  return Date.now() - room.hostSeen < HOST_TIMEOUT_MS;
}
const isHost = (room, token) => typeof token === "string" && !!room.hostToken && token === room.hostToken;
function publicState(g, room) {
  const active = hostActive(g, room);
  const hostPid = active && room.hostCid && room.users[room.hostCid] ? room.users[room.hostCid].pid : null;
  return {
    group: g,
    round: room.round,
    count: Object.keys(room.votes).length,
    hostTaken: active,
    lives: room.lives,
    users: Object.values(room.users).map((u) => ({ pid: u.pid, name: u.name, lives: u.lives, host: u.pid === hostPid })),
  };
}
function broadcast(g) {
  const room = rooms[g];
  if (!room) return;
  const msg = `data: ${JSON.stringify(publicState(g, room))}\n\n`;
  for (const c of roomClients(g)) c.res.write(msg);
}
function findByPid(room, pid) {
  for (const [cid, u] of Object.entries(room.users)) if (u.pid === pid) return { cid, user: u };
  return null;
}

function send(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve) => {
    let raw = "";
    req.on("data", (chunk) => { raw += chunk; if (raw.length > 4096) req.destroy(); });
    req.on("end", () => { try { resolve(JSON.parse(raw || "{}")); } catch (_) { resolve({}); } });
  });
}
const badGroup = (res) => send(res, 400, { error: "Enter a group number using digits only (up to 12)." });

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  const p = url.pathname;

  // ---------- page ----------
  if (req.method === "GET" && (p === "/" || p === "/index.html")) {
    fs.readFile(INDEX, (err, buf) => {
      if (err) return send(res, 500, { error: "index.html missing" });
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
      res.end(buf);
    });
    return;
  }

  // ---------- live updates for one group (Server-Sent Events) ----------
  if (req.method === "GET" && p === "/events") {
    const g = url.searchParams.get("g");
    const room = getRoom(g);
    if (!room) return badGroup(res);
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    const client = { res, isHost: isHost(room, url.searchParams.get("host")) };
    roomClients(g).add(client);
    if (client.isHost) room.hostSeen = Date.now();
    res.write(`retry: 3000\ndata: ${JSON.stringify(publicState(g, room))}\n\n`);
    req.on("close", () => {
      const set = roomClients(g);
      set.delete(client);
      if (set.size === 0) clients.delete(g);
      const r = rooms[g];
      if (r) {
        r.lastActive = Date.now();
        if (client.isHost) r.hostSeen = Date.now();
        save();
      }
    });
    return;
  }

  // ---------- room state plus my own vote and profile ----------
  if (req.method === "GET" && p === "/api/me") {
    const g = url.searchParams.get("g");
    const room = getRoom(g);
    if (!room) return badGroup(res);
    const cid = url.searchParams.get("cid");
    const ok = validCid(cid);
    const vote = ok && cid in room.votes ? room.votes[cid] : null;
    const u = ok ? room.users[cid] : null;
    return send(res, 200, { ...publicState(g, room), vote, me: u ? { pid: u.pid, name: u.name, lives: u.lives } : null });
  }

  if (req.method !== "POST") return send(res, 404, { error: "Not found" });
  const body = await readBody(req);
  const g = body.g;
  const room = getRoom(g);
  if (!room) return badGroup(res);
  const cid = body.cid;
  const asHost = isHost(room, body.token);

  switch (p) {
    case "/api/join": {
      if (!validCid(cid)) return send(res, 400, { error: "Bad request" });
      const { name, error } = cleanName(body.name);
      if (error) return send(res, 400, { error });
      const lower = name.toLowerCase();
      const taken = Object.entries(room.users).some(([c, u]) => c !== cid && u.name.toLowerCase() === lower);
      if (taken) return send(res, 409, { error: "This name is already taken in this group." });
      let u = room.users[cid];
      if (u) u.name = name;
      else u = room.users[cid] = { pid: crypto.randomBytes(6).toString("hex"), name, lives: room.lives };
      save(); broadcast(g);
      return send(res, 200, { pid: u.pid, name: u.name, lives: u.lives });
    }
    case "/api/host/claim": {
      if (!validCid(cid) || !room.users[cid]) return send(res, 400, { error: "Please enter your name first." });
      if (hostActive(g, room) && !asHost) return send(res, 409, { error: "Host has existed!" });
      if (!asHost) room.hostToken = crypto.randomUUID();
      room.hostCid = cid;
      room.hostSeen = Date.now();
      save(); broadcast(g);
      return send(res, 200, { token: room.hostToken, lives: room.lives });
    }
    case "/api/host/release": {
      if (!asHost) return send(res, 403, { error: "Not the host" });
      room.hostToken = null;
      room.hostCid = null;
      for (const c of roomClients(g)) c.isHost = false;
      save(); broadcast(g);
      return send(res, 200, { ok: true });
    }
    case "/api/lives/set": {
      if (!asHost) return send(res, 403, { error: "Not the host" });
      const n = body.lives;
      if (!Number.isInteger(n) || n < 1 || n > 10) return send(res, 400, { error: "Enter a whole number from 1 to 10." });
      room.lives = n;
      // Everyone without lives yet starts with the set number; nobody keeps more than the new number.
      for (const u of Object.values(room.users)) {
        if (u.lives === null || u.lives === undefined) u.lives = n;
        else if (u.lives > n) u.lives = n;
      }
      save(); broadcast(g);
      return send(res, 200, { lives: n });
    }
    case "/api/lives/deduct": {
      const t = findByPid(room, body.pid);
      if (!t) return send(res, 404, { error: "User not found" });
      const self = validCid(cid) && t.cid === cid;
      if (!asHost && !self) return send(res, 403, { error: "You can only deduct your own life." });
      if (t.user.lives === null) return send(res, 409, { error: "Lives haven't been set yet." });
      if (t.user.lives > 0) { t.user.lives -= 1; save(); broadcast(g); }
      return send(res, 200, { lives: t.user.lives });
    }
    case "/api/lives/restore": {
      if (!asHost) return send(res, 403, { error: "Only the host can restore." });
      const t = findByPid(room, body.pid);
      if (!t) return send(res, 404, { error: "User not found" });
      if (room.lives === null) return send(res, 409, { error: "Lives haven't been set yet." });
      t.user.lives = room.lives;
      save(); broadcast(g);
      return send(res, 200, { lives: t.user.lives });
    }
    case "/api/vote": {
      const { v, round } = body;
      if (!validCid(cid) || (v !== 0 && v !== 1)) return send(res, 400, { error: "Bad request" });
      const u = room.users[cid];
      if (!u) return send(res, 400, { error: "Please enter your name first." });
      if (u.lives === 0 && !asHost) return send(res, 403, { error: "You are out of lives!" });
      if (round !== room.round) return send(res, 409, { error: "round_changed", round: room.round });
      // A person can change their choice any time; only their latest choice counts.
      const isNew = !(cid in room.votes);
      if (room.votes[cid] !== v) {
        room.votes[cid] = v;
        save();
        if (isNew) broadcast(g);
      }
      return send(res, 200, { vote: v });
    }
    case "/api/cpt": {
      if (!asHost) return send(res, 403, { error: "Not the host" });
      let zeros = 0, ones = 0;
      for (const v of Object.values(room.votes)) v === 1 ? ones++ : zeros++;
      return send(res, 200, { sum: ones, zeros, ones, total: zeros + ones, at: Date.now() });
    }
    case "/api/reset": {
      if (!asHost) return send(res, 403, { error: "Not the host" });
      room.votes = {};
      room.round += 1;
      save(); broadcast(g);
      return send(res, 200, { round: room.round });
    }
    default:
      return send(res, 404, { error: "Not found" });
  }
});

// Heartbeat: keeps connections alive through proxies and updates each group's host status.
setInterval(() => { for (const g of clients.keys()) broadcast(g); }, 25000);

// Delete groups nobody has used for ROOM_IDLE_MS (no open pages and no requests).
setInterval(() => {
  const now = Date.now();
  let removed = 0;
  for (const g of Object.keys(rooms)) {
    const open = clients.has(g) && clients.get(g).size > 0;
    if (open) { rooms[g].lastActive = now; continue; }
    if (now - rooms[g].lastActive > ROOM_IDLE_MS) { delete rooms[g]; removed++; }
  }
  if (removed) { save(); console.log(`Deleted ${removed} idle group(s)`); }
}, 60 * 1000);

server.listen(PORT, () => console.log(`Please Choose running on http://localhost:${PORT}`));
