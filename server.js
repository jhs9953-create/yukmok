'use strict';
/**
 * 육목 온라인 서버
 * - 방, 턴, 6목 판정을 모두 서버가 검증하고 WebSocket으로 실시간 동기화합니다.
 * - 같은 포트에서 게임 화면(public/index.html)도 함께 제공합니다.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const SIZE = 19;
const DIRS = [[1, 0], [0, 1], [1, 1], [1, -1]];
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const MAX_ROOMS = 500;
const ROOM_IDLE_MS = 60 * 60 * 1000;      // 아무도 접속해 있지 않은 방은 1시간 뒤 삭제
const LOBBY_GRACE_MS = 30 * 1000;         // 대국 시작 전 접속이 끊기면 30초 뒤 자리를 비움

const INDEX_HTML = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));

/** @type {Map<string, {code:string, mode:2|3, seats:Array<null|{token:string,name:string,ws:any,dropTimer:any}>, history:Array<{x:number,y:number,p:number}>, lastActive:number}>} */
const rooms = new Map();

// ---------- 규칙 ----------
function hasSix(board, x, y, p) {
  for (const [dx, dy] of DIRS) {
    let count = 1;
    let cx = x + dx, cy = y + dy;
    while (cx >= 0 && cx < SIZE && cy >= 0 && cy < SIZE && board[cy][cx] === p) { count++; cx += dx; cy += dy; }
    cx = x - dx; cy = y - dy;
    while (cx >= 0 && cx < SIZE && cy >= 0 && cy < SIZE && board[cy][cx] === p) { count++; cx -= dx; cy -= dy; }
    if (count >= 6) return true;
  }
  return false;
}

// 첫 차례만 돌 1개, 그 이후에는 모두 2개씩 둡니다.
function computeState(history, mode) {
  const board = Array.from({ length: SIZE }, () => new Array(SIZE).fill(null));
  let orderIdx = 0, placed = 0, target = 1;
  for (const mv of history) {
    board[mv.y][mv.x] = mv.p;
    placed++;
    if (placed >= target) { orderIdx = (orderIdx + 1) % mode; placed = 0; target = 2; }
  }
  let winner = null;
  if (history.length) {
    const last = history[history.length - 1];
    if (hasSix(board, last.x, last.y, last.p)) winner = last.p;
  }
  const draw = winner === null && history.length === SIZE * SIZE;
  return { board, orderIdx, winner, draw, gameOver: winner !== null || draw };
}

// ---------- 통신 보조 ----------
function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function fail(ws, code, msg, fatal) {
  send(ws, { t: 'error', code, msg, fatal: !!fatal });
}
function snapshot(room, you) {
  return {
    code: room.code,
    mode: room.mode,
    history: room.history,
    seats: room.seats.map(s => (s ? { name: s.name, connected: !!s.ws } : null)),
    you,
  };
}
function broadcast(room) {
  room.seats.forEach((s, i) => {
    if (s && s.ws) send(s.ws, { t: 'room', room: snapshot(room, i) });
  });
}
function cleanName(v) {
  const s = String(v == null ? '' : v).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 12);
  return s || '플레이어';
}
function makeCode() {
  for (let n = 0; n < 30; n++) {
    let c = '';
    for (let i = 0; i < 5; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}

// ---------- 좌석 관리 ----------
function seatPlayer(ws, room, idx, name) {
  const token = crypto.randomBytes(16).toString('hex');
  room.seats[idx] = { token, name, ws, dropTimer: null };
  room.lastActive = Date.now();
  ws.ctx = { room, idx };
  send(ws, { t: 'joined', code: room.code, token });
  broadcast(room);
}

function releaseSeat(room, idx) {
  const s = room.seats[idx];
  if (!s) return;
  clearTimeout(s.dropTimer);
  room.seats[idx] = null;
  if (room.seats.every(x => x === null)) rooms.delete(room.code);
  else broadcast(room);
}

function leaveSeat(ws) {
  if (!ws.ctx) return;
  const { room, idx } = ws.ctx;
  ws.ctx = null;
  const s = room.seats[idx];
  if (s && s.ws === ws) releaseSeat(room, idx);
}

function detach(ws) {
  if (!ws.ctx) return;
  const { room, idx } = ws.ctx;
  ws.ctx = null;
  const s = room.seats[idx];
  if (!s || s.ws !== ws) return;
  s.ws = null;
  room.lastActive = Date.now();
  // 대국 시작 전에는 끊긴 자리를 잡아두지 않습니다 (잠깐 끊긴 경우를 위해 유예 시간을 둠)
  s.dropTimer = setTimeout(() => {
    if (room.seats[idx] === s && !s.ws && room.history.length === 0) releaseSeat(room, idx);
  }, LOBBY_GRACE_MS);
  broadcast(room);
}

// ---------- 메시지 처리 ----------
function handle(ws, m) {
  switch (m.t) {
    case 'ping':
      return send(ws, { t: 'pong' });

    case 'create': {
      const mode = m.mode === 2 ? 2 : m.mode === 3 ? 3 : 0;
      if (!mode) return fail(ws, 'bad_request', '잘못된 요청입니다.');
      if (rooms.size >= MAX_ROOMS) return fail(ws, 'busy', '서버가 가득 찼습니다. 잠시 후 다시 시도해주세요.');
      const code = makeCode();
      if (!code) return fail(ws, 'busy', '방을 만들 수 없습니다. 잠시 후 다시 시도해주세요.');
      leaveSeat(ws);
      const room = { code, mode, seats: new Array(mode).fill(null), history: [], lastActive: Date.now() };
      rooms.set(code, room);
      return seatPlayer(ws, room, 0, cleanName(m.name));
    }

    case 'join': {
      const code = String(m.code || '').trim().toUpperCase();
      const room = rooms.get(code);
      if (!room) return fail(ws, 'no_room', '방을 찾을 수 없습니다. 코드를 확인해주세요.');
      if (ws.ctx && ws.ctx.room === room) return broadcast(room);
      const idx = room.seats.findIndex(s => s === null);
      if (idx === -1) return fail(ws, 'full', '방이 가득 찼습니다.');
      leaveSeat(ws);
      return seatPlayer(ws, room, idx, cleanName(m.name));
    }

    case 'resume': {
      const room = rooms.get(String(m.code || '').trim().toUpperCase());
      if (!room) return fail(ws, 'no_room', '방이 사라졌습니다. 새로 방을 만들거나 다시 참가해주세요.', true);
      const token = String(m.token || '');
      const idx = room.seats.findIndex(s => s && s.token === token);
      if (idx === -1) return fail(ws, 'bad_token', '이 방의 자리가 비워졌습니다. 다시 참가해주세요.', true);
      const s = room.seats[idx];
      clearTimeout(s.dropTimer);
      if (s.ws && s.ws !== ws) {            // 같은 사람이 다른 연결로 돌아온 경우
        const old = s.ws;
        old.ctx = null;
        old.close(4000, 'replaced');
      }
      s.ws = ws;
      room.lastActive = Date.now();
      ws.ctx = { room, idx };
      return broadcast(room);
    }

    case 'move': {
      if (!ws.ctx) return fail(ws, 'no_room', '방에 입장해 있지 않습니다.');
      const { room, idx } = ws.ctx;
      if (!room.seats.every(Boolean)) return fail(ws, 'wait', '아직 모든 플레이어가 입장하지 않았습니다.');
      const x = m.x, y = m.y;
      if (!Number.isInteger(x) || !Number.isInteger(y) || x < 0 || x >= SIZE || y < 0 || y >= SIZE) {
        return fail(ws, 'bad_request', '잘못된 위치입니다.');
      }
      const st = computeState(room.history, room.mode);
      if (st.gameOver) return fail(ws, 'over', '이미 끝난 게임입니다.');
      if (st.orderIdx !== idx) return fail(ws, 'turn', '아직 내 차례가 아닙니다.');
      if (st.board[y][x] !== null) return fail(ws, 'occupied', '이미 돌이 놓인 자리입니다.');
      room.history.push({ x, y, p: idx });
      room.lastActive = Date.now();
      return broadcast(room);
    }

    case 'undo': {
      if (!ws.ctx) return;
      const { room, idx } = ws.ctx;
      const last = room.history[room.history.length - 1];
      const st = computeState(room.history, room.mode);
      if (!last || last.p !== idx || st.gameOver) return fail(ws, 'undo', '무를 수 있는 내 수가 없습니다.');
      room.history.pop();
      room.lastActive = Date.now();
      return broadcast(room);
    }

    case 'rematch': {
      if (!ws.ctx) return;
      const { room } = ws.ctx;
      if (!computeState(room.history, room.mode).gameOver) return fail(ws, 'rematch', '게임이 끝난 뒤에 다시 시작할 수 있습니다.');
      room.history = [];
      room.lastActive = Date.now();
      return broadcast(room);
    }

    case 'leave':
      leaveSeat(ws);
      return send(ws, { t: 'left' });

    default:
      return;
  }
}

// ---------- HTTP + WebSocket ----------
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('ok');
  }
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-cache',
      'X-Content-Type-Options': 'nosniff',
    });
    return res.end(INDEX_HTML);
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: 2048 });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.ctx = null;
  ws.rate = { count: 0, t: Date.now() };
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    const now = Date.now();
    if (now - ws.rate.t > 1000) ws.rate = { count: 0, t: now };
    if (++ws.rate.count > 20) { ws.close(1008, 'too many messages'); return; }
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    try { handle(ws, m); } catch (e) { console.error('handler error', e); }
  });
  ws.on('close', () => detach(ws));
  ws.on('error', () => {});
});

// 끊어진 연결을 정리하고, 프록시가 유휴 연결을 끊지 않도록 주기적으로 ping
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  }
}, 25000);

// 오래 방치된 방 삭제
const sweeper = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const anyone = room.seats.some(s => s && s.ws);
    if (!anyone && now - room.lastActive > ROOM_IDLE_MS) rooms.delete(code);
  }
}, 5 * 60 * 1000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`육목 서버가 포트 ${PORT}에서 실행 중입니다.`);
});

function shutdown() {
  clearInterval(heartbeat);
  clearInterval(sweeper);
  for (const ws of wss.clients) ws.terminate();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
