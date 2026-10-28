// Crunchy Party relay server: zero dependencies, just `node server.js`.
// Keeps a tiny bit of state per room and forwards play/pause/seek updates
// to everyone else in the same room over WebSockets.

const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_PAYLOAD = 64 * 1024;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// roomId -> { clients: Set<client>, state: { playing, time, at, by } | null }
const rooms = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Crunchy Party relay is running (v2).\n');
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') {
    socket.destroy();
    return;
  }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  socket.setNoDelay(true);

  const client = { socket, room: null, name: 'Someone', role: 'player', alive: true };
  clients.add(client);
  let buf = Buffer.alloc(0);

  socket.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    let frame;
    while ((frame = readFrame(buf))) {
      if (frame.error) { socket.destroy(); return; }
      buf = buf.subarray(frame.consumed);
      client.alive = true;
      if (frame.opcode === 0x8) { leave(client); sendFrame(socket, 0x8, Buffer.alloc(0)); socket.end(); return; }
      if (frame.opcode === 0x9) { sendFrame(socket, 0xA, frame.payload); continue; }
      if (frame.opcode === 0x1) handleMessage(client, frame.payload.toString('utf8'));
    }
  });
  socket.on('close', () => { leave(client); clients.delete(client); });
  socket.on('error', () => { leave(client); clients.delete(client); });
});

// Ping everyone every 30s; drop connections that didn't answer the last ping.
// Also keeps hosting proxies from closing idle connections.
const clients = new Set();
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { leave(c); clients.delete(c); c.socket.destroy(); continue; }
    c.alive = false;
    sendFrame(c.socket, 0x9, Buffer.alloc(0));
  }
}, 30000);

function readFrame(buf) {
  if (buf.length < 2) return null;
  const opcode = buf[0] & 0x0f;
  const masked = (buf[1] & 0x80) !== 0;
  let len = buf[1] & 0x7f;
  let off = 2;
  if (len === 126) {
    if (buf.length < 4) return null;
    len = buf.readUInt16BE(2);
    off = 4;
  } else if (len === 127) {
    if (buf.length < 10) return null;
    len = Number(buf.readBigUInt64BE(2));
    off = 10;
  }
  if (len > MAX_PAYLOAD) return { error: true };
  const maskOff = off;
  if (masked) off += 4;
  if (buf.length < off + len) return null;
  const payload = Buffer.from(buf.subarray(off, off + len));
  if (masked) for (let i = 0; i < len; i++) payload[i] ^= buf[maskOff + (i & 3)];
  return { opcode, payload, consumed: off + len };
}

function sendFrame(socket, opcode, payload) {
  if (socket.destroyed) return;
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.from([0x80 | opcode, len]);
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2);
  }
  socket.write(Buffer.concat([header, payload]));
}

function send(client, msg) {
  sendFrame(client.socket, 0x1, Buffer.from(JSON.stringify(msg)));
}

function broadcast(room, msg, except) {
  for (const c of room.clients) if (c !== except) send(c, msg);
}

// Where the video should be right now, given the last known state.
function currentState(state) {
  if (!state) return null;
  const time = state.playing ? state.time + (Date.now() - state.at) / 1000 : state.time;
  return { playing: state.playing, time, by: state.by };
}

// Only video players count as people watching; 'nav' connections just track the episode.
function presence(room) {
  const names = [...room.clients].filter((c) => c.role !== 'nav').map((c) => c.name);
  broadcast(room, { type: 'presence', names });
}

function leave(client) {
  const room = client.room && rooms.get(client.room);
  if (!room) return;
  room.clients.delete(client);
  client.room = null;
  if (client.role !== 'nav') console.log(`[${new Date().toLocaleTimeString()}] ${client.name} left room ${room.id}`);
  if (room.clients.size === 0) rooms.delete(room.id);
  else presence(room);
}

function handleMessage(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch { return; }

  if (msg.type === 'join') {
    leave(client);
    const id = String(msg.room || '').slice(0, 64);
    if (!id) return;
    client.name = String(msg.name || 'Someone').slice(0, 32);
    client.role = msg.role === 'nav' ? 'nav' : 'player';
    let room = rooms.get(id);
    if (!room) { room = { id, clients: new Set(), state: null, episode: null }; rooms.set(id, room); }
    room.clients.add(client);
    client.room = id;
    if (client.role === 'nav') {
      send(client, { type: 'episode', initial: true, ...(room.episode || { id: null }) });
      return;
    }
    const watching = [...room.clients].filter((c) => c.role !== 'nav').length;
    console.log(`[${new Date().toLocaleTimeString()}] ${client.name} joined room ${id} (${watching} watching)`);
    const state = currentState(room.state);
    if (state) send(client, { type: 'sync', ...state, action: 'join' });
    presence(room);
    return;
  }

  if (msg.type === 'episode') {
    const room = client.room && rooms.get(client.room);
    if (!room || typeof msg.id !== 'string' || typeof msg.path !== 'string' || !msg.path.startsWith('/')) return;
    if (room.episode && room.episode.id === msg.id) return; // e.g. everyone autoplayed into it together
    room.episode = { id: msg.id.slice(0, 64), path: msg.path.slice(0, 300), by: client.name };
    room.state = null;
    console.log(`[${new Date().toLocaleTimeString()}] ${client.name} moved room ${room.id} to episode ${room.episode.id}`);
    broadcast(room, { type: 'episode', ...room.episode }, client);
    return;
  }

  if (msg.type === 'update') {
    const room = client.room && rooms.get(client.room);
    if (!room || typeof msg.time !== 'number' || !isFinite(msg.time)) return;
    room.state = { playing: !!msg.playing, time: msg.time, at: Date.now(), by: client.name };
    console.log(`[${new Date().toLocaleTimeString()}] ${client.name}: ${msg.action} at ${msg.time.toFixed(1)}s -> sent to ${room.clients.size - 1} other(s)`);
    broadcast(room, { type: 'sync', ...currentState(room.state), action: String(msg.action || '') }, client);
  }
}

server.listen(PORT, () => console.log(`Crunchy Party relay listening on port ${PORT}`));
