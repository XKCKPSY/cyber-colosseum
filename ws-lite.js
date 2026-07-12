/**
 * ws-lite.js — 의존성 0의 초경량 WebSocket 서버 (RFC 6455 최소 구현)
 * 외부 패키지 없이 Node 내장 모듈(http/crypto)만으로 동작.
 * 텍스트 프레임 송수신 + ping/pong + close 처리. (MVP용)
 */
const crypto = require('crypto');
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

class Conn {
  constructor(socket) {
    this.socket = socket;
    this.readyState = 1; // OPEN
    this._handlers = { message: [], close: [] };
    this._buf = Buffer.alloc(0);
    this._frag = [];       // 연속 프레임(fragmentation) 조립용
    this._fragOp = 0;
    socket.on('data', (d) => this._onData(d));
    socket.on('close', () => this._closed());
    socket.on('error', () => this._closed());
  }
  on(ev, cb) { if (this._handlers[ev]) this._handlers[ev].push(cb); return this; }
  _emit(ev, arg) { (this._handlers[ev] || []).forEach((cb) => { try { cb(arg); } catch (e) { console.error(e); } }); }

  _closed() {
    if (this.readyState === 3) return;
    this.readyState = 3; // CLOSED
    this._emit('close');
  }

  send(str) {
    if (this.readyState !== 1) return;
    const payload = Buffer.from(str, 'utf8');
    const len = payload.length;
    let header;
    if (len < 126) { header = Buffer.from([0x81, len]); }
    else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x81; header[1] = 126; header.writeUInt16BE(len, 2); }
    else { header = Buffer.alloc(10); header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
    try { this.socket.write(Buffer.concat([header, payload])); } catch { this._closed(); }
  }

  close() { try { this.socket.write(Buffer.from([0x88, 0x00])); this.socket.end(); } catch {} this._closed(); }

  _onData(chunk) {
    this._buf = Buffer.concat([this._buf, chunk]);
    // 가능한 만큼 프레임 파싱
    while (true) {
      if (this._buf.length < 2) return;
      const b0 = this._buf[0], b1 = this._buf[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;
      if (len === 126) { if (this._buf.length < 4) return; len = this._buf.readUInt16BE(2); offset = 4; }
      else if (len === 127) { if (this._buf.length < 10) return; len = Number(this._buf.readBigUInt64BE(2)); offset = 10; }
      let mask;
      if (masked) { if (this._buf.length < offset + 4) return; mask = this._buf.slice(offset, offset + 4); offset += 4; }
      if (this._buf.length < offset + len) return; // 아직 페이로드 덜 옴
      let payload = this._buf.slice(offset, offset + len);
      if (masked) { const out = Buffer.alloc(len); for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i & 3]; payload = out; }
      this._buf = this._buf.slice(offset + len);

      if (opcode === 0x8) { this.close(); return; }              // close
      else if (opcode === 0x9) { this._pong(payload); }          // ping → pong
      else if (opcode === 0xA) { /* pong: 무시 */ }
      else if (opcode === 0x1 || opcode === 0x2 || opcode === 0x0) {
        // 텍스트/바이너리/연속 프레임 조립
        if (opcode !== 0x0) this._fragOp = opcode;
        this._frag.push(payload);
        if (fin) {
          const full = Buffer.concat(this._frag);
          this._frag = [];
          if (this._fragOp === 0x1) this._emit('message', full.toString('utf8'));
          this._fragOp = 0;
        }
      }
    }
  }
  _pong(payload) {
    const len = payload.length;
    const header = len < 126 ? Buffer.from([0x8A, len]) : Buffer.from([0x8A, 126, (len >> 8) & 255, len & 255]);
    try { this.socket.write(Buffer.concat([header, payload])); } catch {}
  }
}

/** http 서버에 WebSocket 업그레이드를 붙인다. onConnection(conn) 콜백 호출. */
function attach(server, onConnection) {
  server.on('upgrade', (req, socket) => {
    const key = req.headers['sec-websocket-key'];
    if (!key) { socket.destroy(); return; }
    const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
    );
    socket.setNoDelay(true);
    onConnection(new Conn(socket));
  });
}

module.exports = { attach };
