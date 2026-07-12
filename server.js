/**
 * ⚔ 사이버 투기장 — 실시간 백엔드 (Node + WebSocket)
 *
 * 설계 원칙 (요금·성능 안전):
 *  - 관중이 던지는 팝콘/표를 개별 저장·전송하지 않는다.
 *  - 방 단위로 카운터만 모으고(집계), 상태는 TICK 주기(400ms)에 "한 번만" 브로드캐스트한다.
 *  - 모든 클라이언트는 방의 요약 상태 하나만 구독한다.  ← 팬아웃 폭발 방지
 *
 * 저장소: 인메모리 (MVP). 실제 서비스화 시 이 부분만 DB(Redis/Postgres 등)로 교체.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const wsLite = require('./ws-lite'); // 의존성 0 — 내장 모듈만 사용

const PORT = process.env.PORT || 3000;
const TICK_MS = 400; // 상태 브로드캐스트 주기 (집계)

/* ---------------- 정적 파일 서버 ---------------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };
const server = http.createServer((req, res) => {
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(__dirname, 'public', path.normalize(urlPath).replace(/^(\.\.[/\\])+/, ''));
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------------- 방(링) 데이터 ---------------- */
// stance: -2..+2  ( +2 = 강하게 A, -2 = 강하게 B, 0 = 중립 )
// A-share 점수(0~100)로 환산: (stance + 2) / 4 * 100
const stanceToPoints = (s) => ((s + 2) / 4) * 100;
const sideOf = (s) => (s > 0 ? 'A' : s < 0 ? 'B' : 'N');

function makeRoom(id, cat, topic, a, b, aClaim, bClaim, seedOpinion, seedAudience) {
  // 가상 관중(baseline) — 소수의 실제 접속자만으로도 여론바가 그럴듯하게 보이도록
  const vCount = 400 + Math.floor(Math.random() * 400);
  const vAvg = seedOpinion; // 목표 평균 A-share
  return {
    id, cat, topic,
    fighterA: a, fighterB: b, aClaim, bClaim,
    status: 'live',
    clients: new Set(),          // 이 방의 ws 집합
    // 집계 카운터
    corn: 0, rock: 0, heat: 0,
    // 여론(가상 baseline)
    virtualSum: vAvg * vCount, virtualCount: vCount,
    baseAudience: seedAudience,
    // 이번 tick 동안 쌓인 반응 (스톰 연출용 → 브로드캐스트 후 비움)
    reactionBuf: [],
    dirty: true,
    ended: false,
  };
}

const rooms = new Map();
[
  ['g1', 'game', '롤 듀오, 정글이 안 봐준 게 잘못 vs 라인전을 못 이긴 게 잘못', '정글탓_클리어', '라인전의정석', '정글이 안 봐준 게 잘못', '라인전을 못 이긴 게 잘못', 52, 8240],
  ['l1', 'love', '기념일에 게임한 남친, 헤어질 사유 된다 vs 오버다', '현실주의연애', '낭만파', '헤어질 사유 된다', '그건 오버다', 48, 6110],
  ['w1', 'work', '퇴근 후 업무 카톡, 무시해도 된다 vs 답은 해야 한다', '워라밸사수대', '조직생활만렙', '무시해도 된다', '답은 해야 한다', 57, 4300],
  ['f1', 'life', '민트초코는 음식이다 vs 치약이다', '민초단_수호자', '반민초연합', '민초는 음식이다', '민초는 치약이다', 50, 3920],
].forEach(r => rooms.set(r[0], makeRoom(...r)));

/* ---------------- 여론/상태 계산 ---------------- */
function computeOpinionA(room) {
  let sum = room.virtualSum, count = room.virtualCount;
  for (const ws of room.clients) {
    if (ws.stance != null) { sum += stanceToPoints(ws.stance); count += 1; }
  }
  return count ? sum / count : 50;
}
function roomState(room) {
  return {
    type: 'state',
    ringId: room.id,
    opinionA: Math.round(computeOpinionA(room) * 10) / 10,
    corn: room.corn, rock: room.rock, heat: room.heat,
    audience: room.baseAudience + room.clients.size,
    reactions: room.reactionBuf, // [{side, type, n}]
  };
}
function lobbyPayload() {
  return {
    type: 'lobby',
    rooms: [...rooms.values()].map(r => ({
      id: r.id, cat: r.cat, topic: r.topic,
      a: r.fighterA, b: r.fighterB, aClaim: r.aClaim, bClaim: r.bClaim,
      audience: r.baseAudience + r.clients.size,
      heat: r.heat, opinionA: Math.round(computeOpinionA(r)),
      status: r.status,
    })),
  };
}

/* ---------------- WebSocket ---------------- */
let nextId = 1;
const NICKS = ['아무개', '이름없는검투사', '관전러', '팝콘요정', '판사지망생', '중립기어', '키배관찰자'];

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(room, obj) { for (const ws of room.clients) send(ws, obj); }

wsLite.attach(server, (ws) => {
  ws.id = nextId++;
  ws.nick = NICKS[Math.floor(Math.random() * NICKS.length)] + (100 + Math.floor(Math.random() * 900));
  ws.room = null;
  ws.stance = null;
  ws.stanceBefore = null;
  ws.isRealName = false;

  send(ws, lobbyPayload());

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const room = ws.room ? rooms.get(ws.room) : null;

    switch (m.type) {
      case 'lobby':
        send(ws, lobbyPayload());
        break;

      case 'join': {
        const r = rooms.get(m.ringId);
        if (!r) { send(ws, { type: 'error', msg: '없는 링입니다' }); break; }
        leaveRoom(ws);
        ws.room = r.id;
        ws.stance = clampStance(m.stance);
        ws.stanceBefore = ws.stance;
        ws.isRealName = !!m.isRealName;
        r.clients.add(ws);
        r.dirty = true;
        send(ws, {
          type: 'joined',
          ring: { id: r.id, cat: r.cat, topic: r.topic, a: r.fighterA, b: r.fighterB, aClaim: r.aClaim, bClaim: r.bClaim, audience: r.baseAudience + r.clients.size },
          state: roomState(r),
        });
        break;
      }

      case 'vote': // 라이브 중 입장 변경 → 여론바가 모두에게 실시간 이동
        if (room) { ws.stance = clampStance(m.stance); room.dirty = true; }
        break;

      case 'react': { // 팝콘/돌 (클라이언트가 0.5s 묶어서 보냄)
        if (!room) break;
        const corn = Math.min(50, Math.max(0, m.corn | 0));
        const rock = Math.min(50, Math.max(0, m.rock | 0));
        const side = m.side === 'A' || m.side === 'B' ? m.side : 'A';
        room.corn += corn; room.rock += rock;
        room.heat += corn + rock * 2;
        if (corn) room.reactionBuf.push({ side, type: 'corn', n: corn });
        if (rock) room.reactionBuf.push({ side, type: 'rock', n: rock });
        room.dirty = true;
        break;
      }

      case 'chat': { // 링 안 발언 (A/B 진영 지지 발언)
        if (!room) break;
        const text = String(m.text || '').slice(0, 200);
        const side = m.side === 'A' || m.side === 'B' ? m.side : 'N';
        if (text.trim()) broadcast(room, { type: 'chat', side, nick: ws.nick, text });
        break;
      }

      case 'close': { // 판결 (누구나 트리거 — MVP)
        if (!room || room.ended) break;
        endMatch(room);
        break;
      }

      case 'leave':
        leaveRoom(ws);
        send(ws, lobbyPayload());
        break;
    }
  });

  ws.on('close', () => leaveRoom(ws));
});

function clampStance(s) { s = s | 0; return s < -2 ? -2 : s > 2 ? 2 : s; }
function leaveRoom(ws) {
  if (!ws.room) return;
  const r = rooms.get(ws.room);
  if (r) { r.clients.delete(ws); r.dirty = true; }
  ws.room = null;
}

/* ---------------- 판결 ---------------- */
function endMatch(room) {
  room.ended = true; room.status = 'ended';
  const opinionA = computeOpinionA(room);
  const aWin = opinionA >= 50;
  const totalN = room.baseAudience + room.clients.size;

  // 실명 배심원 (실접속자 중 isRealName) — 가상 baseline은 익명으로 간주
  let realSum = 0, realCount = 0;
  for (const ws of room.clients) if (ws.isRealName && ws.stance != null) { realSum += stanceToPoints(ws.stance); realCount += 1; }
  // 실접속 실명자가 적으면 전체 여론에 소폭 노이즈를 섞어 표시(데모용)
  const realA = realCount >= 3 ? realSum / realCount : Math.max(5, Math.min(95, opinionA + (Math.random() * 8 - 4)));

  // 델타: 실접속자 중 입장 전→후 side가 바뀐 수 + 가상 baseline 추정
  let bToA = 0, aToB = 0;
  for (const ws of room.clients) {
    const before = sideOf(ws.stanceBefore ?? 0), after = sideOf(ws.stance ?? 0);
    if (before !== 'A' && after === 'A') bToA++;
    if (before !== 'B' && after === 'B') aToB++;
  }
  const vChanged = Math.round(totalN * (0.05 + Math.random() * 0.05));
  const vB2A = Math.round(vChanged * (aWin ? 0.66 : 0.34));
  bToA += vB2A; aToB += vChanged - vB2A;

  broadcast(room, {
    type: 'verdict',
    winner: aWin ? room.aClaim : room.bClaim,
    winSide: aWin ? 'A' : 'B',
    totalA: Math.round(opinionA), totalB: Math.round(100 - opinionA), totalN,
    realA: Math.round(realA), realB: Math.round(100 - realA), realN: Math.max(realCount, 120 + Math.floor(Math.random() * 300)),
    changed: bToA + aToB, bToA, aToB,
  });

  // 재사용을 위해 잠시 후 리셋
  setTimeout(() => { room.ended = false; room.status = 'live'; room.corn = room.rock = room.heat = 0; }, 8000);
}

/* ---------------- TICK: 집계 브로드캐스트 ---------------- */
setInterval(() => {
  for (const room of rooms.values()) {
    if (room.clients.size === 0) { room.reactionBuf = []; room.dirty = false; continue; }
    if (!room.dirty && room.reactionBuf.length === 0) continue;
    broadcast(room, roomState(room));
    room.reactionBuf = [];
    room.dirty = false;
  }
}, TICK_MS);

server.listen(PORT, () => {
  console.log(`⚔ 사이버 투기장 서버 실행 중 → http://localhost:${PORT}`);
});
