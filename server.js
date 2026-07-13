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

/* ---------------- 매치(경기) 진행 시간 ----------------
 * 1부 → 중간 휴식(우세 1회 공개) → 2부 → 판결 → (쿨다운 후) 자동 재시작
 * 기본값 = 운영용(1부 10분·2부 10분). 로컬 테스트는 환경변수로 짧게 override.
 */
const PART1_MS   = +process.env.PART1_MS   || 10 * 60 * 1000; // 1부 10분
const BREAK_MS   = +process.env.BREAK_MS   || 60 * 1000;      // 중간 휴식 60초 (우세 공개)
const PART2_MS   = +process.env.PART2_MS   || 10 * 60 * 1000; // 2부 10분
const COOLDOWN_MS= +process.env.COOLDOWN_MS|| 20 * 1000;      // 판결 후 20초 뒤 새 경기
const MATCH_TICK = 1000; // 경기 페이즈 점검 주기
const now = () => Date.now();

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

function makeRoom(id, cat, topic, aClaim, bClaim) {
  return {
    id, cat, topic, aClaim, bClaim,
    clients: new Set(),          // 이 방의 ws 집합 (= 실제 접속자)
    seatA: null, seatB: null,    // 선수 자리 (ws 참조) — 비어 있으면 도전자 모집중
    // 경기 진행 상태
    phase: 'part1',              // part1 | break | part2 | ended
    phaseEndsAt: now() + PART1_MS,
    verdict: null,               // 판결 결과 스냅샷 (ended 동안 신규 입장자에게 전송)
    // 집계 카운터
    corn: 0, rock: 0, heat: 0,
    // 이번 tick 동안 쌓인 반응 (스톰 연출용 → 브로드캐스트 후 비움)
    reactionBuf: [],
    dirty: true,
    emptySince: null,            // 유저 방 자동 정리용
  };
}
function seatInfo(room) {
  return { a: room.seatA ? room.seatA.nick : null, b: room.seatB ? room.seatB.nick : null };
}
// 라운드 중(part1/part2)에는 우세 비공개, 휴식/판결에만 공개
function isRevealed(room) { return room.phase === 'break' || room.phase === 'ended'; }
function partOf(room) { return room.phase === 'part2' ? 2 : 1; }
function msLeftOf(room) { return Math.max(0, room.phaseEndsAt - now()); }

const rooms = new Map();
[
  ['g1', 'game', '롤 듀오, 정글이 안 봐준 게 잘못 vs 라인전을 못 이긴 게 잘못', '정글이 안 봐준 게 잘못', '라인전을 못 이긴 게 잘못'],
  ['l1', 'love', '기념일에 게임한 남친, 헤어질 사유 된다 vs 오버다', '헤어질 사유 된다', '그건 오버다'],
  ['w1', 'work', '퇴근 후 업무 카톡, 무시해도 된다 vs 답은 해야 한다', '무시해도 된다', '답은 해야 한다'],
  ['f1', 'life', '민트초코는 음식이다 vs 치약이다', '민초는 음식이다', '민초는 치약이다'],
].forEach(r => rooms.set(r[0], makeRoom(...r)));

/* ---------------- 여론/상태 계산 (전부 실제 접속자 기반) ---------------- */
function computeOpinionA(room) {
  let sum = 0, count = 0;
  for (const ws of room.clients) {
    if (ws.stance != null) { sum += stanceToPoints(ws.stance); count += 1; }
  }
  return count ? sum / count : 50; // 투표한 사람 없으면 50:50
}
// 실제 표 집계 (A편 / 중립 / B편)
function voteTally(room) {
  let a = 0, b = 0, n = 0, voters = 0;
  for (const ws of room.clients) {
    if (ws.stance == null) continue;
    voters++;
    const side = sideOf(ws.stance);
    if (side === 'A') a++; else if (side === 'B') b++; else n++;
  }
  return { a, b, n, voters };
}
function roomState(room) {
  const tally = voteTally(room);
  const revealed = isRevealed(room);
  const s = {
    type: 'state',
    ringId: room.id,
    phase: room.phase,
    part: partOf(room),
    msLeft: msLeftOf(room),
    revealed,
    seats: seatInfo(room),         // 선수 자리 현황
    audience: room.clients.size,   // 실제 접속자 수 (항상 공개)
    votes: tally.voters,           // 총 투표 수 (항상 공개)
    corn: room.corn, rock: room.rock, heat: room.heat,
    reactions: room.reactionBuf,   // [{side, type, n}]
  };
  if (revealed) {                  // 우세는 휴식/판결에만 공개
    s.opinionA = Math.round(computeOpinionA(room) * 10) / 10;
    s.tally = tally;               // A/중립/B 세부 표
  }
  return s;
}
function lobbyPayload() {
  return {
    type: 'lobby',
    rooms: [...rooms.values()].map(r => {
      const revealed = isRevealed(r);
      const o = {
        id: r.id, cat: r.cat, topic: r.topic,
        seats: seatInfo(r), aClaim: r.aClaim, bClaim: r.bClaim,
        audience: r.clients.size, heat: r.heat,
        phase: r.phase, part: partOf(r), msLeft: msLeftOf(r), revealed,
        votes: voteTally(r).voters,
      };
      if (revealed) o.opinionA = Math.round(computeOpinionA(r));
      return o;
    }),
    recentVerdicts: verdictLog.slice(0, 8), // 로비 하단 최근 판결
  };
}

/* ---------------- WebSocket ---------------- */
let nextId = 1;
let nextRoomId = 1;                 // 유저가 만든 방 id 카운터
let nextMsgId = 1;                  // 채팅 메시지 id (신고용)
const allClients = new Set();       // 접속한 모든 클라이언트 (로비 갱신 브로드캐스트용)
const NICKS = ['아무개', '이름없는검투사', '관전러', '팝콘요정', '판사지망생', '중립기어', '키배관찰자'];
const chatMeta = new Map();         // msgId → {reporters:Set, roomId, blinded} (최근 500건)
const verdictLog = [];              // 최근 판결 아카이브 (최대 20건)

function sanitizeNick(n) {
  const s = String(n || '').replace(/[<>"'&\s]/g, '').slice(0, 16);
  return s.length >= 2 ? s : null;
}

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(room, obj) { for (const ws of room.clients) send(ws, obj); }
function broadcastAll(obj) { for (const ws of allClients) send(ws, obj); } // 전체에게

wsLite.attach(server, (ws) => {
  ws.id = nextId++;
  ws.nick = NICKS[Math.floor(Math.random() * NICKS.length)] + (100 + Math.floor(Math.random() * 900));
  ws.room = null;
  ws.stance = null;
  ws.stanceBefore = null;
  ws.isRealName = false;
  // 어뷰징 방어용 타임스탬프
  ws.lastChat = 0;
  ws.lastCreate = 0;
  ws.lastSeat = 0;
  allClients.add(ws);

  send(ws, lobbyPayload());

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const room = ws.room ? rooms.get(ws.room) : null;

    switch (m.type) {
      case 'hello': { // 저장된 닉네임으로 접속 (익명 전적 유지)
        const nick = sanitizeNick(m.nick);
        if (nick) ws.nick = nick;
        send(ws, { type: 'hello', you: ws.nick });
        break;
      }

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
          you: ws.nick, // 내 닉네임 (선수 자리 표시용)
          ring: { id: r.id, cat: r.cat, topic: r.topic, aClaim: r.aClaim, bClaim: r.bClaim, audience: r.clients.size },
          state: roomState(r),
        });
        if (r.phase === 'ended' && r.verdict) send(ws, r.verdict); // 판결 진행 중 입장 시 결과 표시
        break;
      }

      case 'seat': { // 선수 참전/기권
        if (!room) break;
        const t = Date.now();
        if (t - ws.lastSeat < 1000) break; // 자리 스팸 방지
        ws.lastSeat = t;
        if (m.side === 'none') { // 기권
          let left = null;
          if (room.seatA === ws) { room.seatA = null; left = 'A'; }
          if (room.seatB === ws) { room.seatB = null; left = 'B'; }
          if (left) { room.dirty = true; broadcast(room, { type: 'seat', side: left, nick: null }); }
          break;
        }
        const side = m.side === 'A' ? 'A' : m.side === 'B' ? 'B' : null;
        if (!side) break;
        const key = side === 'A' ? 'seatA' : 'seatB';
        if (room[key]) { send(ws, { type: 'error', msg: '이미 다른 선수가 있어요' }); break; }
        // 반대편에 앉아 있었으면 그 자리는 비움
        if (room.seatA === ws) { room.seatA = null; broadcast(room, { type: 'seat', side: 'A', nick: null }); }
        if (room.seatB === ws) { room.seatB = null; broadcast(room, { type: 'seat', side: 'B', nick: null }); }
        room[key] = ws;
        room.dirty = true;
        broadcast(room, { type: 'seat', side, nick: ws.nick });
        broadcastAll(lobbyPayload()); // 로비 카드의 선수 현황 갱신
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
        if (room.reactionBuf.length < 200) { // 연출 버퍼 상한 (대량 도배 방어)
          if (corn) room.reactionBuf.push({ side, type: 'corn', n: corn });
          if (rock) room.reactionBuf.push({ side, type: 'rock', n: rock });
        }
        room.dirty = true;
        break;
      }

      case 'chat': { // 링 안 발언 — 선수면 선수 발언, 아니면 관중 응원/관전평
        if (!room) break;
        const t = Date.now();
        if (t - ws.lastChat < 600) break; // 도배 방지 (0.6초당 1건)
        const text = String(m.text || '').slice(0, 200);
        if (!text.trim()) break;
        ws.lastChat = t;
        let side, fighter = null;
        if (room.seatA === ws) { side = 'A'; fighter = 'A'; }
        else if (room.seatB === ws) { side = 'B'; fighter = 'B'; }
        else side = m.side === 'A' || m.side === 'B' ? m.side : 'N';
        const msgId = nextMsgId++;
        chatMeta.set(msgId, { reporters: new Set(), roomId: room.id, blinded: false });
        if (chatMeta.size > 500) chatMeta.delete(chatMeta.keys().next().value); // 오래된 것부터 정리
        broadcast(room, { type: 'chat', msgId, side, fighter, nick: ws.nick, text });
        break;
      }

      case 'report': { // 메시지 신고 — 3인 이상이면 블라인드
        if (!room) break;
        const t = Date.now();
        if (t - (ws.lastReport || 0) < 2000) break; // 신고 스팸 방지
        ws.lastReport = t;
        const meta = chatMeta.get(m.msgId | 0);
        if (!meta || meta.blinded || meta.roomId !== room.id) break;
        meta.reporters.add(ws.id);
        if (meta.reporters.size >= 3) {
          meta.blinded = true;
          broadcast(room, { type: 'blind', msgId: m.msgId | 0 });
        }
        break;
      }

      case 'createRing': { // 유저가 새 방 개설
        const t = Date.now();
        if (t - ws.lastCreate < 30000) { send(ws, { type: 'error', msg: '방 개설은 30초에 한 번만 가능해요' }); break; }
        const topic = String(m.topic || '').slice(0, 120).trim();
        if (!topic) { send(ws, { type: 'error', msg: '논점을 입력하세요' }); break; }
        if (rooms.size >= 60) { send(ws, { type: 'error', msg: '방이 너무 많아요. 잠시 후 다시 시도하세요' }); break; }
        ws.lastCreate = t;
        const cat = ['game', 'love', 'work', 'life'].includes(m.cat) ? m.cat : 'life';
        const aClaim = String(m.aClaim || '').slice(0, 60).trim() || 'A 입장';
        const bClaim = String(m.bClaim || '').slice(0, 60).trim() || 'B 입장';
        const id = 'u' + (nextRoomId++);
        rooms.set(id, makeRoom(id, cat, topic, aClaim, bClaim));
        broadcastAll(lobbyPayload());        // 모두의 로비에 새 방 표시
        send(ws, { type: 'created', ringId: id });
        break;
      }

      case 'leave':
        leaveRoom(ws);
        send(ws, lobbyPayload());
        break;
    }
  });

  ws.on('close', () => { leaveRoom(ws); allClients.delete(ws); });
});

function clampStance(s) { s = s | 0; return s < -2 ? -2 : s > 2 ? 2 : s; }
function leaveRoom(ws) {
  if (!ws.room) return;
  const r = rooms.get(ws.room);
  if (r) {
    r.clients.delete(ws);
    // 선수가 나가면 자리 비움 + 모두에게 알림
    if (r.seatA === ws) { r.seatA = null; broadcast(r, { type: 'seat', side: 'A', nick: null }); }
    if (r.seatB === ws) { r.seatB = null; broadcast(r, { type: 'seat', side: 'B', nick: null }); }
    r.dirty = true;
  }
  ws.room = null;
}

/* ---------------- 판결 ---------------- */
function endMatch(room) {
  const tally = voteTally(room);
  const opinionA = computeOpinionA(room);
  const aWin = opinionA >= 50;
  const totalN = tally.voters;   // 실제로 투표한 사람 수

  // 실명 배심원 (실접속자 중 isRealName) — 전부 실제
  let realSum = 0, realCount = 0;
  for (const ws of room.clients) if (ws.isRealName && ws.stance != null) { realSum += stanceToPoints(ws.stance); realCount += 1; }
  const realA = realCount ? Math.round(realSum / realCount) : null; // 실명 배심원 없으면 null

  // 델타: 입장 전(stanceBefore) → 지금(stance) side가 바뀐 실제 인원
  let bToA = 0, aToB = 0;
  for (const ws of room.clients) {
    if (ws.stance == null) continue;
    const before = sideOf(ws.stanceBefore ?? 0), after = sideOf(ws.stance ?? 0);
    if (before !== after) {
      if (after === 'A') bToA++;
      else if (after === 'B') aToB++;
    }
  }

  room.verdict = {
    type: 'verdict',
    ringId: room.id,
    topic: room.topic,
    winner: aWin ? room.aClaim : room.bClaim,
    winSide: aWin ? 'A' : 'B',
    totalA: Math.round(opinionA), totalB: Math.round(100 - opinionA),
    totalN, tally,
    realA, realB: realA == null ? null : 100 - realA, realN: realCount,
    changed: bToA + aToB, bToA, aToB,
  };
  broadcast(room, room.verdict);
  // 최근 판결 아카이브 (표가 1표라도 있었던 판만)
  if (totalN > 0) {
    verdictLog.unshift({
      ringId: room.id, cat: room.cat, topic: room.topic,
      winner: room.verdict.winner, winSide: room.verdict.winSide,
      totalA: room.verdict.totalA, totalB: room.verdict.totalB,
      totalN, changed: bToA + aToB, ts: Date.now(),
    });
    if (verdictLog.length > 20) verdictLog.pop();
    broadcastAll(lobbyPayload()); // 로비의 최근 판결 갱신
  }
}

/* ---------------- 경기 페이즈 이벤트 ---------------- */
function phaseEvent(room) {
  const tally = voteTally(room);
  const e = {
    type: 'phase',
    ringId: room.id,
    phase: room.phase,
    part: partOf(room),
    msLeft: msLeftOf(room),
    audience: room.clients.size,
    votes: tally.voters,
  };
  if (room.phase === 'break') {   // 중간 우세 공개
    e.opinionA = Math.round(computeOpinionA(room) * 10) / 10;
    e.tally = tally;
  }
  return e;
}
function startPart1(room, t) {
  room.phase = 'part1'; room.phaseEndsAt = t + PART1_MS;
  room.corn = room.rock = room.heat = 0; room.verdict = null; room.reactionBuf = [];
  // 새 경기: 델타 기준을 현재 입장으로 리셋 (이번 판에서 바뀐 마음만 집계)
  for (const ws of room.clients) ws.stanceBefore = ws.stance;
}
/* 한 페이즈가 끝나면 다음 단계로 */
function advancePhase(room, t) {
  if (room.phase === 'part1') {
    room.phase = 'break'; room.phaseEndsAt = t + BREAK_MS;   // 중간 휴식 + 우세 공개
  } else if (room.phase === 'break') {
    room.phase = 'part2'; room.phaseEndsAt = t + PART2_MS;   // 2부 (우세 다시 숨김)
  } else if (room.phase === 'part2') {
    room.phase = 'ended'; room.phaseEndsAt = t + COOLDOWN_MS; // 판결
    endMatch(room);
  } else { // ended → 새 경기 자동 시작
    startPart1(room, t);
  }
  room.dirty = true;
  if (room.phase !== 'ended') broadcast(room, phaseEvent(room)); // 판결은 endMatch가 이미 전송
  broadcastAll(lobbyPayload()); // 로비 카드(페이즈/우세)도 갱신
}

/* ---------------- MATCH TICK: 페이즈 전환 + 빈 유저방 정리 ---------------- */
const EMPTY_ROOM_TTL = 10 * 60 * 1000; // 유저 방이 10분간 비면 자동 삭제
setInterval(() => {
  const t = now();
  let lobbyChanged = false;
  for (const room of rooms.values()) {
    if (t >= room.phaseEndsAt) advancePhase(room, t);
    // 유저가 만든 방(u*)만 정리 대상. 기본 방 4개는 유지.
    if (room.id[0] === 'u') {
      if (room.clients.size === 0) {
        if (room.emptySince == null) room.emptySince = t;
        else if (t - room.emptySince > EMPTY_ROOM_TTL) { rooms.delete(room.id); lobbyChanged = true; }
      } else room.emptySince = null;
    }
  }
  if (lobbyChanged) broadcastAll(lobbyPayload());
}, MATCH_TICK);

/* ---------------- 하트비트: 죽은 소켓 정리 (유령 관중 방지) ---------------- */
setInterval(() => {
  for (const ws of allClients) {
    if (!ws.isAlive) { leaveRoom(ws); allClients.delete(ws); ws.destroy(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

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
