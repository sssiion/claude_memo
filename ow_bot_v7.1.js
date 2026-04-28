const { Client, GatewayIntentBits, PermissionsBitField, AttachmentBuilder } = require('discord.js');
const admin = require('firebase-admin');
const express = require('express');
const { renderPersonalCard, renderAbuseCard, renderServerCard, renderShuffleCard } = require('./cards');
require('dotenv').config();

// [U번 수정] 필수 환경변수 사전 검증 - 모호한 라이브러리 내부 에러 회피
const REQUIRED_ENV = ['TOKEN', 'FIREBASE_SERVICE_ACCOUNT_B64', 'FIREBASE_DB_URL'];
const missingEnv = REQUIRED_ENV.filter(k => !process.env[k]);
if (missingEnv.length) {
    console.error(`[U번] 필수 환경변수 누락: ${missingEnv.join(', ')}`);
    console.error('       .env 파일 또는 배포 환경의 환경변수 설정을 확인하세요.');
    process.exit(1);
}

// --- [ Firebase 초기화 ] ---
// [JJ번 수정] DB 경로 버전을 상수로 분리 - 마이그레이션 시 한 곳만 변경
const DB_VERSION = 'v71';
// [YY번 수정] Firebase init 실패 시 모호한 에러 대신 친절한 진단 메시지 + 종료
let serviceAccount;
try {
    serviceAccount = JSON.parse(
        Buffer.from(process.env.FIREBASE_SERVICE_ACCOUNT_B64, 'base64').toString('utf8')
    );
} catch (e) {
    console.error('[YY번] FIREBASE_SERVICE_ACCOUNT_B64 디코딩/파싱 실패:', e.message);
    console.error('       유효한 base64 인코딩된 service account JSON인지 확인하세요.');
    console.error('       (예: cat key.json | base64 -w0)');
    process.exit(1);
}
try {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: process.env.FIREBASE_DB_URL
    });
} catch (e) {
    console.error('[YY번] Firebase 초기화 실패:', e.message);
    console.error('       FIREBASE_DB_URL 형식(예: https://xxx.firebaseio.com)과');
    console.error('       service account 권한(Realtime DB 접근)을 확인하세요.');
    process.exit(1);
}
const dbRef = admin.database().ref(`ow_bot_${DB_VERSION}`);

// --- [ Keep-alive HTTP 서버 (Render Web Service용) ] ---
const app = express();
app.get('/', (_, res) => res.send('Bot alive'));
app.listen(process.env.PORT || 3000, () => console.log('[Keep-alive] HTTP 서버 시작'));

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildVoiceStates,
        GatewayIntentBits.GuildPresences,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildMembers
    ]
});

// --- [ 설정 ] ---
const CONFIG = {
    GUILDS: {
        '1379424022823043152': {
            LOBBY_ID: '1498565479017742396',
            GAME_ROOMS: ['1379548215954772133', '1379548256790380644', '1379548321835651154', '1448325891762426017', '1482791089328095323']
        }
    },
    POTG_BUFFER: 1000, //15000
    LIMITS: { tank: 1, damage: 2, support: 2 }, // 1 2 2
    RETENTION: 1000 * 60 * 60 * 24 * 30 * 6,
    SAVE_INTERVAL: 5000,
    GC_INTERVAL: 1000 * 60 * 60,
    ROTATION_COOLDOWN: 1000*60*60, //1000 * 60 * 60,   // 채널별 로테이션 최소 간격 (1시간)
    MIN_PLAYERS_TO_ROTATE: 3     // 셔플 트리거 최소 인원 - 3 미만이면 셔플 후 소스에 1명만 남게 됨
};

// [II번 수정] CONFIG.GUILDS 키가 placeholder('서버ID_1' 등)면 봇이 모든 이벤트를 무음 무시
// → startup 시 fail-fast로 명확한 에러 출력
for (const key of Object.keys(CONFIG.GUILDS)) {
    if (!/^\d{17,20}$/.test(key)) {
        console.error(`[II번] CONFIG.GUILDS 키 '${key}'는 유효한 Discord 서버 ID가 아닙니다.`);
        console.error('       17~20자리 숫자 ID로 교체 후 재시작하세요.');
        process.exit(1);
    }
    const g = CONFIG.GUILDS[key];
    if (!/^\d{17,20}$/.test(g.LOBBY_ID)) {
        console.error(`[II번] LOBBY_ID '${g.LOBBY_ID}'가 유효하지 않습니다.`);
        process.exit(1);
    }
    for (const rid of g.GAME_ROOMS) {
        if (!/^\d{17,20}$/.test(rid)) {
            console.error(`[II번] GAME_ROOMS 항목 '${rid}'가 유효하지 않습니다.`);
            process.exit(1);
        }
    }
    // [WW번 수정] LOBBY_ID와 GAME_ROOMS 사이 중복 ID 검증
    // 같은 채널이 양쪽에 있으면 voiceStateUpdate에서 fromGameRoom && toGameRoom으로 클리어 침묵 실패
    const allChannelIds = [g.LOBBY_ID, ...g.GAME_ROOMS];
    if (new Set(allChannelIds).size !== allChannelIds.length) {
        console.error(`[WW번] CONFIG.GUILDS['${key}']: LOBBY_ID와 GAME_ROOMS 사이에 중복 ID 존재`);
        console.error(`       전체: ${allChannelIds.join(', ')}`);
        process.exit(1);
    }
}

// --- [ DB ] ---
let db = {};
let saveTimer = null;
let saving = false;
let pendingWrite = false;     // [TT번] flushDB 진행 중 들어온 추가 변경 표시
let saveDisabled = false;     // [T번] 로드 실패 시 빈 객체 덮어쓰기 방지

const initDB = async () => {
    try {
        const snap = await dbRef.once('value');
        db = snap.val() || {};
        console.log('[DB] Firebase 로드 완료');
    } catch (e) {
        console.error('[DB 로드 실패]:', e.message);
        // [T번 수정] Firebase 로드 실패 시 saveDB 비활성 - 빈 db로 원격 데이터 덮어쓰기 방지
        // (네트워크/권한 일시 장애와 진짜 빈 DB를 구분 못 하므로 안전 측 선택)
        saveDisabled = true;
        db = {};
        console.error('[T번] saveDB 비활성화. 원인 해결 후 봇 재시작으로 복구.');
    }
};

const flushDB = async () => {
    if (saveDisabled) return;
    // [TT번 수정] 동시 호출 시 큐잉 - 앞 write 완료 후 누락 없이 재시도
    if (saving) {
        pendingWrite = true;
        return;
    }
    saving = true;
    try {
        do {
            pendingWrite = false;
            await dbRef.set(db);
        } while (pendingWrite); // write 도중 새 변경이 들어왔으면 한번 더
    } catch (e) {
        console.error('[DB 저장 실패]:', e.message);
        pendingWrite = false; // 실패 시 무한 루프 방지 (다음 saveDB가 새로 트리거)
    } finally {
        saving = false;
    }
};

const saveDB = async (force = false) => {
    if (saveDisabled) return;
    if (force) {
        if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
        await flushDB();
        return;
    }
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
        saveTimer = null;
        flushDB();
    }, CONFIG.SAVE_INTERVAL);
};

const runGC = () => {
    const now = Date.now();
    let removed = 0;
    let removedDaily = 0;
    const dailyCutoff = todayKey(-DAILY_RETENTION_DAYS);
    for (const gId in db) {
        for (const uId in db[gId]) {
            if (db[gId][uId].social) {
                for (const tId in db[gId][uId].social) {
                    if (now - db[gId][uId].social[tId].lastPlayed > CONFIG.RETENTION) {
                        delete db[gId][uId].social[tId];
                        removed++;
                    }
                }
            }
            if (db[gId][uId].daily) {
                for (const dKey in db[gId][uId].daily) {
                    if (dKey < dailyCutoff) {
                        delete db[gId][uId].daily[dKey];
                        removedDaily++;
                    }
                }
            }
        }
    }
    console.log(`[GC] 6개월 초과 소셜 ${removed}건 / ${DAILY_RETENTION_DAYS}일 초과 일별 ${removedDaily}건 정리`);
    // [HH번 수정] 정리 결과를 디스크에 반영해야 재시작 후에도 유지됨
    if (removed > 0 || removedDaily > 0) saveDB();
};

// V8.4: 일별 통계 - 차트용 시계열 데이터 적재
// db[gId][uId].daily[YYYY-MM-DD] = { inGameMs, shuffleCount, partnersSet }
const DAILY_RETENTION_DAYS = 30;
const todayKey = (offsetDays = 0) => {
    const d = new Date();
    d.setDate(d.getDate() + offsetDays);
    return d.toISOString().slice(0, 10); // YYYY-MM-DD
};
const ensureDailyEntry = (entry, dateKey = todayKey()) => {
    if (!entry.daily) entry.daily = {};
    if (!entry.daily[dateKey]) entry.daily[dateKey] = { inGameMs: 0, shuffleCount: 0, partnersSet: {} };
    const d = entry.daily[dateKey];
    if (typeof d.inGameMs !== 'number') d.inGameMs = 0;
    if (typeof d.shuffleCount !== 'number') d.shuffleCount = 0;
    if (!d.partnersSet || typeof d.partnersSet !== 'object') d.partnersSet = {};
    return d;
};

const getDB = (gId, uId) => {
    if (!db[gId]) db[gId] = {};
    if (!db[gId][uId]) {
        db[gId][uId] = {
            roles: [], currentRole: null, social: {},
            undoCount: 0, lastUndoAt: 0,
            undoUseCount: 0, lastUndoUseAt: 0,
            summonCount: 0, lastSummonAt: 0,
            activeRoles: null, followup: null,
            seenGameCycle: false,
            daily: {},
            roleStats: { tank: 0, damage: 0, support: 0 },
            hourly: {},
            firstPlayedAt: 0
        };
    } else {
        // [EEE번 수정] Firebase는 빈 객체/배열을 저장 안 함 → 로드 시 누락 필드 보강
        const e = db[gId][uId];
        if (!Array.isArray(e.roles)) e.roles = [];
        if (e.currentRole === undefined) e.currentRole = null;
        if (!e.social || typeof e.social !== 'object') e.social = {};
        // [JJJ번 수정] V8.0 남용 추적 필드 보강
        if (typeof e.undoCount !== 'number') e.undoCount = 0;
        if (typeof e.lastUndoAt !== 'number') e.lastUndoAt = 0;
        if (typeof e.summonCount !== 'number') e.summonCount = 0;
        if (typeof e.lastSummonAt !== 'number') e.lastSummonAt = 0;
        // V7.2: 일회성 역할셋 + !랜덤 실패 후속 상태
        if (e.activeRoles === undefined) e.activeRoles = null;
        if (e.followup === undefined) e.followup = null;
        // 게임 사이클 관찰 게이트: 채널 입장 후 게임중→온라인 전환 1회 관찰됐는지
        if (typeof e.seenGameCycle !== 'boolean') e.seenGameCycle = false;
        // V8.4 일별 통계 (차트용)
        if (typeof e.undoUseCount !== 'number') e.undoUseCount = 0;
        if (typeof e.lastUndoUseAt !== 'number') e.lastUndoUseAt = 0;
        if (!e.daily || typeof e.daily !== 'object') e.daily = {};
        // V8.5: 역할 분포 / 시간대 / 첫 활동
        if (!e.roleStats || typeof e.roleStats !== 'object') e.roleStats = {};
        if (typeof e.roleStats.tank !== 'number') e.roleStats.tank = 0;
        if (typeof e.roleStats.damage !== 'number') e.roleStats.damage = 0;
        if (typeof e.roleStats.support !== 'number') e.roleStats.support = 0;
        if (!e.hourly || typeof e.hourly !== 'object') e.hourly = {};
        if (typeof e.firstPlayedAt !== 'number') e.firstPlayedAt = 0;
    }
    return db[gId][uId];
};

// [MMM번 수정] V8.3 - 채널별 셔플 멤버 리스트 (구경꾼 제외)
// db[gId]._shuffleMembers[channelId] = { userId: true, ... }
// 규칙: !랜덤/자동배정으로 들어오면 추가, 셔플 이동 시 갱신, 자발 이탈 시 제거
const _ensureShuffleMembers = (gId, channelId) => {
    if (!db[gId]) db[gId] = {};
    if (!db[gId]._shuffleMembers) db[gId]._shuffleMembers = {};
    if (!db[gId]._shuffleMembers[channelId]) db[gId]._shuffleMembers[channelId] = {};
    return db[gId]._shuffleMembers[channelId];
};
const addShuffleMember = (gId, channelId, userId) => {
    _ensureShuffleMembers(gId, channelId)[userId] = true;
};
const removeShuffleMember = (gId, channelId, userId) => {
    if (db[gId]?._shuffleMembers?.[channelId]?.[userId]) {
        delete db[gId]._shuffleMembers[channelId][userId];
    }
};
const isShuffleMember = (gId, channelId, userId) =>
    !!db[gId]?._shuffleMembers?.[channelId]?.[userId];

// [KKK번 수정] V8.1 - 게임 중 시간 추적 헬퍼
// 기존 social[partnerId]는 { count, lastPlayed }였고 V8.1부터 { count, lastPlayed, durationMs } 추가
// 누락 시 0으로 보강하여 backwards compat
const ensureSocialEntry = (entry, partnerId) => {
    if (!entry.social[partnerId]) entry.social[partnerId] = { count: 0, lastPlayed: 0, durationMs: 0 };
    const s = entry.social[partnerId];
    if (typeof s.count !== 'number') s.count = 0;
    if (typeof s.lastPlayed !== 'number') s.lastPlayed = 0;
    if (typeof s.durationMs !== 'number') s.durationMs = 0;
    return s;
};

// [JJJ번 수정] 직전 셔플 스냅샷 헬퍼 - 채널별로 movements 저장
const getLastShuffle = (gId, channelId) => db[gId]?._lastShuffle?.[channelId] || null;
const setLastShuffle = (gId, channelId, movements) => {
    if (!db[gId]) db[gId] = {};
    if (!db[gId]._lastShuffle) db[gId]._lastShuffle = {};
    db[gId]._lastShuffle[channelId] = { movements, at: Date.now() };
};
const clearLastShuffle = (gId, channelId) => {
    if (db[gId]?._lastShuffle?.[channelId]) {
        delete db[gId]._lastShuffle[channelId];
    }
};

// [O번 수정] 미등록 서버 경고 로그 추가
const getGuildConfig = (gId) => {
    const config = CONFIG.GUILDS[gId] || null;
    if (!config) console.warn(`[CONFIG 누락] 미등록 서버 이벤트 무시됨: gId=${gId}`);
    return config;
};

const getRoomComp = (gId, room) => {
    const comp = { tank: 0, damage: 0, support: 0 };
    room.members.forEach(m => {
        // [MMM번 수정] 구경꾼은 정원에서 제외 - 셔플 멤버만 카운트
        if (!isShuffleMember(gId, room.id, m.id)) return;
        const r = db[gId]?.[m.id]?.currentRole;
        if (r) comp[r]++;
    });
    return comp;
};

// V7.2: 이분매칭 - 멤버들을 1탱/2딜/2힐로 배정 가능한지 검사 + 배정 결과 반환
// members: [{ id, roles }] (roles는 가능한 역할 집합)
// 백트래킹 (최대 3^5=243), 가장 비유연한 멤버부터 배치
function tryMatching(members) {
    if (members.length > 5) return { ok: false };
    const limits = CONFIG.LIMITS;
    const used = { tank: 0, damage: 0, support: 0 };
    const assignments = {};
    const sorted = [...members].sort((a, b) => a.roles.length - b.roles.length);
    function backtrack(idx) {
        if (idx >= sorted.length) return true;
        const m = sorted[idx];
        const candidates = [...m.roles].sort(() => Math.random() - 0.5);
        for (const role of candidates) {
            if (used[role] < limits[role]) {
                used[role]++;
                assignments[m.id] = role;
                if (backtrack(idx + 1)) return true;
                used[role]--;
                delete assignments[m.id];
            }
        }
        return false;
    }
    if (backtrack(0)) return { ok: true, assignments };
    return { ok: false };
}

// V7.2: 신규 멤버가 어떤 역할들로 매칭 가능한지 (overlap 계산 + 역할 후보)
function getPlayerPossibleRoles(existingMembers, player) {
    const possible = [];
    for (const r of player.roles) {
        const test = [...existingMembers, { id: player.id, roles: [r] }];
        if (tryMatching(test).ok) possible.push(r);
    }
    return possible;
}

// V7.2: 방의 셔플 멤버를 매칭용 객체로 변환 (excludeId 제외)
// roles는 activeRoles(일회성) 우선, 없으면 영구 roles 사용
function getRoomMembers(gId, room, excludeId = null) {
    const members = [];
    room.members.forEach(m => {
        if (m.id === excludeId) return;
        if (m.user.bot) return;
        if (!isShuffleMember(gId, room.id, m.id)) return;
        const data = db[gId]?.[m.id];
        const roles = (data?.activeRoles?.length ? data.activeRoles : data?.roles) || [];
        if (!roles.length) return;
        members.push({ id: m.id, roles });
    });
    return members;
}

const assignLocks = new Set();

async function smartAssign(member, roles = null) {
    const gId = member.guild.id;
    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return "⚠️ 미등록 서버입니다.";

    if (assignLocks.has(member.id)) return "⏳ 배정 중입니다.";
    assignLocks.add(member.id);

    try {
        const data = getDB(gId, member.id);
        const target = roles?.length ? roles : data.roles;
        if (!target?.length) return "⚠️ 역할을 먼저 설정해주세요.";

        // [X번 수정] presence 캐시 미수신과 실제 오프라인 구분
        // 음성채널 입장은 온라인 보장 → presence === null이어도 진행
        const presence = member.presence;
        if (presence?.status === 'offline') return "⚠️ 오프라인 상태입니다.";

        const SOCIAL_CAP = 10;
        const ROLE_NAMES = { tank: '탱커', damage: '딜러', support: '힐러' };

        // V7.2: 각 방마다 (기존 멤버 + 신규)로 매칭 가능한지 + 점수 계산
        // 점수 = 100 + 방채움×30 + overlap×5 - social×20 + presence다양성×20
        // 방채움이 가장 큰 가중치 (혼자 배정 방지), overlap은 동률 깨는 용도
        // presence 다양성: 신규의 오프라인 여부와 반대 타입이 방에 있으면 가산 (오프라인-only 방 회피)
        const newcomerOffline = _isOffline(member);
        const evaluations = guildConfig.GAME_ROOMS
            .map(id => member.guild.channels.cache.get(id))
            .filter(r => r)
            .map(room => {
                const existing = getRoomMembers(gId, room, member.id);
                if (existing.length >= 5) return null;
                const player = { id: member.id, roles: target };
                const possibleRoles = getPlayerPossibleRoles(existing, player);
                if (!possibleRoles.length) return null;
                let socialCost = 0;
                for (const m of existing) {
                    const count = Math.min(data.social?.[m.id]?.count || 0, SOCIAL_CAP);
                    socialCost += count * 20;
                }
                let presenceBonus = 0;
                if (existing.length > 0) {
                    const oppositeCount = existing.filter(e => {
                        const dm = room.members.get(e.id);
                        return dm && _isOffline(dm) !== newcomerOffline;
                    }).length;
                    if (oppositeCount > 0) presenceBonus = 20;
                }
                const score = 100 + existing.length * 30 + possibleRoles.length * 5 - socialCost + presenceBonus;
                return { room, score: Math.max(score, 1), possibleRoles, existing };
            })
            .filter(e => e !== null)
            .sort((a, b) =>
                b.existing.length - a.existing.length
                || b.score - a.score
            );

        if (!evaluations.length) {
            // V7.2: 갈 수 있는 방 없음 - 가능한 추가 역할 안내 + !혼자 안내
            const allRooms = guildConfig.GAME_ROOMS
                .map(id => member.guild.channels.cache.get(id))
                .filter(r => r);
            const extraRoles = new Set();
            for (const room of allRooms) {
                const existing = getRoomMembers(gId, room, member.id);
                if (existing.length >= 5) continue;
                for (const r of ['tank', 'damage', 'support']) {
                    if (target.includes(r)) continue;
                    const test = [...existing, { id: member.id, roles: [r] }];
                    if (tryMatching(test).ok) extraRoles.add(r);
                }
            }
            const hint = extraRoles.size > 0
                ? `\n💡 \`!변경 ${[...extraRoles].map(r => ROLE_NAMES[r]).join('/')}\` 으로 추가 역할 시도 가능`
                : '';
            return `❌ 배정 가능한 자리가 없습니다.${hint}\n💡 \`!혼자\` 로 빈 방 대기 가능`;
        }

        const best = evaluations[0];
        // V7.2: 가능한 역할 중 무작위 (선호 순서 X, 방 필요에 따라 랜덤)
        const chosenRole = best.possibleRoles[Math.floor(Math.random() * best.possibleRoles.length)];

        // 신규를 chosenRole로 고정해서 전체 매칭 (기존 멤버 currentRole 갱신용)
        const finalMatching = tryMatching([...best.existing, { id: member.id, roles: [chosenRole] }]);
        if (!finalMatching.ok) return "⚠️ 매칭 실패 (내부 오류)";

        if (!best.room.permissionsFor(client.user).has(PermissionsBitField.Flags.MoveMembers)) {
            return "⚠️ 봇 권한이 부족합니다.";
        }

        // [K번 수정] setChannel 직전 음성채널 존재 재확인
        if (!member.voice?.channel) {
            console.warn(`[K번] setChannel 직전 음성채널 없음: ${member.id}`);
            return "⚠️ 음성 채널에 먼저 입장해주세요.";
        }

        // [N번 수정] race condition 방지 - 체크 시점과 이동 시점의 채널 동일 여부 확인
        const channelAtCheck = member.voice.channel.id;

        // V7.2: 매칭 결과로 기존 멤버 + 신규 currentRole 일괄 갱신
        const prevRoles = {};
        for (const [id, role] of Object.entries(finalMatching.assignments)) {
            if (db[gId]?.[id]) {
                prevRoles[id] = db[gId][id].currentRole;
                db[gId][id].currentRole = role;
            }
        }
        // roles 인자가 일회성으로 들어왔으면 activeRoles에 저장 (이후 매칭에서 이 set 사용)
        if (roles?.length) data.activeRoles = roles;
        saveDB();

        // 이동 직전 채널이 바뀌었는지 재확인
        if (member.voice?.channel?.id !== channelAtCheck) {
            console.warn(`[N번] race condition 감지: ${member.id} 채널 이동됨 - 배정 취소`);
            // V7.2: 매칭으로 바꾼 기존 멤버 currentRole도 원복
            for (const [id, prevRole] of Object.entries(prevRoles)) {
                if (db[gId]?.[id]) db[gId][id].currentRole = prevRole;
            }
            data.currentRole = null;
            saveDB();
            return "⚠️ 채널이 변경되어 배정을 취소했습니다.";
        }

        // [Q번 수정] setChannel 실패 시 DB 롤백 + 명시적 catch (unhandled rejection 방지)
        try {
            await member.voice.setChannel(best.room.id);
        } catch (e) {
            // [BB번 수정] HTTP 응답만 분실되고 실제 이동은 성공했을 수 있음 - 1초 후 재확인
            await new Promise(r => setTimeout(r, 1000));
            if (member.voice?.channel?.id === best.room.id) {
                console.warn(`[BB번] HTTP 실패하나 실제 이동 성공 확인: ${member.id}`);
                addShuffleMember(gId, best.room.id, member.id); // [MMM번] 셔플 멤버 등록
                data.roleStats[chosenRole] = (data.roleStats[chosenRole] || 0) + 1;
                saveDB();
                return '✅';
            }
            console.error(`[Q번] setChannel 실패 - DB 롤백: ${member.id}:`, e.message);
            // V7.2: 매칭으로 바꾼 기존 멤버 currentRole도 원복
            for (const [id, prevRole] of Object.entries(prevRoles)) {
                if (db[gId]?.[id]) db[gId][id].currentRole = prevRole;
            }
            data.currentRole = null;
            saveDB();
            return "⚠️ 이동 실패 (권한/채널 상태 문제로 추정)";
        }
        // [MMM번 수정] 봇이 배정한 사람을 셔플 멤버 리스트에 추가
        addShuffleMember(gId, best.room.id, member.id);
        // V8.5: 역할 분포 카운트 (입장 시 1회)
        data.roleStats[chosenRole] = (data.roleStats[chosenRole] || 0) + 1;
        saveDB();
        return '✅';

    } finally {
        assignLocks.delete(member.id);
    }
}

// --- [ 이벤트 감시 ] ---
client.on('voiceStateUpdate', (oldS, newS) => {
    const gId = oldS.guild?.id;
    if (!gId) return;
    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return;

    const memberId = oldS.member?.id || newS.member?.id;
    if (!memberId) return;

    // [CC번 수정] 봇 자신의 voice 변경 무시 - getDB로 빈 entry 생성되는 DB 오염 방지
    if (newS.member?.user?.bot || oldS.member?.user?.bot) return;

    // [P번 수정] 게임방 → 비게임방(대기실/이탈)일 때만 currentRole 초기화
    // 대기실 → 게임방(배정), 게임방 → 게임방(로테이션)은 보존해야 매칭/로테이션이 동작함
    if (oldS.channelId !== newS.channelId && db[gId]?.[memberId]) {
        const fromGameRoom = guildConfig.GAME_ROOMS.includes(oldS.channelId);
        const toGameRoom = guildConfig.GAME_ROOMS.includes(newS.channelId);
        if (fromGameRoom && !toGameRoom) {
            // V7.2: 게임방 떠나면 일회성 역할셋도 초기화 (다음 세션은 영구 roles 사용)
            if (db[gId][memberId].currentRole) db[gId][memberId].currentRole = null;
            if (db[gId][memberId].activeRoles) db[gId][memberId].activeRoles = null;
            db[gId][memberId].seenGameCycle = false;
            saveDB();
        }
        // 게임방 입장/이동 시에도 사이클 관찰 플래그 리셋 (새 세션은 다시 관찰 필요)
        if (toGameRoom) {
            db[gId][memberId].seenGameCycle = false;
            saveDB();
        }
    }

    // [MMM번 수정] 게임방을 떠나면 그 방의 셔플 멤버 리스트에서 제거
    // 봇이 옮긴 경우는 이동 코드에서 새 방에 즉시 add하므로 자연 일관성 유지
    if (oldS.channelId !== newS.channelId
        && guildConfig.GAME_ROOMS.includes(oldS.channelId)
        && isShuffleMember(gId, oldS.channelId, memberId)) {
        removeShuffleMember(gId, oldS.channelId, memberId);
        saveDB();
    }

    // [W번 수정] 다른 보이스/게임방에서 대기실로 들어와도 자동 배정 트리거
    // (mute/deafen은 oldS===newS이므로 자연 제외, 본인이 이미 대기실이면 제외)
    if (newS.channelId === guildConfig.LOBBY_ID && oldS.channelId !== guildConfig.LOBBY_ID) {
        const member = newS.member;
        if (member) {
            // [R번 수정] 자동 트리거 결과를 로그로 표시 (메시지 채널이 없어 사일런트 실패 방지)
            smartAssign(member)
                .then(result => {
                    if (result && !result.startsWith('✅')) {
                        console.log(`[R번] 자동 배정 실패: ${member.user?.tag || member.id} - ${result}`);
                        // V7.2: 자동 배정 실패도 후속(!변경/!혼자) 사용 가능 상태로
                        const data = db[gId]?.[member.id];
                        if (data?.roles?.length) {
                            data.followup = { roles: data.roles, at: Date.now() };
                            saveDB();
                        }
                    }
                })
                .catch(e => console.error(`[R번] smartAssign 예외:`, e.message));
        }
    }

    // [HHH번 수정] 게임방 인원 변동(입장/이탈) 시 보류 중 채널 재시도
    // - 인원 부족으로 보류된 채널이 충원됐을 가능성
    // - 사용자가 게임방 사이를 이동하며 다른 채널의 인원 구성을 바꿨을 가능성
    const fromGameRoom = guildConfig.GAME_ROOMS.includes(oldS.channelId);
    const toGameRoom = guildConfig.GAME_ROOMS.includes(newS.channelId);
    if ((fromGameRoom || toGameRoom) && pendingRotations.size > 0) {
        retryAllPending(gId).catch(e => console.error('[HHH번] voiceStateUpdate 트리거 실패:', e?.message || e));
    }
});

const activeLocks = new Set();

// [XX번 수정] 쿨다운을 Firebase에 영속화 - 봇 재시작 시 리셋되지 않도록
// db[gId]._cooldowns[channelId] = timestamp 구조 (getDB 우회하여 user 구조 침해 방지)
const getCooldown = (gId, channelId) => db[gId]?._cooldowns?.[channelId] || 0;
const setCooldown = (gId, channelId) => {
    if (!db[gId]) db[gId] = {};
    if (!db[gId]._cooldowns) db[gId]._cooldowns = {};
    db[gId]._cooldowns[channelId] = Date.now();
};

// [HHH번 수정] V7.8 시간 기반 셔플 트리거 재설계
// 기존: presenceUpdate(게임 종료) → 쿨다운 통과 시 셔플 → 신호 못 받으면 영영 안 됨
// 변경: 마지막 셔플 + ROTATION_COOLDOWN 시점에 setTimeout 발화 → 조건 검사
//        조건 미달(인원 부족/게임 중)이면 pendingRotations에 등록 → 이벤트 재트리거 시 재시도
const pendingRotations = new Set();    // channelId
const scheduledTimers = new Map();     // channelId → setTimeout handle

const _isOW = a => a?.name?.toLowerCase().includes('overwatch');
const _activitiesInGame = activities => {
    const ow = activities?.find(_isOW);
    return !!(ow && (ow.details || ow.state));
};
const _isInGame = m => _activitiesInGame(m.presence?.activities);
// presence 미수신(null) 또는 status 'offline' = 게임 상태 검증 불가
const _isOffline = m => {
    const status = m.presence?.status;
    return !status || status === 'offline';
};
const _activePlayers = channel => {
    const gId = channel.guild.id;
    // [MMM번 수정] currentRole 있고 + 셔플 멤버 리스트에 등록된 사람만 (구경꾼 제외)
    return channel.members.filter(m =>
        db[gId]?.[m.id]?.currentRole &&
        isShuffleMember(gId, channel.id, m.id)
    );
};

async function tryRotate(channel) {
    const gId = channel.guild.id;
    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return;
    if (activeLocks.has(channel.id)) return;

    const players = _activePlayers(channel);
    if (players.size < CONFIG.MIN_PLAYERS_TO_ROTATE) {
        pendingRotations.add(channel.id);
        console.log(`[HHH번] ${channel.name}: 인원 부족(${players.size}/${CONFIG.MIN_PLAYERS_TO_ROTATE}) - 보류`);
        return;
    }
    if (players.some(_isInGame)) {
        pendingRotations.add(channel.id);
        console.log(`[HHH번] ${channel.name}: 게임 중 멤버 있음 - 게임 종료까지 보류`);
        return;
    }
    // 규칙1: 모든 멤버가 오프라인/투명 → 게임 중일 가능성 높음, 셔플 보류
    if (players.size > 0 && players.every(_isOffline)) {
        pendingRotations.add(channel.id);
        console.log(`[HHH번] ${channel.name}: 모든 멤버 오프라인 - 게임 검증 불가, 보류`);
        return;
    }
    // 규칙2: 오프라인 멤버 동행 시, 온라인 멤버 전원이 게임 사이클(in-game→online)을 1회 관찰돼야 셔플
    if (players.some(_isOffline)) {
        const onlineUnseen = players.filter(m => !_isOffline(m) && !db[gId]?.[m.id]?.seenGameCycle);
        if (onlineUnseen.size > 0) {
            pendingRotations.add(channel.id);
            console.log(`[HHH번] ${channel.name}: 오프라인 동행 + 게임 사이클 미관찰 온라인 ${onlineUnseen.size}명 - 보류`);
            return;
        }
    }

    pendingRotations.delete(channel.id);
    activeLocks.add(channel.id);
    try {
        // POTG 버퍼 - KillCam/POTG 감상 시간
        await new Promise(r => setTimeout(r, CONFIG.POTG_BUFFER));
        // 버퍼 동안 상황이 바뀔 수 있으므로 재검증
        const stillPlayers = _activePlayers(channel);
        if (stillPlayers.size < CONFIG.MIN_PLAYERS_TO_ROTATE) {
            pendingRotations.add(channel.id);
            console.log(`[HHH번] ${channel.name}: POTG 버퍼 중 인원 이탈 - 보류`);
            return;
        }
        if (stillPlayers.some(_isInGame)) {
            pendingRotations.add(channel.id);
            console.log(`[HHH번] ${channel.name}: POTG 버퍼 중 새 게임 시작 - 보류`);
            return;
        }
        if (stillPlayers.size > 0 && stillPlayers.every(_isOffline)) {
            pendingRotations.add(channel.id);
            console.log(`[HHH번] ${channel.name}: POTG 버퍼 중 전원 오프라인 - 보류`);
            return;
        }
        if (stillPlayers.some(_isOffline)) {
            const onlineUnseen = stillPlayers.filter(m => !_isOffline(m) && !db[gId]?.[m.id]?.seenGameCycle);
            if (onlineUnseen.size > 0) {
                pendingRotations.add(channel.id);
                console.log(`[HHH번] ${channel.name}: POTG 버퍼 중 게임 사이클 미관찰 온라인 ${onlineUnseen.size}명 - 보류`);
                return;
            }
        }
        const moved = await executeRotation(channel);
        if (moved) {
            scheduleNextRotation(channel);
        } else {
            // 이동 실패 - 30초 후 재시도
            scheduleRetry(channel, 30000);
        }
    } catch (e) {
        console.error(`[HHH번] tryRotate 오류 (${channel.name}):`, e.message);
        scheduleRetry(channel, 30000);
    } finally {
        activeLocks.delete(channel.id);
    }
}

function scheduleNextRotation(channel) {
    const gId = channel.guild.id;
    const lastRot = getCooldown(gId, channel.id);
    const elapsed = Date.now() - lastRot;
    const delay = Math.max(CONFIG.ROTATION_COOLDOWN - elapsed, 0);

    if (scheduledTimers.has(channel.id)) {
        clearTimeout(scheduledTimers.get(channel.id));
    }
    scheduledTimers.set(channel.id, setTimeout(() => {
        scheduledTimers.delete(channel.id);
        tryRotate(channel).catch(e => console.error('[HHH번] tryRotate 예외:', e?.message || e));
    }, delay));
    if (delay > 0) {
        console.log(`[HHH번] ${channel.name}: 다음 셔플 예약 ${Math.round(delay/1000)}초 후`);
    }
}

function scheduleRetry(channel, delay) {
    if (scheduledTimers.has(channel.id)) {
        clearTimeout(scheduledTimers.get(channel.id));
    }
    scheduledTimers.set(channel.id, setTimeout(() => {
        scheduledTimers.delete(channel.id);
        tryRotate(channel).catch(e => console.error('[HHH번] tryRotate 예외:', e?.message || e));
    }, delay));
    console.log(`[HHH번] ${channel.name}: 재시도 예약 ${Math.round(delay/1000)}초 후`);
}

// pendingRotations에 등록된 모든 채널 재시도 (presence/voice 변화 시 호출)
async function retryAllPending(gId) {
    if (pendingRotations.size === 0) return;
    const guild = client.guilds.cache.get(gId);
    if (!guild) return;
    for (const channelId of [...pendingRotations]) {
        const channel = guild.channels.cache.get(channelId);
        if (channel) {
            tryRotate(channel).catch(e => console.error('[HHH번] retryAllPending:', e?.message || e));
        } else {
            // 채널 삭제됨 - pending에서 제거
            pendingRotations.delete(channelId);
        }
    }
}

// [HHH번 수정] V7.8 단순화: 게임 종료 감지 → pendingRotations에 있는 채널만 재시도
// 기존의 복잡한 게임 종료 트리거 + setTimeout(POTG_BUFFER) 로직은 tryRotate 안으로 통합됨
client.on('presenceUpdate', (oldP, newP) => {
    const member = newP?.member;
    const gId = member?.guild?.id;
    if (!gId) return;
    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return;

    const channel = member?.voice?.channel;
    if (!channel || !guildConfig.GAME_ROOMS.includes(channel.id)) return;

    // 게임 사이클 관찰: 게임중 → 온라인(non-offline) 전환 1회 시 플래그 set
    // 오프라인 멤버 동행 시 셔플 게이트로 사용됨
    const wasInGame = _activitiesInGame(oldP?.activities);
    const isInGame = _activitiesInGame(newP?.activities);
    const newStatus = newP?.status;
    if (wasInGame && !isInGame && newStatus && newStatus !== 'offline' && db[gId]?.[member.id]) {
        if (!db[gId][member.id].seenGameCycle) {
            db[gId][member.id].seenGameCycle = true;
            saveDB();
        }
    }

    if (!pendingRotations.has(channel.id)) return;

    // 보류 중 채널의 멤버 presence 변화 → 게임 종료 가능성 있음 → 재시도
    // 조건은 tryRotate 안에서 다시 검증되므로 여기서 추가 가드 불필요
    tryRotate(channel).catch(e => console.error('[HHH번] presenceUpdate 트리거 실패:', e?.message || e));
});

// [JJJ번 수정] V8.0 - eligibility 검사: 만료 + 사람 있음 + 게임 중 아님
const _isEligibleTarget = (gId, room) => {
    if (Date.now() - getCooldown(gId, room.id) < CONFIG.ROTATION_COOLDOWN) return false;
    const humans = room.members.filter(m => !m.user.bot);
    if (humans.size === 0) return false;
    if (humans.some(_isInGame)) return false;
    return true;
};

// [JJJ번 수정] 셔플 마무리 공통 처리: 영향받은 모든 채널의 쿨다운 갱신 + social + lastShuffle 저장
function finishRotation(gId, sourceChannel, players, movements) {
    const affectedChannels = new Set([sourceChannel.id]);
    movements.forEach(m => {
        affectedChannels.add(m.toCh);
        affectedChannels.add(m.fromCh);
    });
    for (const cid of affectedChannels) setCooldown(gId, cid);

    const now = Date.now();
    const dateKey = todayKey();
    players.forEach(m1 => {
        const d1 = getDB(gId, m1.id);
        // V8.4: 일별 셔플 카운트
        ensureDailyEntry(d1, dateKey).shuffleCount += 1;
        players.forEach(m2 => {
            if (m1.id !== m2.id) {
                // [KKK번 수정] ensureSocialEntry로 durationMs 보존 (덮어쓰기 X)
                const s = ensureSocialEntry(d1, m2.id);
                s.count += 1;
                s.lastPlayed = now;
            }
        });
    });

    // 영향받은 모든 채널에 같은 movements 저장 (되돌리기 시 어느 채널에서든 호출 가능)
    for (const cid of affectedChannels) setLastShuffle(gId, cid, movements);

    // [MMM번 수정] 셔플 멤버 리스트 일관성 갱신 (voiceStateUpdate 타이밍 무관)
    for (const m of movements) {
        if (m.fromCh) removeShuffleMember(gId, m.fromCh, m.userId);
        if (m.toCh) addShuffleMember(gId, m.toCh, m.userId);
    }

    saveDB(true);
}

async function executeRotation(channel) {
    const gId = channel.guild.id;
    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return false;

    // [MMM번 수정] 구경꾼 제외 - 셔플 멤버 리스트에 있는 사람만
    const players = Array.from(channel.members.values())
        .filter(m =>
            db[gId]?.[m.id]?.currentRole &&
            isShuffleMember(gId, channel.id, m.id)
        );

    const allOtherRooms = guildConfig.GAME_ROOMS
        .filter(id => id !== channel.id)
        .map(id => channel.guild.channels.cache.get(id))
        .filter(r => r);
    if (!allOtherRooms.length) {
        console.warn('[로테이션 무산] 이동 가능한 다른 게임방 없음');
        return false;
    }

    // [JJJ번 수정] target 방 분류
    // - eligibleOtherRooms: 만료 + 사람 있음 + 게임 중 아님 (swap/단일이동 대상)
    // - emptyOtherRooms: 사람 없음 (다중이동 대상, 만료/게임 무관)
    const eligibleOtherRooms = allOtherRooms.filter(r => _isEligibleTarget(gId, r));
    const emptyOtherRooms = allOtherRooms.filter(r => r.members.filter(m => !m.user.bot).size === 0);

    let moved = false;
    const movements = [];

    // [III번 수정] swap 우선 시도 - eligible 방들 중에서만
    // 두 방 모두 만료 + 게임 종료 상태일 때만 양방향 맞교환
    if (players.length >= 1 && eligibleOtherRooms.length > 0) {
        const shuffledForSwap = [...players].sort(() => Math.random() - 0.5);
        swapOuter: for (const a of shuffledForSwap) {
            const aRole = db[gId]?.[a.id]?.currentRole;
            if (!aRole) continue;
            if (!channel.guild.members.cache.has(a.id)) continue;

            const otherRoomsShuffled = [...eligibleOtherRooms].sort(() => Math.random() - 0.5);
            for (const room of otherRoomsShuffled) {
                const sameRoleMembers = room.members.filter(b =>
                    !b.user.bot &&
                    db[gId]?.[b.id]?.currentRole === aRole &&
                    channel.guild.members.cache.has(b.id)
                );
                if (sameRoleMembers.size === 0) continue;
                const swapTargets = [...sameRoleMembers.values()].sort(() => Math.random() - 0.5);

                for (const b of swapTargets) {
                    try {
                        await a.voice.setChannel(room.id);
                    } catch (e) {
                        console.error(`[III번] swap 1단계 실패: ${a.id} → ${room.id}:`, e.message);
                        continue;
                    }
                    try {
                        await b.voice.setChannel(channel.id);
                        moved = true;
                        movements.push(
                            { userId: a.id, fromCh: channel.id, toCh: room.id, role: aRole },
                            { userId: b.id, fromCh: room.id, toCh: channel.id, role: aRole }
                        );
                        console.log(`[III번] swap 성공: ${a.user?.tag || a.id} ↔ ${b.user?.tag || b.id} (${aRole}) — ${channel.name} ↔ ${room.name}`);
                        break swapOuter;
                    } catch (e) {
                        console.error(`[III번] swap 2단계 실패: ${b.id} → ${channel.id} - 롤백 시도:`, e.message);
                        try {
                            await a.voice.setChannel(channel.id);
                            console.warn(`[III번] swap 롤백 성공: ${a.id} 원위치`);
                        } catch (re) {
                            console.error(`[III번] swap 롤백 실패 - ${a.id}이 ${room.id}에 잔존:`, re.message);
                        }
                    }
                }
            }
        }
    }

    if (moved) {
        finishRotation(gId, channel, players, movements);
        return true;
    }

    // swap 실패 → 단일/다중 이동 시도. 이때부터는 MIN 검증 적용
    if (players.length < CONFIG.MIN_PLAYERS_TO_ROTATE) {
        console.warn(`[로테이션 무산] swap 실패 + 인원 부족(${players.length}/${CONFIG.MIN_PLAYERS_TO_ROTATE})`);
        return false;
    }

    const shuffled = [...players].sort(() => Math.random() - 0.5);

    if (eligibleOtherRooms.length > 0) {
        // [LL번 수정] 단일 이동: 1명을 eligible(만료+게임끝남) 방으로
        outer: for (const candidate of shuffled) {
            if (!channel.guild.members.cache.has(candidate.id)) {
                console.log(`[20번] 멤버 캐시 없음: ${candidate.id} - 다음 후보`);
                continue;
            }
            const oldRole = db[gId]?.[candidate.id]?.currentRole;
            for (const room of eligibleOtherRooms) {
                const role = getDB(gId, candidate.id).roles.find(
                    r => getRoomComp(gId, room)[r] < CONFIG.LIMITS[r]
                );
                if (!role) continue;

                try {
                    await candidate.voice.setChannel(room.id);
                    db[gId][candidate.id].currentRole = role;
                    saveDB();
                    moved = true;
                    movements.push({ userId: candidate.id, fromCh: channel.id, toCh: room.id, role, oldRole });
                    break outer;
                } catch (e) {
                    await new Promise(r => setTimeout(r, 1000));
                    if (candidate.voice?.channel?.id === room.id) {
                        console.warn(`[BB번] 로테이션 HTTP 실패하나 실제 이동 성공: ${candidate.id}`);
                        db[gId][candidate.id].currentRole = role;
                        saveDB();
                        moved = true;
                        movements.push({ userId: candidate.id, fromCh: channel.id, toCh: room.id, role, oldRole });
                        break outer;
                    }
                    console.error(`[NN번] 이동 실패, 다음 방 시도: ${candidate.id} → ${room.id}:`, e.message);
                }
            }
        }
    } else if (emptyOtherRooms.length > 0 && players.length >= 4) {
        // [UU번] 다중 이동: 빈 방으로 2명을 같이 이동 → 양쪽 모두 ≥ 2명 보장
        // 빈 방은 게임 중일 일이 없으니 eligibility 검사 불필요
        let pair = null;
        outer: for (let i = 0; i < shuffled.length; i++) {
            for (let j = i + 1; j < shuffled.length; j++) {
                const r1 = db[gId]?.[shuffled[i].id]?.currentRole;
                const r2 = db[gId]?.[shuffled[j].id]?.currentRole;
                if (!r1 || !r2) continue;
                const combo = { tank: 0, damage: 0, support: 0 };
                combo[r1]++; combo[r2]++;
                const fits = Object.entries(combo).every(([k, v]) => v <= CONFIG.LIMITS[k]);
                if (fits && channel.guild.members.cache.has(shuffled[i].id)
                         && channel.guild.members.cache.has(shuffled[j].id)) {
                    pair = [shuffled[i], shuffled[j]];
                    break outer;
                }
            }
        }
        if (!pair) {
            console.warn('[로테이션 무산] 다중 이동용 페어 LIMITS 매칭 실패');
            return false;
        }
        const targetRoom = [...emptyOtherRooms].sort(() => Math.random() - 0.5)[0];
        const [m1, m2] = pair;
        const r1 = db[gId][m1.id].currentRole;
        const r2 = db[gId][m2.id].currentRole;
        try {
            await m1.voice.setChannel(targetRoom.id);
        } catch (e) {
            console.error(`[UU번] 첫 번째 이동 실패 - 다중 이동 중단: ${m1.id}:`, e.message);
            return false;
        }
        try {
            await m2.voice.setChannel(targetRoom.id);
            moved = true;
            movements.push(
                { userId: m1.id, fromCh: channel.id, toCh: targetRoom.id, role: r1 },
                { userId: m2.id, fromCh: channel.id, toCh: targetRoom.id, role: r2 }
            );
            console.log(`[UU번] 다중 이동 성공: ${m1.id}, ${m2.id} → ${targetRoom.id}`);
        } catch (e) {
            console.error(`[BBB번] 두 번째 이동 실패 - 첫 번째 롤백 시도: ${m2.id}:`, e.message);
            try {
                await m1.voice.setChannel(channel.id);
                console.warn(`[BBB번] 롤백 성공: ${m1.id} → ${channel.id}`);
            } catch (re) {
                console.error(`[BBB번] 롤백 실패 - ${m1.id}이 ${targetRoom.id}에 외톨이로 잔존:`, re.message);
            }
        }
    } else {
        console.warn('[로테이션 무산] eligible 방 없음 + 빈 방 다중이동 조건도 미충족');
        return false;
    }

    if (!moved) {
        console.warn('[로테이션 무산] 이동 시도 모두 실패 - social 카운트 미적용');
        return false;
    }

    finishRotation(gId, channel, players, movements);
    return true;
}

// [VV번 수정] msg.reply 실패(메시지 삭제/권한 회수/슬로우모드 등) 시 unhandled rejection 방지
const safeReply = async (msg, content) => {
    try {
        await msg.reply(content);
    } catch (e) {
        console.warn(`[VV번] reply 실패: ${msg.author?.tag || msg.author?.id} - ${e.message}`);
    }
};

// [LLL번 수정] V8.2 통계 표시 헬퍼
const formatDuration = (ms) => {
    if (!ms || ms < 60000) return `${Math.round((ms || 0) / 1000)}초`;
    const minutes = Math.floor(ms / 60000);
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    if (hours === 0) return `${mins}분`;
    return `${hours}시간 ${mins}분`;
};
const formatRelativeTime = (timestamp) => {
    if (!timestamp) return '없음';
    const elapsed = Date.now() - timestamp;
    const days = Math.floor(elapsed / (24 * 60 * 60 * 1000));
    if (days > 0) return `${days}일 전`;
    const hours = Math.floor(elapsed / (60 * 60 * 1000));
    if (hours > 0) return `${hours}시간 전`;
    const minutes = Math.floor(elapsed / (60 * 1000));
    if (minutes > 0) return `${minutes}분 전`;
    return '방금 전';
};
// V8.5: 통계 계산 헬퍼들
const computeServerRank = (gId, uid, key) => {
    // key: 'totalInGameMs' | 'totalShuffles'
    const guildData = db[gId] || {};
    const ranking = Object.entries(guildData)
        .filter(([id]) => /^\d{17,20}$/.test(id))
        .map(([id, e]) => {
            if (!e?.daily) return { id, score: 0 };
            const score = key === 'totalInGameMs'
                ? Object.values(e.daily).reduce((s, d) => s + (d?.inGameMs || 0), 0)
                : Object.values(e.daily).reduce((s, d) => s + (d?.shuffleCount || 0), 0);
            return { id, score };
        })
        .filter(r => r.score > 0)
        .sort((a, b) => b.score - a.score);
    const idx = ranking.findIndex(r => r.id === uid);
    return idx >= 0 ? { rank: idx + 1, total: ranking.length } : { rank: null, total: ranking.length };
};
const computeStreak = (daily) => {
    if (!daily) return 0;
    let streak = 0;
    for (let i = 0; i < 365; i++) {
        const d = new Date();
        d.setDate(d.getDate() - i);
        const k = d.toISOString().slice(0, 10);
        const entry = daily[k];
        if (entry && (entry.inGameMs > 0 || entry.shuffleCount > 0)) streak++;
        else if (i > 0) break;  // 오늘은 활동 없을 수 있으니 1일째까지는 봐줌
    }
    return streak;
};
const computeWeekdayBreakdown = (daily) => {
    // 0=일, 1=월, ..., 6=토
    const buckets = [0, 0, 0, 0, 0, 0, 0];
    const counts = [0, 0, 0, 0, 0, 0, 0];
    for (const [dateKey, entry] of Object.entries(daily || {})) {
        const dow = new Date(dateKey + 'T00:00:00').getDay();
        if (Number.isNaN(dow)) continue;
        buckets[dow] += entry?.inGameMs || 0;
        counts[dow] += 1;
    }
    // 평균 (해당 요일이 며칠 있었는지로 나눔)
    return buckets.map((sum, i) => counts[i] > 0 ? sum / counts[i] : 0);
};
const findLastPlayed = (data) => {
    let max = 0;
    for (const s of Object.values(data?.social || {})) {
        if (s?.lastPlayed > max) max = s.lastPlayed;
    }
    return max;
};

const resolveUserName = async (guild, uid) => {
    try {
        const m = await guild.members.fetch(uid);
        return m.user.username;
    } catch {
        return `user_${uid.slice(-4)}`;
    }
};

// --- [ 명령어 처리 ] ---
// V8.5: !셔플설명 - 사용자용 사용 가이드 (디스코드 마크다운으로 렌더)
const HELP_TEXT_PART1 = `# 🎮 오버워치 봇 사용법

## 📌 처음 사용하기 (2단계)

**1단계** — 음성채널 입장 후 \`!랜덤 [내 가능 역할]\`
\`\`\`
!랜덤 탱딜힐
\`\`\`
봇이 빈자리 있는 게임방으로 옮겨주고 역할을 배정합니다.
- \`!랜덤 탱\` — 탱커만
- \`!랜덤 딜힐\` — 딜러 또는 힐러
- \`!랜덤 탱딜힐\` — 셋 다 가능

**2단계** — 게임 시작
마지막 셔플 후 1시간이 지난 시점에 게임이 끝나 있으면 자동 셔플 (게임 중이면 끝날 때까지 대기).

> 💡 **팁**: \`!역할변경 탱딜힐\` 한 번만 등록해두면 대기실 입장만으로 자동 배정됩니다.
> 💡 **역할 입력 형식**: \`탱딜힐\`, \`탱/딜/힐\`, \`탱,딜,힐\`, \`탱 딜 힐\` 다 동일 동작.`;

const HELP_TEXT_PART2 = `## 🎯 명령어 목록

**역할/배정**
- \`!역할변경 탱딜힐\` — 영구 역할 등록 (1회)
- \`!역할\` — 현재 배정된 역할 확인
- \`!랜덤\` / \`!랜덤 탱\` — 자동 / 일회성 배정
- \`!변경 탱딜\` — \`!랜덤\` 실패 직후 다른 역할로 재시도 (5분 내)
- \`!혼자\` — \`!랜덤\` 실패 직후 빈 방 혼자 입장 (5분 내)

**이동**
- \`!되돌려\` — 직전 셔플로 이동한 **전원** 원래 방 복귀 (5분 내)
- \`!소환 @사용자\` — 다른 방의 사람을 내 음성채널로

## 🔀 자동 셔플 조건

**셔플 발생:** 3명 이상 + 모두 게임 안 함 + 쿨다운(1시간) 경과
**보류:** 누군가 게임 중 / 전원 오프라인 / 오프라인 섞여있고 온라인이 아직 한판 안 마침

**분할:** 4명 이상 + 빈 방 있으면 양쪽 게임 가능하도록 일부 이동

## ❓ 자주 보는 상황

**"❌ 배정 가능한 자리가 없습니다"** → \`!변경 [추천 역할]\` 또는 \`!혼자\`
**셔플이 마음에 안 든다** → 5분 내 \`!되돌려\`
**친구 데려오기** → \`!소환 @친구\`
**역할 바꾸기** → 영구 \`!역할변경 딜힐\` / 일회성 \`!랜덤 딜힐\``;

client.on('messageCreate', async (msg) => {
    if (msg.author.bot) return;
    const gId = msg.guild?.id;
    if (!gId) return;

    const args = msg.content.trim().split(/\s+/);

    if (args[0] === '!셔플설명') {
        try {
            await msg.reply(HELP_TEXT_PART1);
            await msg.channel.send(HELP_TEXT_PART2);
        } catch (e) {
            console.warn('[!셔플설명] 전송 실패:', e?.message || e);
        }
        return;
    }

    if (args[0] === '!역할변경') {
        const r = [];
        const i = args.slice(1).join('');
        if (i.includes('탱')) r.push('tank');
        if (i.includes('딜')) r.push('damage');
        if (i.includes('힐')) r.push('support');
        if (r.length) {
            getDB(gId, msg.author.id).roles = r;
            saveDB(true);
            await safeReply(msg, `📝 역할 업데이트 완료: ${r.join(', ')}`);
        } else {
            await safeReply(msg, '⚠️ 사용법: `!역할변경 탱커`, `!역할변경 딜러힐러` 등');
        }
    }

    // V7.2: 한국어 역할 키워드 → 내부 코드 파싱 (공통 헬퍼)
    const parseRoles = (rest) => {
        const out = [];
        const i = rest.join('');
        if (i.includes('탱')) out.push('tank');
        if (i.includes('딜')) out.push('damage');
        if (i.includes('힐')) out.push('support');
        return out;
    };
    const FOLLOWUP_TTL = 5 * 60 * 1000;
    const ROLE_NAMES = { tank: '탱커', damage: '딜러', support: '힐러' };

    // !역할 - 현재 배정된 역할 확인
    if (args[0] === '!역할') {
        const data = getDB(gId, msg.author.id);
        if (data.currentRole) {
            await safeReply(msg, `🎯 현재 역할: ${ROLE_NAMES[data.currentRole]}`);
        } else {
            await safeReply(msg, '⚠️ 현재 배정된 역할이 없습니다.');
        }
    }

    if (args[0] === '!랜덤') {
        if (!msg.member?.voice?.channel) {
            return safeReply(msg, '⚠️ 먼저 음성 채널에 입장해주세요.');
        }
        let roles = null;
        if (args.length > 1) {
            roles = parseRoles(args.slice(1));
            if (!roles.length) return safeReply(msg, '⚠️ 올바른 역할을 입력해주세요.');
        }
        const data = getDB(gId, msg.author.id);
        const resolvedRoles = roles?.length ? roles : data.roles;
        const result = await smartAssign(msg.member, roles);
        if (result.startsWith('✅')) {
            data.followup = null;
        } else if (resolvedRoles?.length) {
            // V7.2: 실패 시 후속(!변경/!혼자) 사용 가능 상태 기록
            data.followup = { roles: resolvedRoles, at: Date.now() };
        }
        saveDB();
        if (!result.startsWith('✅')) await safeReply(msg, result);
    }

    // V7.2: !변경 [역할] - !랜덤 실패 후 더 넓은 역할셋으로 재시도
    if (args[0] === '!변경') {
        if (!msg.member?.voice?.channel) {
            return safeReply(msg, '⚠️ 먼저 음성 채널에 입장해주세요.');
        }
        const data = getDB(gId, msg.author.id);
        if (!data.followup || (Date.now() - data.followup.at) > FOLLOWUP_TTL) {
            return safeReply(msg, '⚠️ `!랜덤` 실패 직후(5분 내)에만 사용할 수 있습니다.');
        }
        if (args.length < 2) {
            return safeReply(msg, '⚠️ 사용법: `!변경 탱/딜/힐` 등');
        }
        const newRoles = parseRoles(args.slice(1));
        if (!newRoles.length) return safeReply(msg, '⚠️ 올바른 역할을 입력해주세요.');
        const result = await smartAssign(msg.member, newRoles);
        if (result.startsWith('✅')) {
            data.followup = null;
        } else {
            data.followup = { roles: newRoles, at: Date.now() };
        }
        saveDB();
        if (!result.startsWith('✅')) await safeReply(msg, result);
    }

    // V7.2: !혼자 - !랜덤 실패 후, 직전 시도 역할셋 그대로 빈 방에 입장
    if (args[0] === '!혼자') {
        if (!msg.member?.voice?.channel) {
            return safeReply(msg, '⚠️ 먼저 음성 채널에 입장해주세요.');
        }
        const data = getDB(gId, msg.author.id);
        if (!data.followup || (Date.now() - data.followup.at) > FOLLOWUP_TTL) {
            return safeReply(msg, '⚠️ `!랜덤` 실패 직후(5분 내)에만 사용할 수 있습니다.');
        }
        const guildConfig = getGuildConfig(gId);
        if (!guildConfig) return safeReply(msg, '⚠️ 미등록 서버입니다.');

        const emptyRooms = guildConfig.GAME_ROOMS
            .map(id => msg.guild.channels.cache.get(id))
            .filter(r => r && getRoomMembers(gId, r).length === 0);
        if (!emptyRooms.length) return safeReply(msg, '⚠️ 빈 게임방이 없습니다.');

        const targetRoom = emptyRooms[Math.floor(Math.random() * emptyRooms.length)];
        if (!targetRoom.permissionsFor(client.user).has(PermissionsBitField.Flags.MoveMembers)) {
            return safeReply(msg, '⚠️ 봇 권한이 부족합니다.');
        }

        const followupRoles = data.followup.roles;
        const chosenRole = followupRoles[Math.floor(Math.random() * followupRoles.length)];
        try {
            await msg.member.voice.setChannel(targetRoom.id);
            data.currentRole = chosenRole;
            data.activeRoles = followupRoles;  // 이 세션 동안 이 역할셋 유지
            data.followup = null;
            addShuffleMember(gId, targetRoom.id, msg.author.id);
            data.roleStats[chosenRole] = (data.roleStats[chosenRole] || 0) + 1;
            saveDB();
            await safeReply(msg, `✅ ${targetRoom.name}에 ${ROLE_NAMES[chosenRole]}로 혼자 대기 중`);
        } catch (e) {
            console.error(`[혼자] 이동 실패:`, e.message);
            await safeReply(msg, '⚠️ 이동 실패');
        }
    }

    // [JJJ번 수정] !되돌려 - 직전 셔플 무효화 (5분 이내, 셔플 대상자만)
    if (args[0] === '!되돌려') {
        const callerCh = msg.member?.voice?.channel;
        if (!callerCh) return safeReply(msg, '⚠️ 음성 채널 안에서 사용해주세요.');

        const guildConfig = getGuildConfig(gId);
        if (!guildConfig) return safeReply(msg, '⚠️ 미등록 서버입니다.');
        if (!guildConfig.GAME_ROOMS.includes(callerCh.id)) {
            return safeReply(msg, '⚠️ 게임방 안에서만 되돌리기 가능합니다.');
        }

        const lastShuffle = getLastShuffle(gId, callerCh.id);
        if (!lastShuffle) return safeReply(msg, '⚠️ 되돌릴 직전 셔플 기록이 없습니다.');

        const elapsed = Date.now() - lastShuffle.at;
        const FIVE_MIN = 5 * 60 * 1000;
        if (elapsed > FIVE_MIN) {
            return safeReply(msg, `⚠️ 셔플 후 ${Math.round(elapsed/60000)}분 경과 - 5분 초과 시 되돌리기 불가`);
        }

        const inMovements = lastShuffle.movements.some(m => m.userId === msg.author.id);
        if (!inMovements) {
            return safeReply(msg, '⚠️ 직전 셔플에 휘말린 사람만 되돌릴 수 있습니다.');
        }

        // 역방향 setChannel - movements 역순으로
        const reversed = [...lastShuffle.movements].reverse();
        const movedUserIds = new Set();
        const affectedChannels = new Set();
        for (const m of reversed) {
            const member = msg.guild.members.cache.get(m.userId);
            if (!member) {
                console.warn(`[JJJ번] 되돌리기 - 멤버 캐시 없음: ${m.userId}`);
                continue;
            }
            // 현재 toCh에 있어야 되돌림 의미 있음
            if (member.voice?.channel?.id !== m.toCh) {
                console.warn(`[JJJ번] ${member.user?.tag || m.userId} 이미 다른 곳 (${member.voice?.channel?.id}) - 스킵`);
                continue;
            }
            try {
                await member.voice.setChannel(m.fromCh);
                if (m.oldRole !== undefined) db[gId][m.userId].currentRole = m.oldRole;
                // [MMM번 수정] 셔플 멤버 리스트도 역방향 갱신 (toCh에서 제거 + fromCh에 복원)
                removeShuffleMember(gId, m.toCh, m.userId);
                addShuffleMember(gId, m.fromCh, m.userId);
                movedUserIds.add(m.userId);
                affectedChannels.add(m.fromCh);
                affectedChannels.add(m.toCh);
            } catch (e) {
                console.error(`[JJJ번] 되돌리기 실패: ${m.userId} → ${m.fromCh}:`, e.message);
            }
        }

        if (movedUserIds.size === 0) {
            return safeReply(msg, '⚠️ 되돌릴 사람이 없습니다 (이미 다른 곳에 있거나 캐시 누락).');
        }

        // 되돌리기 카운트 +1 - 이동된 모든 사람에게 누적
        const now = Date.now();
        for (const uid of movedUserIds) {
            const ud = getDB(gId, uid);
            ud.undoCount = (ud.undoCount || 0) + 1;
            ud.lastUndoAt = now;
        }
        // 사용횟수 - 명령어 호출한 사람만 별도 누적
        const callerData = getDB(gId, msg.author.id);
        callerData.undoUseCount = (callerData.undoUseCount || 0) + 1;
        callerData.lastUndoUseAt = now;
        for (const cid of affectedChannels) {
            if (db[gId]?._cooldowns?.[cid]) delete db[gId]._cooldowns[cid];
            clearLastShuffle(gId, cid);
        }
        saveDB(true);

        // 되돌린 채널들에 다시 셔플 스케줄 (즉시 시도 → 게임 끝나면 발화)
        for (const cid of affectedChannels) {
            const room = msg.guild.channels.cache.get(cid);
            if (room) scheduleNextRotation(room);
        }

        await safeReply(msg, `↩️ ${movedUserIds.size}명 되돌리기 완료 (${msg.author.username} 사용 ${callerData.undoUseCount}회)`);
        console.log(`[JJJ번] 되돌리기 by ${msg.author.tag} (사용 ${callerData.undoUseCount}회) - ${movedUserIds.size}명 복귀`);
    }

    // [JJJ번 수정] !소환 @user - 특정인을 호출자 음성채널로 이동
    if (args[0] === '!소환') {
        if (!msg.member?.voice?.channel) {
            return safeReply(msg, '⚠️ 먼저 음성 채널에 입장해주세요.');
        }
        const target = msg.mentions.members?.first();
        if (!target) return safeReply(msg, '⚠️ 사용법: `!소환 @사용자`');
        if (target.user.bot) return safeReply(msg, '⚠️ 봇은 소환할 수 없습니다.');
        if (target.id === msg.author.id) return safeReply(msg, '⚠️ 자기 자신은 소환할 수 없습니다.');
        if (!target.voice?.channel) return safeReply(msg, `⚠️ ${target.user.username}님이 음성 채널에 없습니다.`);
        if (target.voice.channel.id === msg.member.voice.channel.id) {
            return safeReply(msg, `⚠️ ${target.user.username}님은 이미 같은 채널에 있습니다.`);
        }

        try {
            await target.voice.setChannel(msg.member.voice.channel.id);
            const callerData = getDB(gId, msg.author.id);
            callerData.summonCount = (callerData.summonCount || 0) + 1;
            callerData.lastSummonAt = Date.now();
            saveDB(true);
            await safeReply(msg, `📢 ${target.user.username}님을 소환했습니다 (누적 ${callerData.summonCount}회)`);
            console.log(`[JJJ번] 소환 by ${msg.author.tag} → ${target.user.tag} (누적 ${callerData.summonCount}회)`);
        } catch (e) {
            console.error(`[JJJ번] 소환 실패:`, e.message);
            await safeReply(msg, `⚠️ 소환 실패: ${e.message}`);
        }
    }

    // V8.4: !통계 - 카드 이미지로 출력
    // 사용법: !통계 / !통계 횟수 / !통계 @user
    if (args[0] === '!통계') {
        const data = getDB(gId, msg.author.id);
        const target = msg.mentions.users?.first();

        // @user — 1:1 통계는 텍스트 캡션 + 카드 같이 첨부
        let comparisonCaption = null;
        if (target) {
            if (target.bot) return safeReply(msg, '⚠️ 봇과의 통계는 없습니다.');
            const s = data.social?.[target.id];
            comparisonCaption = (s && (s.count || s.durationMs))
                ? `📊 **${msg.author.username} ↔ ${target.username}** — 같이 ${s.count || 0}회 / ${formatDuration(s.durationMs)} (마지막: ${formatRelativeTime(s.lastPlayed)})`
                : `📊 **${msg.author.username} ↔ ${target.username}** — 함께 한 기록 없음`;
        }

        try {
            const sortByCount = args[1] === '횟수';
            const sortKey = sortByCount ? 'count' : 'durationMs';
            // count 또는 durationMs 중 하나라도 값이 있으면 포함, 정렬만 선택한 키로
            const topEntries = Object.entries(data.social || {})
                .filter(([, s]) => (s?.count || 0) > 0 || (s?.durationMs || 0) > 0)
                .sort(([, a], [, b]) => (b[sortKey] || 0) - (a[sortKey] || 0))
                .slice(0, 5);
            const topPartners = await Promise.all(topEntries.map(async ([uid, s]) => ({
                name: await resolveUserName(msg.guild, uid),
                count: s.count || 0,
                durationMs: s.durationMs || 0
            })));

            // 최근 14일 일별 (오래된 → 최신)
            const dailyArr = [];
            for (let i = 13; i >= 0; i--) {
                const date = todayKey(-i);
                const d = data.daily?.[date] || {};
                dailyArr.push({
                    date,
                    inGameMs: d.inGameMs || 0,
                    shuffleCount: d.shuffleCount || 0
                });
            }

            // 총 메트릭
            const totalPartners = Object.values(data.social || {})
                .filter(s => (s?.count || 0) > 0 || (s?.durationMs || 0) > 0).length;
            const totalShuffles = Object.values(data.daily || {})
                .reduce((sum, d) => sum + (d?.shuffleCount || 0), 0);
            const totalInGameMs = Object.values(data.daily || {})
                .reduce((sum, d) => sum + (d?.inGameMs || 0), 0);

            // 활동 요약: 최장일 / 가장 활발한 요일 / 최근 7일 합 / 활동일 평균
            const dailyValues = Object.values(data.daily || {});
            const peakDayMs = dailyValues.reduce((max, d) => Math.max(max, d?.inGameMs || 0), 0);
            const wb = computeWeekdayBreakdown(data.daily);
            const peakWeekday = wb.some(v => v > 0) ? wb.indexOf(Math.max(...wb)) : null;
            const last7DaysMs = dailyArr.slice(-7).reduce((s, d) => s + (d.inGameMs || 0), 0);
            const activeDays = dailyValues.filter(d => (d?.inGameMs || 0) > 0).length;
            const avgPerActiveDayMs = activeDays > 0 ? totalInGameMs / activeDays : 0;

            const png = await renderPersonalCard({
                username: msg.member?.displayName || msg.author.username,
                joinedAt: msg.member?.joinedTimestamp,
                avatarURL: msg.author.displayAvatarURL({ extension: 'png', size: 128 }),
                totalPartners, totalShuffles, totalInGameMs,
                topPartners,
                daily: dailyArr,
                currentRole: data.currentRole,
                roles: data.roles,
                // V8.5 추가 필드
                undoCount: data.undoCount || 0,
                undoUseCount: data.undoUseCount || 0,
                summonCount: data.summonCount || 0,
                serverRank: computeServerRank(gId, msg.author.id, 'totalInGameMs'),
                lastPlayed: findLastPlayed(data),
                firstPlayedAt: data.firstPlayedAt || 0,
                streak: computeStreak(data.daily),
                weekdayBreakdown: wb,
                hourly: data.hourly || {},
                activitySummary: { peakDayMs, peakWeekday, last7DaysMs, avgPerActiveDayMs }
            });

            await msg.reply({
                content: comparisonCaption || undefined,
                files: [new AttachmentBuilder(png, { name: 'stats.png' })]
            });
        } catch (e) {
            console.error('[!통계] 카드 렌더 실패:', e?.message || e);
            await safeReply(msg, '⚠️ 통계 카드 생성 실패');
        }
    }

    // V8.4: !남용 - 카드 이미지로 출력 (기록 없어도 카드 렌더)
    if (args[0] === '!남용') {
        const guildData = db[gId] || {};
        const userEntries = Object.entries(guildData)
            .filter(([uid]) => /^\d{17,20}$/.test(uid))
            .filter(([, e]) => e && (e.undoCount > 0 || e.undoUseCount > 0 || e.summonCount > 0));

        try {
            const buildTop = async (key, lastKey) => {
                const top = userEntries
                    .filter(([, e]) => (e[key] || 0) > 0)
                    .sort(([, a], [, b]) => (b[key] || 0) - (a[key] || 0))
                    .slice(0, 5);
                return Promise.all(top.map(async ([uid, e]) => ({
                    name: await resolveUserName(msg.guild, uid),
                    count: e[key],
                    lastAt: e[lastKey]
                })));
            };

            const [undoTop, undoUseTop, summonTop] = await Promise.all([
                buildTop('undoCount', 'lastUndoAt'),
                buildTop('undoUseCount', 'lastUndoUseAt'),
                buildTop('summonCount', 'lastSummonAt')
            ]);

            const png = await renderAbuseCard({
                guildName: msg.guild.name,
                undoTop, undoUseTop, summonTop
            });
            await msg.reply({ files: [new AttachmentBuilder(png, { name: 'abuse.png' })] });
        } catch (e) {
            console.error('[!남용] 카드 렌더 실패:', e?.message || e);
            await safeReply(msg, '⚠️ 남용 통계 카드 생성 실패');
        }
    }

    // V8.5: !서버통계 - 서버 전체 활동 요약
    if (args[0] === '!서버통계') {
        try {
            const guildData = db[gId] || {};
            const userEntries = Object.entries(guildData)
                .filter(([uid]) => /^\d{17,20}$/.test(uid));

            const SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000;
            const now = Date.now();
            const activeUsers7d = userEntries.filter(([, e]) => {
                if (!e?.daily) return false;
                return Object.entries(e.daily).some(([dKey]) => {
                    const t = new Date(dKey + 'T00:00:00').getTime();
                    return now - t < SEVEN_DAYS;
                });
            }).length;

            // 총합 (더블카운트 방지: 시간은 sum, 셔플도 sum — 셔플은 본래 사람당 +1이라 OK)
            let totalInGameMs = 0, totalShuffles = 0;
            const aggRoleStats = { tank: 0, damage: 0, support: 0 };
            const aggHourly = {};
            for (const [, e] of userEntries) {
                for (const d of Object.values(e?.daily || {})) {
                    totalInGameMs += d?.inGameMs || 0;
                    totalShuffles += d?.shuffleCount || 0;
                }
                for (const k of ['tank', 'damage', 'support']) {
                    aggRoleStats[k] += e?.roleStats?.[k] || 0;
                }
                for (const [h, v] of Object.entries(e?.hourly || {})) {
                    aggHourly[h] = (aggHourly[h] || 0) + (v || 0);
                }
            }

            // Top 10 by inGameMs
            const topPlayersRaw = userEntries
                .map(([uid, e]) => {
                    const inGameMs = Object.values(e?.daily || {}).reduce((s, d) => s + (d?.inGameMs || 0), 0);
                    const shuffleCount = Object.values(e?.daily || {}).reduce((s, d) => s + (d?.shuffleCount || 0), 0);
                    return { uid, inGameMs, shuffleCount };
                })
                .filter(p => p.inGameMs > 0)
                .sort((a, b) => b.inGameMs - a.inGameMs)
                .slice(0, 10);
            const topPlayers = await Promise.all(topPlayersRaw.map(async p => ({
                name: await resolveUserName(msg.guild, p.uid),
                inGameMs: p.inGameMs,
                shuffleCount: p.shuffleCount
            })));

            // 14일 일별 합계
            const dailyTotals = [];
            for (let i = 13; i >= 0; i--) {
                const date = todayKey(-i);
                let inGameMs = 0, shuffleCount = 0;
                for (const [, e] of userEntries) {
                    const d = e?.daily?.[date];
                    if (d) {
                        inGameMs += d.inGameMs || 0;
                        shuffleCount += d.shuffleCount || 0;
                    }
                }
                dailyTotals.push({ date, inGameMs, shuffleCount });
            }

            const png = await renderServerCard({
                guildName: msg.guild.name,
                totalUsers: userEntries.length,
                activeUsers7d,
                totalInGameMs,
                totalShuffles,
                topPlayers,
                roleStats: aggRoleStats,
                hourly: aggHourly,
                dailyTotals
            });
            await msg.reply({ files: [new AttachmentBuilder(png, { name: 'server.png' })] });
        } catch (e) {
            console.error('[!서버통계] 카드 렌더 실패:', e?.message || e);
            await safeReply(msg, '⚠️ 서버 통계 카드 생성 실패');
        }
    }

    // V8.5: !셔플통계 - 셔플 분석
    if (args[0] === '!셔플통계') {
        try {
            const guildData = db[gId] || {};
            const userEntries = Object.entries(guildData)
                .filter(([uid]) => /^\d{17,20}$/.test(uid));

            // 일별 셔플 합 (14일)
            const dailyShuffles = [];
            for (let i = 13; i >= 0; i--) {
                const date = todayKey(-i);
                let count = 0;
                for (const [, e] of userEntries) {
                    count += e?.daily?.[date]?.shuffleCount || 0;
                }
                dailyShuffles.push({ date, count });
            }

            // 본 봇은 셔플 1번에 평균 4명 휘말림 가정 → 발생 수 추정
            const totalSessions = dailyShuffles.reduce((s, d) => s + d.count, 0); // 사람-셔플 누적
            const totalShuffles = Math.round(totalSessions / 4); // 발생 수 추정
            const days = dailyShuffles.filter(d => d.count > 0).length || 1;
            const avgShufflePerDay = totalSessions / days;

            const mostShuffledRaw = userEntries
                .map(([uid, e]) => ({
                    uid,
                    count: Object.values(e?.daily || {}).reduce((s, d) => s + (d?.shuffleCount || 0), 0)
                }))
                .filter(p => p.count > 0)
                .sort((a, b) => b.count - a.count)
                .slice(0, 5);
            const mostShuffled = await Promise.all(mostShuffledRaw.map(async p => ({
                name: await resolveUserName(msg.guild, p.uid),
                count: p.count
            })));

            const png = await renderShuffleCard({
                guildName: msg.guild.name,
                totalShuffles,
                totalSessions,
                avgShufflePerDay,
                mostShuffled,
                dailyShuffles
            });
            await msg.reply({ files: [new AttachmentBuilder(png, { name: 'shuffle.png' })] });
        } catch (e) {
            console.error('[!셔플통계] 카드 렌더 실패:', e?.message || e);
            await safeReply(msg, '⚠️ 셔플 통계 카드 생성 실패');
        }
    }
});

// [M번 수정] ready 시 모든 등록 서버 멤버 캐시 워밍업
// [MM번 수정] discord.js v15에서 'ready'→'clientReady'로 변경 - 양쪽 모두 등록하되 한 번만 실행
let readyDone = false;
const onReady = async () => {
    if (readyDone) return;
    readyDone = true;
    console.log(`[V7.1] ${client.user.tag} 온라인`);
    runGC();

    for (const gId of Object.keys(CONFIG.GUILDS)) {
        const guild = client.guilds.cache.get(gId);
        if (!guild) {
            console.warn(`[M번] 서버 캐시 없음: ${gId}`);
            continue;
        }
        try {
            await guild.members.fetch();
            console.log(`[M번] 멤버 캐시 완료: ${guild.name} (${guild.memberCount}명)`);
        } catch (e) {
            console.error(`[M번] 멤버 캐시 실패: ${gId}:`, e.message);
        }

        // [HHH번 수정] 모든 게임방에 셔플 스케줄링 시작
        // 마지막 쿨다운 시각을 기준으로 남은 시간만큼 setTimeout 등록 (이미 지났으면 즉시 시도)
        const guildConfig = CONFIG.GUILDS[gId];
        for (const roomId of guildConfig.GAME_ROOMS) {
            const room = guild.channels.cache.get(roomId);
            if (!room) {
                console.warn(`[HHH번] 게임방 캐시 없음: ${roomId}`);
                continue;
            }
            scheduleNextRotation(room);
        }
    }
};
client.once('clientReady', onReady);  // discord.js v15 권장명 (v14.16+ alias 동작 확인)

// [S번 수정] graceful shutdown - 종료 시그널에 saveDB 강제 실행하여 미저장 데이터 보존
const shutdown = async (sig) => {
    console.log(`[S번] ${sig} 수신 - DB 강제 저장 후 종료`);
    try {
        await saveDB(true);
        // [GG번 수정] 진행 중 flushDB(saving=true)에 막혀 pendingWrite만 세팅된 경우
        // 큐의 두 번째 iteration까지 완료되도록 폴링 (최대 5초)
        const deadline = Date.now() + 5000;
        while ((saving || pendingWrite) && Date.now() < deadline) {
            await new Promise(r => setTimeout(r, 50));
        }
        if (saving || pendingWrite) {
            console.warn('[GG번] 5초 내 write 미완료 - 강제 종료 (잔여 변경 손실 가능)');
        }
    } catch (e) {
        console.error('[S번] 종료 저장 실패:', e.message);
    }
    process.exit(0);
};
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// [FFF번 수정] discord.js Client의 'error' 이벤트 핸들러 부재 시 unhandled로 봇 크래시
client.on('error', e => console.error('[FFF번] Discord Client error:', e?.message || e));
client.on('shardError', e => console.error('[FFF번] Discord shard error:', e?.message || e));

// [GGG번 수정] process 레벨 안전망 - 어디서든 unhandled rejection/exception 발생 시 로그만 찍고 봇은 살림
process.on('unhandledRejection', (reason) => {
    console.error('[GGG번] unhandledRejection:', reason?.message || reason);
    if (reason?.stack) console.error(reason.stack);
});
process.on('uncaughtException', (e) => {
    console.error('[GGG번] uncaughtException:', e?.message || e);
    if (e?.stack) console.error(e.stack);
});

// [KKK번 수정] V8.1 - 게임 중 시간 샘플링: 30초마다 각 게임방 sweep
// 두 사람 모두 게임 중(_isInGame=true)인 시간만 카운트 → 통계용 누적 durationMs
const PLAYTIME_SAMPLE_MS = 30 * 1000;
const samplePlaytime = () => {
    for (const gId of Object.keys(CONFIG.GUILDS)) {
        const guild = client.guilds.cache.get(gId);
        if (!guild) continue;
        const guildConfig = CONFIG.GUILDS[gId];
        for (const roomId of guildConfig.GAME_ROOMS) {
            const room = guild.channels.cache.get(roomId);
            if (!room) continue;
            // [MMM번 수정] 구경꾼 제외 - 셔플 멤버만 같이 한 시간 카운트
            const inGame = [...room.members.values()].filter(m =>
                !m.user.bot && _isInGame(m) &&
                db[gId]?.[m.id]?.currentRole &&
                isShuffleMember(gId, roomId, m.id)
            );
            if (inGame.length < 2) continue;
            const now = Date.now();
            const dateKey = todayKey();
            const hourKey = String(new Date().getHours());
            for (const a of inGame) {
                const da = getDB(gId, a.id);
                // V8.4: 일별 게임 시간 + 그날 같이 한 사람 set
                const daily = ensureDailyEntry(da, dateKey);
                daily.inGameMs += PLAYTIME_SAMPLE_MS;
                // V8.5: 시간대별 누적 + 첫 활동 시각
                da.hourly[hourKey] = (da.hourly[hourKey] || 0) + PLAYTIME_SAMPLE_MS;
                if (!da.firstPlayedAt) da.firstPlayedAt = now;
                for (const b of inGame) {
                    if (a.id === b.id) continue;
                    const s = ensureSocialEntry(da, b.id);
                    s.durationMs += PLAYTIME_SAMPLE_MS;
                    s.lastPlayed = now;
                    daily.partnersSet[b.id] = true;
                }
            }
        }
    }
    saveDB();
};

async function main() {
    await initDB();
    setInterval(runGC, CONFIG.GC_INTERVAL);
    setInterval(samplePlaytime, PLAYTIME_SAMPLE_MS);
    await client.login(process.env.TOKEN);
}
main().catch(e => { console.error('[FATAL]', e); process.exit(1); });
