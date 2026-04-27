const { Client, GatewayIntentBits, ActivityType, PermissionsBitField } = require('discord.js');
const fs = require('fs');
require('dotenv').config();

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
    // 멀티 서버 대응: 서버별 설정 [J번 수정]
    GUILDS: {
        '서버ID_1': {
            LOBBY_ID: '대기실_ID',
            GAME_ROOMS: ['방1', '방2', '방3', '방4', '방5']
        }
        // 서버 추가 시 여기에 등록
    },
    DATA_FILE: './ow_v70_master.json',
    POTG_BUFFER: 15000,
    LIMITS: { tank: 1, damage: 2, support: 2 },
    RETENTION: 1000 * 60 * 60 * 24 * 30 * 6, // 6개월
    SAVE_INTERVAL: 5000,      // [18번] 5초 쓰로틀링
    GC_INTERVAL: 1000 * 60 * 60 // [G번 수정] GC는 1시간마다 별도 실행
};

// --- [ DB 로드 ] ---
let db = {};
try {
    if (fs.existsSync(CONFIG.DATA_FILE)) {
        db = JSON.parse(fs.readFileSync(CONFIG.DATA_FILE, 'utf8'));
    }
} catch (e) {
    console.error('[DB 로드 실패] 새 DB로 시작합니다:', e.message);
}

let lastSaveTime = 0;

// [G번 수정] GC를 saveDB에서 분리 - 1시간마다 별도 실행
const runGC = () => {
    const now = Date.now();
    for (const gId in db) {
        for (const uId in db[gId]) {
            if (db[gId][uId].social) {
                for (const tId in db[gId][uId].social) {
                    if (now - db[gId][uId].social[tId].lastPlayed > CONFIG.RETENTION) {
                        delete db[gId][uId].social[tId];
                    }
                }
            }
        }
    }
    console.log('[GC] 6개월 초과 소셜 데이터 정리 완료');
};
setInterval(runGC, CONFIG.GC_INTERVAL);

// [H번 수정] Atomic Write - 임시 파일 → rename 방식으로 파일 손상 방지
const saveDB = (force = false) => {
    const now = Date.now();
    if (!force && now - lastSaveTime < CONFIG.SAVE_INTERVAL) return;

    const tmpFile = CONFIG.DATA_FILE + '.tmp';
    try {
        fs.writeFileSync(tmpFile, JSON.stringify(db, null, 2), 'utf8');
        fs.renameSync(tmpFile, CONFIG.DATA_FILE); // atomic
        lastSaveTime = now;
    } catch (e) {
        console.error('[DB 저장 실패]:', e.message);
        if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
    }
};

const getDB = (gId, uId) => {
    if (!db[gId]) db[gId] = {};
    if (!db[gId][uId]) db[gId][uId] = { roles: [], currentRole: null, social: {} };
    return db[gId][uId];
};

// --- [ 서버별 설정 헬퍼 ] [J번 수정] ---
const getGuildConfig = (gId) => CONFIG.GUILDS[gId] || null;

// --- [ 핵심 함수 ] ---
const getRoomComp = (gId, room) => {
    const comp = { tank: 0, damage: 0, support: 0 };
    room.members.forEach(m => {
        const r = db[gId]?.[m.id]?.currentRole;
        if (r) comp[r]++;
    });
    return comp;
};

// [F번 수정] smartAssign 전용 잠금
const assignLocks = new Set();

async function smartAssign(member, roles = null) {
    const gId = member.guild.id;
    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return "⚠️ 미등록 서버입니다.";

    // [F번 수정] 중복 smartAssign 방지
    if (assignLocks.has(member.id)) return "⏳ 배정 중입니다.";
    assignLocks.add(member.id);

    try {
        const data = getDB(gId, member.id);
        const target = roles?.length ? roles : data.roles;

        if (!target?.length) return "⚠️ 역할을 먼저 설정해주세요.";

        // [17번] 온라인 상태 재검증 (presence 캐시 기준)
        const presence = member.presence;
        if (!presence || presence.status === 'offline') return "⚠️ 오프라인 상태입니다. 온라인으로 변경 후 재시도하세요.";

        const scores = guildConfig.GAME_ROOMS
            .map(id => member.guild.channels.cache.get(id))
            .filter(r => r)
            .map(room => {
                const comp = getRoomComp(gId, room);
                const role = target.find(r => comp[r] < CONFIG.LIMITS[r]);
                if (!role) return { room, score: -9999 };

                let score = 100;
                room.members.forEach(m => {
                    if (db[gId]?.[m.id]?.currentRole) {
                        score -= ((data.social[m.id]?.count || 0) * 20);
                    }
                });
                return { room, score, role };
            })
            .sort((a, b) => b.score - a.score);

        const best = scores[0];
        if (!best || best.score < -5000) return "❌ 현재 배정 가능한 자리가 없습니다.";

        // [15번] 권한 사전 체크
        if (!best.room.permissionsFor(client.user).has(PermissionsBitField.Flags.MoveMembers)) {
            return "⚠️ 봇 권한이 부족합니다. 서버 관리자에게 문의하세요.";
        }

        data.currentRole = best.role;
        saveDB();
        await member.voice.setChannel(best.room.id);
        return `✅ ${{ tank: '탱커', damage: '딜러', support: '힐러' }[best.role]}로 배정되었습니다!`;

    } finally {
        // [F번 수정] 성공/실패 무관하게 잠금 해제
        assignLocks.delete(member.id);
    }
}

// --- [ 이벤트 감시 ] ---

// [A번 수정] oldS.id → oldS.member?.id 로 수정
client.on('voiceStateUpdate', (oldS, newS) => {
    const gId = oldS.guild?.id;
    if (!gId) return;

    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return;

    const memberId = oldS.member?.id || newS.member?.id;
    if (!memberId) return;

    // 채널 이탈 시 currentRole 초기화
    if (oldS.channelId && oldS.channelId !== newS.channelId && db[gId]?.[memberId]) {
        db[gId][memberId].currentRole = null;
        saveDB();
    }

    // 로비 입장 시 자동 배정
    if (!oldS.channelId && newS.channelId === guildConfig.LOBBY_ID) {
        const member = newS.member;
        if (member) smartAssign(member); // [F번] assignLocks로 내부에서 중복 방지
    }
});

const activeLocks = new Set();

client.on('presenceUpdate', (oldP, newP) => {
    const member = newP?.member;
    const gId = member?.guild?.id;
    if (!gId) return;

    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return;

    const channel = member?.voice?.channel;
    if (!channel || !guildConfig.GAME_ROOMS.includes(channel.id)) return;
    if (!db[gId]?.[member.id]?.currentRole) return;
    if (activeLocks.has(channel.id)) return;

    // [D번 수정] oldP null 방어 처리
    const wasOW = oldP?.activities?.some(
        a => a.name === 'Overwatch 2' && (a.details || a.state)
    ) ?? false;

    const currentOW = newP.activities?.find(a => a.name === 'Overwatch 2');

    // [19번] 백필/연습방 검증: 타임스탬프 1분 이내 갱신 시 새 게임으로 간주
    const recentlyStarted = currentOW?.timestamps?.start
        && (Date.now() - new Date(currentOW.timestamps.start).getTime()) < 60000;

    const isActuallyFinished = !currentOW || (!currentOW.details && !currentOW.state && !recentlyStarted);

    // [E번 수정] 플레이어(역할 보유자) 5명 기준 통일
    const activePlayers = channel.members.filter(m => db[gId]?.[m.id]?.currentRole);
    if (activePlayers.size < 5) return;

    if (wasOW && isActuallyFinished) {
        activeLocks.add(channel.id);

        // [17번] 버퍼 시점 상태 스냅샷 저장
        const snapshotTime = Date.now();
        const snapshotStatus = member.presence?.status;

        setTimeout(async () => {
            try {
                // [17번] 예약 시점 vs 실행 시점 유저 상태 재검증
                const currentStatus = member.presence?.status;
                if (currentStatus === 'offline' && snapshotStatus !== 'offline') {
                    console.log(`[17번] 상태 변경 감지: ${member.id} - 로테이션 취소`);
                    return;
                }

                // [E번 수정] 5인 재확인도 동일 기준으로
                const currentPlayers = channel.members.filter(m => db[gId]?.[m.id]?.currentRole);
                if (currentPlayers.size >= 5) {
                    await executeRotation(channel);
                }
            } catch (e) {
                console.error('[executeRotation 오류]:', e.message);
            } finally {
                // [C번 수정] 예외 발생해도 반드시 잠금 해제
                activeLocks.delete(channel.id);
            }
        }, CONFIG.POTG_BUFFER);
    }
});

async function executeRotation(channel) {
    const gId = channel.guild.id;
    const guildConfig = getGuildConfig(gId);
    if (!guildConfig) return;

    const players = Array.from(channel.members.values())
        .filter(m => db[gId]?.[m.id]?.currentRole);

    if (players.length < 5) return;

    const now = Date.now();

    // 소셜 매트릭스 업데이트
    players.forEach(m1 => {
        const d1 = getDB(gId, m1.id);
        players.forEach(m2 => {
            if (m1.id !== m2.id) {
                const s = d1.social[m2.id] || { count: 0, lastPlayed: 0 };
                d1.social[m2.id] = { count: s.count + 1, lastPlayed: now };
            }
        });
    });
    saveDB(true);

    const otherRooms = guildConfig.GAME_ROOMS
        .filter(id => id !== channel.id)
        .map(id => channel.guild.channels.cache.get(id))
        .filter(r => r);

    const mover = players.sort(() => Math.random() - 0.5)[0];

    // [20번] 멤버 존재 여부 최종 확인
    if (!mover) return;
    if (!channel.guild.members.cache.has(mover.id)) {
        console.log(`[20번] 멤버 없음: ${mover.id} - 이동 건너뜀`);
        return;
    }

    for (const room of otherRooms) {
        const role = getDB(gId, mover.id).roles.find(
            r => getRoomComp(gId, room)[r] < CONFIG.LIMITS[r]
        );
        if (role) {
            db[gId][mover.id].currentRole = role;
            try {
                await mover.voice.setChannel(room.id);
            } catch (e) {
                console.error(`[이동 실패] ${mover.id}:`, e.message);
            }
            break;
        }
    }
}

// --- [ 명령어 처리 ] ---
client.on('messageCreate', async (msg) => {
    if (msg.author.bot) return;
    const gId = msg.guild?.id;
    if (!gId) return;

    const args = msg.content.trim().split(/\s+/);

    if (args[0] === '!역할변경') {
        const r = [];
        const i = args.slice(1).join('');
        if (i.includes('탱')) r.push('tank');
        if (i.includes('딜')) r.push('damage');
        if (i.includes('힐')) r.push('support');
        if (r.length) {
            getDB(gId, msg.author.id).roles = r;
            saveDB(true);
            msg.reply(`📝 역할 업데이트 완료: ${r.join(', ')}`);
        } else {
            msg.reply('⚠️ 사용법: `!역할변경 탱커`, `!역할변경 딜러힐러` 등');
        }
    }

    // [I번 수정] !랜덤 로직 수정: 인자 있으면 파싱, 없으면 DB 사용
    if (args[0] === '!랜덤') {
        if (!msg.member?.voice?.channel) {
            return msg.reply('⚠️ 먼저 음성 채널에 입장해주세요.');
        }
        let roles = null;
        if (args.length > 1) {
            roles = [];
            const i = args.slice(1).join('');
            if (i.includes('탱')) roles.push('tank');
            if (i.includes('딜')) roles.push('damage');
            if (i.includes('힐')) roles.push('support');
            if (!roles.length) return msg.reply('⚠️ 올바른 역할을 입력해주세요.');
        }
        msg.reply(await smartAssign(msg.member, roles));
    }
});

client.once('ready', () => {
    console.log(`[V7.0] ${client.user.tag} 온라인`);
    runGC(); // 시작 시 1회 GC
});

client.login(process.env.TOKEN);
