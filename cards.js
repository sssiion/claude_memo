// V8.5: 통계 카드 렌더링 모듈
// @napi-rs/canvas로 PNG Buffer 생성 → Discord에 첨부 이미지로 전송
const { createCanvas, GlobalFonts, loadImage } = require('@napi-rs/canvas');
const path = require('path');
const fs = require('fs');

// 폰트 등록
// fonts/ 폴더 안에서:
//   - 파일명에 'emoji' 포함 → EmojiFont 패밀리로 등록 (이모지용)
//   - 그 외 → AppFont 패밀리로 등록 (한글/영문)
// ctx.font에 두 패밀리를 chain으로 주면 글리프별 자동 fallback
const FONTS_DIR = path.join(__dirname, 'fonts');
let HAS_TEXT = false, HAS_EMOJI = false;
try {
    if (fs.existsSync(FONTS_DIR)) {
        const files = fs.readdirSync(FONTS_DIR).filter(f => /\.(ttf|otf)$/i.test(f));
        for (const f of files) {
            const fp = path.join(FONTS_DIR, f);
            if (/emoji/i.test(f)) {
                GlobalFonts.registerFromPath(fp, 'EmojiFont');
                HAS_EMOJI = true;
            } else {
                GlobalFonts.registerFromPath(fp, 'AppFont');
                HAS_TEXT = true;
            }
        }
        console.log(`[cards] 폰트 로드: text=${HAS_TEXT}, emoji=${HAS_EMOJI}`);
    }
} catch (e) {
    console.warn('[cards] 폰트 로드 실패, sans-serif 사용:', e.message);
}
const FONT_CHAIN = [
    HAS_TEXT ? 'AppFont' : null,
    HAS_EMOJI ? 'EmojiFont' : null,
    'sans-serif'
].filter(Boolean).join(', ');

const COLORS = {
    bg: '#0e1116',
    panel: '#1a1f29',
    panelAlt: '#252b38',
    border: '#2f3645',
    text: '#e6edf3',
    textDim: '#8b95a7',
    accent: '#5865f2',
    green: '#3ba55c',
    pink: '#eb459e',
    yellow: '#faa61a',
    red: '#ed4245',
    blue: '#5865f2',
    purple: '#9b59b6',
    cyan: '#1abc9c'
};

const ROLE_COLORS = { tank: '#3498db', damage: '#e74c3c', support: '#2ecc71' };
const ROLE_NAMES = { tank: '탱커', damage: '딜러', support: '힐러' };

const fmt = {
    duration(ms) {
        if (!ms || ms <= 0) return '0분';
        const min = Math.floor(ms / 60000);
        const h = Math.floor(min / 60);
        const m = min % 60;
        if (h === 0) return `${m}분`;
        if (m === 0) return `${h}시간`;
        return `${h}시간 ${m}분`;
    },
    hoursOnly(ms) {
        return ((ms || 0) / 3600000).toFixed(1) + 'h';
    },
    dateK(ts) {
        if (!ts) return '-';
        const d = new Date(ts);
        return `${d.getFullYear()}.${String(d.getMonth() + 1).padStart(2, '0')}.${String(d.getDate()).padStart(2, '0')}`;
    },
    relative(ts) {
        if (!ts) return '없음';
        const e = Date.now() - ts;
        const d = Math.floor(e / 86400000);
        if (d > 0) return `${d}일 전`;
        const h = Math.floor(e / 3600000);
        if (h > 0) return `${h}시간 전`;
        const m = Math.floor(e / 60000);
        if (m > 0) return `${m}분 전`;
        return '방금 전';
    },
    short(s, max) {
        if (!s) return '';
        return s.length > max ? s.slice(0, max - 1) + '…' : s;
    }
};

function roundRect(ctx, x, y, w, h, r, fillStyle, strokeStyle = null) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
    if (fillStyle) {
        ctx.fillStyle = fillStyle;
        ctx.fill();
    }
    if (strokeStyle) {
        ctx.strokeStyle = strokeStyle;
        ctx.lineWidth = 1;
        ctx.stroke();
    }
}

function setFont(ctx, size, weight = 'normal') {
    ctx.font = `${weight} ${size}px ${FONT_CHAIN}`;
}

function drawText(ctx, text, x, y, { size = 16, weight = 'normal', color = COLORS.text, align = 'left', baseline = 'top' } = {}) {
    setFont(ctx, size, weight);
    ctx.fillStyle = color;
    ctx.textAlign = align;
    ctx.textBaseline = baseline;
    ctx.fillText(text, x, y);
}

function drawBadge(ctx, label, x, y, color) {
    setFont(ctx, 12, 'bold');
    const padX = 10, h = 22;
    const w = ctx.measureText(label).width + padX * 2;
    roundRect(ctx, x, y, w, h, 11, color);
    drawText(ctx, label, x + w / 2, y + h / 2, { size: 12, weight: 'bold', color: '#fff', align: 'center', baseline: 'middle' });
    return w;
}

// ─── 라인 차트 (이중축) ───
function drawLineChart(ctx, x, y, w, h, datasets, xLabels, opts = {}) {
    const padL = 50, padR = datasets[1] ? 50 : 20, padT = 15, padB = 28;
    const chartX = x + padL, chartY = y + padT;
    const chartW = w - padL - padR, chartH = h - padT - padB;

    ctx.strokeStyle = COLORS.border;
    ctx.lineWidth = 1;
    const gridLines = 4;
    for (let i = 0; i <= gridLines; i++) {
        const gy = chartY + (chartH * i / gridLines);
        ctx.beginPath();
        ctx.moveTo(chartX, gy);
        ctx.lineTo(chartX + chartW, gy);
        ctx.stroke();
    }

    const ds0Max = Math.max(1, ...(datasets[0]?.values || [0]));
    const ds1Max = datasets[1] ? Math.max(1, ...datasets[1].values) : 1;

    setFont(ctx, 10);
    ctx.fillStyle = COLORS.textDim;
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= gridLines; i++) {
        const gy = chartY + (chartH * i / gridLines);
        const v = ds0Max * (1 - i / gridLines);
        ctx.fillText(v >= 1 ? Math.round(v).toString() : v.toFixed(1), chartX - 6, gy);
    }
    if (datasets[1]) {
        ctx.textAlign = 'left';
        for (let i = 0; i <= gridLines; i++) {
            const gy = chartY + (chartH * i / gridLines);
            const v = ds1Max * (1 - i / gridLines);
            ctx.fillText(v >= 1 ? Math.round(v).toString() : v.toFixed(1), chartX + chartW + 6, gy);
        }
    }

    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    const N = xLabels.length;
    const stepX = N > 1 ? chartW / (N - 1) : 0;
    const labelEvery = Math.max(1, Math.ceil(N / 8));
    for (let i = 0; i < N; i++) {
        if (i % labelEvery !== 0 && i !== N - 1) continue;
        ctx.fillText(xLabels[i], chartX + stepX * i, chartY + chartH + 5);
    }

    datasets.forEach((ds, idx) => {
        const max = idx === 0 ? ds0Max : ds1Max;
        ctx.strokeStyle = ds.color;
        ctx.lineWidth = 2.5;
        ctx.lineJoin = 'round';
        ctx.beginPath();
        ds.values.forEach((v, i) => {
            const px = chartX + stepX * i;
            const py = chartY + chartH * (1 - v / max);
            if (i === 0) ctx.moveTo(px, py);
            else ctx.lineTo(px, py);
        });
        ctx.stroke();
        ctx.fillStyle = ds.color;
        ds.values.forEach((v, i) => {
            const px = chartX + stepX * i;
            const py = chartY + chartH * (1 - v / max);
            ctx.beginPath();
            ctx.arc(px, py, 3, 0, Math.PI * 2);
            ctx.fill();
        });
    });

    const allZero = datasets.every(ds => ds.values.every(v => !v));
    if (allZero) {
        ctx.fillStyle = COLORS.textDim;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        setFont(ctx, 13);
        ctx.fillText(opts.emptyMsg || '아직 데이터 없음 — 오늘부터 기록 시작', chartX + chartW / 2, chartY + chartH / 2);
    }
}

// ─── 도넛 차트 ───
// segments: [{ label, value, color }]
function drawDonut(ctx, cx, cy, rOuter, rInner, segments) {
    const total = segments.reduce((s, x) => s + x.value, 0);
    if (total <= 0) {
        // 빈 도넛
        ctx.beginPath();
        ctx.arc(cx, cy, rOuter, 0, Math.PI * 2);
        ctx.arc(cx, cy, rInner, 0, Math.PI * 2, true);
        ctx.fillStyle = COLORS.panelAlt;
        ctx.fill();
        drawText(ctx, '데이터 없음', cx, cy, { size: 12, color: COLORS.textDim, align: 'center', baseline: 'middle' });
        return;
    }
    let start = -Math.PI / 2;
    for (const seg of segments) {
        const ang = (seg.value / total) * Math.PI * 2;
        ctx.beginPath();
        ctx.arc(cx, cy, rOuter, start, start + ang);
        ctx.arc(cx, cy, rInner, start + ang, start, true);
        ctx.closePath();
        ctx.fillStyle = seg.color;
        ctx.fill();
        start += ang;
    }
}

// ─── 막대 차트 ───
// values: [{ label, value, color }]
function drawBars(ctx, x, y, w, h, items, opts = {}) {
    const padL = 30, padR = 10, padT = 10, padB = 25;
    const chartX = x + padL, chartY = y + padT;
    const chartW = w - padL - padR, chartH = h - padT - padB;
    const max = Math.max(1, ...items.map(i => i.value));
    const barW = chartW / items.length * 0.7;
    const gap = chartW / items.length * 0.3;

    items.forEach((it, i) => {
        const bx = chartX + i * (barW + gap) + gap / 2;
        const bh = chartH * (it.value / max);
        const by = chartY + chartH - bh;
        roundRect(ctx, bx, by, barW, Math.max(bh, 2), 4, it.color);
        drawText(ctx, it.label, bx + barW / 2, chartY + chartH + 5, {
            size: 11, color: COLORS.textDim, align: 'center'
        });
        if (it.value > 0 && opts.showValue) {
            drawText(ctx, opts.formatValue ? opts.formatValue(it.value) : String(it.value),
                bx + barW / 2, by - 14, { size: 10, color: COLORS.text, align: 'center' });
        }
    });
}

// ─── 24시간 히트맵 ───
// hourly: { 0: ms, 1: ms, ... 23: ms }
function drawHourlyHeatmap(ctx, x, y, w, h, hourly) {
    const cellW = w / 24;
    const cellH = h - 18;
    const values = [];
    for (let i = 0; i < 24; i++) values.push(hourly[i] || hourly[String(i)] || 0);
    const max = Math.max(1, ...values);

    for (let i = 0; i < 24; i++) {
        const intensity = max > 0 ? values[i] / max : 0;
        // 0 → panelAlt, 1 → cyan
        const r = Math.round(37 + (26 - 37) * intensity);
        const g = Math.round(43 + (188 - 43) * intensity);
        const b = Math.round(56 + (156 - 56) * intensity);
        const cx = x + i * cellW + 2;
        roundRect(ctx, cx, y, cellW - 4, cellH, 4, `rgb(${r},${g},${b})`);
        if (i % 3 === 0) {
            drawText(ctx, String(i), cx + (cellW - 4) / 2, y + cellH + 2, {
                size: 10, color: COLORS.textDim, align: 'center'
            });
        }
    }
    // 21시 마지막 라벨
    drawText(ctx, '24', x + 24 * cellW - 6, y + cellH + 2, { size: 10, color: COLORS.textDim, align: 'right' });
}

// ─── 개인 통계 카드 (V8.5 확장) ───
async function renderPersonalCard(data) {
    const W = 1280, H = 1100;
    const canvas = createCanvas(W, H);
    const ctx = canvas.getContext('2d');

    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, W, H);

    // ━━ 헤더 패널 ━━
    roundRect(ctx, 30, 30, W - 60, 110, 16, COLORS.panel, COLORS.border);

    const avatarSize = 80;
    const avatarX = 50, avatarY = 45;
    if (data.avatarURL) {
        try {
            const img = await loadImage(data.avatarURL);
            ctx.save();
            ctx.beginPath();
            ctx.arc(avatarX + avatarSize / 2, avatarY + avatarSize / 2, avatarSize / 2, 0, Math.PI * 2);
            ctx.clip();
            ctx.drawImage(img, avatarX, avatarY, avatarSize, avatarSize);
            ctx.restore();
        } catch {
            roundRect(ctx, avatarX, avatarY, avatarSize, avatarSize, avatarSize / 2, COLORS.accent);
        }
    } else {
        roundRect(ctx, avatarX, avatarY, avatarSize, avatarSize, avatarSize / 2, COLORS.accent);
    }

    drawText(ctx, fmt.short(data.username, 22), 150, 50, { size: 28, weight: 'bold' });
    drawText(ctx, '👤 User Stats', 150, 86, { size: 14, color: COLORS.textDim });

    // 가능 역할 뱃지
    let bx = 150, by = 110;
    (data.roles || []).forEach(r => {
        const w = drawBadge(ctx, ROLE_NAMES[r] || r, bx, by, ROLE_COLORS[r] || COLORS.accent);
        bx += w + 5;
    });

    // 우측 정보
    drawText(ctx, '랭킹', W - 380, 45, { size: 12, color: COLORS.textDim });
    drawText(ctx, data.serverRank.rank ? `#${data.serverRank.rank} / ${data.serverRank.total}` : '미집계', W - 380, 62, { size: 16, weight: 'bold', color: COLORS.yellow });

    drawText(ctx, '현재 역할', W - 250, 45, { size: 12, color: COLORS.textDim });
    drawText(ctx, data.currentRole ? ROLE_NAMES[data.currentRole] : '-', W - 250, 62, {
        size: 16, weight: 'bold', color: data.currentRole ? ROLE_COLORS[data.currentRole] : COLORS.textDim
    });

    drawText(ctx, '마지막 활동', W - 130, 45, { size: 12, color: COLORS.textDim });
    drawText(ctx, fmt.relative(data.lastPlayed), W - 130, 62, { size: 16, weight: 'bold' });

    drawText(ctx, '첫 활동', W - 380, 100, { size: 12, color: COLORS.textDim });
    drawText(ctx, fmt.dateK(data.firstPlayedAt), W - 380, 117, { size: 14, weight: 'bold' });

    drawText(ctx, '연속 활동', W - 250, 100, { size: 12, color: COLORS.textDim });
    drawText(ctx, `${data.streak}일`, W - 250, 117, { size: 14, weight: 'bold', color: COLORS.cyan });

    drawText(ctx, '서버 가입', W - 130, 100, { size: 12, color: COLORS.textDim });
    drawText(ctx, fmt.dateK(data.joinedAt), W - 130, 117, { size: 14, weight: 'bold' });

    // ━━ 상단 메트릭 4패널 ━━
    const metricY = 165;
    const metricH = 100;
    const metricW = (W - 60 - 60) / 4;
    const metrics = [
        { label: '같이 한 사람', value: `${data.totalPartners}명`, color: COLORS.green },
        { label: '셔플 횟수', value: `${data.totalShuffles}회`, color: COLORS.pink },
        { label: '총 인게임 시간', value: fmt.duration(data.totalInGameMs), color: COLORS.yellow },
        { label: '남용 (당함/사용/소환)', value: `${data.undoCount}/${data.undoUseCount}/${data.summonCount}`, color: COLORS.purple }
    ];
    metrics.forEach((m, i) => {
        const mx = 30 + i * (metricW + 20);
        roundRect(ctx, mx, metricY, metricW, metricH, 12, COLORS.panel, COLORS.border);
        drawText(ctx, m.label, mx + 18, metricY + 22, { size: 13, color: COLORS.textDim });
        drawText(ctx, m.value, mx + 18, metricY + 50, { size: 24, weight: 'bold', color: m.color });
    });

    // ━━ Top 1 강조 + Top 2-5 리스트 ━━
    const topY = 290;
    const topH = 240;
    const halfW = (W - 60 - 20) / 2;

    // 좌측 - Top 1 큰 카드 + Top 2-5
    roundRect(ctx, 30, topY, halfW, topH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '🤝 베스트 파트너', 50, topY + 18, { size: 16, weight: 'bold' });

    const top = data.topPartners || [];
    if (top.length === 0) {
        drawText(ctx, '아직 같이 한 기록이 없습니다.', 50, topY + 60, { size: 14, color: COLORS.textDim });
    } else {
        // Top 1 강조
        const t1 = top[0];
        roundRect(ctx, 50, topY + 50, halfW - 40, 70, 10, COLORS.panelAlt);
        drawText(ctx, '👑 1위', 65, topY + 60, { size: 12, color: COLORS.yellow });
        drawText(ctx, fmt.short(t1.name, 20), 65, topY + 78, { size: 22, weight: 'bold' });
        drawText(ctx, `${fmt.duration(t1.durationMs)} / ${t1.count}회`, halfW - 10, topY + 90, {
            size: 14, color: COLORS.textDim, align: 'right', baseline: 'middle'
        });

        // Top 2-5
        top.slice(1, 5).forEach((p, i) => {
            const ly = topY + 135 + i * 24;
            drawText(ctx, `${i + 2}. ${fmt.short(p.name, 16)}`, 60, ly, { size: 14 });
            drawText(ctx, `${fmt.duration(p.durationMs)} / ${p.count}회`, halfW - 10, ly, {
                size: 13, color: COLORS.textDim, align: 'right'
            });
        });
    }

    // 우측 - 활동 요약 (4개 미니 메트릭)
    const rightX = 30 + halfW + 20;
    roundRect(ctx, rightX, topY, halfW, topH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '📊 활동 요약', rightX + 20, topY + 18, { size: 16, weight: 'bold' });

    const summary = data.activitySummary || { peakDayMs: 0, peakWeekday: null, last7DaysMs: 0, avgPerActiveDayMs: 0 };
    const wdNamesK = ['일', '월', '화', '수', '목', '금', '토'];
    const summaryItems = [
        { label: '최장 단일 일', value: fmt.duration(summary.peakDayMs), color: COLORS.yellow },
        { label: '가장 활발한 요일', value: summary.peakWeekday !== null ? `${wdNamesK[summary.peakWeekday]}요일` : '-', color: COLORS.cyan },
        { label: '최근 7일 합', value: fmt.duration(summary.last7DaysMs), color: COLORS.green },
        { label: '평균 (활동일 기준)', value: fmt.duration(summary.avgPerActiveDayMs), color: COLORS.purple }
    ];
    const itemH = 38;
    summaryItems.forEach((it, i) => {
        const ly = topY + 55 + i * itemH;
        drawText(ctx, it.label, rightX + 25, ly + 6, { size: 13, color: COLORS.textDim });
        drawText(ctx, it.value, rightX + halfW - 25, ly + 6, { size: 16, weight: 'bold', color: it.color, align: 'right' });
        if (i < summaryItems.length - 1) {
            ctx.strokeStyle = COLORS.border;
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(rightX + 20, ly + itemH);
            ctx.lineTo(rightX + halfW - 20, ly + itemH);
            ctx.stroke();
        }
    });

    // ━━ 14일 추이 차트 ━━
    const chartY = 555;
    const chartH = 200;
    roundRect(ctx, 30, chartY, W - 60, chartH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '📈 최근 14일 활동 추이', 50, chartY + 18, { size: 16, weight: 'bold' });
    drawText(ctx, '● 게임 시간(h)', W - 200, chartY + 22, { size: 12, color: COLORS.green });
    drawText(ctx, '● 셔플 횟수', W - 100, chartY + 22, { size: 12, color: COLORS.pink });

    const dailyList = data.daily || [];
    drawLineChart(
        ctx, 40, chartY + 45, W - 80, chartH - 60,
        [
            { color: COLORS.green, values: dailyList.map(d => (d.inGameMs || 0) / 3600000) },
            { color: COLORS.pink, values: dailyList.map(d => d.shuffleCount || 0) }
        ],
        dailyList.map(d => d.date.slice(5))
    );

    // ━━ 요일별 막대 + 시간대 히트맵 ━━
    const bottomY = 780;
    const bottomH = 240;

    // 좌: 요일별
    roundRect(ctx, 30, bottomY, halfW, bottomH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '📅 요일별 평균 게임 시간', 50, bottomY + 18, { size: 16, weight: 'bold' });
    const wdLabels = ['일', '월', '화', '수', '목', '금', '토'];
    drawBars(ctx, 40, bottomY + 50, halfW - 20, bottomH - 70,
        (data.weekdayBreakdown || [0, 0, 0, 0, 0, 0, 0]).map((v, i) => ({
            label: wdLabels[i],
            value: v,
            color: i === 0 || i === 6 ? COLORS.pink : COLORS.green
        })),
        { showValue: true, formatValue: fmt.hoursOnly }
    );

    // 우: 시간대 히트맵
    roundRect(ctx, rightX, bottomY, halfW, bottomH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '🕐 시간대별 활동 (0~24시)', rightX + 20, bottomY + 18, { size: 16, weight: 'bold' });
    drawHourlyHeatmap(ctx, rightX + 20, bottomY + 60, halfW - 40, bottomH - 100, data.hourly || {});
    drawText(ctx, `진한색 = 활동 많음`, rightX + 20, bottomY + bottomH - 30, { size: 11, color: COLORS.textDim });

    // ━━ 풋터 ━━
    drawText(ctx, `Generated ${fmt.dateK(Date.now())} · OW Bot Stats`, W / 2, H - 25, {
        size: 12, color: COLORS.textDim, align: 'center'
    });

    return canvas.encode('png');
}

// ─── 남용 통계 카드 ───
async function renderAbuseCard(data) {
    const W = 1280, H = 600;
    const canvas = createCanvas(W, H);
    const ctx = canvas.getContext('2d');

    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, W, H);

    roundRect(ctx, 30, 30, W - 60, 80, 16, COLORS.panel, COLORS.border);
    drawText(ctx, '📊 남용 통계', 50, 55, { size: 28, weight: 'bold' });
    drawText(ctx, fmt.short(data.guildName || '', 40), 50, 88, { size: 14, color: COLORS.textDim });
    drawText(ctx, fmt.dateK(Date.now()), W - 50, 60, { size: 14, color: COLORS.textDim, align: 'right' });

    const colW = (W - 60 - 40) / 3;
    const colY = 135;
    const colH = H - colY - 60;

    const sections = [
        { title: '↩️ 되돌리기 당함', color: COLORS.green, list: data.undoTop || [] },
        { title: '🔁 되돌리기 사용', color: COLORS.yellow, list: data.undoUseTop || [] },
        { title: '📢 소환', color: COLORS.pink, list: data.summonTop || [] }
    ];

    sections.forEach((sec, i) => {
        const cx = 30 + i * (colW + 20);
        roundRect(ctx, cx, colY, colW, colH, 12, COLORS.panel, COLORS.border);
        drawText(ctx, sec.title, cx + 20, colY + 18, { size: 18, weight: 'bold', color: sec.color });

        if (sec.list.length === 0) {
            drawText(ctx, '기록 없음', cx + 20, colY + 60, { size: 14, color: COLORS.textDim });
            return;
        }
        sec.list.slice(0, 5).forEach((item, idx) => {
            const ly = colY + 60 + idx * 50;
            drawText(ctx, `${idx + 1}`, cx + 20, ly + 6, { size: 22, weight: 'bold', color: COLORS.textDim });
            drawText(ctx, fmt.short(item.name, 16), cx + 55, ly, { size: 16, weight: 'bold' });
            drawText(ctx, `${item.count}회`, cx + 55, ly + 22, { size: 13, color: sec.color });
            drawText(ctx, fmt.dateK(item.lastAt), cx + colW - 20, ly + 22, { size: 12, color: COLORS.textDim, align: 'right' });
        });
    });

    drawText(ctx, `Generated ${fmt.dateK(Date.now())} · OW Bot`, W / 2, H - 25, {
        size: 12, color: COLORS.textDim, align: 'center'
    });

    return canvas.encode('png');
}

// ─── 서버 전체 통계 카드 ───
// data: { guildName, totalUsers, activeUsers7d, totalInGameMs, totalShuffles,
//         topPlayers: [{name, inGameMs, shuffleCount}], roleStats: {tank,damage,support},
//         hourly: {0..23}, dailyTotals: [{date, inGameMs, shuffleCount}] }
async function renderServerCard(data) {
    const W = 1280, H = 900;
    const canvas = createCanvas(W, H);
    const ctx = canvas.getContext('2d');

    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, W, H);

    // 헤더
    roundRect(ctx, 30, 30, W - 60, 90, 16, COLORS.panel, COLORS.border);
    drawText(ctx, `🏠 ${fmt.short(data.guildName || '서버', 30)} 통계`, 50, 50, { size: 28, weight: 'bold' });
    drawText(ctx, '서버 전체 활동 요약', 50, 90, { size: 14, color: COLORS.textDim });
    drawText(ctx, fmt.dateK(Date.now()), W - 50, 60, { size: 14, color: COLORS.textDim, align: 'right' });

    // 메트릭 4패널
    const metricY = 145;
    const metricH = 100;
    const metricW = (W - 60 - 60) / 4;
    const metrics = [
        { label: '활성 유저 (7일)', value: `${data.activeUsers7d}명`, color: COLORS.green },
        { label: '전체 등록 유저', value: `${data.totalUsers}명`, color: COLORS.blue },
        { label: '총 인게임 시간', value: fmt.duration(data.totalInGameMs), color: COLORS.yellow },
        { label: '총 셔플 횟수', value: `${data.totalShuffles}회`, color: COLORS.pink }
    ];
    metrics.forEach((m, i) => {
        const mx = 30 + i * (metricW + 20);
        roundRect(ctx, mx, metricY, metricW, metricH, 12, COLORS.panel, COLORS.border);
        drawText(ctx, m.label, mx + 18, metricY + 22, { size: 13, color: COLORS.textDim });
        drawText(ctx, m.value, mx + 18, metricY + 50, { size: 24, weight: 'bold', color: m.color });
    });

    // Top 10 활동 유저 (전체 너비)
    const midY = 270;
    const midH = 280;

    roundRect(ctx, 30, midY, W - 60, midH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '🏆 Top 10 활동 유저 (시간 기준)', 50, midY + 18, { size: 16, weight: 'bold' });
    const tops = data.topPlayers || [];
    if (tops.length === 0) {
        drawText(ctx, '기록 없음', 50, midY + 60, { size: 14, color: COLORS.textDim });
    } else {
        // 2열 5행으로 배치
        tops.slice(0, 10).forEach((p, i) => {
            const col = Math.floor(i / 5);
            const row = i % 5;
            const colX = 50 + col * ((W - 100) / 2);
            const ly = midY + 55 + row * 40;
            const trophy = i < 3 ? ['🥇', '🥈', '🥉'][i] : `${i + 1}`;
            drawText(ctx, trophy, colX, ly + 4, { size: 18, color: COLORS.yellow });
            drawText(ctx, fmt.short(p.name, 16), colX + 40, ly, { size: 15, weight: 'bold' });
            drawText(ctx, `${fmt.duration(p.inGameMs)} / ${p.shuffleCount}셔플`,
                colX + (W - 100) / 2 - 30, ly + 6, { size: 12, color: COLORS.textDim, align: 'right' });
        });
    }

    // 14일 추이
    const chartY = 575;
    const chartH = 180;
    roundRect(ctx, 30, chartY, W - 60, chartH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '📈 서버 전체 14일 추이', 50, chartY + 18, { size: 16, weight: 'bold' });
    drawText(ctx, '● 시간(h)', W - 180, chartY + 22, { size: 12, color: COLORS.green });
    drawText(ctx, '● 셔플', W - 100, chartY + 22, { size: 12, color: COLORS.pink });
    const dt = data.dailyTotals || [];
    drawLineChart(
        ctx, 40, chartY + 45, W - 80, chartH - 60,
        [
            { color: COLORS.green, values: dt.map(d => (d.inGameMs || 0) / 3600000) },
            { color: COLORS.pink, values: dt.map(d => d.shuffleCount || 0) }
        ],
        dt.map(d => d.date.slice(5))
    );

    // 시간대 히트맵
    const heatY = 775;
    const heatH = 90;
    roundRect(ctx, 30, heatY, W - 60, heatH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '🕐 인기 시간대', 50, heatY + 14, { size: 14, weight: 'bold' });
    drawHourlyHeatmap(ctx, 40, heatY + 35, W - 80, heatH - 20, data.hourly || {});

    drawText(ctx, `Generated ${fmt.dateK(Date.now())} · OW Bot`, W / 2, H - 18, {
        size: 12, color: COLORS.textDim, align: 'center'
    });

    return canvas.encode('png');
}

// ─── 셔플 분석 카드 ───
// data: { guildName, totalShuffles, totalSessions, mostShuffled: [{name,count}],
//         dailyShuffles: [{date,count}], avgShufflePerDay }
async function renderShuffleCard(data) {
    const W = 1280, H = 700;
    const canvas = createCanvas(W, H);
    const ctx = canvas.getContext('2d');

    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, W, H);

    roundRect(ctx, 30, 30, W - 60, 90, 16, COLORS.panel, COLORS.border);
    drawText(ctx, '🔀 셔플 분석', 50, 50, { size: 28, weight: 'bold' });
    drawText(ctx, fmt.short(data.guildName || '', 40), 50, 90, { size: 14, color: COLORS.textDim });
    drawText(ctx, fmt.dateK(Date.now()), W - 50, 60, { size: 14, color: COLORS.textDim, align: 'right' });

    // 메트릭
    const metricY = 145;
    const metricH = 100;
    const metricW = (W - 60 - 40) / 3;
    const metrics = [
        { label: '총 셔플 발생 수', value: `${data.totalShuffles}회`, color: COLORS.pink },
        { label: '일평균 셔플 수', value: `${data.avgShufflePerDay.toFixed(1)}회`, color: COLORS.yellow },
        { label: '셔플당한 사람 누적', value: `${data.totalSessions}회`, color: COLORS.cyan }
    ];
    metrics.forEach((m, i) => {
        const mx = 30 + i * (metricW + 20);
        roundRect(ctx, mx, metricY, metricW, metricH, 12, COLORS.panel, COLORS.border);
        drawText(ctx, m.label, mx + 18, metricY + 22, { size: 13, color: COLORS.textDim });
        drawText(ctx, m.value, mx + 18, metricY + 55, { size: 22, weight: 'bold', color: m.color });
    });

    // 가장 많이 셔플된 사람
    const midY = 270;
    const midH = 220;
    const halfW = (W - 60 - 20) / 2;

    roundRect(ctx, 30, midY, halfW, midH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '🌀 가장 자주 셔플된 사람', 50, midY + 18, { size: 16, weight: 'bold' });
    const ms = data.mostShuffled || [];
    if (ms.length === 0) {
        drawText(ctx, '기록 없음', 50, midY + 60, { size: 14, color: COLORS.textDim });
    } else {
        ms.slice(0, 5).forEach((p, i) => {
            const ly = midY + 60 + i * 30;
            drawText(ctx, `${i + 1}.`, 60, ly, { size: 14, weight: 'bold', color: COLORS.textDim });
            drawText(ctx, fmt.short(p.name, 18), 90, ly, { size: 14 });
            drawText(ctx, `${p.count}회`, halfW + 10, ly, {
                size: 13, color: COLORS.pink, align: 'right'
            });
        });
    }

    // 일별 셔플 차트
    const rightX = 30 + halfW + 20;
    roundRect(ctx, rightX, midY, halfW, midH, 12, COLORS.panel, COLORS.border);
    drawText(ctx, '📊 14일 셔플 발생', rightX + 20, midY + 18, { size: 16, weight: 'bold' });
    const ds = data.dailyShuffles || [];
    drawLineChart(
        ctx, rightX + 5, midY + 45, halfW - 10, midH - 60,
        [{ color: COLORS.pink, values: ds.map(d => d.count || 0) }],
        ds.map(d => d.date.slice(5))
    );

    // 풋터
    drawText(ctx, `Generated ${fmt.dateK(Date.now())} · OW Bot`, W / 2, H - 25, {
        size: 12, color: COLORS.textDim, align: 'center'
    });

    return canvas.encode('png');
}

module.exports = { renderPersonalCard, renderAbuseCard, renderServerCard, renderShuffleCard, fmt };
