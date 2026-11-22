/**
 * 连接监视器（st-conn-monitor）
 *
 * 解决的问题：点发送之后「一直没消息，不知道死活」。
 *
 * 原理：包装 window.fetch，拦截生成请求（/api/backends/ 各后端的 generate、/api/novelai/generate），
 * 用 TransformStream 旁路统计流式数据：首字节延迟（TTFB）、字节数、块数、近似字符数、
 * 最后一次收到数据的时间。页面上常驻一枚状态球：
 *   蓝色 = 已发出，等待服务器响应（超时变黄/红）
 *   绿色 = 流式接收中（数据在流 = 连接活着）
 *   黄色 = 短暂无新数据
 *   红色 = 长时间无数据，疑似断流 / 请求失败
 * 点击状态球展开面板：请求历史 + 告警阈值设置 + 链路自检。
 * 卡顿时可闪烁浏览器标签页标题（后台也看得见）。
 *
 * 不改动酒馆任何数据流：块只透传，读不读、怎么读由酒馆自己决定。
 */

import { extension_settings } from '../../../extensions.js';
import { saveSettingsDebounced, eventSource, event_types } from '../../../../script.js';

const TAG = '[连接监视]';
const VERSION = 'v1.13.1-2026-11-23';

// 防重复安装（扩展被重复 import 时直接退出，绝不能包两层）
if (window.__stConnMonitorInstalled) {
	console.warn(TAG, '已安装过，跳过本次加载');
} else {
	window.__stConnMonitorInstalled = true;
	boot();
}

function boot() {
	const origFetch = window.fetch.bind(window);

	// 拖动状态提到 boot 作用域：tick 里的越界兜底需要知道是否正在拖
	let drag = null;
	let lastClampCheck = 0;
	// 面板拖动（标题栏）
	let panelDrag = null;

	// ---------- 设置 ----------
	const DEFAULTS = {
		showIdle: true,         // 空闲时也显示状态球（灰色「空闲」），保证随时能看到它
		defaultsVersion: 2,     // 默认值版本：旧版曾把 showIdle:false 持久化，升级时强制恢复常显一次
		ttfbWarnSec: 15,        // 等待响应超过 N 秒 -> 黄色
		ttfbDeadSec: 45,        // 等待响应超过 N 秒 -> 红色（上游一直不回话）
		stallWarnSec: 10,       // 流式无新数据超过 N 秒 -> 黄色
		stallDeadSec: 30,       // 流式无新数据超过 N 秒 -> 红色（疑似断流）
		flashTitle: true,       // 卡顿时闪烁标签页标题
		pinNewMessageTop: true, // 生成结束后把视口停在最新消息开头（而非底部）
		posX: null,             // 状态球位置（null = 默认右下）
		posY: null,
		requestUsage: true,     // 给流式请求补 stream_options.include_usage，换取 token 用量
		autoRetry: true,        // 失败自动重试（仅网络错误 / 5xx / 429，且此前零字节）
		maxRetries: 2,          // 最多重试几次（不含首次）
		retryMaxMs: 300000,     // 重试总时长上限（毫秒），超过就放弃
		ctxLimit: 0,            // 上下文上限（0 = 跟随酒馆设置）
		panelX: null,           // 悬浮面板位置（null = 跟随状态球）
		panelY: null,
	};

	function getSettings() {
		if (!extension_settings.connMonitor) {
			extension_settings.connMonitor = {};
		}
		const s = extension_settings.connMonitor;
		// 一次性迁移：v1 把 showIdle:false 存进了设置文件，直接改默认值对老用户无效，
		// 必须按版本号强制覆盖一次，之后尊重用户在面板里的手动开关。
		if ((s.defaultsVersion ?? 0) < DEFAULTS.defaultsVersion) {
			s.showIdle = true;
			s.defaultsVersion = DEFAULTS.defaultsVersion;
			saveSettingsDebounced();
		}
		for (const [k, v] of Object.entries(DEFAULTS)) {
			if (s[k] === undefined) {
				s[k] = v;
			}
		}
		return s;
	}
	const settings = getSettings();

	// ---------- 请求记录 ----------
	const GEN_URL = (u) => (u.includes('/api/backends/') && u.includes('/generate')) || u.includes('/api/novelai/generate');

	// 成功请求也只留开头这么多字符，用于确认「这一帧到底回了什么」
	const HEAD_LIMIT = 1200;

	// 下游（酒馆）迟迟不消费时，最多先替它缓存这么多字节，超过就暂停读上游
	const BUFFER_LIMIT = 8 * 1024 * 1024;

	// ---------- 历史持久化：按角色卡存在 localStorage，刷新/切回角色还在 ----------
	const HIST_PREFIX = 'stcm-hist:';
	const HIST_MAX = 60;           // 每个角色最多留多少条（localStorage 只有 5MB，要留余量给多角色）
	const HIST_HEAD_KEEP = 600;    // 每条保留的「返回开头」字符数
	const HIST_BODY_KEEP = 1500;   // 失败详情原文保留字符数

	function histKeyFor(name) {
		return HIST_PREFIX + (name || 'default');
	}

	function currentCharName() {
		try {
			return globalThis.SillyTavern?.getContext?.()?.name2 || 'default';
		} catch { return 'default'; }
	}

	// 存盘时裁掉大字段，只保留能展示的部分
	function slimRec(rec) {
		const dur = rec.end ? Math.round(rec.end - rec.start) : 0;
		return {
			id: rec.id,
			short: rec.short,
			method: rec.method,
			startWall: rec.startWall instanceof Date ? rec.startWall.toISOString() : rec.startWall,
			dur,
			ttfb: Math.round(rec.ttfb || 0),
			status: rec.status,
			state: rec.state,
			bytes: rec.bytes,
			chunks: rec.chunks,
			tokens: rec.tokens,
			note: rec.note || '',
			model: rec.model || '',
			attempts: (rec.attempts || []).slice(0, 6).map((a) => ({ n: a.n, status: a.status || 0, error: a.error || '', dur: a.dur || 0 })),
			retryLog: String(rec.retryLog || '').slice(0, 400),
			finishReason: rec.finishReason || '',
			promptTokens: rec.promptTokens || 0,
			completionTokens: rec.completionTokens || 0,
			head: String(rec.head || '').slice(0, HIST_HEAD_KEEP),
			errDetail: rec.errDetail ? {
				status: rec.errDetail.status,
				statusText: rec.errDetail.statusText || '',
				errorCode: rec.errDetail.errorCode || '',
				message: String(rec.errDetail.message || '').slice(0, 300),
				body: String(rec.errDetail.body || '').slice(0, HIST_BODY_KEEP),
			} : null,
		};
	}

	// 读回来时把「相对时长」还原成 performance.now 基准的时间点，展示逻辑不用改
	function reviveRec(o) {
		const dur = o.dur || 0;
		const start = performance.now() - dur;
		const startWall = o.startWall ? new Date(o.startWall) : new Date(Date.now() - dur);
		if (o.model) { o.model = decodeJsonString(o.model); }
		return Object.assign({}, o, {
			startWall,
			start,
			end: start + dur,
			lastData: start,
			buffered: 0,
			backpressure: false,
			scanTail: '',
			contentChars: 0,
			argChars: 0,
			fromStorage: true,
		});
	}

	// 酒馆设置的上下文上限（ctx.maxContext 会给到 8192 这类默认值，不准，
	// 真实值在设置里的 oai_settings.openai_max_context，启动时异步取一次）
	let tavernCtxLimit = 0;
	async function loadCtxLimit() {
		try {
			const headers = typeof globalThis.getRequestHeaders === 'function'
				? globalThis.getRequestHeaders()
				: { 'Content-Type': 'application/json' };
			const r = await origFetch('/api/settings/get', { method: 'POST', headers, body: '{}' });
			const j = await r.json();
			const v = Number(j?.oai_settings?.openai_max_context);
			if (Number.isFinite(v) && v > 0) {
				tavernCtxLimit = v;
				renderPanel();
			}
		} catch { /* 拿不到就显示「上限未知」 */ }
	}

	// 给流式请求补 stream_options.include_usage：官方兼容端点只有带上它才会在流末尾返回
	// prompt_tokens / completion_tokens。只处理普通对象 body，Request 对象或非 JSON 一律不动。
	// 顺便把请求里的模型名带出来（响应里通常也有，但部分中转不给）
	function maybeRequestUsage(args) {
		let model = '';
		try {
			const cfg = args[1];
			if (!cfg || typeof cfg.body !== 'string') { return model; }
			const body = JSON.parse(cfg.body);
			if (!body) { return model; }
			if (body.model) { model = String(body.model); }
			if (!settings.requestUsage || body.stream !== true) { return model; }
			if (body.stream_options && body.stream_options.include_usage === true) { return model; }
			body.stream_options = Object.assign({}, body.stream_options, { include_usage: true });
			args[1] = Object.assign({}, cfg, { body: JSON.stringify(body) });
		} catch { /* 非 JSON 或结构异常：原样发送 */ }
		return model;
	}

	// ---------- 失败自动重试 ----------
	// 只对「确定没有产出」的失败重试：网络错误 / 5xx / 429，且此时尚未向下游吐过任何字节。
	// 这样重放不会让酒馆看到重复内容，也不会误伤「已经流了一半」的响应。
	const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504, 520, 522, 524]);
	const isRetryableStatus = (code) => RETRYABLE_STATUS.has(Number(code));

	// 退避：1s → 3s → 6s，带 ±20% 抖动，避免多标签页同时重试撞在一起
	function retryBackoffMs(attempt) {
		const base = [1000, 3000, 6000][Math.min(attempt - 1, 2)];
		return Math.round(base * (0.8 + Math.random() * 0.4));
	}

	// 等待期间让状态球显示「重试 n/m · 等待 Ns」，用户点停止则立刻中断
	function waitBeforeRetry(rec, attempt, sig, deadline) {
		let ms = retryBackoffMs(attempt);
		const left = deadline - performance.now();
		if (left <= 0) { return Promise.resolve(); }
		ms = Math.min(ms, left);

		rec.state = 'retrying';
		rec.retryUntil = performance.now() + ms;
		rec.retryMax = Math.max(1, Number(settings.maxRetries) || 1);   // 总重试次数（不含首次）
		renderPanel();
		tick();

		return new Promise((resolve) => {
			const timer = setTimeout(done, ms);
			function done() {
				clearTimeout(timer);
				if (sig) { sig.removeEventListener('abort', done); }
				rec.retryUntil = 0;
				// 关键：复位状态。否则重发后的整个等待/生成期间都停在 replaying 分支，
				// 因为 retryUntil 已清零，界面会永远显示「重试 n/m · 等待 0s」，看着就像不读秒。
				rec.state = 'wait';
				resolve();
			}
			if (sig) { sig.addEventListener('abort', done, { once: true }); }
		});
	}

	// 带重试的请求：返回最终响应；彻底失败时抛出最后一个错误（交给外层 catch 记录）
	// 读错误体补详情（重试与最终失败共用）
	async function collectErrDetail(rec, resp) {
		let raw = '';
		try { raw = String(await resp.clone().text()); } catch { raw = ''; }
		let errorCode = '';
		let message = '';
		let parsed = null;
		try { parsed = JSON.parse(raw); } catch { /* 非 JSON，例如 Caddy 的 HTML 502 页 */ }
		if (parsed) {
			errorCode = String(parsed.code || (parsed.error && parsed.error.code) || '');
			message = parsed.message
				|| (parsed.error && parsed.error.message)
				|| (typeof parsed.error === 'string' ? parsed.error : '')
				|| '';
		}
		if (!message) {
			const t = String(raw).trim();
			if (/^\s*<(!doctype|html|head|body|pre|div)/i.test(t)) {
				message = t
					.replace(/<script[\s\S]*?<\/script>/gi, ' ')
					.replace(/<style[\s\S]*?<\/style>/gi, ' ')
					.replace(/<[^>]+>/g, ' ')
					.replace(/\s+/g, ' ')
					.trim() || `HTTP ${resp.status}`;
			} else {
				message = t.length > 300 ? t.slice(0, 300) : t;
			}
		}
		rec.errDetail = {
			status: resp.status,
			statusText: resp.statusText || '',
			errorCode,
			message: String(message).slice(0, 600),
			body: String(raw).slice(0, 4000),
		};
		if (errorCode) { rec.note = `HTTP ${resp.status} · ${errorCode}`; }
		return errorCode;
	}

	// 收尾一条「失败尝试」：独立成条，这样它的详情不会因为后续重试而丢失
	async function closeFailedAttempt(rec, resp, attemptNo, inherited) {
		rec.attemptNo = attemptNo;
		if (resp) {
			rec.status = resp.status;
			rec.state = 'error';
			rec.note = `HTTP ${resp.status}`;
			await collectErrDetail(rec, resp);
		} else {
			rec.state = 'error';
		}
		if (inherited && !rec.model) { rec.model = inherited; }
		finish(rec);
	}

	// 带重试的请求：返回 { resp, rec }。每次失败尝试各自成条，成功那次单独成条。
	async function fetchWithRetry(args, firstRec, url, method, baseModel) {
		const extra = Math.max(0, Number(settings.maxRetries) || 0);
		const maxAttempts = settings.autoRetry ? 1 + extra : 1;
		const deadline = performance.now() + Math.max(1000, Number(settings.retryMaxMs) || 300000);
		const sig = (args[1] && args[1].signal) || null;
		const chainId = firstRec.id;
		let rec = firstRec;

		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			rec.attemptNo = attempt;
			rec.retryMax = Math.max(1, extra);
			rec.retryChain = chainId;
			if (baseModel && !rec.model) { rec.model = baseModel; }
			if (sig && sig.aborted) {
				throw new DOMException('Aborted', 'AbortError');
			}

			try {
				const resp = await origFetch(...args);
				if (!rec.ttfb) { rec.ttfb = performance.now() - rec.start; }
				rec.status = resp.status;

				const canRetry = attempt < maxAttempts
					&& isRetryableStatus(resp.status)
					&& performance.now() < deadline;

				if (!canRetry) { return { resp, rec }; }

				// 这一条失败：独立归档（含完整错误详情），再新开一条继续重试
				await closeFailedAttempt(rec, resp, attempt, baseModel);
				rec = beginRecord(url, method);
				rec.attemptNo = attempt + 1;
				rec.retryChain = chainId;
				rec.retryMax = Math.max(1, extra);
				if (baseModel) { rec.model = baseModel; }
				await waitBeforeRetry(rec, attempt, sig, deadline);
				continue;
			} catch (err) {
				if (err && err.name === 'AbortError') { throw err; }
				if (attempt >= maxAttempts || performance.now() >= deadline) { throw err; }

				rec.state = 'error';
				rec.note = `网络错误：${String((err && err.message) || err).slice(0, 80)}`;
				await closeFailedAttempt(rec, null, attempt, baseModel);
				rec = beginRecord(url, method);
				rec.attemptNo = attempt + 1;
				rec.retryChain = chainId;
				rec.retryMax = Math.max(1, extra);
				if (baseModel) { rec.model = baseModel; }
				await waitBeforeRetry(rec, attempt, sig, deadline);
				continue;
			}
		}
		return { resp: await origFetch(...args), rec };
	}

	// 把重试动作写进记录的 note，方便事后回看
	function appendRetryLog(rec, line) {
		rec.retryLog = rec.retryLog ? `${rec.retryLog}；${line}` : line;
	}

	// 把正则抠出来的 JSON 字符串内容还原（处理 \uXXXX、\n 等转义）
	function decodeJsonString(raw) {
		if (!raw || !raw.includes('\\')) { return raw; }
		try {
			return JSON.parse(`"${raw}"`);
		} catch {
			return raw;
		}
	}

	// 从流数据里扫出结束原因、token 用量、正文长度（轻量正则，不 JSON.parse 每一块）
	function scanChunk(rec, txt) {
		if (!txt) { return; }
		// finish_reason / usage 可能被拆在相邻两块里，拼上上一块的尾巴再匹配
		const buf = rec.scanTail + txt;
		rec.scanTail = buf.slice(-200);

		let m = /"finish_reason"\s*:\s*"([^"]+)"/.exec(buf);
		if (m) { rec.finishReason = m[1]; }
		m = /"prompt_tokens"\s*:\s*(\d+)/.exec(buf);
		if (m) { rec.promptTokens = Number(m[1]); }
		m = /"completion_tokens"\s*:\s*(\d+)/.exec(buf);
		if (m) { rec.completionTokens = Number(m[1]); }
		m = /"model"\s*:\s*"([^"]+)"/.exec(buf);
		if (m && m[1]) {
			// JSON 里中文常被写成 \uXXXX 转义，直接取原文会显示成「\u6297\u622a\u65ad」，
			// 借 JSON.parse 还原成正常文字
			rec.model = decodeJsonString(m[1]);
		}

		// 正文长度只统计当前块，避免和 scanTail 重复计数
		let cm;
		// ① OpenAI 流式：choices[].delta.content
		const reContent = /"content"\s*:\s*"((?:[^"\\]|\\.)*)"/g;
		while ((cm = reContent.exec(txt))) { rec.contentChars += cm[1].length; }
		// ② tool_calls 的 arguments（破限预设常把正文放这里）
		const reArgs = /"arguments"\s*:\s*"((?:[^"\\]|\\.)*)"/g;
		while ((cm = reArgs.exec(txt))) { rec.argChars = (rec.argChars || 0) + cm[1].length; }
		// ③ Gemini 原生：candidates[].content.parts[].text
		const reText = /"text"\s*:\s*"((?:[^"\\]|\\.)*)"/g;
		while ((cm = reText.exec(txt))) { rec.contentChars += cm[1].length; }

		// Gemini 的收尾字段：finishReason 大写枚举 + usageMetadata 的 token 统计
		m = /"finishReason"\s*:\s*"([A-Z_]+)"/.exec(buf);
		if (m && m[1] && GEMINI_FINISH[m[1]]) { rec.finishReason = GEMINI_FINISH[m[1]]; }
		m = /"promptTokenCount"\s*:\s*(\d+)/.exec(buf);
		if (m) { rec.promptTokens = Number(m[1]); }
		m = /"candidatesTokenCount"\s*:\s*(\d+)/.exec(buf);
		if (m) { rec.completionTokens = Number(m[1]); }
	}

	const activeMap = new Map();   // id -> record（进行中）
	const history = [];            // 已结束，新的在前，上限 40
	let uid = 0;
	let lastFinished = null;       // 最近结束的一条（状态球短暂展示）

	function shortName(url) {
		const m = url.match(/\/api\/([^?#]+)/);
		let s = m ? m[1] : url;
		s = s.replace(/^backends\//, '').replace(/\/generate$/, '/generate');
		return s.length > 34 ? s.slice(0, 33) + '…' : s;
	}

	function beginRecord(url, method) {
		const rec = {
			id: ++uid,
			url,
			short: shortName(url),
			method,
			startWall: new Date(),
			start: performance.now(),
			ttfb: 0,
			status: 0,
			state: 'wait',        // wait | receiving | done | error | aborted
			bytes: 0,
			chunks: 0,
			lastData: 0,          // perf.now，收到数据时更新
			end: 0,
			note: '',
			errDetail: null,      // 失败详情：{ status, statusText, errorCode, message, body }
			head: '',             // 响应开头（最多 HEAD_LIMIT 字符），成功时也能点开看
			tokens: 0,            // 生成结束后用酒馆 tokenizer 数出来的真实 token 数
			buffered: 0,          // 已从上游读到、但下游还没取走的字节数（内存保护用）
			backpressure: false,  // 下游是否没在积极消费（true=界面滞后，false=下游在等数据）
			attempts: [],         // 每次失败尝试的记录 [{n,status|error,dur}]，用于展示重试过程
			attempt: 1,           // 当前第几次尝试
			retryUntil: 0,        // 重试等待的结束时刻（performance.now 基准）
			retryMax: 1,          // 本次允许的最大尝试次数
			model: '',            // 本次请求的模型名（优先取响应里给的，其次请求体）
			finishReason: '',     // 上游给的结束原因：stop / length / content_filter / tool_calls
			promptTokens: 0,      // 本次请求的输入 token（来自响应 usage）
			completionTokens: 0,  // 本次输出 token
			contentChars: 0,      // 从流里累计到的正文长度（content 字段）
			argChars: 0,          // tool_calls 的 arguments 长度（破限预设把正文放这儿）
			scanTail: '',         // 上一块尾部 200 字，用于跨块匹配 finish_reason / usage
		};
		rec.lastData = rec.start;
		activeMap.set(rec.id, rec);
		renderPanel();
		return rec;
	}

	function finish(rec) {
		if (rec.end) {
			return;
		}
		rec.end = performance.now();
		// 空响应：HTTP 成功、但确实没有任何产出。要同时排除这些「有内容」的形态：
		// 正文 content、tool_calls 的 arguments（破限预设常用）、输出 token > 0、结束原因为工具调用
		if (rec.state === 'done') {
			const wroteSomething = (rec.contentChars || 0) > 0
				|| (rec.argChars || 0) > 0
				|| (rec.completionTokens || 0) > 0
				|| rec.finishReason === 'tool_calls'
				|| rec.finishReason === 'function_call'
				// 兜底：任何响应形式（含将来新增的格式）只要体积够大就一定是有产出的，
				// 真正的「空响应」通常只有几百字节的元数据
				|| (rec.bytes || 0) > 4096;
			if (!wroteSomething) {
				rec.state = 'empty';
				rec.note = rec.finishReason ? `空响应 · ${rec.finishReason}` : '空响应';
			}
		}
		if (rec.head && rec.head.length > HEAD_LIMIT) {
			rec.head = rec.head.slice(0, HEAD_LIMIT);
		}
		if (activeMap.has(rec.id)) {
			activeMap.delete(rec.id);
		}
		history.unshift(rec);
		if (history.length > 40) {
			history.length = 40;
		}
		rec.buffered = 0;
		lastFinished = rec;
		saveHistorySoon();
		if (rec.state === 'done') {
			// 响应里给了精确的输出 token 就直接用；否则才回退到酒馆分词器估算
			if (rec.completionTokens > 0) {
				rec.tokens = rec.completionTokens;
				rec.tokensApprox = false;
			} else {
				scheduleTokenCount(rec);
			}
		}
		renderPanel();
	}

	// ---------- fetch 包装 ----------
	window.fetch = async function (...args) {
		const input = args[0];
		const url = typeof input === 'string' ? input : (input && input.url) || '';
		const method = String((args[1] && args[1].method) || (input && input.method) || 'GET').toUpperCase();

		if (!GEN_URL(url)) {
			return origFetch(...args);
		}

		// 官方兼容端点默认不在流里返回 token 用量，补一个 stream_options 才能拿到 prompt_tokens
		// （上下文占用条要用）。改不动就原样发，绝不影响请求。
		const reqModel = maybeRequestUsage(args);

		let rec = beginRecord(url, method);
		if (reqModel) { rec.model = reqModel; }

		try {
			const attemptResult = await fetchWithRetry(args, rec, url, method, reqModel);
			rec = attemptResult.rec;
			const resp = attemptResult.resp;

			if (!resp.ok) {
				rec.state = 'error';
				rec.note = `HTTP ${resp.status}`;
				finish(rec);

				// 异步补详情：先 clone 出独立流，绝不阻塞酒馆读取原响应
				let errStream = null;
				try {
					errStream = resp.clone();
				} catch { /* clone 失败就算了 */ }
				if (errStream) {
					(async () => {
						try {
							const raw = await errStream.text();
							let errorCode = '';
							let message = '';
							let parsed = null;
							try {
								parsed = JSON.parse(raw);
							} catch { /* 非 JSON，例如 Caddy 的 HTML 502 页 */ }
							if (parsed) {
								errorCode = String(parsed.code || (parsed.error && parsed.error.code) || '');
								message = parsed.message
									|| (parsed.error && parsed.error.message)
									|| (typeof parsed.error === 'string' ? parsed.error : '')
									|| '';
							}
							if (!message) {
								const s = String(raw).trim();
								// 非 JSON（Caddy/Express 的 HTML 错误页）：剥掉标签只留可读文字，
								// 否则整页 <!DOCTYPE html> 会糊满「原因」那一栏
								if (/^\s*<(!doctype|html|head|body|pre|div)/i.test(s)) {
									const plain = s
										.replace(/<script[\s\S]*?<\/script>/gi, ' ')
										.replace(/<style[\s\S]*?<\/style>/gi, ' ')
										.replace(/<[^>]+>/g, ' ')
										.replace(/\s+/g, ' ')
										.trim();
									message = plain || `HTTP ${resp.status}`;
								} else {
									message = s.length > 300 ? s.slice(0, 300) : s;
								}
							}
							rec.errDetail = {
								status: resp.status,
								statusText: resp.statusText || '',
								errorCode,
								message: String(message).slice(0, 600),
								body: String(raw).slice(0, 4000),
							};
							if (errorCode) {
								rec.note = `HTTP ${resp.status} · ${errorCode}`;
							}
							renderPanel();
						} catch {
							rec.errDetail = {
								status: resp.status,
								statusText: resp.statusText || '',
								errorCode: '',
								message: '（读取错误响应失败）',
								body: '',
							};
							renderPanel();
						}
					})();
				}
				return resp;
			}

			if (!resp.body || typeof resp.body.getReader !== 'function') {
				// 拿不到流（理论少见），只记录 TTFB
				rec.state = 'done';
				finish(rec);
				return resp;
			}

			const dec = new TextDecoder('utf-8');
			const reader = resp.body.getReader();

			// 主动泵流：由扩展自己不断读上游，而不是等下游（酒馆）来 read 才动。
			//
			// 为什么不能用 pull 驱动：pull 只在消费者调用 read() 时才触发。酒馆在后台标签页
			// 时渲染被浏览器节流、读得慢甚至不读，pull 就不会被调用 → lastData 不更新 →
			// 面板误报「长时间无数据·疑似断流」，而实际上数据一直堆在缓冲区里等着。
			// （实测：后台 10 秒仍能收到 40/40 块，流读取本身不受节流影响。）
			//
			// 现在改为：上游数据一到就记录（时间戳真实反映上游），再进队列等下游按需取；
			// 队列积压超过上限才暂停读上游，此时面板显示「本地积压 · 界面滞后」而非断流。
			// 主动推送模式：扩展自己不断读上游，直接用 controller.enqueue 推给下游。
			// 不用 pull 驱动 —— pull 返回的 Promise resolve 后，若流内部队列已满，
			// 浏览器不会继续调用 pull，会出现「队列有数据、下游却取不到」的死锁（实测复现）。
			let upstreamDone = false;
			const pumped = new ReadableStream({
				start(controller) {
					(async () => {
						try {
							while (true) {
								// 内存保护：下游长期不消费时暂停读上游（单位是「块」，够用且简单）
								while (controller.desiredSize !== null && controller.desiredSize < -2000) {
									rec.backpressure = true;
									await new Promise((r) => setTimeout(r, 100));
								}
								const { done, value } = await reader.read();
								if (done) {
									upstreamDone = true;
									if (!rec.end) {
										rec.state = rec.status >= 200 && rec.status < 300 ? 'done' : 'error';
										finish(rec);
									}
									controller.close();
									return;
								}
								rec.bytes += value.byteLength || 0;
								rec.chunks += 1;
								rec.lastData = performance.now();   // 上游真实数据到达时间
								try {
									const txt = dec.decode(value, { stream: true });
									if (txt) {
										scanChunk(rec, txt);   // 抓 finish_reason / usage / 正文长度
										// 只留开头一小段：够看出这一帧是不是正常内容，又不占内存
										if (rec.head.length < HEAD_LIMIT) {
											rec.head += txt;
										}
									}
								} catch { /* 解码失败不影响计数 */ }
								controller.enqueue(value);
								// desiredSize <= 0 表示下游没在积极消费（后台标签页渲染被节流）
								const ds = controller.desiredSize;
								rec.backpressure = ds !== null && ds <= 0;
							}
						} catch (err) {
							if (!rec.end) {
								rec.state = 'aborted';
								rec.note = '读取中断';
								finish(rec);
							}
							try { controller.error(err); } catch { /* ignore */ }
						}
					})();
				},
				cancel(reason) {
					try { reader.cancel(reason); } catch { /* ignore */ }
					if (!rec.end) {
						rec.state = 'aborted';
						rec.note = '读取被取消';
						finish(rec);
					}
				},
			});

			const wrapped = new Response(pumped, {
				status: resp.status,
				statusText: resp.statusText,
				headers: resp.headers,
			});

			rec.state = 'receiving';
			return wrapped;
		} catch (err) {
			if (!rec.ttfb) {
				rec.ttfb = performance.now() - rec.start;
			}
			const aborted = err && (err.name === 'AbortError' || String(err).includes('abort'));
			rec.state = aborted ? 'aborted' : 'error';
			rec.note = aborted ? '已取消' : String((err && err.message) || err).slice(0, 120);
			finish(rec);
			throw err;
		}
	};

	// ---------- 事件兜底：即使流统计失效也能收尾 ----------
	eventSource.on(event_types.GENERATION_ENDED, () => {
		for (const rec of [...activeMap.values()]) {
			if (rec.state === 'receiving' || rec.state === 'wait') {
				rec.state = rec.status >= 200 && rec.status < 300 ? 'done' : 'error';
				finish(rec);
			}
		}
	});

	// ---------- 生成结束后：视口停在最新消息开头 ----------
	// 酒馆每次消息更新都会调 scrollChatToBottom（直接滚到底），用户想回看这层楼的开头得自己滚。
	// 这里完全由开关决定，不去猜流式/非流式 —— 「假流式」（一次返回却分块发送）靠事件判断不了。
	eventSource.on(event_types.GENERATION_ENDED, () => {
		if (!settings.pinNewMessageTop) { return; }
		pinLastMessageTop();
	});

	function pinLastMessageTop() {
		const chat = document.getElementById('chat');
		if (!chat) { return; }

		let interrupted = false;
		const onUser = () => { interrupted = true; };
		// 用户只要一滚就立刻停手，不跟人抢滚动条
		chat.addEventListener('wheel', onUser, { passive: true });
		chat.addEventListener('touchstart', onUser, { passive: true });
		const cleanup = () => {
			chat.removeEventListener('wheel', onUser);
			chat.removeEventListener('touchstart', onUser);
		};

		const started = performance.now();
		const fix = () => {
			if (interrupted) { cleanup(); return; }
			const mes = chat.querySelectorAll('.mes');
			if (mes.length) {
				const last = mes[mes.length - 1];
				const lr = last.getBoundingClientRect();
				const cr = chat.getBoundingClientRect();
				// 用 getBoundingClientRect 差值算，不受 offsetParent 影响
				const target = chat.scrollTop + (lr.top - cr.top) - 10;
				if (Math.abs(chat.scrollTop - target) > 2) {
					chat.scrollTop = target;
				}
			}
			// 600ms 内每帧纠正：酒馆自己的滚动可能在这段时间里再执行一次
			if (performance.now() - started < 600) {
				requestAnimationFrame(fix);
			} else {
				cleanup();
			}
		};
		requestAnimationFrame(fix);
	}
	eventSource.on(event_types.GENERATION_STOPPED, () => {
		for (const rec of [...activeMap.values()]) {
			if (rec.state !== 'done' && rec.state !== 'error') {
				rec.state = 'aborted';
				rec.note = rec.note || '手动停止';
				finish(rec);
			}
		}
	});

	// ---------- 工具 ----------
	function fmtBytes(n) {
		if (n < 1024) { return `${n} B`; }
		if (n < 1024 * 1024) { return `${(n / 1024).toFixed(1)} KB`; }
		return `${(n / 1024 / 1024).toFixed(2)} MB`;
	}
	function fmtSec(ms) {
		return ms >= 10000 ? `${(ms / 1000).toFixed(0)}s` : `${(ms / 1000).toFixed(1)}s`;
	}
	// 速度数值格式化：<100 留一位小数，>=100 取整，>=1000 用 k
	function fmtRate(n) {
		if (!Number.isFinite(n) || n <= 0) { return '—'; }
		if (n >= 1000) { return `${(n / 1000).toFixed(1)}k`; }
		if (n >= 100) { return String(Math.round(n)); }
		return n.toFixed(1);
	}

	function fmtClock(d) {
		const p = (x) => String(x).padStart(2, '0');
		return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
	}

	function escHtml(s) {
		return String(s)
			.replace(/&/g, '&amp;')
			.replace(/</g, '&lt;')
			.replace(/>/g, '&gt;')
			.replace(/"/g, '&quot;');
	}

	// 展开中的失败行 id（面板每 400ms 全量重绘，状态必须存在这里才不会被刷掉）
	const expandedIds = new Set();

	function hasErrDetail(rec) {
		const d = rec && rec.errDetail;
		return !!(d && (d.body || d.message || d.errorCode || d.status));
	}

	// 可展开的记录：失败的看错误详情，成功/停止的看耗时与返回开头
	function canShowDetail(rec) {
		if (!rec || !rec.end) { return false; }
		if (hasErrDetail(rec)) { return true; }
		return rec.state === 'done' || rec.state === 'aborted' || rec.state === 'empty';
	}

	function findRecordById(id) {
		if (activeMap.has(id)) { return activeMap.get(id); }
		return history.find((r) => r.id === id) || null;
	}

	// ---------- token 速度 ----------
	// 动态 import，拿不到 tokenizer 也不能拖累扩展加载
	let tokenCounter = null;   // null=未加载 / false=不可用 / function=可用
	async function getTokenizer() {
		if (tokenCounter === null) {
			try {
				const mod = await import('../../../tokenizers.js');
				tokenCounter = typeof mod.getTokenCountAsync === 'function' ? mod.getTokenCountAsync : false;
			} catch {
				tokenCounter = false;
			}
		}
		return tokenCounter;
	}

	function lastAiMessageText() {
		try {
			const chat = globalThis.SillyTavern?.getContext?.()?.chat || [];
			for (let i = chat.length - 1; i >= 0 && i >= chat.length - 3; i--) {
				const m = chat[i];
				if (m && !m.is_user && m.mes) { return String(m.mes); }
			}
		} catch { /* ignore */ }
		return '';
	}

	// 生成结束后稍等片刻（等酒馆把消息写进 chat），再数 token
	function scheduleTokenCount(rec) {
		setTimeout(async () => {
			if (rec.tokens > 0) { return; }
			try {
				const text = lastAiMessageText();
				if (!text) { return; }
				const counter = await getTokenizer();
				if (!counter) { return; }
				const n = await counter(text, 0);
				// 合理性检查：中文 1 token 至少 2 字节，数出的 token 超过响应字节数说明
				// 数到的是整条消息（续写场景），不是本次返回，宁可不用也不显示假数据
				if (n > 0 && n <= rec.bytes) {
					rec.tokens = n;
					rec.tokensApprox = false;
					renderPanel();
				}
			} catch { /* 失败就不显示 token 速度，不猜 */ }
		}, 500);
	}
	const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

	// ---------- 状态球 ----------
	let pill, dot, text, panel, drawer;

	function buildUI() {
		const css = `
/* 颜色全部引用酒馆主题变量（带兜底），跟随页面主题自适应 */
#stcm-pill{position:fixed;z-index:60000;display:none;align-items:center;gap:8px;padding:7px 13px 7px 10px;
 border-radius:999px;background:var(--SmartThemeBlurTintColor,rgba(20,22,28,.88));color:var(--SmartThemeBodyColor,#e8eaf0);font-size:13px;line-height:1;
 box-shadow:0 4px 16px rgba(0,0,0,.25);
 cursor:pointer;user-select:none;-webkit-user-select:none;backdrop-filter:blur(6px);border:1px solid var(--SmartThemeBorderColor,rgba(255,255,255,.12));touch-action:none;max-width:calc(100vw - 16px)}
#stcm-pill .stcm-dot{width:10px;height:10px;border-radius:50%;background:var(--SmartThemeBorderColor,#5b6270);flex:0 0 auto;transition:background .2s}
#stcm-pill.stcm-flow .stcm-dot{background:#2ecc71;box-shadow:0 0 8px rgba(46,204,113,.8)}
#stcm-pill.stcm-wait .stcm-dot{background:#4da3ff}
#stcm-pill.stcm-warn .stcm-dot{background:#f5a623;box-shadow:0 0 8px rgba(245,166,35,.7)}
#stcm-pill.stcm-dead .stcm-dot{background:#ff4d4f;box-shadow:0 0 10px rgba(255,77,79,.9);animation:stcm-blink .8s infinite}
#stcm-pill.stcm-done .stcm-dot{background:#2ecc71}
#stcm-pill.stcm-abort .stcm-dot{background:color-mix(in srgb,var(--SmartThemeBodyColor,#888) 45%,transparent)}
@keyframes stcm-blink{0%,100%{opacity:1}50%{opacity:.25}}
#stcm-panel{position:fixed;z-index:60001;display:none;flex-direction:column;width:430px;max-width:94vw;max-height:70vh;overflow:hidden;
 background-color:#16180f;background-image:linear-gradient(var(--SmartThemeBlurTintColor,rgba(24,26,32,.96)),var(--SmartThemeBlurTintColor,rgba(24,26,32,.96)));color:var(--SmartThemeBodyColor,#e8eaf0);border-radius:12px;border:1px solid var(--SmartThemeBorderColor,rgba(255,255,255,.12));
 box-shadow:0 8px 32px rgba(0,0,0,.3);font-size:13px;backdrop-filter:blur(16px) saturate(1.15);-webkit-backdrop-filter:blur(16px) saturate(1.15)}
#stcm-panel h3{margin:0;padding:12px 14px 8px;font-size:14px;display:flex;justify-content:space-between;align-items:center;flex:0 0 auto;
 cursor:move;user-select:none;-webkit-user-select:none;touch-action:none}
#stcm-panel h3 .stcm-x{cursor:pointer;opacity:.6;padding:2px 6px;border-radius:6px}
#stcm-panel h3 .stcm-x:hover{opacity:1;background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 10%,transparent)}
#stcm-panel .stcm-sec{padding:8px 14px;border-top:1px solid color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 8%,transparent);flex:0 0 auto}
#stcm-panel .stcm-sec.stcm-hist{flex:1 1 auto;min-height:96px;overflow-y:auto;overscroll-behavior:contain}
#stcm-panel .stcm-hist::-webkit-scrollbar{width:8px}
#stcm-panel .stcm-hist::-webkit-scrollbar-thumb{background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 18%,transparent);border-radius:4px}
#stcm-panel .stcm-hist::-webkit-scrollbar-thumb:hover{background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 30%,transparent)}
#stcm-panel .stcm-hist::-webkit-scrollbar-track{background:transparent}
#stcm-panel .stcm-hint{opacity:.62;font-size:12px;line-height:1.6;margin:4px 0 6px}
#stcm-panel .stcm-ctx{display:flex;align-items:center;gap:8px;padding:0 14px 8px;font-size:11.5px;flex:0 0 auto}
#stcm-panel .stcm-ctxlabel{opacity:.6;white-space:nowrap}
#stcm-panel .stcm-ctxbar{flex:1;height:6px;border-radius:3px;overflow:hidden;
 background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 12%,transparent)}
#stcm-panel .stcm-ctxfill{height:100%;width:0;background:#2ecc71;border-radius:3px;transition:width .25s ease,background .25s ease}
#stcm-panel .stcm-ctxfill.stcm-ctx-warn{background:#f5a623}
#stcm-panel .stcm-ctxfill.stcm-ctx-dead{background:#ff4d4f}
#stcm-panel .stcm-ctxtxt{opacity:.75;white-space:nowrap;font-variant-numeric:tabular-nums}
#stcm-panel label{display:flex;align-items:center;gap:6px;margin:6px 0;opacity:.85}
#stcm-panel input[type=number]{width:64px;background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 8%,transparent);
 border:1px solid color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 15%,transparent);
 color:inherit;border-radius:6px;padding:3px 6px;font-size:12px}
#stcm-panel button{background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 10%,transparent);border:1px solid color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 18%,transparent);color:inherit;
 border-radius:6px;padding:4px 10px;font-size:12px;cursor:pointer}
#stcm-panel button:hover{background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 18%,transparent)}
#stcm-panel .stcm-row{display:flex;justify-content:space-between;gap:10px;padding:6px 0;border-bottom:1px solid color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 6%,transparent)}
#stcm-panel .stcm-row:last-child{border-bottom:none}
#stcm-panel .stcm-r1{display:flex;align-items:center;gap:7px;min-width:0}
#stcm-panel .stcm-r1 .stcm-dot{width:8px;height:8px;border-radius:50%;flex:0 0 auto}
#stcm-panel .stcm-name{font-family:var(--monoFontFamily,ui-monospace,Menlo,monospace);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#stcm-panel .stcm-sub{opacity:.62;font-size:11.5px;margin-top:2px}
#stcm-panel .stcm-time{opacity:.62;font-size:11.5px;white-space:nowrap}
/* ---------- 失败行展开的详情 ---------- */
#stcm-panel .stcm-row.stcm-expandable{cursor:pointer;border-radius:6px}
#stcm-panel .stcm-row.stcm-expandable:hover{background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 6%,transparent)}
#stcm-panel .stcm-caret{display:inline-block;width:13px;opacity:.55;font-size:10px;margin-right:2px}
#stcm-panel .stcm-detail{margin:3px 0 9px 15px;padding:8px 10px;border-radius:8px;
 background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 5%,transparent);
 border:1px solid color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 10%,transparent);
 font-size:11.5px;line-height:1.65}
#stcm-panel .stcm-kv{display:flex;gap:10px;margin:0;padding:1px 0;align-items:baseline;line-height:1.45}
#stcm-panel .stcm-kv>span{opacity:.55;flex:0 0 auto;min-width:56px;white-space:nowrap}
#stcm-panel .stcm-kv>b{font-weight:500;word-break:break-word;min-width:0}
#stcm-panel .stcm-raw{margin:7px 0 0;padding:7px 9px;max-height:148px;overflow:auto;
 background:color-mix(in srgb,var(--SmartThemeBodyColor,#000) 16%,transparent);border-radius:6px;
 font-family:var(--monoFontFamily,ui-monospace,Menlo,monospace);font-size:11px;line-height:1.5;
 white-space:pre-wrap;word-break:break-word}
#stcm-panel .stcm-headlabel{opacity:.6;margin:9px 0 5px}
#stcm-panel .stcm-loglink{display:inline-block;margin-top:7px;font-size:11px;opacity:.65;
 color:inherit;text-decoration:underline}
#stcm-panel .stcm-loglink:hover{opacity:1}
#stcm-selftest{margin-top:6px;opacity:.62;font-size:12px}
/* ---------- 扩展设置页（插件页）里的区块：沿用酒馆原生 .inline-drawer 折叠 ---------- */
#stcm-drawer{margin-bottom:5px;color:inherit}
#stcm-drawer.inline-drawer > .inline-drawer-toggle.inline-drawer-header{margin-bottom:0}
#stcm-drawer > .inline-drawer-content{padding-top:5px}
/* 酒馆没给 content 写 display:none，必须自加，否则首次渲染就是摊开状态 */
#stcm-drawer .inline-drawer-content{display:none;padding-bottom:8px}
#stcm-drawer .stcmd-head{display:flex;align-items:baseline;gap:8px}
#stcm-drawer .stcmd-head b{font-size:inherit;font-weight:700}
#stcm-drawer .stcmd-ver{font-size:11.5px;opacity:.55;font-weight:400}
#stcm-drawer .stcmd-state{display:flex;align-items:center;gap:7px;margin:7px 0 9px;font-size:12.5px;opacity:.85}
#stcm-drawer .stcmd-dot{width:9px;height:9px;border-radius:50%;flex:0 0 auto;
 background:color-mix(in srgb,var(--SmartThemeBodyColor,#888) 40%,transparent);transition:background .2s}
#stcm-drawer label{display:flex;align-items:center;gap:6px;margin:5px 0;font-size:12.5px;opacity:.9}
#stcm-drawer input[type=number]{width:62px;background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 8%,transparent);
 border:1px solid color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 15%,transparent);
 color:inherit;border-radius:6px;padding:2px 6px;font-size:12px}
#stcm-drawer .stcmd-btns{display:flex;gap:8px;flex-wrap:wrap;margin-top:9px}
#stcm-drawer button{background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 10%,transparent);
 border:1px solid color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 18%,transparent);color:inherit;
 border-radius:6px;padding:4px 10px;font-size:12px;cursor:pointer}
#stcm-drawer button:hover{background:color-mix(in srgb,var(--SmartThemeBodyColor,#fff) 18%,transparent)}
#stcm-drawer .stcmd-hint{margin-top:8px;font-size:11.5px;line-height:1.6;opacity:.6}
#stcm-drawer .stcmd-selftest{margin-top:7px;font-size:12px;opacity:.75}
`;
		const style = document.createElement('style');
		style.id = 'stcm-style';
		style.textContent = css;
		document.head.appendChild(style);

		pill = document.createElement('div');
		pill.id = 'stcm-pill';
		pill.innerHTML = '<span class="stcm-dot"></span><span class="stcm-text"></span>';
		dot = pill.querySelector('.stcm-dot');
		text = pill.querySelector('.stcm-text');
		document.body.appendChild(pill);

		panel = document.createElement('div');
		panel.id = 'stcm-panel';
		panel.innerHTML = `
<h3><span>连接监视器</span><span class="stcm-x" title="关闭">✕</span></h3>
<div class="stcm-ctx" id="stcm-ctx" title="本次请求的输入 token / 上限">
	<span class="stcm-ctxlabel">上下文</span>
	<div class="stcm-ctxbar"><div class="stcm-ctxfill" id="stcm-ctxfill"></div></div>
	<span class="stcm-ctxtxt" id="stcm-ctxtxt">—</span>
</div>
<div class="stcm-sec stcm-hist">
	<div class="stcm-hint">绿＝流入　黄＝卡顿　红＝断流/失败</div>
	<div id="stcm-history"></div>
</div>
<div class="stcm-sec">
	<label>流式无新数据 ≥ <input type="number" min="2" step="1" id="stcm-stall-warn"> 秒 → 黄色告警</label>
	<label>流式无新数据 ≥ <input type="number" min="5" step="1" id="stcm-stall-dead"> 秒 → 红色疑似断流</label>
	<label>等待响应 ≥ <input type="number" min="3" step="1" id="stcm-ttfb-warn"> 秒 → 黄色告警</label>
	<label>等待响应 ≥ <input type="number" min="10" step="1" id="stcm-ttfb-dead"> 秒 → 红色疑似无响应</label>
	<label><input type="checkbox" id="stcm-flash"> 卡顿时闪烁标签页标题（后台可见）</label>
	<label><input type="checkbox" id="stcm-idle"> 空闲时也显示状态球</label>
	<label><input type="checkbox" id="stcm-pin-top"> 生成结束后停在最新消息开头（不自动置底）</label>
	<label><input type="checkbox" id="stcm-auto-retry"> 失败自动重试（网络错误 / 5xx / 429，且未收到任何数据）</label>
	<label>最多重试 <input type="number" min="0" max="5" step="1" id="stcm-max-retry"> 次</label>
	<label><input type="checkbox" id="stcm-usage"> 请求流式 token 用量（用于上下文条，默认开）</label>
	<label>上下文上限 <input type="number" min="0" step="1000" id="stcm-ctx-limit" placeholder="0=跟随酒馆"> token</label>
	<div style="display:flex;gap:8px;margin-top:8px;flex-wrap:wrap">
		<button id="stcm-clear-hist">清除本角色历史</button>
		<button id="stcm-reset-pos">重置状态球位置</button>
		<button id="stcm-selftest-btn">链路自检（页面→酒馆）</button>
		<span id="stcm-selftest"></span>
	</div>
</div>`;
		document.body.appendChild(panel);

		const bindNum = (root, sel, key, min) => {
			root.querySelector(sel).addEventListener('change', (e) => {
				let v = parseInt(e.target.value, 10);
				if (!Number.isFinite(v) || v < min) { v = min; }
				settings[key] = v;
				syncControls();
				saveSettingsDebounced();
			});
		};
		const bindCheck = (root, sel, key) => {
			root.querySelector(sel).addEventListener('change', (e) => {
				settings[key] = e.target.checked;
				syncControls();
				saveSettingsDebounced();
			});
		};
		bindNum(panel, '#stcm-stall-warn', 'stallWarnSec', 2);
		bindNum(panel, '#stcm-stall-dead', 'stallDeadSec', 5);
		bindNum(panel, '#stcm-ttfb-warn', 'ttfbWarnSec', 3);
		bindNum(panel, '#stcm-ttfb-dead', 'ttfbDeadSec', 10);
		bindCheck(panel, '#stcm-flash', 'flashTitle');
		bindCheck(panel, '#stcm-idle', 'showIdle');
		bindCheck(panel, '#stcm-pin-top', 'pinNewMessageTop');
		bindCheck(panel, '#stcm-auto-retry', 'autoRetry');
		bindNum(panel, '#stcm-max-retry', 'maxRetries', 0);
		bindCheck(panel, '#stcm-usage', 'requestUsage');
		bindNum(panel, '#stcm-ctx-limit', 'ctxLimit', 0);
		panel.querySelector('#stcm-clear-hist').addEventListener('click', () => {
			if (!confirm('清除当前角色保存的请求历史？（只删本机记录，不影响聊天）')) { return; }
			history.length = 0;
			try { localStorage.removeItem(histKeyFor(currentCharName())); } catch { /* ignore */ }
			renderHistory();
			updateContextBar();
		});
		panel.querySelector('#stcm-reset-pos').addEventListener('click', resetPillPos);
		panel.querySelector('#stcm-selftest-btn').addEventListener('click', () => runSelfTest('#stcm-selftest'));
		panel.querySelector('.stcm-x').addEventListener('click', () => {
			panel.style.display = 'none';
		});

		// ---------- 面板拖动（按住标题栏拖；✕ 按钮除外） ----------
		const panelHead = panel.querySelector('h3');
		panelHead.addEventListener('pointerdown', (e) => {
			if (e.button !== 0 || e.target.closest('.stcm-x')) { return; }
			const rect = panel.getBoundingClientRect();
			panelDrag = { sx: e.clientX, sy: e.clientY, ox: rect.left, oy: rect.top };
			try { panelHead.setPointerCapture(e.pointerId); } catch { /* ignore */ }
			e.preventDefault();
		});
		panelHead.addEventListener('pointermove', (e) => {
			if (!panelDrag) { return; }
			const rect = panel.getBoundingClientRect();
			// 拖动中就钳制：面板贴边等，不会跑到屏幕外（与状态球同一套逻辑）
			const p = clampToViewport(panelDrag.ox + (e.clientX - panelDrag.sx), panelDrag.oy + (e.clientY - panelDrag.sy), rect.width, rect.height);
			panel.style.left = `${p.x}px`;
			panel.style.top = `${p.y}px`;
		});
		const endPanelDrag = (e) => {
			if (!panelDrag) { return; }
			panelDrag = null;
			try { panelHead.releasePointerCapture(e.pointerId); } catch { /* ignore */ }
			const rect = panel.getBoundingClientRect();
			// 松手按实际位置再钳一次，写回元素并保存
			const p = clampToViewport(rect.left, rect.top, rect.width, rect.height);
			panel.style.left = `${p.x}px`;
			panel.style.top = `${p.y}px`;
			settings.panelX = p.x;
			settings.panelY = p.y;
			saveSettingsDebounced();
		};
		panelHead.addEventListener('pointerup', endPanelDrag);
		panelHead.addEventListener('pointercancel', endPanelDrag);

		buildDrawer();       // 挂到「扩展设置」页
		syncControls();      // 两处控件的初值统一写入

		// 拖动 + 点击
		pill.addEventListener('pointerdown', (e) => {
			if (e.button !== 0) { return; }
			const rect = pill.getBoundingClientRect();
			drag = { sx: e.clientX, sy: e.clientY, ox: rect.left, oy: rect.top, moved: false };
			try { pill.setPointerCapture(e.pointerId); } catch { /* 合成事件/无活动指针时忽略 */ }
		});
		pill.addEventListener('pointermove', (e) => {
			if (!drag) { return; }
			const dx = e.clientX - drag.sx;
			const dy = e.clientY - drag.sy;
			if (Math.abs(dx) + Math.abs(dy) > 5) {
				drag.moved = true;
				const rect = pill.getBoundingClientRect();
				// 拖动过程中就钳制：手指拖出去，球贴在边上等，不会跑到屏幕外
				const p = clampToViewport(drag.ox + dx, drag.oy + dy, rect.width, rect.height);
				pill.style.left = `${p.x}px`;
				pill.style.top = `${p.y}px`;
				pill.style.right = 'auto';
				pill.style.bottom = 'auto';
			}
		});
		const endDrag = (e) => {
			if (!drag) { return; }
			const moved = drag.moved;
			drag = null;
			try { pill.releasePointerCapture(e.pointerId); } catch { /* ignore */ }
			if (!moved) {
				panel.style.display = panel.style.display === 'flex' ? 'none' : 'flex';
				positionPanel();
				renderPanel();
				return;
			}
			// 松手时按实际 DOM 位置再钳一次，并把结果写回元素本身
			// （旧版只写了 settings、没写 style，所以球留在屏幕外，只有刷新才回来）
			const rect = pill.getBoundingClientRect();
			const p = clampToViewport(rect.left, rect.top, rect.width, rect.height);
			pill.style.left = `${p.x}px`;
			pill.style.top = `${p.y}px`;
			pill.style.right = 'auto';
			pill.style.bottom = 'auto';
			settings.posX = p.x;
			settings.posY = p.y;
			saveSettingsDebounced();
		};
		pill.addEventListener('pointerup', endDrag);
		pill.addEventListener('pointercancel', (e) => {
			// 手机上来电、系统手势打断：同样收尾并归位，别把球丢在外面
			if (!drag) { return; }
			drag.moved = true;
			endDrag(e);
		});

		applyPillPos();
		window.addEventListener('resize', () => clampPillPos());
		window.addEventListener('orientationchange', () => setTimeout(clampPillPos, 300));
		if (window.visualViewport) {
			// 手机软键盘弹出 / 地址栏收起时 innerHeight 不一定变，但 visualViewport 会变
			window.visualViewport.addEventListener('resize', () => clampPillPos());
		}
		document.addEventListener('visibilitychange', () => {
			if (!document.hidden) { clampPillPos(); }
		});
		// 面板已拖过位时，窗口/视口变化也要把面板拉回可视区
		const clampPanelPos = () => {
			if (!panel || panel.style.display !== 'flex') { return; }
			if (!Number.isFinite(settings.panelX) || !Number.isFinite(settings.panelY)) { return; }
			if (panelDrag) { return; }
			const r = panel.getBoundingClientRect();
			const p = clampToViewport(r.left, r.top, r.width, r.height);
			panel.style.left = `${p.x}px`;
			panel.style.top = `${p.y}px`;
		};
		window.addEventListener('resize', clampPanelPos);
		window.addEventListener('orientationchange', () => setTimeout(clampPanelPos, 300));
		if (window.visualViewport) {
			window.visualViewport.addEventListener('resize', clampPanelPos);
		}
		document.addEventListener('visibilitychange', () => {
			if (!document.hidden) { clampPanelPos(); }
		});
		// 计时器放 Web Worker 里：页面失焦/切到后台时浏览器会把主线程的 setInterval
		// 强制节流（最狠压到 1 次/分钟），导致计时不动、断流告警延迟甚至误报。
		// Worker 的定时器不受页面生命周期节流，由它每 400ms 通知主线程跑一次 tick。
		let tickWorker = null;
		function startTicker() {
			try {
				const src = 'let id=null;self.onmessage=(e)=>{if(e.data==="start"){clearInterval(id);id=setInterval(()=>self.postMessage(1),400);}else if(e.data==="stop"){clearInterval(id);id=null;}};';
				tickWorker = new Worker(URL.createObjectURL(new Blob([src], { type: 'text/javascript' })));
				tickWorker.onmessage = () => tick();
				tickWorker.postMessage('start');
			} catch (err) {
				// Worker 不可用（CSP / 老浏览器）→ 回退普通定时器
				console.warn(TAG, 'Worker 计时器不可用，回退 setInterval:', err);
				setInterval(tick, 400);
			}
		}
		startTicker();

		// 历史持久化：启动时载入当前角色的记录；切换角色时先存旧的再载新的
		loadHistoryForCurrentChar();
		eventSource.on(event_types.CHAT_CHANGED, () => {
			const nextKey = histKeyFor(currentCharName());
			if (nextKey === activeHistKey) { return; }   // 同一角色的不同聊天共享历史
			saveHistoryNow(activeHistKey);
			loadHistoryForCurrentChar();
		});

		// 回到前台/窗口重新获得焦点时，立刻刷新一次状态（并校正位置）
		window.addEventListener('focus', () => { clampPillPos(); clampPanelPos(); tick(); });
		document.addEventListener('visibilitychange', () => {
			if (!document.hidden) { tick(); }
		});
		tick();
	}

	// 存下来的坐标是否可用（酒馆持久化后可能变成 undefined，不能只判 !== null）
	function hasSavedPos() {
		return Number.isFinite(settings.posX) && Number.isFinite(settings.posY);
	}

	// 可见区域的真实尺寸：手机上取 innerWidth/innerHeight 与 visualViewport 的较小值，
	// 避免软键盘弹出时把球算到键盘底下（酒馆页面里 visualViewport 偶发返回异常大值，故取 min）
	function viewportSize() {
		let w = window.innerWidth || document.documentElement.clientWidth || 0;
		let h = window.innerHeight || document.documentElement.clientHeight || 0;
		const vv = window.visualViewport;
		if (vv && vv.width > 0 && vv.height > 0) {
			w = Math.min(w, Math.round(vv.width));
			h = Math.min(h, Math.round(vv.height));
		}
		return { w, h };
	}

	// 把 (x,y) 处、宽 w 高 h 的矩形夹进可视区，四周留 4px
	function clampToViewport(x, y, w, h) {
		const vp = viewportSize();
		const bw = w || 120;
		const bh = h || 30;
		const maxX = Math.max(4, vp.w - bw - 4);
		const maxY = Math.max(4, vp.h - bh - 4);
		return {
			x: Math.min(Math.max(4, x), maxX),
			y: Math.min(Math.max(4, y), maxY),
		};
	}

	function applyPillPos() {
		pill.style.right = 'auto';
		pill.style.bottom = 'auto';
		if (hasSavedPos()) {
			pill.style.left = `${settings.posX}px`;
			pill.style.top = `${settings.posY}px`;
			clampPillPos();
		} else {
			pill.style.left = '';
			pill.style.top = '';
			pill.style.right = '18px';
			pill.style.bottom = '110px';
		}
	}

	// 把状态球拉回可视区域：换窗口大小 / 换设备（Windows→手机）/ 软键盘弹出后旧坐标可能跑到屏幕外
	function clampPillPos() {
		if (!pill) { return; }
		const rect = pill.getBoundingClientRect();
		const w = rect.width || 120;
		const h = rect.height || 30;
		const x = hasSavedPos() ? settings.posX : rect.left;
		const y = hasSavedPos() ? settings.posY : rect.top;
		const p = clampToViewport(x, y, w, h);
		if (p.x !== x || p.y !== y) {
			settings.posX = p.x;
			settings.posY = p.y;
			pill.style.left = `${p.x}px`;
			pill.style.top = `${p.y}px`;
			pill.style.right = 'auto';
			pill.style.bottom = 'auto';
			saveSettingsDebounced();
		}
	}

	// 廉价越界检查：不碰 DOM，只比对设置的坐标与当前视口
	function isPillOffscreen() {
		if (!hasSavedPos()) { return false; }
		const vp = viewportSize();
		return settings.posX < 0 || settings.posY < 0
			|| settings.posX > vp.w - 12 || settings.posY > vp.h - 12;
	}

	function resetPillPos() {
		settings.posX = null;
		settings.posY = null;
		saveSettingsDebounced();
		applyPillPos();
	}

	// 面板与「扩展设置」页的控件保持同步（两处都能改，改完互相刷新）
	function syncControls() {
		const set = (root, sel, val, isCheck) => {
			const el = root && root.querySelector(sel);
			if (!el) { return; }
			if (isCheck) { el.checked = !!val; } else { el.value = val; }
		};
		for (const root of [panel, drawer]) {
			if (!root) { continue; }
			set(root, '#stcm-stall-warn, [data-k="stallWarnSec"]', settings.stallWarnSec);
			set(root, '#stcm-stall-dead, [data-k="stallDeadSec"]', settings.stallDeadSec);
			set(root, '#stcm-ttfb-warn, [data-k="ttfbWarnSec"]', settings.ttfbWarnSec);
			set(root, '#stcm-ttfb-dead, [data-k="ttfbDeadSec"]', settings.ttfbDeadSec);
			set(root, '#stcm-flash, [data-k="flashTitle"]', settings.flashTitle, true);
			set(root, '#stcm-idle, [data-k="showIdle"]', settings.showIdle, true);
			set(root, '#stcm-pin-top, [data-k="pinNewMessageTop"]', settings.pinNewMessageTop, true);
			set(root, '#stcm-auto-retry, [data-k="autoRetry"]', settings.autoRetry, true);
			set(root, '#stcm-max-retry, [data-k="maxRetries"]', settings.maxRetries);
			set(root, '#stcm-usage, [data-k="requestUsage"]', settings.requestUsage, true);
			set(root, '#stcm-ctx-limit, [data-k="ctxLimit"]', settings.ctxLimit);
		}
	}

	// ---------- 「扩展设置」页里的区块 ----------
	function buildDrawer() {
		const host = document.getElementById('extensions_settings2') || document.getElementById('extensions_settings');
		if (!host) {
			console.warn(TAG, '未找到扩展设置容器，跳过设置页挂载');
			return;
		}
		if (host.querySelector('#stcm-drawer')) { return; }

		drawer = document.createElement('div');
		drawer.id = 'stcm-drawer';
		drawer.className = 'inline-drawer';
		drawer.innerHTML = `
<div class="inline-drawer-toggle inline-drawer-header">
	<div class="stcmd-head"><b>连接监视器</b><span class="stcmd-ver">${VERSION}</span></div>
	<div class="inline-drawer-icon fa-solid fa-circle-chevron-down down"></div>
</div>
<div class="inline-drawer-content">
	<div class="stcmd-state"><span class="stcmd-dot"></span><span class="stcmd-text">空闲</span></div>
	<label>流式无新数据 ≥ <input type="number" min="2" step="1" data-k="stallWarnSec"> 秒 → 黄色告警</label>
	<label>流式无新数据 ≥ <input type="number" min="5" step="1" data-k="stallDeadSec"> 秒 → 红色疑似断流</label>
	<label>等待响应 ≥ <input type="number" min="3" step="1" data-k="ttfbWarnSec"> 秒 → 黄色告警</label>
	<label>等待响应 ≥ <input type="number" min="10" step="1" data-k="ttfbDeadSec"> 秒 → 红色疑似无响应</label>
	<label><input type="checkbox" data-k="flashTitle"> 卡顿时闪烁标签页标题（后台可见）</label>
	<label><input type="checkbox" data-k="showIdle"> 空闲时也显示状态球</label>
	<label><input type="checkbox" data-k="pinNewMessageTop"> 生成结束后停在最新消息开头（不自动置底）</label>
	<label><input type="checkbox" data-k="autoRetry"> 失败自动重试（网络错误 / 5xx / 429）</label>
	<label>最多重试 <input type="number" min="0" max="5" step="1" data-k="maxRetries"> 次</label>
	<label><input type="checkbox" data-k="requestUsage"> 请求流式 token 用量（上下文条）</label>
	<label>上下文上限 <input type="number" min="0" step="1000" data-k="ctxLimit" placeholder="0=跟随酒馆"> token</label>
	<div class="stcmd-btns">
		<button class="stcmd-reset">状态球归位</button>
		<button class="stcmd-test">链路自检（页面→酒馆）</button>
	</div>
	<div class="stcmd-selftest"></div>
	<div class="stcmd-hint">绿＝数据在流入，连接活着；黄＝短暂无新数据；红＝长时间无数据，疑似断流。<br>「N 块」＝已收到的数据片段数，流式输出时持续增长即连接正常。</div>
</div>`;
		host.prepend(drawer);

		const bindNumD = (sel, key, min) => {
			drawer.querySelector(sel).addEventListener('change', (e) => {
				let v = parseInt(e.target.value, 10);
				if (!Number.isFinite(v) || v < min) { v = min; }
				settings[key] = v;
				syncControls();
				saveSettingsDebounced();
			});
		};
		bindNumD('[data-k="stallWarnSec"]', 'stallWarnSec', 2);
		bindNumD('[data-k="stallDeadSec"]', 'stallDeadSec', 5);
		bindNumD('[data-k="ttfbWarnSec"]', 'ttfbWarnSec', 3);
		bindNumD('[data-k="ttfbDeadSec"]', 'ttfbDeadSec', 10);
		drawer.querySelector('[data-k="flashTitle"]').addEventListener('change', (e) => {
			settings.flashTitle = e.target.checked;
			syncControls();
			saveSettingsDebounced();
		});
		drawer.querySelector('[data-k="showIdle"]').addEventListener('change', (e) => {
			settings.showIdle = e.target.checked;
			syncControls();
			saveSettingsDebounced();
		});
		drawer.querySelector('[data-k="autoRetry"]').addEventListener('change', (e) => {
			settings.autoRetry = e.target.checked;
			syncControls();
			saveSettingsDebounced();
		});
		drawer.querySelector('[data-k="maxRetries"]').addEventListener('change', (e) => {
			let v = parseInt(e.target.value, 10);
			if (!Number.isFinite(v) || v < 0) { v = 0; }
			if (v > 5) { v = 5; }
			settings.maxRetries = v;
			syncControls();
			saveSettingsDebounced();
		});
		drawer.querySelector('[data-k="requestUsage"]').addEventListener('change', (e) => {
			settings.requestUsage = e.target.checked;
			syncControls();
			saveSettingsDebounced();
		});
		drawer.querySelector('[data-k="ctxLimit"]').addEventListener('change', (e) => {
			let v = parseInt(e.target.value, 10);
			if (!Number.isFinite(v) || v < 0) { v = 0; }
			settings.ctxLimit = v;
			syncControls();
			saveSettingsDebounced();
		});
		drawer.querySelector('[data-k="pinNewMessageTop"]').addEventListener('change', (e) => {
			settings.pinNewMessageTop = e.target.checked;
			syncControls();
			saveSettingsDebounced();
		});
		drawer.querySelector('.stcmd-reset').addEventListener('click', resetPillPos);
		drawer.querySelector('.stcmd-test').addEventListener('click', () => runSelfTest('#stcm-drawer .stcmd-selftest'));
	}

	function positionPanel() {
		panel.style.visibility = 'hidden';
		panel.style.display = 'flex';
		const pw = panel.offsetWidth;
		const ph = panel.offsetHeight;
		panel.style.visibility = '';

		// 拖过的面板记住位置（按实际尺寸钳制，防止换设备后跑到屏幕外）
		if (Number.isFinite(settings.panelX) && Number.isFinite(settings.panelY)) {
			const p = clampToViewport(settings.panelX, settings.panelY, pw, ph);
			panel.style.left = `${p.x}px`;
			panel.style.top = `${p.y}px`;
			return;
		}

		// 没拖过：默认出现在状态球旁边
		const pr = pill.getBoundingClientRect();
		let x = pr.left - 8;
		let y = pr.top - 8;
		if (x + pw > window.innerWidth - 8) { x = window.innerWidth - pw - 8; }
		if (y + ph > window.innerHeight - 8) { y = Math.max(8, window.innerHeight - ph - 8); }
		panel.style.left = `${Math.max(8, x)}px`;
		panel.style.top = `${Math.max(8, y)}px`;
	}

	function classify(rec) {
		// 返回 {cls, text}；rec 为进行中的请求
		const now = performance.now();
		const dead1 = settings.stallWarnSec * 1000;
		const dead2 = settings.stallDeadSec * 1000;
		const w1 = settings.ttfbWarnSec * 1000;
		const w2 = settings.ttfbDeadSec * 1000;

		if (rec.state === 'wait') {
			const el = now - rec.start;
			if (el >= w2) { return { cls: 'stcm-dead', text: `等待响应 ${fmtSec(el)} · 上游无响应` }; }
			if (el >= w1) { return { cls: 'stcm-warn', text: `等待响应 ${fmtSec(el)}` }; }
			return { cls: 'stcm-wait', text: `等待响应 ${fmtSec(el)}` };
		}
		if (rec.state === 'retrying' && rec.retryUntil > now) {
			const left = (rec.retryUntil - now) / 1000;
			return { cls: 'stcm-warn', text: `重试 ${rec.attempt}/${rec.retryMax} · 等待 ${left.toFixed(0)}s` };
		}

		// receiving
		const since = now - rec.lastData;
		if (since >= dead2) {
			// 下游正等着要数据（desiredSize>0）却等不到 → 上游真断流
			if (!rec.backpressure) {
				return { cls: 'stcm-dead', text: `${fmtSec(since)} 无数据 · 疑似断流` };
			}
			// 下游自己没在消费（后台标签页渲染被节流）→ 不是断流，是界面滞后
			return { cls: 'stcm-warn', text: `界面滞后 · 上游已 ${fmtSec(since)} 无新数据` };
		}
		if (since >= dead1) {
			if (rec.backpressure) {
				return { cls: 'stcm-warn', text: `界面滞后 · 待消费 ${fmtBytes(rec.buffered)}` };
			}
			return { cls: 'stcm-warn', text: `${fmtSec(since)} 无新数据 · ${fmtBytes(rec.bytes)}` };
		}
		return { cls: 'stcm-flow', text: `生成中 ${fmtSec(now - rec.start)} · ${rec.chunks} 块` };
	}

	function endedView(rec) {
		const dur = rec.end - rec.start;
		switch (rec.state) {
			case 'done': {
				const n = (rec.attempts || []).length;
				const prefix = n > 0 ? `✓ 重试${n}次后成功` : '✓ 完成';
				return { cls: 'stcm-done', text: `${prefix} ${fmtSec(dur)} · ${rec.chunks} 块` };
			}
			case 'error': return { cls: 'stcm-dead', text: `✗ 失败 ${rec.note || ''} · ${fmtSec(dur)}` };
			case 'aborted': return { cls: 'stcm-abort', text: `■ 已停止 · ${fmtSec(dur)}` };
			case 'empty': return { cls: 'stcm-warn', text: `⚠ ${rec.note || '空响应'} · ${fmtSec(dur)}` };
			default: return { cls: 'stcm-abort', text: rec.state };
		}
	}

	let titleFlashed = false;
	const origTitle = document.title;

	function tick() {
		const latest = [...activeMap.values()].pop();
		let cls = '', show = false;


		if (latest) {
			const v = classify(latest);
			cls = v.cls;
			text.textContent = v.text;
			show = true;
		} else if (lastFinished && performance.now() - lastFinished.end < 6000) {
			const v = endedView(lastFinished);
			cls = v.cls;
			text.textContent = v.text;
			show = true;
		} else if (settings.showIdle) {
			cls = '';
			text.textContent = '空闲';
			show = true;
		}

		pill.className = cls;
		pill.style.display = show ? 'flex' : 'none';
		updateDrawerState(cls, text.textContent);

		// 标题闪烁
		const stalled = [...activeMap.values()].some((r) => {
			if (!r || r.end) { return false; }
			const v = classify(r);
			return v.cls === 'stcm-warn' || v.cls === 'stcm-dead';
		});
		if (settings.flashTitle && stalled && !titleFlashed) {
			titleFlashed = true;
			document.title = `⚠️ ${origTitle}`;
		} else if ((!stalled || !settings.flashTitle) && titleFlashed) {
			titleFlashed = false;
			document.title = origTitle;
		}

		if (panel.style.display === 'flex') {
			renderHistory();
			updateContextBar();   // 上限设置改了也要即时反映
		}

		// 兜底：每 2 秒用「实际渲染位置」校正一次。
		// 球的宽度会随文字变化（"空闲" 与 "生成中 12.3s · 45 块" 差很多），
		// 只比对存下来的数字会漏；但每 400ms 读 rect 会强制布局，故降频。
		const nowMs = performance.now();
		if (!drag && nowMs - lastClampCheck > 2000) {
			lastClampCheck = nowMs;
			const r = pill.getBoundingClientRect();
			const vp = viewportSize();
			if (r.left < 0 || r.top < 0 || r.right > vp.w + 1 || r.bottom > vp.h + 1) {
				clampPillPos();
			}
		}
	}

	const STATE_DOT = {
		receiving: '#2ecc71', wait: '#4da3ff', done: '#2ecc71',
		error: '#ff4d4f', aborted: '#8a8f9a', warn: '#f5a623', dead: '#ff4d4f',
		empty: '#f5a623',   // 空响应：HTTP 成功但没正文，用橙色区分于「完成」和「失败」
	};

	function historyRow(rec) {
		const live = !rec.end;
		let dotColor = STATE_DOT[rec.state] || '#5b6270';
		let sub;
		if (live) {
			const v = classify(rec);
			if (v.cls === 'stcm-warn' || v.cls === 'stcm-dead') { dotColor = STATE_DOT[v.cls]; }
			sub = v.text;
		} else {
			const dur = fmtSec(rec.end - rec.start);
			// 同一次生成的重试会各自成条，用「第 N 次」标出先后，便于对照
			// 只有确实发生过重试（同一链上有 >1 条）时才标序号，单次请求保持干净
			const sameChain = rec.retryChain && history.filter((x) => x.retryChain === rec.retryChain).length > 1;
			const nth = sameChain ? `第${rec.attemptNo || 1}次 · ` : '';
			const label = rec.state === 'done' ? '完成'
				: rec.state === 'error' ? `失败 ${rec.note}`
				: rec.state === 'empty' ? `⚠ ${rec.note || '空响应'}`
				: '已停止';
			sub = `${nth}${label} · 用时 ${dur} · TTFB ${fmtSec(rec.ttfb)} · ${fmtBytes(rec.bytes)} · ${rec.chunks} 块`;
		}

		const canExpand = !live && canShowDetail(rec);
		const open = canExpand && expandedIds.has(rec.id);
		const caret = canExpand ? `<span class="stcm-caret">${open ? '▾' : '▸'}</span>` : '';

		const head = `
<div class="stcm-row${canExpand ? ' stcm-expandable' : ''}">
	<div class="stcm-r1">
		<span class="stcm-dot" style="background:${dotColor}"></span>
		<div style="min-width:0">
			<div class="stcm-name">${caret}${rec.method} ${rec.short}</div>
			<div class="stcm-sub">${sub}</div>
		</div>
	</div>
	<div class="stcm-time">${fmtClock(rec.startWall)}</div>
</div>`;
		const detail = open ? (hasErrDetail(rec) ? errDetailHtml(rec) : runDetailHtml(rec)) : '';
		return `<div class="stcm-item" data-stcm-id="${rec.id}">${head}${detail}</div>`;
	}

	// 上游结束原因的中文解释
	// Gemini 原生格式的 finishReason 是大写枚举，映射成 OpenAI 那套说法
	const GEMINI_FINISH = {
		STOP: 'stop',
		MAX_TOKENS: 'length',
		SAFETY: 'content_filter',
		RECITATION: 'content_filter',
		BLOCKLIST: 'content_filter',
		PROHIBITED_CONTENT: 'content_filter',
		SPII: 'content_filter',
		MALFORMED_FUNCTION_CALL: 'tool_calls',
		OTHER: '',
	};

	const FINISH_TEXT = {
		stop: '正常结束',
		length: '达到长度上限，被截断',
		content_filter: '内容被上游过滤（安全拦截）',
		tool_calls: '工具调用',
		function_call: '函数调用',
	};
	const finishLabel = (r) => (r ? `${r}${FINISH_TEXT[r] ? ` · ${FINISH_TEXT[r]}` : ''}` : '');

	// 成功 / 主动停止的行：耗时明细 + 返回开头
	function runDetailHtml(rec) {
		const dur = rec.end - rec.start;
		const kv = [];
		const stText = `HTTP ${rec.status || '?'}${rec.state === 'aborted' ? ' · 已停止' : ''}`;
		kv.push(`<div class="stcm-kv"><span>状态</span><b>${escHtml(stText)}</b></div>`);
		kv.push(`<div class="stcm-kv"><span>请求</span><b>${escHtml(rec.short)}</b></div>`);
		if (rec.model) {
			kv.push(`<div class="stcm-kv"><span>模型</span><b>${escHtml(rec.model)}</b></div>`);
		}
		kv.push(`<div class="stcm-kv"><span>耗时</span><b>${fmtSec(dur)}${(rec.attempts || []).length ? '（含重试）' : ''} · 首字节 ${fmtSec(rec.ttfb)}</b></div>`);
		if ((rec.attempts || []).length) {
			const steps = rec.attempts
				.map((a) => (a.status ? `HTTP ${a.status}` : (a.error || '网络错误')).replace(/^HTTP HTTP /, 'HTTP '))
				.join(' → ');
			kv.push(`<div class="stcm-kv"><span>尝试</span><b>${escHtml(steps)} → ${rec.state === 'done' ? '成功' : '仍失败'}</b></div>`);
		}
		const durSecEarly = dur / 1000;
		const speedTxt = (rec.tokens > 0 && durSecEarly > 0.15) ? ` · ${fmtRate(rec.tokens / durSecEarly)} token/s` : '';
		kv.push(`<div class="stcm-kv"><span>数据</span><b>${fmtBytes(rec.bytes)} · ${rec.chunks} 块${speedTxt}</b></div>`);
		if (rec.finishReason && rec.finishReason !== 'stop') {
			kv.push(`<div class="stcm-kv"><span>结束</span><b>${escHtml(finishLabel(rec.finishReason))}</b></div>`);
		}
		if (rec.promptTokens > 0 || rec.completionTokens > 0) {
			kv.push(`<div class="stcm-kv"><span>用量</span><b>输入 ${(rec.promptTokens || 0).toLocaleString()} · 输出 ${(rec.completionTokens || 0).toLocaleString()}</b></div>`);
		}

		const headText = String(rec.head || '').trim();
		const headBlock = headText
			? `<div class="stcm-headlabel">返回开头（前 ${Math.min(headText.length, HEAD_LIMIT)} 字）</div><pre class="stcm-raw">${escHtml(headText)}</pre>`
			: '';
		return `
<div class="stcm-detail">
	${kv.join('\n\t')}
	${headBlock}
</div>`;
	}

	function errDetailHtml(rec) {
		const d = rec.errDetail || {};
		const kv = [];
		const st = `HTTP ${d.status || '?'}${d.statusText ? ' ' + d.statusText : ''}`;
		kv.push(`<div class="stcm-kv"><span>状态</span><b>${escHtml(st)}</b></div>`);
		if (d.errorCode) {
			kv.push(`<div class="stcm-kv"><span>错误码</span><b>${escHtml(d.errorCode)}</b></div>`);
		}
		if (d.message) {
			kv.push(`<div class="stcm-kv"><span>原因</span><b>${escHtml(d.message)}</b></div>`);
		}
		const raw = String(d.body || '').trim();
		const rawBlock = raw ? `<pre class="stcm-raw">${escHtml(raw)}</pre>` : '';
		return `
<div class="stcm-detail">
	${kv.join('\n\t')}
	${rawBlock}
	<a class="stcm-loglink" href="http://127.0.0.1:9180/logs" target="_blank" rel="noopener">打开后端日志（仅本机可看）</a>
</div>`;
	}

	// 在详情里选字准备复制时，不要因为这次点击把详情收起来
	let suppressToggle = false;
	function selectionInsidePanel() {
		try {
			const sel = window.getSelection?.();
			return !!(sel && String(sel).length > 0 && sel.anchorNode && panel.contains(sel.anchorNode));
		} catch { return false; }
	}

	function onHistoryClick(e) {
		// 点日志链接时不触发展开收合
		if (e.target.closest('a')) { return; }
		if (suppressToggle) { suppressToggle = false; return; }
		// 只有点标题行才开合；点详情内容（常见于要选文字复制）不响应
		const row = e.target.closest('.stcm-row');
		if (!row) { return; }
		const item = row.closest('.stcm-item');
		if (!item) { return; }
		const id = Number(item.getAttribute('data-stcm-id'));
		if (!id) { return; }
		const rec = findRecordById(id);
		if (!canShowDetail(rec)) { return; }
		if (expandedIds.has(id)) {
			expandedIds.delete(id);
		} else {
			expandedIds.add(id);
		}
		renderHistory();
	}

	// ---------- 历史持久化的读写 ----------
	let histSaveTimer = null;
	let activeHistKey = null;

	function saveHistoryNow(key) {
		// 必须显式传 key：切换角色时 ctx.name2 已经是新角色，用当前名会存错地方
		const k = key || activeHistKey || histKeyFor(currentCharName());
		const write = (list) => {
			if (!list.length) { localStorage.removeItem(k); return; }
			localStorage.setItem(k, JSON.stringify(list));
		};
		try {
			write(history.slice(0, HIST_MAX).map(slimRec));
		} catch {
			// 配额超限：砍一半再试，还不行就放弃（绝不影响生成流程）
			try { write(history.slice(0, Math.floor(HIST_MAX / 2)).map(slimRec)); } catch { /* give up */ }
		}
	}

	function saveHistorySoon() {
		clearTimeout(histSaveTimer);
		histSaveTimer = setTimeout(() => saveHistoryNow(), 700);
	}

	function loadHistoryForCurrentChar() {
		history.length = 0;
		activeHistKey = histKeyFor(currentCharName());
		try {
			const raw = localStorage.getItem(activeHistKey);
			if (raw) {
				const arr = JSON.parse(raw);
				if (Array.isArray(arr)) {
					for (const o of arr.slice(0, HIST_MAX)) { history.push(reviveRec(o)); }
				}
			}
		} catch { /* 数据损坏就当作空历史 */ }
		renderPanel();
	}

	// 展开详情的内容指纹：errDetail 异步到位、head 流式增长时版本号变化，才允许重建详情块
	function detailVer(rec) {
		if (hasErrDetail(rec)) {
			const d = rec.errDetail;
			return `e:${(d.body || '').length}:${d.message || ''}`;
		}
		return `r:${(rec.head || '').length}`;
	}

	function renderHistory() {
		const box = panel.querySelector('#stcm-history');
		if (!box) { return; }
		if (!box.dataset.stcmBound) {
			box.dataset.stcmBound = '1';
			box.addEventListener('pointerdown', () => { suppressToggle = selectionInsidePanel(); });
			box.addEventListener('pointerup', () => { if (selectionInsidePanel()) { suppressToggle = true; } });
			box.addEventListener('click', onHistoryClick);
		}
		// 每 400ms 重绘会打断用户滚动：重绘前后保持滚动位置
		const scroller = box.closest('.stcm-hist');
		const keepTop = scroller ? scroller.scrollTop : 0;
		const atBottom = scroller ? (scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 4) : false;

		const rows = [...activeMap.values()].reverse().concat(history).slice(0, 20);
		if (!rows.length) {
			box.replaceChildren();
			box.innerHTML = '<div class="stcm-hint">还没有请求记录。发一条消息试试。</div>';
			return;
		}

		// 增量更新：已展开的详情块尽量保留原 DOM —— 否则 innerHTML 全量重建会把
		// .stcm-raw 的滚动位置每 400ms 重置一次，内容长时根本滚不下去（触摸拖动也会被打断）
		const existing = new Map();
		box.querySelectorAll('.stcm-item[data-stcm-id]').forEach((el) => existing.set(el.getAttribute('data-stcm-id'), el));
		const tmp = document.createElement('div');

		// 先为每条记录准备目标节点（能复用就复用）
		const targets = rows.map((rec) => {
			const key = String(rec.id);
			const old = existing.get(key) || null;
			existing.delete(key);
			return { rec, old };
		});

		// 顺序与节点都完全没变 -> 原位更新，一个节点都不移动（滚动位置绝对安全）
		const sameOrder = box.children.length === targets.length
			&& targets.every((t, i) => t.old && box.children[i] === t.old);

		if (sameOrder) {
			for (const { rec, old } of targets) {
				const wantOpen = expandedIds.has(rec.id) && canShowDetail(rec);
				const ver = detailVer(rec);
				const keepDetail = wantOpen && old.querySelector('.stcm-detail') && old.getAttribute('data-stcm-dv') === ver;
				tmp.innerHTML = historyRow(rec);
				const newRow = tmp.firstElementChild.querySelector('.stcm-row');
				const newDetail = tmp.firstElementChild.querySelector('.stcm-detail');
				const curRow = old.querySelector('.stcm-row');
				// 头部变了才替换（避免每 400ms 重建导致 hover 闪烁）
				if (curRow && newRow && curRow.innerHTML !== newRow.innerHTML) {
					curRow.replaceWith(newRow);
				}
				if (wantOpen && !keepDetail) {
					// 详情刚展开或内容版本变化：原位替换 detail（位置不变，只丢这一次滚动）
					const curDetail = old.querySelector('.stcm-detail');
					if (newDetail) {
						if (curDetail) { curDetail.replaceWith(newDetail); } else { old.appendChild(newDetail); }
						old.setAttribute('data-stcm-dv', ver);
					}
				} else if (!wantOpen && old.getAttribute('data-stcm-dv')) {
					const curDetail = old.querySelector('.stcm-detail');
					if (curDetail) { curDetail.remove(); }
					old.removeAttribute('data-stcm-dv');
				}
			}
		} else {
			// 新记录到来 / 行被挤出：重排。此帧展开行的滚动会丢，属可接受的偶发
			const frag = document.createDocumentFragment();
			for (const { rec, old } of targets) {
				const wantOpen = expandedIds.has(rec.id) && canShowDetail(rec);
				if (old && wantOpen && old.querySelector('.stcm-detail') && old.getAttribute('data-stcm-dv') === detailVer(rec)) {
					// 保留旧节点（移动会丢滚动，但此分支只在重排帧发生）
					frag.appendChild(old);
				} else {
					tmp.innerHTML = historyRow(rec);
					const nu = tmp.firstElementChild;
					if (wantOpen) { nu.setAttribute('data-stcm-dv', detailVer(rec)); }
					frag.appendChild(nu);
				}
			}
			box.replaceChildren(frag);
		}

		if (scroller) {
			// 用户本来贴着底部看最新记录 -> 重绘后继续贴底；否则恢复原滚动位置
			scroller.scrollTop = atBottom ? scroller.scrollHeight : keepTop;
		}
	}

	// 上下文占用：取最近一次拿到 token 用量的请求
	function updateContextBar() {
		const fill = panel.querySelector('#stcm-ctxfill');
		const txt = panel.querySelector('#stcm-ctxtxt');
		if (!fill || !txt) { return; }

		let rec = null;
		for (const r of activeMap.values()) {
			if (r.promptTokens > 0) { rec = r; }
		}
		if (!rec) {
			rec = history.find((r) => r.promptTokens > 0) || null;
		}
		if (!rec) {
			fill.style.width = '0';
			fill.className = 'stcm-ctxfill';
			txt.textContent = '—';
			return;
		}

		// 上限只认用户自己填的：酒馆前端拿不到真实的 openai_max_context
		// （ctx.maxContext 会给 8192 这类默认值，拿它算百分比会误导）
		const limit = settings.ctxLimit > 0 ? settings.ctxLimit : 0;

		const used = rec.promptTokens;
		const fmtK = (n) => n >= 1000000 ? `${(n / 1000000).toFixed(n >= 10000000 ? 0 : 1)}M`
			: n >= 1000 ? `${(n / 1000).toFixed(n >= 100000 ? 0 : 1)}k`
			: String(n);
		if (limit > 0) {
			const pct = Math.min(100, (used / limit) * 100);
			fill.style.width = `${pct.toFixed(1)}%`;
			fill.className = 'stcm-ctxfill' + (pct >= 90 ? ' stcm-ctx-dead' : pct >= 70 ? ' stcm-ctx-warn' : '');
			txt.textContent = `${fmtK(used)} / ${fmtK(limit)}（${pct.toFixed(0)}%）`;
		} else {
			// 没配上限就不画进度，只报本次用量（避免用错的上限算出误导性的百分比）
			fill.style.width = '0';
			fill.className = 'stcm-ctxfill';
			txt.textContent = `${used.toLocaleString()} token`;
		}
	}

	function renderPanel() {
		if (panel && panel.style.display === 'flex') {
			renderHistory();
			updateContextBar();
		}
	}

	const CLS_COLOR = {
		'stcm-flow': '#2ecc71', 'stcm-wait': '#4da3ff', 'stcm-warn': '#f5a623',
		'stcm-dead': '#ff4d4f', 'stcm-done': '#2ecc71', 'stcm-abort': '#8a8f9a', '': '',
	};

	// 把同一份状态同步到「扩展设置」页里的区块
	function updateDrawerState(cls, txt) {
		if (!drawer) { return; }
		const t = drawer.querySelector('.stcmd-text');
		const d = drawer.querySelector('.stcmd-dot');
		if (t) { t.textContent = txt || '空闲'; }
		if (d) { d.style.background = CLS_COLOR[cls] || ''; }
	}

	async function runSelfTest(sel) {
		const out = document.querySelector(sel || '#stcm-selftest');
		if (!out) { return; }
		out.textContent = '自检中…';
		const times = [];
		try {
			for (let i = 0; i < 3; i++) {
				const t0 = performance.now();
				const r = await origFetch(`/version?_=${Date.now()}`, { cache: 'no-store' });
				await r.text();
				times.push(performance.now() - t0);
			}
			const avg = times.reduce((a, b) => a + b, 0) / times.length;
			out.textContent = `页面→酒馆 平均 ${avg.toFixed(0)} ms（${times.map((t) => t.toFixed(0)).join('/')}）`;
		} catch (err) {
			out.textContent = `自检失败：${String((err && err.message) || err).slice(0, 60)}`;
		}
	}

	// ---------- 启动 ----------
	jQuery(async () => {
		buildUI();
		console.log(TAG, `已加载 ${VERSION}（状态球 + 请求历史；只旁观数据流，不做任何改动）`);
	});
}
