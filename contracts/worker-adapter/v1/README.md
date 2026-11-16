# worker-adapter/1 — 引擎适配器契约（草案）

本契约定义「工具接入方如何实现一个适配器，使宿主体（cli-bridge / Agent Link）
能发现它、读它的数据、（可选）驱动它」。目标是：**宿主不认识任何 vendor 名单**，
vendor 知识全部封在适配器内；第三方交付物 = 一个适配器目录，不需要改宿主代码。

## 状态分层（诚实标注）

| 部分 | 状态 |
|---|---|
| 桥 API 面（§5）、`store` 只读语义、`ENGINE_READ_ONLY`、devin 参考实现 | **已落地**（`cli-bridge/host.mjs`） |
| 目录装载器、adapter.mjs 交付形态、`detect.process`/`binCandidates`（§2–§4） | **已落地**——devin/opencode 是前两个目录装载适配器（`cli-bridge/adapters/`） |
| `adapter.json` manifest 形式、`versions[]` 版本协商（§6）、`UNSUPPORTED_VERSION` | **草案**——当前 manifest 是 adapter.mjs 内联对象，版本协商未实现 |

标注「草案」的部分是适配器作者应遵循的目标形态；落地前任何字段改名都会
走本文件版本号（worker-adapter/2），不静默漂移。

## 1. 两类适配器

| kind | 含义 | 生命周期 | 例子 |
|---|---|---|---|
| `spawn` | 驱动一个可执行 CLI 跑任务 | 托管条目 → spawn 子进程 → 事件回流 | claude / codex / pi / gemini |
| `store` | 只读消费一个本地数据源，不 spawn | 桥进程本体即全部，无子进程 | devin（Devin 桌面会话库） |

`store` 必须满足：零 vendor 可执行依赖（目标 CLI 没装也能跑）、`/engines`
返回**一行只读 scope 行**而非空表——`availability:'AVAILABLE'`（使 scope 可授，
否则无 launchScope 的后端连 `/history` 都被设备授权拦下）配合
`available:false` + `reasonCode:'ENGINE_READ_ONLY'` + 全 false 能力表
（不上任务目标选择器）；对任务提交回 `ENGINE_READ_ONLY`（409）。
旁支提问/接管导入由**调用方选择的某个 spawn 引擎**执行——`store` 自己不答题。

## 2. 适配器交付形态（已落地）

```
adapters/<id>/
  adapter.mjs         # 单文件 ESM，default-exports { manifest, spawn?, store? }
  README.md           # vendor 数据结构说明、版本覆盖范围（可选）
```

放置位置（装载器扫描两处，先到先得）：

- 内置：`<bridge-dir>/adapters/<id>/`——安装包内为
  `components/cli-bridge/adapters/<id>/`，随桥分发
- 用户/第三方：`<workers-root>/adapters/<id>/`——即托管桥 `--state` 目录的
  同级 `adapters/`（托管安装下为 `<state-root>\agents\adapters\`）

目录扫描装载，桥进程启动即生效；新增/升级适配器**不改宿主**。
适配器加载失败只记入 `identity.adapterProblems`，不拖垮桥。

```js
// adapter.mjs 实际形态（当前实现，JS 模块内联 manifest）
export default {
  manifest: {
    id: 'devin',                  // ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$，全局唯一
    kind: 'store',                // spawn | store | both
    displayName: 'Devin',
    detect: {                     // 全部可选；spawn 类通常要 bin/process
      process: {                  // live 徽标进程匹配
        exe: 'opencode|opencode-cli',   // Win exe 名正则（不含扩展名）
        posix: '...',                   // POSIX argv 正则（可选，默认由 exe 推导）
        excludeArgv: '\\s(run|serve)\\b' // 我方无头子命令排除项（可选）
      },
      binCandidates: () => [...],  // ()=>绝对路径数组，resolveBin 附加候选
      configDirs: [...],           // 宣告用；宿主不消费（§4 草案扩展位）
    },
  },
  // spawn 类：即 ENGINES 条目形状
  spawn: {
    bin: 'opencode',                             // resolveBin 的逻辑名
    argv: (instruction, sessionId, task, ctx) => [...],  // ctx={model,stateDir,projectDir,skip}
    onEvent: (row, task, addMessage) => 'SUCCEEDED'|'FAILED'|null,
  },
  // store 类：即 STORES 条目形状
  store: {
    sessions: ({ id, limit, before } = {}) => [{ id, title, cwd, at, rawAt }],
    transcript: (id) => [{ role: 'user'|'assistant', text }] | null,
    row: (id) => <sessions 单行> | undefined,
  },
};
```

## 3. manifest（部分草案）

当前 manifest 为 adapter.mjs 内联对象（见 §2）——`adapter.json` 分离形式
与 `versions[]`/`capabilities` 声明面仍是草案，见 §6。

```jsonc
{
  "schema": "worker-adapter/1",
  "id": "devin",                  // [a-z0-9][a-z0-9-]{1,31}，全局唯一
  "kind": "store",                // spawn | store
  "display": { "name": "Devin", "iconHint": "sparkle" },
  "versions": [                   // 数据结构版本，新→旧排列，见 §6
    { "id": "devin-db/1", "match": "sessions.db:schema>=1" }
  ],
  "capabilities": {               // 桥如实上报给 phone 的能力面
    "history": true, "transcript": true, "adoptSource": true,
    "sideQuestion": "delegate"    // delegate=由所选 spawn 引擎作答
  }
}
```

## 4. 发现流程（部分已落地）

宿主收集器按以下顺序、且只做以下事——**禁止全盘遍历**：

1. **进程扫描**【已落地】：`LIVE_PROCS` 表驱动，编译内置 claude/codex/gemini
   三条，适配器 `manifest.detect.process` 在装载时追加——进程名/argv 正则与
   headless 排除全在适配器声明里，宿主不做 vendor 判断。
2. **bin 候选**【已落地】：`resolveBin` 依次查 PATH、`binCandidates()`（适配器
   函数，安装目录扫描归它管）、`.cmd` shim 解包——opencode 的
   `@opencode-*\resources\opencode-cli.exe` 发现即此路径。
3. **声明路径 stat**【草案】：`installDirs`/`configDirs` 声明式扫描尚未进
   注册侧（注册仍走 `agents.json`/worker-card）。
4. **用户覆盖**【草案】：`<state>\agents.json` 条目可带 `paths: {configRoot,
   executable}`——个性化/非默认安装位置的唯一正解，优先级高于扫描结果。
5. **结果**：`{installed, installRoot?, configRoot?, executable?, running}`。
   `store` 类以 `configRoot` 存在为可用条件；`spawn` 类还要求 `executable` 可运行
   （`--version` 探测即当前实现，见 host.mjs `execFileSync(...,'--version')`）。

未发现 ≠ 错误：条目如实显示「未检测到」，用户可手动补路径。

## 5. 桥 API 面（已落地，worker-local/1 子集）

适配器经 cli-bridge 暴露这些路由（phone/gateway 消费面）：

| 路由 | spawn | store | 说明 |
|---|---|---|---|
| `GET /local/v1/identity` | ✔ | ✔ | 桥身份 |
| `GET /local/v1/engines` | 1 项 | 1 行只读 scope 行 | `available:false` 防入选题器；空表会让会话失去授权入口 |
| `GET /local/v1/projects` | ✔ | ✔ | |
| `GET /local/v1/history?engine=` | 本引擎 | `devin` | 分页/搜索游标语义不变 |
| `GET /local/v1/history/sessions/{id}?engine=` | ✔ | ✔ | transcript；`SESSION_NOT_FOUND`/`LOCAL_SOURCE_UNAVAILABLE` |
| `POST /local/v1/history/questions` | ✔ | ✔（delegate） | store 收到的 question 含 `engine=` 目标 spawn 引擎 |
| `POST /local/v1/tasks` | ✔ | **`ENGINE_READ_ONLY`** | adoptSessionId+adoptEngine 仍可用（导入目标在 spawn 侧） |

错误码沿用 worker-local/1：`INVALID_REQUEST` `SESSION_NOT_FOUND`
`LOCAL_SOURCE_UNAVAILABLE` `ENGINE_READ_ONLY`；本契约新增
`UNSUPPORTED_VERSION`（§6，草案，尚未由桥发出）。

## 6. 数据结构版本（部分已落地）

vendor 升级改格式是常态。当前实现把协商压缩成一个问题：

```js
// adapter.mjs
supported: (ctx) => true | '原因串'
//   ctx = { home, platform, exists(p), openSqlite(file) }
```

- 适配器自检自己的数据源（它才认识自己的格式——sqlite 表存在性、文件
  布局、magic 都行），返回 `true` 或一条原因。
- 装载时不通过 → 该引擎注册为 UNSUPPORTED 存根：`/history`、`/history/
  sessions/{id}`、questions/adopt 回 `UNSUPPORTED_VERSION`(409)，
  `identity.adapterProblems` 携带原因——**绝不静默当空数据**。
  数据源缺席（没装）≠ 不识（装了但格式变了）：前者仍 `supported→true`，
  store 如实报空。
- `versions[]` 声明式多实现（每版本一套 ops）仍是草案——opencode 的
  v1/v2 union 已证明「新旧并存归适配器内部处理」，多数适配器一个
  `supported` 就够。
- 宿主只提供 `openSqlite` 等只读助手，不提供任何格式知识。

## 7. 信任边界

- `<workers-root>\adapters\` 下的适配器 = 用户自装代码，以用户权限运行，
  自负——与「用户手动放了个 CLI 在 PATH」同级信任。
- `components\cli-bridge\adapters\`（随安装包发布）才涉及签名/审核，本契约不定义。

## 8. 参考实现

`devin`（store 类）：`cli-bridge/adapters/devin/adapter.mjs`——多平台
configDirs 候选首个命中 → `sessions()`（分页/搜索）→ `transcript()`；
`ENGINE === 'devin'` 时 `engine = null` 路径即「无 spawn 的 store」形态——
`/engines` 一行只读 scope 行、`POST /tasks` 回 `ENGINE_READ_ONLY`、questions 经
`q.engine` 委托给所选真实引擎（缺失 CLI 以 spawn 'error' 软失败，不拖垮桥）。
**第一个目录装载适配器**，证明 §2 形态可行。

`opencode`（spawn+store 双形态试点）：桌面版自带
`%LOCALAPPDATA%\Programs\@opencode-*\resources\opencode-cli.exe`
（含 `.update-<hash>` 并排目录，发现按目录名倒序取新）；任务跑
`opencode run --format json --standalone --archived --auto`（无头不碰桌面
后台服务，`--archived` 使托管运行不进"原生会话"列表，`--auto` 因无头没有
权限问答通道）；store 读 `~/.local/share/opencode/opencode.db`，同时覆盖
`session/message/part` 与 `session_v2/session_message` 两代结构、按 id 去重
——即 §6"新旧并存"的现实样例。同引擎 store 接管走 `--session` 原生续接。

## 9. 试点暴露的宿主义务（已落地修复）

第一批第三方接入（opencode）实测暴露出两条与 vendor 无关的宿主责任，
写进契约防止适配器重复踩坑：

- **spawn cwd 必须经宿主校验存在性**：会话/接管记录里的 cwd 可能已被删
  （会话活得比目录久），缺 cwd 的 spawn 报 ENOENT、表现成"引擎不可用"。
  宿主统一回退 `项目目录 → 用户主目录`（`spawnCwd`），适配器不负责兜底。
- **同引擎派生调用用注册的可执行路径**：旁支提问/接管续跑这类"同一引擎再
  起一个进程"的调用，优先用托管注册时解析出的 `resolved`（含显式
  `executable`），而不是重新 PATH 探测——后者会命中更新器留下的旧版
  并排目录。跨引擎调用（如在 opencode 桥上问 claude）才走 PATH/已知目录
  发现，且应容忍缺失（软失败，不拖垮桥）。
