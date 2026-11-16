# worker-adapter/1 — 引擎适配器契约（草案）

本契约定义「工具接入方如何实现一个适配器，使宿主体（cli-bridge / Agent Link）
能发现它、读它的数据、（可选）驱动它」。目标是：**宿主不认识任何 vendor 名单**，
vendor 知识全部封在适配器内；第三方交付物 = 一个适配器目录，不需要改宿主代码。

## 状态分层（诚实标注）

| 部分 | 状态 |
|---|---|
| 桥 API 面（§5）、`store` 只读语义、`ENGINE_READ_ONLY`、devin 参考实现 | **已落地**（`cli-bridge/host.mjs`） |
| manifest 字段（§3）、发现流程（§4）、版本协商（§6） | **草案**——字段已冻结讨论稿，装载器未实现；当前 `ENGINES` 仍是编译期表 |

标注「草案」的部分是适配器作者应遵循的目标形态；落地前任何字段改名都会
走本文件版本号（worker-adapter/2），不静默漂移。

## 1. 两类适配器

| kind | 含义 | 生命周期 | 例子 |
|---|---|---|---|
| `spawn` | 驱动一个可执行 CLI 跑任务 | 托管条目 → spawn 子进程 → 事件回流 | claude / codex / pi / gemini |
| `store` | 只读消费一个本地数据源，不 spawn | 桥进程本体即全部，无子进程 | devin（Devin 桌面会话库） |

`store` 必须满足：零 vendor 可执行依赖（目标 CLI 没装也能跑）、`/engines`
返回空表（不上任务目标选择器）、对任务提交回 `ENGINE_READ_ONLY`（409）。
旁支提问/接管导入由**调用方选择的某个 spawn 引擎**执行——`store` 自己不答题。

## 2. 适配器交付形态（草案）

```
adapters/<id>/
  adapter.json        # manifest，§3
  <impl>.mjs          # 实现，导出 manifest 声明的 ops
  README.md           # vendor 数据结构说明、版本覆盖范围
```

放置位置：内置 `components/adapters/`；社区/第三方 `<state>\adapters\`。
目录扫描装载，宿主重启即生效；新增/升级适配器**不改宿主**。

## 3. manifest（草案）

```jsonc
{
  "schema": "worker-adapter/1",
  "id": "devin",                  // [a-z0-9][a-z0-9-]{1,31}，全局唯一
  "kind": "store",                // spawn | store
  "display": { "name": "Devin", "iconHint": "sparkle" },
  "detect": {                     // 发现 hint，见 §4；store 类可省略 processes/bin
    "processes": ["Devin.exe"],
    "installDirs": ["%LOCALAPPDATA%/Programs/Devin"],
    "configDirs": ["%APPDATA%/Devin/cli", "~/.local/share/devin/cli"],
    "bin": "devin"                // spawn 类必填：PATH 可执行名
  },
  "versions": [                   // 数据结构版本，新→旧排列，见 §6
    { "id": "devin-db/1", "match": "sessions.db:schema>=1" }
  ],
  "capabilities": {               // 桥如实上报给 phone 的能力面
    "history": true, "transcript": true, "adoptSource": true,
    "sideQuestion": "delegate"    // delegate=由所选 spawn 引擎作答
  }
}
```

## 4. 发现流程（草案）

宿主收集器按以下顺序、且只做以下事——**禁止全盘遍历**：

1. **进程扫描**：遍历进程表匹配 `detect.processes` → 发现「运行中但未托管」的
   agent（可在托盘提示接管）。
2. **声明路径 stat**：逐个检查 `installDirs`/`configDirs`（含环境变量与 `~`
   展开），存在即取为候选 `installRoot`/`configRoot`。
3. **用户覆盖**：`<state>\agents.json` 条目可带 `paths: {configRoot, executable}`
   ——个性化/非默认安装位置的唯一正解，优先级高于扫描结果。
4. **结果**：`{installed, installRoot?, configRoot?, executable?, running}`。
   `store` 类以 `configRoot` 存在为可用条件；`spawn` 类还要求 `executable` 可运行
   （`--version` 探测即当前实现，见 host.mjs `execFileSync(...,'--version')`）。

未发现 ≠ 错误：条目如实显示「未检测到」，用户可手动补路径。

## 5. 桥 API 面（已落地，worker-local/1 子集）

适配器经 cli-bridge 暴露这些路由（phone/gateway 消费面）：

| 路由 | spawn | store | 说明 |
|---|---|---|---|
| `GET /local/v1/identity` | ✔ | ✔ | 桥身份 |
| `GET /local/v1/engines` | 1 项 | **空表** | store 必须空，防入选题器 |
| `GET /local/v1/projects` | ✔ | ✔ | |
| `GET /local/v1/history?engine=` | 本引擎 | `devin` | 分页/搜索游标语义不变 |
| `GET /local/v1/history/sessions/{id}?engine=` | ✔ | ✔ | transcript；`SESSION_NOT_FOUND`/`LOCAL_SOURCE_UNAVAILABLE` |
| `POST /local/v1/history/questions` | ✔ | ✔（delegate） | store 收到的 question 含 `engine=` 目标 spawn 引擎 |
| `POST /local/v1/tasks` | ✔ | **`ENGINE_READ_ONLY`** | adoptSessionId+adoptEngine 仍可用（导入目标在 spawn 侧） |

错误码沿用 worker-local/1：`INVALID_REQUEST` `SESSION_NOT_FOUND`
`LOCAL_SOURCE_UNAVAILABLE` `ENGINE_READ_ONLY`；本契约新增
`UNSUPPORTED_VERSION`（§6，草案，尚未由桥发出）。

## 6. 数据结构版本（草案）

vendor 升级改格式是常态。规则：

- `versions[]` 新→旧排列；`match` 由适配器自评（它才认识自己的格式——
  sqlite 版本表、文件布局、magic 都行）。
- 装载时取**第一个 match 为真**的版本实现；全不匹配 → 上报
  `UNSUPPORTED_VERSION`（携带检测到的版本线索），托盘显示「适配器待更新」，
  **不得静默当空数据**。
- 新旧并存期间多版本条目共存；旧版下线由适配器作者决定，与宿主无关。
- 宿主只提供只读打开助手（sqlite/JSONL），不提供任何格式知识。

## 7. 信任边界

- `<state>\adapters\` 下的适配器 = 用户自装代码，以用户权限运行，自负——
  与「用户手动放了个 CLI 在 PATH」同级信任。
- `components/adapters\`（随安装包发布）才涉及签名/审核，本契约不定义。

## 8. 参考实现

`devin`（store 类）：`cli-bridge/host.mjs` 中 `DEVIN_DBS`（多平台 configDirs
候选，首个命中）→ `devinSessions()`（分页/搜索）→ `devinTranscript()`；
`ENGINE === 'devin'` 时 `engine = null` 路径即「无 spawn 的 store」形态——
`/engines` 空表、`POST /tasks` 回 `ENGINE_READ_ONLY`、questions 经
`q.engine` 委托给所选真实引擎（缺失 CLI 以 spawn 'error' 软失败，不拖垮桥）。
当前以编译期代码存在，是把 §3–§6 草案落成目录装载时的迁移目标。

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
