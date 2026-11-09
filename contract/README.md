# workers/contract — worker-local/1 跨引擎合同套

一份 spec 对**每个被选中的 engine host 跑同一组断言**。目的不是替代各引擎自己的
深度测试，而是证明 pi / codex 在同一 `worker-local/1` + `worker-host/1` 合同下
可互换——两边各自演进时，任何一方偏离合同（路由缺失、形状漂移、幂等语义不同）
都会被同一 spec 抓到。

## 运行

```sh
# pi（自启动：进程内 PiHost + fixture-child，确定性，不发模型请求）
cd ../pi && pnpm build   # 需 dist/
node --test local-contract.test.mjs

# 同时跑 codex（需外部提供已构建 host 二进制 + 初始化 state 模板）
WORKER_CONTRACT_ENGINES=pi,codex \
CODEX_HOST_EXE=/path/to/codex-host \
CODEX_STATE_TEMPLATE=/path/to/initialized-state \
CODEX_PROJECT_ID=<模板内 projectId> \
node --test local-contract.test.mjs
```

缺 `CODEX_*` 环境时 codex 记 skipped——合同缺口显式可见而不是静默缺席。

## 断言组

- **A 线合同**（所有引擎）：无/错 bearer → 401；identity/manifest/capabilities
  形状与 `engine.id` 如实；`supportedPaths` 逐条探测 <500；engines/projects
  列表形状；`UNCONFIGURED` 时 `POST /tasks` 4xx 结构化拒绝且同 requestKey
  重放同码。
- **B 执行合同**（`det=true` 引擎，当前 pi 经 fixture-child）：preflight→
  create 202→终态 SUCCEEDED→events 游标单调+cursorExpired→artifacts→
  requestKey 幂等重放回原回执不双派发→`waiting-never` 任务 STOP 命令
  带 generation/nativeSessionId/targetCommandId 护板→CANCELLED。

codex 执行合同激活：提供 `CODEX_CONTRACT_DET=1` + 可确定性执行的 state
模板（脚本化模型端点）。真模型不归这里管，在 t3 层。

## 不归本套断言

各引擎独有面（codex 的 handoff/materials/terminal、pi 的扩展加载）由各自
`test/` 覆盖；这里只锁**交集**——`manifest.supportedPaths` 声明的差集允许
存在，但声明必须如实（A3 逐条探测）。
