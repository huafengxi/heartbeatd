# heartbeatd/ — 声明式定时触发层

## 收录判据（本仓边界）

本仓**只住机制**：读一份声明式定时注册表、到点执行其中声明的动作的常驻执行层，及其回归网。
**不住**（一律在它处，本仓不复述、只留指针）：

- **任何具体动作的定义与实现体**——注册表（动作 profile 与逐机 timer 声明）住调用方工作区；被触发的脚本、它登记的任务与它的日志住它自己的仓。
- **主机名、机器清单、仓名单、内网端点、凭据面路径**——凡「按部署面变化」的值都不进本仓代码（需要机器面时一律现场发现 ∨ 由调用方注入 ∨ 走环境变量覆写口）。
- **单动作运维册与部署面事实**（含内网信息 ⇒ 不能公开）。
- **本进程自己怎么被保活**——由调用方的服务层负责（本仓不自我监督、不自我重启）。

判据一句话：**改一处部署（加机器、换动作、改时机、改期望态）不应产生本仓的 diff。**

## 是什么

- `heartbeatd.py`：定时触发的唯一执行层（读两层 JSON 注册表 → run/status/fire/check）。**机制语义的单一事实源 = 它的模块 docstring**（注册表形态与本机面、五种 schedule 形态、全部子命令、Pointers、Invariants：misfire=skip 不补跑、同一 timer 不重叠、注册表每轮重读且坏改动保留上一份好面、子进程自成会话组可整树信号、停机带走子进程、环境 = 本进程环境 + profile 的 `env`、状态文件原子写且坏文件当空、单实例 flock、有子进程在跑时轮询收紧到 ~1s ⇒ 记录的时长与 timeout 生效不随 poll 周期量子化）+ 各函数 docstring ⇒ 本 README 不复述、只留下面两条入口性事实。
- `test_heartbeatd.py`：注册表 loader、schedule → next-fire 算术、状态与触发循环的回归网（合成夹具 + 沙箱子进程；不触发任何真实注册表的动作）。
- 零第三方依赖（Python 3 标准库），无跨仓 import（调用方只需给出注册表与可选的 host-id 映射）。

入口性事实（调用方最常踩的两条，权威仍在代码内）：**错过的触发不补跑**（唯一补跑口 = `fire <timer>`）；注册表的任何解析/校验失败在一次性子命令上一律 config error 退出、在常驻循环上保留上一份好面并响亮记一行（一份解析不了的声明既不能从它治理的面上消失，也不能把守护打死）。

## 常用命令

```
python3 heartbeatd/heartbeatd.py run               # 常驻（由调用方的服务层监督）
python3 heartbeatd/heartbeatd.py status            # 本机 timer 面：schedule / next / last / exit / fires
python3 heartbeatd/heartbeatd.py status --json     # 同一份数据的 JSON 一行（聚合面输入）
python3 heartbeatd/heartbeatd.py fire <timer>      # 人工立即触发一枚并等它跑完（唯一补跑口）
python3 heartbeatd/heartbeatd.py check             # 只校验整份注册表（含不属本机的声明）
python3 heartbeatd/test_heartbeatd.py              # 单测：/tmp 沙箱，不写真实工作区
```

覆写口（测试与异构部署用，缺省即调用方工作区的常规布局）：`HEARTBEATD_ROOT`、`HEARTBEATD_REGISTRY`、`HEARTBEATD_HOST_ID`、`HEARTBEATD_STATE`、`HEARTBEATD_LOCK`。

单实例：`<workspace>/run/locks/heartbeatd.lock` 上的 flock（第二个 `run` 退出 1 并报持有者 pid）。

## 部署形态（调用方侧，本仓不含）

- 服务声明（`cmd`/`match`/`version` 住 profile，期望态住逐机声明）住调用方的服务注册表，执行层 = 调用方的服务生命周期层；**本仓不含任何服务定义，也不自我监督**。
- 动作注册表（动作 profile + 逐机 timer 声明 + schema）住调用方工作区。
- 触发时刻一律按**本机本地时区**解释；需要别的时区由调用方给该服务设 `TZ`（本仓不含时区表）。

## 指针

- 注册表 schema 与字段语义：调用方工作区的注册表自身（本工作区 = `heartbeats/README.md`）
- 机制语义与不变量：`heartbeatd.py` 模块 docstring（Invariants）
- 启停纪律、版本纪律、重启纪律：工作区根 `AGENTS.md`「服务与后台进程（make）」
  （按名引用形态 = `@ws-agents#behavior-rules`，只在工作区内可解）
- 各动作的实现体与其运维册：均不在本仓（本仓只从注册表读到它们的 argv）
