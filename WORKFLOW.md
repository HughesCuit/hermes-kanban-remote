# hermes-agent-cluster 开发流程

## 人员配置

| 角色 | Profile | 职责 | Toolsets |
|------|---------|------|----------|
| 项目经理 | group-pm | 需求拆解、任务创建、进度跟踪 | kanban, delegation, terminal, file, web, skills |
| 架构师 | dev-architect | 技术架构设计、API 设计 | terminal, file, web, kanban, skills |
| 后端开发 | dev-backend | Go 代码实现、单元测试 | terminal, file, web, kanban, skills, delegation |
| CTO | group-cto | 技术决策、代码审查、架构评审 | terminal, file, web, kanban, delegation |
| DevOps | devops | 部署、CI/CD、监控 | terminal, file, web, kanban, skills |

## 开发流程

```
需求分析 (PM)
    ↓
架构设计 (dev-architect)
    ↓
技术选型 (dev-architect + group-cto)
    ↓
任务拆解 (PM) → Kanban Tasks
    ↓
编码实现 (dev-backend → Claude Code)
    ↓
代码审查 (group-cto / dev-architect)
    ↓
测试验证 (dev-backend)
    ↓
部署上线 (devops)
```

## 任务生命周期

1. **PM** 创建任务 → 状态 `ready`
2. **Dispatcher** 自动 spawn worker → 状态 `running`
3. Worker 完成 → 状态 `done`，自动推进子任务
4. Review 任务 → 通过则 unblock deploy，不通过则创建 fix 任务

## Review 流程

- 每个 coding task 后面跟一个 review task
- Review 通过 → LGTM → unblock 下游
- Review 不通过 → 创建新的 fix task → 重新 review → 循环直到 LGTM

## 部署流程

- 每个 deploy task 需要：
  1. 停止旧进程
  2. 启动新服务
  3. 健康检查
  4. 失败则回滚

## GitHub 集成

- 仓库: https://github.com/HughesCuit/hermes-agent-cluster
- Branch 策略: main (生产) + feature branches
- PR 需要 review 通过后合并

## 相关链接

| 资源 | 地址 |
|------|------|
| GitHub | https://github.com/HughesCuit/hermes-agent-cluster |
| 项目文档 | ~/.hermes/projects/hermes-agent-cluster/SPEC.md |
| Discord Thread | #projects / hermes-agent-cluster |
| Kanban Board | `hermes kanban --board hermes-agent-cluster list` |

## ⚠️ 质量门规则（Phase 1 踩坑总结）

**每个 coding task 后面必须跟 review → merge → publish 流程，不能跳过。**

### 标准 Pipeline

```
coding (dev-backend) → done
    ↓ auto-promote
review (group-cto) → LGTM?
    ↓ yes                    ↓ no
merge (dev-backend)      fix tasks → coding → review → loop
    ↓
E2E test (dev-backend) → pass?
    ↓ yes                    ↓ no
publish (dev-backend)    fix → back to E2E
    ↓
release ✅
```

### 规则

1. **Coding task** — 在 feature branch 开发，不能直接推 main
2. **Review task** — 必须由非作者审查（group-cto 或 dev-architect）
3. **Merge task** — Review LGTM 后才能 merge，附带 `--no-ff` 保留分支历史
4. **E2E task** — main 分支上跑完整验证（build + test + 运行时验证）
5. **Publish task** — 打 tag + GitHub Release + 通知
6. **Fix 循环** — Review 不通过时创建新 coding task（assignee=原作者），link 到 review task 的 child

## ⚠️ Review-Fix 循环规则（2026-05-14 踩坑）

### 问题
CTO review 不通过时创建 fix task，如果设为 review task 的 child，
review 是 blocked 状态 → fix task 永远停在 todo → pipeline 卡死。

### 正确做法
1. Review 不通过 → review task 状态改为 `blocked`
2. 创建 fix task，**parent 设为原 coding task**（不是 review task）
3. Fix task 会自动 promote 到 ready → dispatcher pickup
4. Fix 完成后 → unblock review task → re-review
5. Re-review 通过 → review done → 下游任务 promote

### 禁止
- ❌ fix task 的 parent 设为 review task
- ❌ 在 blocked 的 review task 上评论期望触发下游

## ⚠️ Worker 不能自己 blocked（2026-05-14 踩坑）

### 问题
Fix worker 完成修复后执行 `blocked("review-required")`，等 CTO re-review。
但 blocked = 停止不动，没人手动 unblock 就永远卡死。

### 正确做法
- Worker 完成工作 → 直接 `done`
- 不要用 blocked 表示"等待上游"
- 依赖关系由 parent→child 链管理
- 需要人工介入时用 comment 通知，不要 block

### 规则
- `blocked` = 有外部依赖未解决（如等决策、等资源）
- `done` = 本任务工作完成
- Worker 只管自己的任务，不管上下游流转
