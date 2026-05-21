# 测试要求

## 测试策略

按层独立测试，验证各层可单独工作，再测试组合场景。

---

## 1. Router 层测试

### 1.1 帧编解码

| 用例 | 验证点 |
|------|--------|
| 正常帧编码/解码 | encode 后 decode 还原出相同 header + payload |
| payload 为空 | encode/decode 正确处理 0 长度 |
| CRC 错误 | 篡改帧任一字节，decode 返回 `ROUTE_ERR_PARAM` |
| 帧过短 | 长度 < ROUTE_HEADER_SIZE，decode 返回 `ROUTE_ERR_PARAM` |
| 自定义 codec | 注入自定义 codec，验证 encode/decode 被调用 |

### 1.2 路由与转发

| 用例 | 验证点 |
|------|--------|
| 单播发送 | 查路由表 → 对应 port 的 send 被调用，帧内容正确 |
| 路由不存在 | 返回 `ROUTE_ERR_NO_ROUTE`，stats.drop_no_route++ |
| 转发帧 | dst != self，TTL 递减，从正确 port 转发 |
| TTL=0 丢弃 | stats.drop_ttl++ |
| 广播发送 | 所有 port 均被调用 |
| 广播转发 | 从非来源 port 转发，不回送来源 port |

### 1.3 转发去重（seen_table）

| 用例 | 验证点 |
|------|--------|
| 首次广播帧 | 正常转发 + 送达 |
| 重复广播帧（相同 src+seq） | stats.drop_duplicate++，不转发不送达 |
| seen 过期后重新接受 | 超过 seen_expire_ms 后同 src+seq 帧被正常处理 |
| seen_table 满覆盖 | 环形覆盖旧条目，不崩溃 |

### 1.4 接收队列

| 用例 | 验证点 |
|------|--------|
| route_router_input 入队 | poll 后 deliver_cb 被调用 |
| 队列满 | input 返回 `ROUTE_ERR_FULL`，stats.drop_queue_full++ |
| 多帧入队 | poll 按序处理所有帧 |

### 1.5 deliver 回调

| 用例 | 验证点 |
|------|--------|
| 单播送达本机 | deliver_cb 被调用，hdr/payload 正确 |
| 广播送达本机 | deliver_cb 被调用 |
| 转发帧不送达 | dst != self 且 != broadcast，deliver_cb 不被调用 |

### 1.6 统计

| 用例 | 验证点 |
|------|--------|
| 正常发送/接收 | tx_packets/rx_packets/tx_bytes/rx_bytes 正确递增 |
| stats 为 NULL | 不崩溃 |

---

## 2. Frag 层测试

### 2.1 分片发送

| 用例 | 验证点 |
|------|--------|
| 数据 <= frag_size | 单帧发送，frag_total=1 |
| 数据 = 2*frag_size | 发 2 帧，frag_idx=0,1，frag_total=2 |
| 数据 = N*frag_size + 余 | 发 N+1 帧，最后一帧 chunk < frag_size |
| lower_send 失败 | 中途失败立即返回错误码，不继续发后续分片 |

### 2.2 重组

| 用例 | 验证点 |
|------|--------|
| 单帧消息 | complete_cb 立即触发，数据正确 |
| 多帧按序到达 | 全部到齐后 complete_cb 触发，拼接正确 |
| 多帧乱序到达 | 全部到齐后 complete_cb 触发 |
| 重复分片 | 忽略，不影响重组 |
| 重组超时 | 超过 reasm_timeout_ms 未齐，slot 释放，stats.reasm_timeouts++ |
| frag_total 非法（0 或超 max） | 返回 `ROUTE_ERR_PARAM` |
| pool 耗尽 | 返回 `ROUTE_ERR_NO_MEM`，stats.drop_no_mem++ |
| 所有 reasm slot 占满 | 新消息返回 `ROUTE_ERR_FULL` |

### 2.3 Reliability（可靠性，内嵌）

| 用例 | 验证点 |
|------|--------|
| 发送后注册 pending_ack | pending_ack 条目 active=1，数据正确 |
| 收到 ACK | 对应 pending_ack 清除 |
| 超时重传 | tick 触发重传，retry_count++，stats.retransmissions++ |
| 达到最大重试 | pending_ack 清除，不再重传 |
| 逐片发 ACK | 每收到一个数据帧，lower_send 发出 ACK 帧 |
| reliability 禁用 | pending_acks=NULL 时，不发 ACK 不跟踪重传 |
| pending_ack 槽满 | reliability_register 返回 `ROUTE_ERR_FULL` |

### 2.4 ACK 帧处理

| 用例 | 验证点 |
|------|--------|
| 收到 ACK 帧 | 不触发 complete_cb，不进入重组 |
| ACK 匹配清除 | 对应 src+seq 的所有 pending_ack 被清除 |

---

## 3. Transaction 层测试

### 3.1 异步发送

| 用例 | 验证点 |
|------|--------|
| 正常发送 | lower_send 被调用，trans_id = slot index |
| 收到 RESPONSE | callback(ROUTE_OK, data, len) 被调用 |
| 超时 | callback(ROUTE_ERR_TIMEOUT, NULL, 0) 被调用，stats.trans_timeouts++ |
| transaction 槽满 | 返回 `ROUTE_ERR_FULL` |
| lower_send 失败 | slot 释放，返回错误码 |

### 3.2 同步发送

| 用例 | 验证点 |
|------|--------|
| 正常请求-响应 | sem_wait 返回后 resp_buf 包含响应数据 |
| 超时 | 返回 `ROUTE_ERR_TIMEOUT` |
| 无 OS 层 | 返回 `ROUTE_ERR_PARAM` |
| 响应数据超过 resp_max_len | 截断到 resp_max_len |

### 3.3 Reply

| 用例 | 验证点 |
|------|--------|
| 正常回复 | lower_send 被调用，type=ROUTE_TYPE_RESPONSE，trans_id 匹配 |

### 3.4 重复/无效响应

| 用例 | 验证点 |
|------|--------|
| trans_id 越界 | 静默丢弃 |
| state != WAITING | stats.drop_duplicate++ |
| src 不匹配 dest_id | stats.drop_duplicate++ |
| 响应到达后再到一次 | 第二次被丢弃 |

### 3.5 并发安全

| 用例 | 验证点 |
|------|--------|
| 多线程 send_async | mutex 保护 slot 分配不冲突 |
| tick 与 on_response 并发 | 不死锁，状态一致 |
| deinit 时有活跃 transaction | 所有等待者被唤醒/回调 |

---

## 4. Stack 层测试（集成）

### 4.1 串联验证

| 用例 | 验证点 |
|------|--------|
| wire(router, frag, trans) | 各层回调正确连接 |
| wire(router, frag, NULL) | 无 transaction，send_sync 返回 ERR_PARAM |
| wire(router, NULL, NULL) | deliver_cb 直接回调 on_recv_cb |
| wire(router, NULL, trans) | transaction 直接构建单帧走 router |

### 4.2 端到端请求-响应

| 用例 | 验证点 |
|------|--------|
| 节点 A send → 节点 B recv → 节点 B reply → 节点 A callback | 完整流程，数据正确 |
| 大消息（需分片） | 分片、重组、ACK、响应，全流程正确 |
| 传输丢包（模拟 port send 随机失败） | 重传后最终成功 |
| 全丢（port 始终失败） | 超时回调 |

### 4.3 广播

| 用例 | 验证点 |
|------|--------|
| broadcast | 所有 port 发送，数据正确 |
| broadcast 数据超 frag_size | 返回 `ROUTE_ERR_PARAM` |

### 4.4 多跳转发

| 用例 | 验证点 |
|------|--------|
| A→B→C | B 转发帧，C 收到并送达 |
| TTL 耗尽 | 中间节点丢弃 |
| 广播多跳 | 每跳转发到其他 port，TTL 递减 |

### 4.5 tick/poll 驱动

| 用例 | 验证点 |
|------|--------|
| 不调 tick | 超时不触发（时间不推进） |
| 不调 poll | 队列中的帧不被处理 |
| tick + poll 组合 | 完整驱动所有超时和接收处理 |

---

## 5. 边界与异常

| 用例 | 验证点 |
|------|--------|
| 所有 init 传 NULL | 返回 `ROUTE_ERR_PARAM`，不崩溃 |
| config 字段为 0 | init 拒绝 |
| deinit 后再调用 API | 不崩溃（行为未定义但不 crash） |
| payload 恰好等于 max_payload | 正常处理 |
| payload 超过 max_payload | 被拒绝 |
| frag_size = 1 | 极端分片数，正常工作 |
| 连续快速发送至 pending_ack 满 | 返回 `ROUTE_ERR_FULL`，不泄漏 |

---

## 6. 多节点拓扑测试

### 6.1 星型拓扑（1 中心 + N 叶子）

```
    [B]
     |
[A]--[Center]--[C]
     |
    [D]
```

Center 有 N 个 port，每个 port 连接一个叶子节点。叶子只有 1 个 port。

| 用例 | 验证点 |
|------|--------|
| 叶子 A → 叶子 B（经 Center 转发） | Center 正确查路由表从 portA→portB 转发 |
| 叶子 A → 广播 | Center 转发到所有其他 port，B/C/D 均收到 |
| 所有叶子同时向 Center 发请求 | Center 的 recv_queue 不溢出，全部处理 |
| 叶子 A → 叶子 B 大消息（需分片） | 多分片均经 Center 正确转发，B 重组成功 |
| Center 宕机（不 poll） | 叶子 A 发送超时，callback 报 TIMEOUT |
| 叶子 A → 不存在的节点 | Center 返回 drop_no_route |

### 6.2 链式拓扑（多跳）

```
[A] --port0-- [B] --port0-- [C] --port0-- [D]
       port1        port1        port1
```

每个中间节点有 2 个 port，分别连左右邻居。

| 用例 | 验证点 |
|------|--------|
| A → D（3 跳） | B 和 C 均正确转发，D 收到，TTL 递减 3 |
| A → D，TTL=2 | C 处 TTL 耗尽丢弃，D 收不到 |
| A → D 大消息 | 每片都经 B→C→D 完整链路，D 重组成功 |
| A → D 请求-响应 | D 的 RESPONSE 沿原路 D→C→B→A 返回 |
| 中间节点 B 丢包（port send 偶尔失败） | reliability 重传，最终成功 |
| A → D 广播 | 沿链逐跳转发，每个节点都收到且不重复（seen_table 去重） |
| 广播在链中 TTL 耗尽 | 超过 TTL 的节点不再转发 |
| 双向同时发送（A→D 和 D→A） | 中间节点正确双向转发，不冲突 |

### 6.3 树形拓扑

```
         [Root]
        /      \
     [B]       [C]
    /   \        \
  [D]   [E]     [F]
```

Root 有 2 port，B 有 3 port（上+左下+右下），C 有 2 port。

| 用例 | 验证点 |
|------|--------|
| D → F（跨子树：D→B→Root→C→F） | 4 跳转发，每跳路由表查询正确 |
| Root 广播 | 树中所有节点收到，无重复 |
| D 广播 | 上行经 B→Root，Root 转发到 C 子树，所有节点收到 |
| 同一子树内通信（D→E 经 B） | B 本地转发，不经 Root |
| F → D 请求-响应 | 请求上行到 Root 再下行到 D，响应反向 |
| 大消息跨子树 | 分片均正确沿路径转发并重组 |
| 子树断开（B 的下行 port 故障） | D/E 不可达，Root 侧报 drop_no_route 或重传超时 |

### 6.4 拓扑通用场景

| 用例 | 验证点 |
|------|--------|
| 广播风暴抑制 | 环形/网状拓扑中 seen_table 阻止无限转发 |
| 多路径（A→C 可经 B 也可经 D） | 按路由表走固定路径，不分叉 |
| 路由表动态更新 | route_table_set 后新路径立即生效 |
| 大规模节点（32 节点链） | TTL 足够大时端到端可达 |
| 并发：多节点同时发往同一目标 | 目标节点 recv_queue 不溢出，或正确报 drop_queue_full |

---

## 7. 测试环境要求

- **Mock port**：记录所有 send 调用的 (port_id, data, len)，可配置返回值（成功/失败/随机丢包）
- **Mock OS**：提供 mutex/sem 的简单实现（单线程下用标志位，多线程用 pthread）
- **时间控制**：手动推进 `now_ms`，不依赖真实时钟
- **多节点仿真框架**：
  - 支持任意数量节点实例
  - port send 通过连接表路由到对端节点的 `route_router_input` / `route_stack_input`
  - 支持配置丢包率、延迟
  - 支持拓扑动态变更（断开/恢复链路）
  - 提供 `sim_tick_all(nodes, count, now_ms)` 和 `sim_poll_all(nodes, count)` 驱动所有节点
- **拓扑构建辅助**：
  - `sim_connect(node_a, port_a, node_b, port_b)` — 双向连接两个节点的指定 port
  - `sim_disconnect(node_a, port_a)` — 断开链路
  - `sim_build_star(center, leaves[], count)` — 快速构建星型
  - `sim_build_chain(nodes[], count)` — 快速构建链式
  - `sim_build_tree(root, children_map)` — 快速构建树形
