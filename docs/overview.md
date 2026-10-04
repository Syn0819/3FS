# 3FS (Fire-Flyer File System) 项目综述

## 目录

- [0. 核心概念图解](#0-核心概念图解)
  - [0.1 四大组件与协作关系](#01-四大组件与协作关系)
  - [0.2 数据的组织方式](#02-数据的组织方式)
  - [0.3 客户端两种访问方式](#03-客户端两种访问方式)
  - [0.4 元数据的组织](#04-元数据的组织)
- [1. 项目概述](#1-项目概述)
- [2. 系统架构](#2-系统架构)
- [3. 核心模块划分](#3-核心模块划分)
- [4. 数据流与交互关系](#4-数据流与交互关系)
  - [4.1 读操作数据流](#41-读操作数据流)
  - [4.2 写操作数据流](#42-写操作数据流)
  - [4.3 元数据操作流](#43-元数据操作流)
- [5. 关键设计原理](#5-关键设计原理)
  - [5.1 数据组织与访问](#51-数据组织与访问)
  - [5.2 故障恢复的流量均衡](#52-故障恢复的流量均衡)
  - [5.3 元数据存储](#53-元数据存储)
  - [5.4 文件长度一致性](#54-文件长度一致性)
  - [5.5 零拷贝 I/O (IoRing)](#55-零拷贝-io-ioring)
  - [5.6 Chunk Engine](#56-chunk-engine)
- [6. 模块详细分析计划](#6-模块详细分析计划)
- [7. 技术栈总览](#7-技术栈总览)
- [8. 进程与部署](#8-进程与部署)
- [9. 关键源码文件索引](#9-关键源码文件索引)

---

## 0. 核心概念图解

### 0.1 四大组件与协作关系

3FS 由四个核心组件构成，所有组件通过 RDMA 网络互连：

```mermaid
graph TB
    subgraph AppLayer["应用层"]
        App["Application"]
    end

    subgraph ClientLayer["客户端层"]
        FUSE["FUSE Client<br/>标准 POSIX 文件接口<br/>所有应用通用"]
        Native["Native Client<br/>异步零拷贝 I/O (IoRing)<br/>性能关键场景"]
    end

    subgraph ServiceLayer["服务层 — RDMA 网络"]
        Mgmtd["mgmtd<br/>集群管理器<br/>——————<br/>心跳检测 · 成员管理<br/>Chain Table 维护 · 配置分发<br/>Lease 选主"]
        Meta["Meta Service<br/>元数据服务 (无状态)<br/>——————<br/>create/open/rename/unlink<br/>Inode · DirEntry · Session · GC"]
        Storage["Storage Service<br/>存储服务<br/>——————<br/>Chunk Engine (Rust)<br/>CRAQ 链式复制<br/>RDMA 读写 · 数据恢复"]
    end

    subgraph StorageLayer["存储层"]
        FDB["FoundationDB<br/>事务性 KV 存储<br/>SSI 可序列化快照隔离"]
        SSDs["本地 SSD × N"]
    end

    App --> FUSE
    App --> Native
    FUSE -->|"元数据操作"| Meta
    FUSE -->|"数据读写"| Storage
    Native -->|"元数据操作"| Meta
    Native -->|"数据读写"| Storage
    Meta -->|"元数据持久化"| FDB
    Meta -->|"查 Chain/Target"| Storage
    Mgmtd -.->|"心跳 & 配置分发"| Meta
    Mgmtd -.->|"心跳 & 配置分发"| Storage
    Storage --> SSDs
```



- **mgmtd**（集群管理器）：集群的"大脑"，通过心跳和租约管理成员，维护 Chain Table 并分发路由信息。多实例部署，一主多备
- **Meta Service**（元数据服务）：无状态设计，所有元数据存于 FoundationDB，客户端可连接任意实例。提供文件/目录的增删改查
- **Storage Service**（存储服务）：管理本地 SSD，实现 CRAQ 链式复制协议，提供数据的读写和恢复能力
- **FoundationDB**：Apple 开源的事务性 KV 存储，SSI 隔离级别保证元数据的可序列化一致性



### 0.2 数据的组织方式

文件数据从逻辑到物理经过五层映射，最终落到 SSD 上：

```mermaid
graph TD
    File["File 文件<br/>用户可见的逻辑文件"] -->|"按 chunkSize 切分"| Chunk["Chunk 数据块<br/>等大逻辑单元<br/>ChunkId = inode_id + chunk_index"]
    Chunk -->|"chunkIndex % stripeSize"| Stripe["Stripe 条带映射<br/>Chunk 循环分配到<br/>stripeSize 条 Chain"]
    Stripe --> Chain["Chain 复制链<br/>多个 Target 按 CRAQ 组成<br/>Head → Mid → Tail"]
    Chain --> Target["Storage Target<br/>SSD 上的逻辑存储单元<br/>由 Chunk Engine 管理"]
    Target --> SSD["SSD 物理块<br/>Group → File → Disk"]

    style File fill:#4a9eff,color:#fff
    style Chunk fill:#6c5ce7,color:#fff
    style Stripe fill:#fd79a8,color:#fff
    style Chain fill:#e17055,color:#fff
    style Target fill:#00b894,color:#fff
    style SSD fill:#636e72,color:#fff
```





#### Chunk — 文件切分

文件按固定大小（chunkSize，如 1MB）切分为等大的逻辑 Chunk，每个 Chunk 由全局唯一的 ChunkId 标识：

```mermaid
graph LR
    File["File (5.5 MB)"]
    File --> C0["Chunk 0<br/>0 ~ 1MB"]
    File --> C1["Chunk 1<br/>1MB ~ 2MB"]
    File --> C2["Chunk 2<br/>2MB ~ 3MB"]
    File --> C3["Chunk 3<br/>3MB ~ 4MB"]
    File --> C4["Chunk 4<br/>4MB ~ 5MB"]
    File --> C5["Chunk 5<br/>5MB ~ 5.5MB"]

    style File fill:#4a9eff,color:#fff
```





#### Stripe — Chunk 到 Chain 的映射

文件按 chunkSize 切成 Chunk 后，需要决定每个 Chunk 存到哪条 Chain 上。Stripe 就是这个分配规则：

1. 创建文件时，从 Chain Table 中选取 **stripeSize** 条连续 Chain，shuffle 打乱后得到一个 Chain 列表
2. Chunk 按索引对 stripeSize 取模，映射到列表中对应位置的 Chain
3. 每 stripeSize 个 Chunk 为一个周期，循环使用同一组 Chain

```mermaid
graph TD
    subgraph FileChunks["文件 data.bin (8 个 Chunk, stripeSize=4)"]
        direction LR
        C0["Chunk 0"]
        C1["Chunk 1"]
        C2["Chunk 2"]
        C3["Chunk 3"]
        C4["Chunk 4"]
        C5["Chunk 5"]
        C6["Chunk 6"]
        C7["Chunk 7"]
    end

    subgraph ChainList["选取 Chain 2~5, shuffle 后得到 Chain 列表"]
        direction LR
        SL0["列表[0]<br/>= Chain 5"]
        SL1["列表[1]<br/>= Chain 2"]
        SL2["列表[2]<br/>= Chain 4"]
        SL3["列表[3]<br/>= Chain 3"]
    end

    C0 -->|"0 % 4 = 0"| SL0
    C1 -->|"1 % 4 = 1"| SL1
    C2 -->|"2 % 4 = 2"| SL2
    C3 -->|"3 % 4 = 3"| SL3
    C4 -->|"4 % 4 = 0"| SL0
    C5 -->|"5 % 4 = 1"| SL1
    C6 -->|"6 % 4 = 2"| SL2
    C7 -->|"7 % 4 = 3"| SL3

    style SL0 fill:#e74c3c,color:#fff
    style SL1 fill:#2ecc71,color:#fff
    style SL2 fill:#f39c12,color:#fff
    style SL3 fill:#9b59b6,color:#fff
```

结果：**Chunk 0, 4, 8, 12... 都存到 Chain 5；Chunk 1, 5, 9, 13... 都存到 Chain 2**，以此类推。stripeSize=4 意味着文件数据同时分散在 4 条 Chain 上，可以并行读写。3FS 默认 stripeSize=200，即一个文件的数据同时分散在 200 条 Chain 上，充分聚合集群带宽。不同文件选取不同的起始 Chain + 不同的 shuffle 种子，所以不会都挤到同样的 Chain 上。

#### Chain — CRAQ 链式复制

每条 Chain 由多个 Storage Target 按 Head → Middle → Tail 链式排列。写从 Head 沿链传播到 Tail 提交，读可从任意 Target 读取：

```mermaid
graph LR
    subgraph Chain["Chain (3 副本 CRAQ)"]
        H["Head Target<br/>接收写请求<br/>Node A · SSD 1"]
        M["Middle Target<br/>转发写请求<br/>Node B · SSD 2"]
        T["Tail Target<br/>提交写请求<br/>Node C · SSD 3"]
        H -->|"写传播"| M -->|"写传播"| T
    end

    WC["Client (写)"] -->|"写请求只能发到 Head"| H
    RC1["Client (读)"] -->|"读可任选副本"| H
    RC2["Client (读)"] -->|"读可任选副本"| M
    RC3["Client (读)"] -->|"读可任选副本"| T

    style H fill:#e74c3c,color:#fff
    style M fill:#e74c3c,color:#fff
    style T fill:#e74c3c,color:#fff
```



- **写**：Client → Head 获取锁 → 读已提交版本 → 应用更新存为 pending → 逐节点转发 → Tail 提交(pending→committed) → 确认回传 → Head 释放锁
- **读**：可从任意 serving 状态的 Target 读取 committed 版本，读带宽线性扩展
- 每个 SSD 上创建多个 Target，每个 Target 加入不同 Chain，实现负载均衡
- **BIBD 排列**：Chain Table 按均衡不完全区组设计排列，节点故障时读流量均匀分散，避免少数节点成为瓶颈



#### Target — SSD 上的物理存储

每个 Target 由 Chunk Engine（Rust 实现）管理，数据按 Node → Disk → File → Group → Chunk 层级组织：

```mermaid
graph LR
    Node["Node<br/>10~20 块 SSD"] -->|"1:N"| Disk["Disk<br/>30TB"]
    Disk -->|"256 files"| File["Data File<br/>~120GB"]
    File -->|"~960 groups"| Group["Group<br/>256 chunks<br/>256-bit bitset"]
    Group --> Chunk["Chunk<br/>64KB/512KB/4MB"]

    style Node fill:#2d3436,color:#fff
    style Disk fill:#636e72,color:#fff
    style File fill:#b2bec3
    style Group fill:#dfe6e9
    style Chunk fill:#74b9ff
```



- **写采用 COW（写时复制）**：分配新物理块 → 读旧数据 → 应用更新 → 写新位置 → 原子更新元数据，旧数据在引用释放前可安全并发读
- **元数据由 RocksDB 持久化**：chunk_id→chunk_meta、group_id→group_state、chunk_pos→chunk_id
- **内存缓存 MetaCache**：chunk_id→chunk_info，加速查询



### 0.3 客户端两种访问方式

```mermaid
graph TB
    subgraph FUSEPath["FUSE 路径 (通用)"]
        App1["Application"] -->|"read()/write()"| Kernel["Linux Kernel<br/>FUSE 模块"]
        Kernel -->|"内核态↔用户态拷贝"| FUSEDaemon["FUSE Daemon<br/>(hf3fs_fuse_main)"]
    end

    subgraph NativePath["Native 路径 (高性能)"]
        App2["Application"] -->|"hf3fs_prep_io()"| IoRing["IoRing<br/>共享内存环形缓冲区"]
        IoRing -->|"零拷贝"| NativeClient["Native Client<br/>(嵌入 FUSE Daemon)"]
    end

    FUSEDaemon --> StorageClient["Storage Client"]
    NativeClient --> StorageClient
    StorageClient -->|"RDMA"| StorageService["Storage Service"]

    style FUSEPath fill:#ffeaa7
    style NativePath fill:#55efc4
```



- **FUSE Client**：通过 Linux FUSE 内核模块重定向 I/O，标准 POSIX 接口，所有应用通用。缺点：内核态-用户态内存拷贝开销，FUSE 共享队列锁竞争限制并发（约 400K 4KiB reads/s）
- **Native Client**：嵌入 FUSE Daemon，提供异步零拷贝 I/O API。应用通过共享内存的 IoRing 提交 I/O 请求，FUSE Daemon 批量处理，全程零拷贝
  - **Iov**：大块共享内存区域，存放读写数据，InfiniBand 内存注册由客户端管理
  - **Ior**：小型共享环形缓冲区，用户进程通过 SQE 提交请求，daemon 通过 CQE 返回结果
  - 受 Linux io_uring 启发，但运行在用户态（用户进程 ↔ FUSE Daemon），而非用户态 ↔ 内核态



### 0.4 元数据的组织

```mermaid
graph TD
    subgraph FoundationDB["FoundationDB (SSI 事务)"]
        InodeKey["INOD + inode_id (小端编码)<br/>→ 均匀分布到 FDB 节点"]
        DentryKey["DENT + parent_inode_id + name<br/>→ 同一目录连续 key，高效 range query"]
    end

    subgraph InodeData["Inode 内容"]
        Attr["基础属性: uid/gid/perm/time"]
        FileAttr["文件属性: length/chunkSize/stripeSize/chainRange"]
        DirAttr["目录属性: parent_inode/default_layout"]
        SymAttr["符号链接属性: target_path"]
    end

    subgraph DentryData["DirEntry 内容"]
        DentryVal["name → inode_id + inode_type"]
    end

    InodeKey --> InodeData
    DentryKey --> DentryData

    style FoundationDB fill:#4a9eff,color:#fff
    style InodeData fill:#dfe6e9
    style DentryData fill:#dfe6e9
```



- Meta Service 无状态，所有元数据存于 FoundationDB，客户端可连接任意 Meta 实例
- FDB 的 SSI 事务提供可序列化一致性，并发冲突自动重试，无需应用层加锁
- **FileSession** 跟踪以写模式打开的 fd，删除写打开的文件会延迟到所有 fd 关闭
- **GcManager** 后台异步回收已删除文件的 Chunk 数据

---



## 1. 项目概述

3FS 是 DeepSeek 开源的高性能分布式文件系统，专为 AI 训练和推理工作负载设计。它利用现代 SSD 和 RDMA 网络（InfiniBand/RoCE）提供一个共享存储层，使分布式应用无需关心数据局部性即可高效访问存储资源。

**核心特性：**

- **分解式架构**：聚合数千块 SSD 的吞吐和数百个存储节点的网络带宽
- **强一致性**：基于 CRAQ（Chain Replication with Apportioned Queries）实现
- **文件接口**：基于 FoundationDB 的无状态元数据服务，提供标准文件系统语义
- **高性能**：180 节点集群聚合读吞吐达 6.6 TiB/s

**典型工作负载：**

- 数据准备（层次化目录结构管理）
- Dataloader（随机访问训练样本，无需预取/打乱）
- Checkpointing（大规模训练的高吞吐并行检查点）
- KVCache（LLM 推理优化，替代 DRAM 缓存的高吞吐低成本方案）

---



## 2. 系统架构

系统架构和核心概念已在第 0 章图解，这里补充各组件间的网络通信协议：

- **客户端 ↔ Meta/Storage**：通过 RDMA（InfiniBand 或 RoCE），自研 IBSocket + Transport + Serde 框架
- **mgmtd ↔ Meta/Storage**：心跳 RPC（TCP 或 RDMA），分发 RoutingInfo 和配置
- **Meta ↔ FoundationDB**：FDB 客户端库，SSI 事务
- **Storage ↔ Storage**：CRAQ 链式转发（RDMA），数据同步恢复（RDMA）

---



## 3. 核心模块划分

根据代码结构和功能职责，3FS 可划分为以下核心模块：


| 序号  | 模块                                       | 代码路径                                                         | 职责                                 |
| --- | ---------------------------------------- | ------------------------------------------------------------ | ---------------------------------- |
| 1   | **集群管理 (mgmtd)**                         | `src/mgmtd/`                                                 | 集群成员管理、心跳检测、Chain Table 维护、配置分发    |
| 2   | **元数据服务 (meta)**                         | `src/meta/`                                                  | 文件系统语义实现、Inode/目录项管理、Session/GC 管理 |
| 3   | **存储服务 (storage)**                       | `src/storage/`                                               | Chunk 存储、CRAQ 复制、RDMA 读写、数据恢复      |
| 4   | **客户端 (client)**                         | `src/client/`                                                | 元数据客户端、存储客户端、集群管理客户端               |
| 5   | **FUSE 层 (fuse)**                        | `src/fuse/`                                                  | FUSE 接口实现、IoRing 零拷贝 I/O、用户空间文件系统  |
| 6   | **网络/RPC (common/net + common/serde)**   | `src/common/net/`, `src/common/serde/`                       | RDMA 通信、RPC 框架、序列化/反序列化            |
| 7   | **基础库 (common/utils + common/kv + fdb)** | `src/common/`, `src/fdb/`, `src/kv/`                         | 通用工具、KV 引擎抽象、FoundationDB 集成       |
| 8   | **FlatBuffers Schema (fbs)**             | `src/fbs/`                                                   | 各服务的数据结构定义和 RPC 接口定义               |
| 9   | **Chunk Engine (chunk_engine)**          | `src/storage/chunk_engine/` (Rust)                           | SSD 上的 Chunk 存储引擎（独立 Rust 实现）      |
| 10  | **核心服务基础 (core)**                        | `src/core/`                                                  | 服务基础设施、用户管理（各服务共用）                 |
| 11  | **客户端 API (lib)**                        | `src/lib/`                                                   | 对外暴露的 C/C++/Python/Rust 客户端 API    |
| 12  | **辅助服务**                                 | `src/analytics/`, `src/migration/`, `src/monitor_collector/` | 追踪分析、数据迁移、监控采集                     |


---



## 4. 数据流与交互关系



### 4.1 读操作数据流

```mermaid
sequenceDiagram
    participant App as Application
    participant Client as FUSE / Native Client
    participant Meta as Meta Service (无状态)
    participant Storage as Storage Service (CRAQ 任意副本可读)
    participant SSD as SSD (via liburing AIO)

    App->>Client: (1) open() 获取 fd
    Client->>Meta: (2) 获取文件布局信息 (inode → chain列表 + chunk映射)
    Meta-->>Client: 返回数据布局
    Note over Client: (3) 客户端本地计算 chunk_id → chain 映射<br/>无需再访问 Meta
    Client->>Storage: (4) 直接发起 batchRead
    Storage->>SSD: (5) 从本地 Chunk Engine 读取数据
    SSD-->>Storage: 返回数据
    Storage-->>Client: (6) RDMA Write 直接写入客户端内存（零拷贝）
    Client-->>App: 数据就绪
```





### 4.2 写操作数据流

```mermaid
sequenceDiagram
    participant App as Application
    participant Client as Client
    participant Head as Storage Service (Head Target)
    participant Mid as Storage Service (Middle Target)
    participant Tail as Storage Service (Tail Target)

    App->>Client: (1) write()
    Client->>Head: (2) 写请求发送到 Chain Head
    Head->>Client: (3) RDMA Read 拉取写数据
    Note over Head: 获取 Chunk 锁
    Note over Head: (4) 读已提交版本 → 应用更新 → 存为 pending 版本
    Head->>Mid: (5) 转发到 Chain 下一节点
    Mid->>Tail: (6a) 继续转发
    Note over Tail: (6b) Tail 提交：pending → committed
    Tail-->>Mid: 确认回传
    Mid-->>Head: 确认回传
    Note over Head: 释放 Chunk 锁
    Head-->>Client: 写入成功
    Client-->>App: 返回成功
```





### 4.3 元数据操作流

```mermaid
sequenceDiagram
    participant App as Application
    participant Client as Client (任意 Meta Service)
    participant Meta as Meta Service
    participant FDB as FoundationDB

    App->>Client: (1) create/mkdir/rename/unlink etc.
    Client->>Meta: 发起元数据操作
    Meta->>FDB: (2) 开启 FDB 事务操作
    FDB-->>Meta: (3) SSI 事务冲突检测
    alt 无冲突
        FDB-->>Meta: 提交成功
    else 有冲突
        FDB-->>Meta: 冲突，自动重试
        Meta->>FDB: 重新执行事务
        FDB-->>Meta: 提交成功
    end
    Meta-->>Client: (4) 返回结果
    Client-->>App: 操作完成
```



---



## 5. 关键设计原理



### 5.1 数据组织与访问

数据组织的五层映射（File → Chunk → Stripe → Chain → Target → SSD）已在第 0 章图解。这里补充端到端的访问路径：

```mermaid
flowchart TD
    Start["应用 read(fd, offset=2.5MB, len=1MB)"]
    Open["1. open 时已获取 Inode<br/>含 Layout 信息"]
    Calc["2. 本地计算<br/>chunkIndex = 2.5MB / 1MB = 2<br/>chainPos = 2 % 4 = 2<br/>→ Chain 4"]
    ChunkId["3. 构造 ChunkId<br/>= inode_id(0x0a3f) + chunkIndex(2)"]
    RoutingInfo["4. 查 RoutingInfo<br/>Chain 4 → Target 列表<br/>[G1(serving), H2(serving), I3(serving)]"]
    Select["5. 选择 Target (负载均衡)<br/>→ 选择 H2"]
    BatchRead["6. 发送 batchRead RPC<br/>到 Node H 上的 Storage Service"]
    ChunkEngine["7. Storage Service 查 Chunk Engine<br/>MetaCache → ChunkMeta → 物理位置"]
    SSD["8. liburing AIO 从 SSD 读取"]
    RDMA["9. RDMA Write 零拷贝<br/>写入客户端 Iov 内存"]
    Done["10. 应用从 Iov 读取数据"]

    Start --> Open --> Calc --> ChunkId --> RoutingInfo --> Select --> BatchRead --> ChunkEngine --> SSD --> RDMA --> Done

    style Start fill:#2d3436,color:#fff
    style Done fill:#00b894,color:#fff
    style Calc fill:#fdcb6e
    style ChunkEngine fill:#74b9ff
```



核心要点：open 时获取 Layout 后，后续读写可纯本地计算目标位置，无需再访问 Meta。

### 5.2 故障恢复的流量均衡

- 使用"均衡不完全区组设计"（BIBD）使节点故障时重定向流量均匀分散
- 通过整数规划求解器获得最优 Chain Table 排列
- 避免单节点故障导致少数节点成为瓶颈



### 5.3 元数据存储

- 基于 FoundationDB 事务性 KV 存储，元数据服务无状态
- Inode key = "INOD" + inode_id（小端编码，均匀分布到 FDB 节点）
- DirEntry key = "DENT" + parent_inode_id + name（同一目录连续 key 范围，高效 range query）
- 利用 FDB SSI 事务实现并发控制，冲突时自动重试



### 5.4 文件长度一致性

- 客户端定期（默认 5s）上报最大写位置
- close/fsync 时从 Storage 精确查询最后一个 chunk 的长度
- 使用 rendezvous hash 按 inode ID 分配长度更新任务到不同 Meta 服务，减少冲突
- 小文件优化：记录可能使用的 chain 数量，避免查询所有 stripe chain



### 5.5 零拷贝 I/O (IoRing)

- **Iov**：大块共享内存区域，用于零拷贝读写，InfiniBand 内存注册由客户端管理
- **Ior**：小型共享环形缓冲区，类似 Linux io_uring 的通信机制
  - 用户进程入队 I/O 请求
  - 客户端出队批量处理
  - 支持多 ring 并行，避免单 ring 的同步开销
- IoRing 内部结构：SQE (Submission Queue Entry) → 处理 → CQE (Completion Queue Entry)
  - 共享内存 + 信号量实现用户态与 FUSE daemon 间通信



### 5.6 Chunk Engine

- **Rust 实现**（`src/storage/chunk_engine/`），独立编译
- 存储层次：Chunk(64KB/512KB/4MB) → Group(256 chunks) → File(960 groups) → Disk(256 files) → Node
- **Allocator**：内存中管理 chunk 分配/回收，使用 bitset 快速查找空闲位
- **MetaStore**：基于 RocksDB 持久化三种映射：
  - `chunk_id → chunk_meta`
  - `group_id → group_state`
  - `chunk_pos → chunk_id`
- **写操作采用 COW**：分配新位置 → 读旧数据 → 应用更新 → 写新位置 → 原子更新元数据
- 后台线程：allocate_thread 维持活跃 group 池，compact_thread 回收稀疏 group

---



## 6. 模块详细分析计划

后续将逐模块深入分析，每个模块产出一份详细文档：


| 序号  | 模块                           | 分析重点                                                              |
| --- | ---------------------------- | ----------------------------------------------------------------- |
| 1   | **集群管理 (mgmtd)**             | 心跳机制、Chain Table 管理、状态机转换、Lease 选主、RoutingInfo 分发                 |
| 2   | **元数据服务 (meta)**             | Inode/DirEntry 数据模型、事务操作实现、Session 管理、GC 流程、路径解析                  |
| 3   | **存储服务 (storage)**           | StorageOperator、CRAQ 写入/读取流程、ReliableForwarding、数据同步恢复、BufferPool |
| 4   | **Chunk Engine**             | Allocator 实现、MetaStore 实现、COW 流程、Compact 策略、Rust FFI 接口           |
| 5   | **客户端 (client)**             | StorageClient 实现、MetaClient 实现、Target 选择策略、重试机制                   |
| 6   | **FUSE 层 (fuse)**            | IoRing 实现、零拷贝数据流、FUSE Ops 映射、IovTable 管理                          |
| 7   | **网络/RPC**                   | IBSocket 实现、RDMA 通信、Transport 层、Serde 序列化框架、Service 定义            |
| 8   | **基础库**                      | ConfigBase、Coroutine 封装、KVEngine 抽象、FDB 集成、监控框架                   |
| 9   | **FlatBuffers Schema (fbs)** | 数据结构定义、RPC 接口规范、跨模块通信协议                                           |
| 10  | **客户端 API (lib)**            | C API (UsrbIo)、C++ API (IClient)、Python/Rust 绑定、共享内存机制            |


---



## 7. 技术栈总览


| 类别        | 技术                                              |
| --------- | ----------------------------------------------- |
| 核心语言      | C++20 (Coroutines), Rust (Chunk Engine)         |
| 构建系统      | CMake + Cargo (Rust 部分)                         |
| RPC/序列化   | 自研 Serde 框架 + FlatBuffers Schema                |
| 网络        | RDMA (InfiniBand/RoCE), 自研 IBSocket + Transport |
| 元数据存储     | FoundationDB 7.1+ (SSI 事务)                      |
| Chunk 元数据 | RocksDB                                         |
| 内存分配      | jemalloc / mimalloc                             |
| 异步 I/O    | liburing (io_uring)                             |
| 文件系统接口    | FUSE 3.16.1+                                    |
| 协程        | folly coroutines + C++20 协程                     |
| 配置        | TOML (toml11)                                   |
| 监控        | ClickHouse (analytics), Prometheus metrics      |
| 第三方库      | fmt, zstd, folly, gtest, leveldb, pybind11      |


---



## 8. 进程与部署

3FS 包含以下独立可执行进程：


| 进程                       | 入口                                            | 配置文件                          | 职责           |
| ------------------------ | --------------------------------------------- | ----------------------------- | ------------ |
| `mgmtd_main`             | `src/mgmtd/mgmtd.cpp`                         | `mgmtd_main.toml`             | 集群管理器        |
| `meta_main`              | `src/meta/meta.cpp`                           | `meta_main.toml`              | 元数据服务        |
| `storage_main`           | `src/storage/storage.cpp`                     | `storage_main.toml`           | 存储服务         |
| `hf3fs_fuse_main`        | `src/fuse/hf3fs_fuse.cpp`                     | `hf3fs_fuse_main.toml`        | FUSE 客户端守护进程 |
| `monitor_collector_main` | `src/monitor_collector/monitor_collector.cpp` | `monitor_collector_main.toml` | 监控采集器        |


所有服务进程基于 `TwoPhaseApplication<T>` 模板类实现两阶段启动（初始化 → 运行）。

---



## 9. 关键源码文件索引


| 文件                                              | 说明                   |
| ----------------------------------------------- | -------------------- |
| `src/mgmtd/mgmtd.cpp`                           | 集群管理器入口              |
| `src/mgmtd/MgmtdServer.h`                       | 集群管理器服务定义            |
| `src/mgmtd/background/MgmtdChainsUpdater.cc`    | Chain Table 更新逻辑     |
| `src/mgmtd/background/MgmtdHeartbeatChecker.cc` | 心跳检测                 |
| `src/mgmtd/background/MgmtdLeaseExtender.cc`    | Lease 选主             |
| `src/meta/meta.cpp`                             | 元数据服务入口              |
| `src/meta/service/MetaOperator.h`               | 元数据操作核心类             |
| `src/meta/store/Inode.h`                        | Inode 数据结构           |
| `src/meta/store/DirEntry.h`                     | 目录项数据结构              |
| `src/meta/store/Operation.h`                    | 元数据操作基类              |
| `src/meta/components/ChainAllocator.h`          | Chain 分配器            |
| `src/meta/components/GcManager.h`               | GC 管理                |
| `src/meta/components/SessionManager.h`          | Session 管理           |
| `src/storage/storage.cpp`                       | 存储服务入口               |
| `src/storage/service/StorageService.h`          | 存储服务 RPC 接口          |
| `src/storage/service/StorageOperator.h`         | 存储操作核心类              |
| `src/storage/service/ReliableForwarding.cc`     | 可靠转发（CRAQ 链式传播）      |
| `src/storage/service/TargetMap.cc`              | Target 状态管理          |
| `src/storage/sync/ResyncWorker.cc`              | 数据同步恢复               |
| `src/storage/chunk_engine/`                     | Chunk Engine (Rust)  |
| `src/fuse/hf3fs_fuse.cpp`                       | FUSE 客户端入口           |
| `src/fuse/IoRing.h`                             | 零拷贝 I/O 环形缓冲区        |
| `src/fuse/FuseOps.cc`                           | FUSE 操作实现            |
| `src/client/storage/StorageClient.h`            | 存储客户端接口              |
| `src/client/meta/MetaClient.h`                  | 元数据客户端接口             |
| `src/client/mgmtd/MgmtdClient.h`                | 集群管理客户端接口            |
| `src/lib/api/hf3fs.h`                           | C++ 客户端 API          |
| `src/lib/api/hf3fs_usrbio.h`                    | C 客户端 API (USRBIO)   |
| `src/common/net/ib/IBSocket.h`                  | RDMA Socket 实现       |
| `src/common/serde/Serde.h`                      | 序列化框架                |
| `src/fdb/FDBKVEngine.h`                         | FoundationDB KV 引擎封装 |


