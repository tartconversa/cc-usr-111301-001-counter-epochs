# 设备累计计数器的纪元差分事实层

本项目保存虚构算力设施的仓库源表、湖文件与Python读取工具。现有工具仅加载输入并显示记录数，不执行事实加工、目录提交、汇总或联接。所有设备、组织和金额均为合成。

## 字段与单位

raw为非负整数，event_seconds为共同实验起点秒数；boot_id区分重启纪元，revision比较只在同sample_id内。counters中声明位宽与增长上界，尚无区间增量输出。

material-index.json记录全部输入内容哈希；fixtures保存原始资料与配置，计算输出可放.workspace-output，原始资料不应被覆盖。

## 运行已有读取器

Python 3.11以上，标准库即可，不需安装第三方依赖。

    python -m src.counter_reader fixtures/samples.csv
    python -m compileall -q src tests
    python -m unittest discover -s tests -v

## ODS→DWD 计数器纪元差分

新增模块（均只依赖标准库）：

* `src/counter_model.py`——纯函数层：解析校验、同键裁决、时刻节点、模 2^bits
  候选差分、按日归属与按日汇总，无文件与数据库副作用；
* `src/counter_store.py`——SQLite 事实层：运行注册表、分区输入指纹、单事务
  邻接替换、遗留 running 的进程恢复；
* `src/counter_pipeline.py`——编排与 CLI；`run_counter_cli.py`、
  `run_counter_demo.py` 为 `python -I` 隔离模式入口，fixtures 仅以路径参数传入。

差分规则：仅同一 `boot_id` 内相邻有效节点可差分；raw 下降时模回绕差是候选而非
结论，超过速率上界（零候选）或上界容许多圈（多个候选）都判 `unknown`，绝不给
负差加绝对值。同刻异值不任意选值，平级 revision 冲突整键隔离。区间统一为
`(start, end]`，首点之前不产生区间；跨日已知增量进入 `UNASSIGNED` 桶，不按时长
拆分。补点/更正按分区指纹增量失效，旧邻接与新邻接在同一事务内替换，旧运行事实
永久可查。

    # 全场景验证（重复、重启、跨日、迟到修订、进程恢复），打印邻接替换前后区间键
    python -I run_counter_demo.py
    # 单步 CLI（产物默认放 .workspace-output，该目录已被 git 忽略）
    python -I run_counter_cli.py run --samples fixtures/samples.csv \
        --counters fixtures/counters.json --db .workspace-output/dwd.db
    python -I run_counter_cli.py facts --db .workspace-output/dwd.db
    python -I run_counter_cli.py daily --db .workspace-output/dwd.db
    python -I run_counter_cli.py runs  --db .workspace-output/dwd.db
    # 复算歧义场景：放宽速率上界会产生新配置版本并重算受影响分区
    python -I run_counter_cli.py run --samples fixtures/samples.csv \
        --counters fixtures/counters.json --db .workspace-output/dwd.db \
        --rate-override bytes=300

隔离模式说明：输入文件目录视为不受信位置，`-I` 使解释器不把 cwd/输入目录加入
模块搜索路径，运行器只显式注入仓库根，防止输入目录中的同名模块（如 `json.py`）
被导入执行。

## 固定容器

    docker build -t counter-epochs:local .
    docker run --rm counter-epochs:local

容器镜像以完整digest固定，默认仅检查输入契约；实现新增分析功能所需依赖由开发者明确登记。
