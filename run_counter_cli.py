#!/usr/bin/env python3
"""隔离模式运行器：以 ``python -I`` 等价环境执行计数器转换。

fixtures 属于不受信输入，脚本自身放在仓库根（可信目录），并显式把仓库根
注入 sys.path，使解释器不会从输入文件所在目录加载任何模块。
所有输入路径都以参数形式传入，不依赖当前工作目录。

用法：
    python -I run_counter_demo.py            # 全场景演示（临时库，自动清理）
    python -I run_counter_demo.py run --samples ... --counters ... --db ...
    python -I run_counter_demo.py facts|daily|runs --db ...
"""

import os
import sys

# -I 下 sys.path 不含 cwd；这里只加入仓库根（本脚本所在目录），不加输入目录。
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.counter_pipeline import main as pipeline_main  # noqa: E402


def main():
    pipeline_main(sys.argv[1:])


if __name__ == "__main__":
    main()
