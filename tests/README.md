# fmdisk 测试框架开发者指南

## 概述

fmdish 的集成测试由一个名为 `fmtest` 的测试运行器驱动。该运行器会解析并执行位于 `fmdisk/tests/scripts/` 目录下的 `.fmtest` 脚本文件。

每个脚本都模拟一系列用户与 FUSE 文件系统的交互，并通过一个模拟的后端存储（`mock_backend`）来验证文件系统的行为，从而无需依赖真实的远端服务。

测试的核心思想是：

1.  **定义场景**: 在 `.fmtest` 脚本中按顺序编写命令，模拟用户操作。
2.  **执行操作**: 调用 FUSE 文件系统接口，如创建文件、写入数据、重命名等。
3.  **验证状态**: 对文件属性（如大小、类型）和后端存储的最终状态（如对象内容、元数据）进行断言。

## 如何编写一个新的测试用例

编写新的测试用例非常简单，因为构建系统会自动发现并运行所有新的测试脚本。

### 第 1 步：创建测试脚本文件

在 `fmdisk/tests/scripts/` 目录下创建一个新文件，并以 `.fmtest` 为扩展名。例如：

`01_my_new_feature.fmtest`

测试脚本会按文件名的字母顺序执行，因此建议使用数字前缀来控制执行顺序。

### 第 2 步：编写测试脚本

在新的 `.fmtest` 文件中，编写一系列命令来定义你的测试场景。一个典型的测试流程如下：

1.  **环境清理 (Setup)**: 使用 `BACKEND_RESET` 和 `BACKEND_CLEAR_CACHE` 确保测试环境是干净的。
2.  **挂载文件系统 (Mount)**: 使用 `MOUNT` 命令来初始化和挂载 fmdisk。
3.  **执行文件操作 (Operations)**: 调用 `MKDIR`, `CREATE`, `WRITE`, `READ`, `RENAME`, `UNLINK` 等命令来模拟对文件系统的操作。
4.  **进行断言 (Assertions)**: 使用 `GETATTR`, `READ`, `BACKEND_EXPECT`, `BACKEND_EXPECT_META` 等命令来验证操作是否符合预期。
5.  **卸载文件系统 (Teardown)**: 使用 `UNMOUNT` 命令卸载文件系统并清理资源。

**示例 (`01_cached_flow.fmtest`)**:

```fmtest
# 这是一个注释。脚本会按顺序执行以下命令。

# 1. 清理环境
BACKEND_CLEAR_CACHE
BACKEND_RESET

# 2. 挂载 (启用缓存)
MOUNT no_cache=false

# 3. 文件操作
MKDIR path="/docs" mode=0755
CREATE path="/docs/report.txt" mode=0644 handle=report
WRITE handle=report data="integration payload"
FLUSH handle=report
RELEASE handle=report

# 4. 断言
GETATTR path="/docs/report.txt" expect_type=file expect_size=19
RENAME old="/docs/report.txt" new="/docs/report_v2.txt"
GETATTR path="/docs/report.txt" expect_error=ENOENT # 验证旧文件已不存在
OPEN path="/docs/report_v2.txt" handle=reader
READ handle=reader size=64 expect_data="integration payload" # 验证重命名后内容不变
RELEASE handle=reader

# 5. 清理
UNMOUNT
BACKEND_CLEAR_CACHE
```

### 第 3 步：运行测试

你**不需要**修改 `CMakeLists.txt`。`fmtest` 运行器会自动包含你的新脚本。

按照以下步骤运行所有测试：

```bash
# 1. 进入或创建构建目录
mkdir -p /path/to/s3disk/build && cd /path/to/s3disk/build

# 2. 如果需要，重新运行 CMake
cmake ..

# 3. 编译项目
make

# 4. 运行所有测试
make test
# 或者使用 ctest
ctest
```

如果只想运行你新写的某个特定测试，可以单独调用 `fmtest` 可执行文件：
```bash
# 假设当前在 build 目录
./fmdisk/tests/fmtest ../fmdisk/tests/scripts/01_my_new_feature.fmtest
```

## 命令参考

脚本采用 `COMMAND key=value ...` 的格式。值可以用单引号或双引号包裹，并支持 C 风格的转义字符（如 `\n`, `\"`）。

> **通用参数**: 大多数文件操作命令都支持 `expect_error=ERR_NAME` 参数，用于断言该操作会返回一个预期的错误码，例如 `expect_error=ENOENT`。

### 一、后端与环境控制

- **MOUNT**: 挂载文件系统。
  - `no_cache` (bool, 可选): 是否禁用缓存，默认为 `false`。
  - `rename_not_supported` (bool, 可选): 是否模拟后端不支持重命名的场景，默认为 `false`。

- **UNMOUNT**: 卸载文件系统。

- **BACKEND_RESET**: 清空模拟后端的所有数据。

- **BACKEND_CLEAR_CACHE**: 删除本地缓存目录。

- **BACKEND_SEED**: 在模拟后端预置一个对象。
  - `path` (string, 必须): 对象路径。
  - `data` (string, 必须): 对象内容。

- **BACKEND_SLEEP**: 脚本休眠。
  - `ms` (int, 必须): 休眠的毫秒数。

### 二、文件与目录操作

- **MKDIR**: 创建目录。
  - `path` (string, 必须): 目录路径。
  - `mode` (int, 必须): 目录权限模式 (例如 `0755`)。

- **RMDIR**: 删除目录。
  - `path` (string, 必须): 目录路径。

- **OPENDIR**: 打开目录。
  - `path` (string, 必须): 目录路径。
  - `handle` (string, 必须): 用于后续操作的句柄名。

- **RELEASEDIR**: 释放目录句柄。
  - `handle` (string, 必须): `OPENDIR` 时获取的句柄名。

- **CREATE**: 创建文件。
  - `path` (string, 必须): 文件路径。
  - `mode` (int, 必须): 文件权限模式 (例如 `0644`)。
  - `handle` (string, 必须): 用于后续操作的句柄名。
  - `flags` (string, 可选): 文件标志，例如 `"RDWR|CREAT"`。

- **OPEN**: 打开文件。
  - `path` (string, 必须): 文件路径。
  - `handle` (string, 必须): 用于后续操作的句柄名。
  - `flags` (string, 可选): 文件标志，例如 `"RDONLY"`。

- **RELEASE**: 释放文件句柄。
  - `handle` (string, 必须): `CREATE` 或 `OPEN` 时获取的句柄名。
  - `sync` (bool, 可选): 是否在释放前执行同步操作。

- **WRITE**: 向文件写入数据。
  - `handle` (string, 必须): 文件句柄名。
  - `data` (string, 必须): 要写入的数据。
  - `offset` (int, 可选): 写入的偏移量，默认为 0。
  - `repeat` (int, 可选): 将 `data` 内容重复写入 N 次。
  - `size` (int, 可选): 写入的总大小（如果大于 `data` 长度，会用 `\0` 填充）。

- **READ**: 从文件读取数据。
  - `handle` (string, 必须): 文件句柄名。
  - `size` (int, 必须): 要读取的字节数。
  - `offset` (int, 可选): 读取的偏移量，默认为 0。

- **TRUNCATE**: 截断文件。
  - `length` (int, 必须): 截断后的文件长度。
  - `path` (string, 可选): 文件路径（与 `handle` 二选一）。
  - `handle` (string, 可选): 文件句柄名（与 `path` 二选一）。

- **UNLINK**: 删除文件。
  - `path` (string, 必须): 文件路径。

- **RENAME**: 重命名文件或目录。
  - `old` (string, 必须): 源路径。
  - `new` (string, 必须): 目标路径。
  - `flags` (int, 可选): 重命名标志。

- **SYMLINK**: 创建符号链接。
  - `target` (string, 必须): 链接指向的目标。
  - `link` (string, 必须): 符号链接本身的路径。

- **FLUSH**: 将文件缓存刷入后端。
  - `handle` (string, 必须): 文件句柄名。

- **FSYNC**: 同步文件数据和元数据。
  - `handle` (string, 必须): 文件句柄名。
  - `data_only` (bool, 可选): 是否只同步数据。

- **UTIMENS**: 修改文件时间戳。
  - `atime_sec` / `atime_nsec`: 访问时间的秒/纳秒。
  - `mtime_sec` / `mtime_nsec`: 修改时间的秒/纳秒。
  - `path` (string, 可选): 文件路径（与 `handle` 二选一）。
  - `handle` (string, 可选): 文件句柄名（与 `path` 二选一）。

### 三、断言与验证

- **GETATTR**: 获取并验证文件属性。
  - `path` (string, 可选): 文件路径（与 `handle` 二选一）。
  - `handle` (string, 可选): 文件句柄名（与 `path` 二选一）。
  - `expect_type` (string, 可选): 期望类型 (`file`, `dir`, `symlink`)。
  - `expect_size` (int, 可选): 期望大小。

- **READ**: 读取并验证文件内容。
  - `expect_data` (string, 可选): 期望读到的确切内容。
  - `expect_bytes` (int, 可选): 期望读到的字节数。

- **WRITE**: 写入并验证返回的字节数。
  - `expect_bytes` (int, 可选): 期望写入的字节数。

- **READLINK**: 读取并验证符号链接的目标。
  - `path` (string, 必须): 符号链接的路径。
  - `size` (int, 必须): 用于读取的缓冲区大小。
  - `expect` (string, 必须): 期望的链接目标内容。

- **STATFS**: 检查文件系统统计信息。
  - `expect_block_size` (int, 可选): 期望的块大小。

- **BACKEND_PATH_EXISTS**: 验证后端是否存在某个路径。
  - `path` (string, 必须): 要检查的对象路径。
  - `expected` (bool, 必须): `true` 或 `false`。

- **BACKEND_EXPECT**: 验证后端对象的完整内容。
  - `path` (string, 必须): 对象路径。
  - `data` (string, 可选): 期望的完整内容。
  - `contains` (string, 可选): 期望内容包含的子字符串。

- **BACKEND_EXPECT_META**: 验证文件 `meta.json` 中的特定字段。
  - `path` (string, 必须): 逻辑文件路径。
  - `field` (string, 必须): JSON 路径 (例如 `blocks[0].key`)。
  - `value` (string, 必须): 期望的字符串值。

- **BACKEND_EXPECT_BLOCK**: 验证文件中特定数据块的内容。
  - `path` (string, 必须): 逻辑文件路径。
  - `block` (int, 必须): 块的索引号。
  - `offset` (int, 可选): 块内偏移量。
  - `data` (string, 可选): 期望的块内容。
  - `data_hex` (string, 可选): 期望的十六进制表示的块内容。
