# PBootCms 数据库迁移工具 1.0 ro 2.0 to 3.x (不服老,屎上雕花系列)

朋友公司的网站找了个网络公司做了个网站三天老头被黑,后来找人看了说版本太老了让升级以下,但又不是二开的没法正常升级,所以找到我让我帮他搞一下数据库所以写了这个脚本,稍微修改一下就可以把所有的数据库迁移到新的数据库我只写了数据量比较大的内容库,还有应该库很多not null字段的，所以我添加了默认值,用个sql删除一下就行。
该工具用于将旧数据库（old.db）中的内容迁移到新数据库（nes.db）中，并处理其中的图片链接。

## 项目结构

- `migrate_content.py`: 主要的迁移脚本，包含所有配置和迁移逻辑
- `nes.db`: 目标数据库文件（新数据库）
- `old.db`: 源数据库文件（旧数据库）
- `image_links.csv`: 图片链接映射文件（CSV格式）
- `image_mapping.txt`: 图片链接映射文件（文本格式）
- `upfiles/`: 存储下载图片的目录

## 配置说明

在`migrate_content.py`脚本中，有以下配置变量：

- `SOURCE_DB_PATH`: 源数据库路径（默认: "old.db"）
- `TARGET_DB_PATH`: 目标数据库路径（默认: "nes.db"）
- `IMAGE_MAPPING_FILE`: 图片映射文件路径（默认: "image_mapping.txt"）
- `IMAGE_LINKS_CSV`: 图片链接CSV文件路径（默认: "image_links.csv"）
- `IMAGE_SAVE_DIR`: 图片保存目录（默认: "upfiles"）
- `IMAGE_NEW_BASE_URL`: 新图片基础URL（默认: "/static/upfiles/old/"）
- `DOWNLOAD_IMAGES`: 是否下载图片（默认: True）
- `IMAGE_RENAME_ON_CONFLICT`: 当图片文件名冲突时是否重命名（默认: True）

## 功能特性

1. **数据迁移**: 从旧数据库迁移内容到新数据库
2. **图片链接替换**: 自动替换内容中的图片链接
3. **图片下载**: 可选择性下载图片到本地
4. **冲突处理**: 当图片文件名冲突时，可选择重命名或跳过

## 使用方法

1. 确保所有文件都在同一目录下
2. 根据需要修改`migrate_content.py`中的配置变量
3. 运行迁移脚本：
   ```
   python migrate_content.py
   ```

## 数据库结构

两个数据库都包含以下主要表：

- `ay_area`: 地区表
- `ay_company`: 公司表
- `ay_config`: 配置表
- `ay_content`: 内容表（主要迁移对象）
- `ay_content_ext`: 内容扩展表
- `ay_label`: 标签表
- `ay_link`: 链接表
- `ay_member`: 会员表
- `ay_message`: 消息表
- `ay_model`: 模型表
- `ay_module`: 模块表
- `ay_role`: 角色表
- `ay_user`: 用户表

注意：新旧数据库在某些表的字段上存在差异，例如`ay_content_ext`和`ay_message`表。

## 图片处理流程

1. 加载图片映射文件（image_mapping.txt 或 image_links.csv）
2. 遍历内容表中的记录
3. 替换内容中的图片链接
4. 如果启用图片下载：
   - 创建保存目录
   - 下载图片到本地
   - 处理文件名冲突（重命名或跳过）

## 注意事项

1. 迁移前请备份数据库文件
2. 根据实际需求调整配置参数
3. 如果图片下载失败，可以重新运行脚本
4. 图片文件名冲突处理：
   - 当`IMAGE_RENAME_ON_CONFLICT`为True时，会在文件名后添加时间戳
   - 当`IMAGE_RENAME_ON_CONFLICT`为False时，会跳过下载