import sqlite3
import os
import re
import requests
from urllib.parse import urlparse, urljoin
from datetime import datetime
import csv

# 配置信息
# 源数据库文件路径
SOURCE_DB_PATH = r'c:\Users\t1835\Desktop\sql\96a553cec1f65d15532da4e6c374fcc2.db'
# 目标数据库文件路径
TARGET_DB_PATH = r'c:\Users\t1835\Desktop\sql\d7ddfc11d8c32226ce93010cef7ba66b.db'
# 是否替换图片链接
REPLACE_IMAGES = True
# 是否下载图片
# 只有当 REPLACE_IMAGES 为 True 时，DOWNLOAD_IMAGES 为 True 才会下载图片
DOWNLOAD_IMAGES = True
# 新图片链接的基础URL
IMAGE_NEW_BASE_URL = "/static/upfiles/old/"
# 图片保存目录
IMAGE_SAVE_DIR = r"c:\Users\t1835\Desktop\sql\upfiles"
# 是否在图片重名时添加时间戳
IMAGE_RENAME_ON_CONFLICT = True
# 图片链接CSV文件路径
CSV_FILE_PATH = r'c:\Users\t1835\Desktop\sql\image_links.csv'
# 图片映射文件路径
IMAGE_MAPPING_FILE_PATH = r'c:\Users\t1835\Desktop\sql\image_mapping.txt'
# 查询条件
# SCODE = '-1' 表示不添加 scode 条件
SCODE = '3'
# ACODE = '-1' 表示不添加 acode 条件
ACODE = 'cn'



def load_image_mapping(csv_file):
    """加载图片链接映射表"""
    image_mapping = {}
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            image_mapping[row['原始链接']] = row['新链接']
    return image_mapping

def replace_image_links(content, image_mapping):
    """使用映射表替换content中的图片链接"""
    # 遍历映射表中的每个原始链接和新链接
    for original_link, new_link in image_mapping.items():
        # 使用正则表达式匹配各种形式的src属性
        # 匹配 src="..." 或 src='...' 或 src=...
        pattern = rf'src=("{re.escape(original_link)}"|\'{re.escape(original_link)}\'|{re.escape(original_link)})(?=\s|/?>)'
        # 替换为新的链接
        content = re.sub(pattern, f'src="{new_link}"', content)
    return content

def record_image_links(content):
    # 查找所有图片链接
    img_pattern = r'<img[^>]*src=(?:"([^"]*)"|\'([^\']*)\'|([^>\s]*))[^>]*/?>'
    matches = re.findall(img_pattern, content, re.IGNORECASE)
    
    # 提取匹配的链接
    links = []
    for match in matches:
        # 选择第一个非空的匹配组
        link = next((group for group in match if group), '')
        if link:
            links.append(link)
    
    # 创建图片映射列表
    image_mapping = []
    
    # 处理每个图片链接
    for img_src in links:
        original_src = img_src
        
        # 判断是否为网络图片
        if img_src.startswith('http'):
            # 网络图片
            parsed_url = urlparse(img_src)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                filename = f"image_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"
            
            # 生成新的本地路径
            new_src = f"{IMAGE_NEW_BASE_URL}{filename}"
            image_mapping.append((original_src, new_src, ""))
        # 本地图片或相对路径图片不再处理
    
    return content, image_mapping

def download_image(url, save_path):
    """下载图片并保存到指定路径"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(response.content)
        return True
    except Exception as e:
        print(f"下载图片失败: {url}, 错误: {e}")
        return False

def migrate_content():
    replace_images = REPLACE_IMAGES
    # 数据库路径
    source_db = SOURCE_DB_PATH
    target_db = TARGET_DB_PATH
    
    # CSV映射文件路径
    csv_file = CSV_FILE_PATH
    
    # 加载图片链接映射表
    image_mapping = {}
    if replace_images:
        image_mapping = load_image_mapping(csv_file)
    
    # 创建保存图片的目录
    if replace_images and DOWNLOAD_IMAGES:
        os.makedirs(IMAGE_SAVE_DIR, exist_ok=True)
    
    # 连接源数据库
    source_conn = sqlite3.connect(source_db)
    source_cursor = source_conn.cursor()
    
    # 查询源数据库中acode='cn'且scode='3'的所有数据
    query = "SELECT * FROM ay_content"
    conditions = []
    if ACODE != '-1':
        conditions.append(f"acode='{ACODE}'")
    if SCODE != '-1':
        conditions.append(f"scode='{SCODE}'")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    source_cursor.execute(query)
    rows = source_cursor.fetchall()

    # 获取源数据库列名
    source_column_names = [description[0] for description in source_cursor.description]

    # 创建一个字典来保存原始title值，以ID为key
    original_titles = {}
    
    for i in range(len(rows)):
        row_dict = dict(zip(source_column_names, rows[i]))
        # 保存原始title值到字典中
        if 'id' in row_dict and row_dict['id'] and 'title' in row_dict:
            original_titles[row_dict['id']] = row_dict['title']
    
    # 关闭源数据库连接
    source_conn.close()
    
    # 连接目标数据库
    target_conn = sqlite3.connect(target_db)
    target_cursor = target_conn.cursor()
    
    # 获取目标数据库列名
    target_cursor.execute("PRAGMA table_info(ay_content);")
    target_columns_info = target_cursor.fetchall()
    target_column_names = [column[1] for column in target_columns_info]
    
    # 确定共同的列名，但不包含'id'列
    common_columns = [col for col in source_column_names if col in target_column_names and col != 'id']
    
    # 处理每行数据
    processed_rows = []
    all_image_mappings = []
    
    for i, row in enumerate(rows):
        # 转换为字典
        row_dict = dict(zip(source_column_names, row))
        
        # 恢复原始title值
        if 'id' in row_dict and row_dict['id'] in original_titles:
            original_title = row_dict['title']
            row_dict['title'] = original_titles[row_dict['id']]
        
        # 确保 title 字段不为空
        if 'title' in row_dict and not row_dict['title']:
            row_dict['title'] = '默认标题'
        
        # 确保 filename 字段不为空
        if 'filename' in row_dict and not row_dict['filename']:
            row_dict['filename'] = '默认文件名'
        
        # 确保 oppose 字段不为空
        if 'oppose' in row_dict and not row_dict['oppose']:
            row_dict['oppose'] = '0'
        
        # 确保 create_user 字段不为空
        if 'create_user' in row_dict and not row_dict['create_user']:
            row_dict['create_user'] = '默认用户'
        
        # 确保 date 字段不为空
        if 'date' in row_dict and not row_dict['date']:
            row_dict['date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 确保 update_user 字段不为空
        if 'update_user' in row_dict and not row_dict['update_user']:
            row_dict['update_user'] = '默认用户'
        
        # 确保 pics 字段不为空
        if 'pics' in row_dict and not row_dict['pics']:
            row_dict['pics'] = ''
        
        # 确保 content 字段不为空
        if 'content' in row_dict and not row_dict['content']:
            row_dict['content'] = ''
        
        # 确保 update_time 字段不为空
        if 'update_time' in row_dict and not row_dict['update_time']:
            row_dict['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 确保 enclosure 字段不为空
        if 'enclosure' in row_dict and not row_dict['enclosure']:
            row_dict['enclosure'] = ''
        
        # 确保 keywords 字段不为空
        if 'keywords' in row_dict and not row_dict['keywords']:
            row_dict['keywords'] = ''
        
        # 确保 isheadline 字段不为空
        if 'isheadline' in row_dict and not row_dict['isheadline']:
            row_dict['isheadline'] = '0'
        
        # 确保 visits 字段不为空
        if 'visits' in row_dict and not row_dict['visits']:
            row_dict['visits'] = '0'
        
        # 确保 create_time 字段不为空
        if 'create_time' in row_dict and not row_dict['create_time']:
            row_dict['create_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 确保 gtype 字段不为空
        if 'gtype' in row_dict and not row_dict['gtype']:
            row_dict['gtype'] = '0'
        
        # 确保 subscode 字段不为空
        if 'subscode' in row_dict and not row_dict['subscode']:
            row_dict['subscode'] = '默认subscode'
        
        # 确保 likes 字段不为空
        if 'likes' in row_dict and not row_dict['likes']:
            row_dict['likes'] = '0'
        
        # 确保 subtitle 字段不为空
        if 'subtitle' in row_dict and not row_dict['subtitle']:
            row_dict['subtitle'] = '默认subtitle'
        
        # 确保 outlink 字段不为空
        if 'outlink' in row_dict and not row_dict['outlink']:
            row_dict['outlink'] = '默认outlink'
        
        # 确保 ico 字段不为空
        if 'ico' in row_dict and not row_dict['ico']:
            row_dict['ico'] = '默认ico'
        
        # 确保 tags 字段不为空
        if 'tags' in row_dict and not row_dict['tags']:
            row_dict['tags'] = '默认tags'
        
        # 确保 isrecommend 字段不为空
        if 'isrecommend' in row_dict and not row_dict['isrecommend']:
            row_dict['isrecommend'] = '0'
        
        # 处理content字段中的图片链接
        if replace_images and 'content' in row_dict and row_dict['content']:
            # 使用CSV映射表替换图片链接
            row_dict['content'] = replace_image_links(row_dict['content'], image_mapping)
            # 记录图片链接映射
            new_content, image_mapping_list = record_image_links(row_dict['content'])
            row_dict['content'] = new_content
            all_image_mappings.extend(image_mapping_list)
            
            # 下载网络图片
            if DOWNLOAD_IMAGES:
                # 创建保存图片的目录
                os.makedirs(IMAGE_SAVE_DIR, exist_ok=True)
                for original, new, local_path in image_mapping_list:
                    if original.startswith('http'):
                        filename = os.path.basename(original)
                        save_path = os.path.join(IMAGE_SAVE_DIR, filename)
                        
                        # 检查文件是否已存在
                        if os.path.exists(save_path):
                            if IMAGE_RENAME_ON_CONFLICT:
                                # 添加时间戳到文件名
                                name, ext = os.path.splitext(filename)
                                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                                filename = f"{name}_{timestamp}{ext}"
                                save_path = os.path.join(IMAGE_SAVE_DIR, filename)
                            else:
                                # 如果不重命名且文件已存在，则跳过下载
                                continue
                        
                        download_image(original, save_path)
        
        # 重新组合行数据，只包含共同的列
        processed_row = [row_dict.get(col, '') for col in common_columns]
        processed_rows.append(processed_row)
    
    # 写入图片映射列表到文件
    if replace_images:
        with open(IMAGE_MAPPING_FILE_PATH, 'w', encoding='utf-8') as f:
            f.write("原始链接|新链接|本地路径\n")
            for original, new, local_path in all_image_mappings:
                f.write(f"{original}|{new}|{local_path}\n")
    
    # 插入数据，只包含共同的列
    placeholders = ','.join(['?' for _ in common_columns])
    insert_sql = f"INSERT INTO ay_content ({','.join(common_columns)}) VALUES ({placeholders})"
    
    for row in processed_rows:
        try:
            target_cursor.execute(insert_sql, row)
        except Exception as e:
            pass
    
    # 提交更改并关闭连接
    target_conn.commit()
    target_conn.close()
    
    print(f"迁移完成，共处理 {len(processed_rows)} 条数据")
    print(f"图片映射列表已保存到 {IMAGE_MAPPING_FILE_PATH}")

if __name__ == "__main__":
    migrate_content()