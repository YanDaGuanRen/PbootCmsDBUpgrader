import sqlite3
import re
import os
import csv

# 配置部分
# 源数据库路径，指定要从中迁移数据的SQLite数据库文件
source_db_path = r'96a553cec1f65d15532da4e6c374fcc2.db'
# 目标数据库路径，指定迁移后数据存储的SQLite数据库文件
target_db_path = r'd7ddfc11d8c32226ce93010cef7ba66b.db'
# 图片链接映射CSV文件路径，用于记录和保存图片链接的映射关系
csv_file = r'image_links.csv'
# 新图片URL前缀，用于替换原始图片链接的前缀部分
# 如果设置为空字符串或None，则不进行图片链接替换，仅记录映射关系
new_image_url = '/static/upfiles/old/'

# 加载图片链接映射表
# 从指定的CSV文件中读取图片链接映射关系，如果文件不存在则返回空字典
# 参数:
#   csv_file (str): 图片链接映射CSV文件的路径
# 返回:
#   dict: 包含原始链接到新链接映射关系的字典

def load_image_mapping(csv_file):
    image_mapping = {}
    if os.path.exists(csv_file):
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过表头
            for row in reader:
                old_url, new_url, source_content_id, target_content_id = row
                image_mapping[old_url] = new_url
    return image_mapping

# 替换content中的图片链接
# 使用提供的映射字典替换HTML内容中的图片链接
# 参数:
#   content (str): 包含图片链接的HTML内容
#   image_mapping (dict): 图片链接映射字典
# 返回:
#   str: 替换后的HTML内容

def replace_image_links(content, image_mapping):
    def replacer(match):
        old_url = match.group(1)
        # 保持原始链接不变
        return f'src="{image_mapping.get(old_url, old_url)}"'

    # 匹配src属性的图片链接
    pattern = r'src=["\']([^"\']*)["\']'
    new_content = re.sub(pattern, replacer, content)
    return new_content

# 记录content中的图片链接并生成映射
# 提取HTML内容中的图片链接，生成新的链接映射并替换原始链接
# 参数:
#   content (str): 包含图片链接的HTML内容
#   content_id (int): 内容ID
#   new_image_url (str): 新图片URL前缀
# 返回:
#   tuple: (替换后的HTML内容, 图片链接映射列表)

def record_image_links(content, content_id, new_image_url):
    image_mapping_list = []
    new_content = content
    # 匹配src属性的图片链接
    pattern = r'src=["\']([^"\']*)["\']'
    matches = re.findall(pattern, content)
    
    for old_url in matches:
        # 使用原始链接
        full_old_url = old_url
        
        # 如果new_image_url未设置（为空或None），则不替换图片URL
        if new_image_url:
            # 生成新文件名
            filename = os.path.basename(full_old_url)
            new_url = f'{new_image_url}{filename}'
            image_mapping_list.append((full_old_url, new_url))
            
            # 替换content中的链接
            new_content = new_content.replace(f'src="{old_url}"', f'src="{new_url}"')
        else:
            # 如果不替换URL，仍然需要记录原始链接
            image_mapping_list.append((full_old_url, full_old_url))
    
    return new_content, image_mapping_list


# 主迁移函数
# 执行数据库内容迁移和图片链接处理的主函数
# 1. 连接源和目标数据库
# 2. 读取源数据库中指定类别的内容
# 3. 处理每条内容的字段和图片链接
# 4. 将处理后的内容插入目标数据库
# 5. 保存图片链接映射关系到CSV文件
def migrate_content():
    # 删除已存在的CSV文件
    if os.path.exists(csv_file):
        os.remove(csv_file)
    
    # 连接源数据库
    source_conn = sqlite3.connect(source_db_path)
    source_cursor = source_conn.cursor()
    
    # 连接目标数据库
    target_conn = sqlite3.connect(target_db_path)
    target_cursor = target_conn.cursor()
    
    # 获取源表的列名
    source_cursor.execute("PRAGMA table_info(ay_content)")
    source_columns = [info[1] for info in source_cursor.fetchall()]
    
    # 获取目标表的列名
    target_cursor.execute("PRAGMA table_info(ay_content)")
    target_columns = [info[1] for info in target_cursor.fetchall()]
    
    # 确定共同的列，但排除'id'字段
    common_columns = list(set(source_columns) & set(target_columns))
    if 'id' in common_columns:
        common_columns.remove('id')
    columns_str = ', '.join(common_columns)
    placeholders_str = ', '.join(['?' for _ in common_columns])
    
    # 加载图片链接映射表
    image_mapping = load_image_mapping(csv_file)
    
    # 查询源数据库中的所有数据
    source_cursor.execute("SELECT * FROM ay_content WHERE scode = '3'")
    rows = source_cursor.fetchall()
    
    # 用于记录所有图片链接
    all_image_mappings = []
    
    # 处理每行数据
    for row in rows:
        row_dict = dict(zip(source_columns, row))
     
        # 确保subscode不是None
        if row_dict.get('subscode') is None:
            row_dict['subscode'] = '默认subscode'
        
        # 确保titlecolor不是None
        if row_dict.get('titlecolor') is None:
            row_dict['titlecolor'] = '#333333'
        
        # 确保subtitle不是None
        if row_dict.get('subtitle') is None:
            row_dict['subtitle'] = '默认subtitle'
        
        # 确保filename不是None
        if row_dict.get('filename') is None:
            row_dict['filename'] = '默认filename'
        
        # 确保source不是None
        if row_dict.get('source') is None:
            row_dict['source'] = '默认'
        
        # 确保outlink不是None
        if row_dict.get('outlink') is None:
            row_dict['outlink'] = '默认outlink'
        
        # 确保likes不是None
        if row_dict.get('likes') is None:
            row_dict['likes'] = 0
        
        # 确保ico不是None
        if row_dict.get('ico') is None:
            row_dict['ico'] = '默认ico'
        
        # 确保oppose不是None
        if row_dict.get('oppose') is None:
            row_dict['oppose'] = 0
        
        # 确保pics不是None
        if row_dict.get('pics') is None:
            row_dict['pics'] = '默认pics'
        
        # 确保create_user不是None
        if row_dict.get('create_user') is None:
            row_dict['create_user'] = 'admin'
        
        # 确保tags不是None
        if row_dict.get('tags') is None:
            row_dict['tags'] = '默认tags'
        
        # 确保create_time不是None
        if row_dict.get('create_time') is None:
            row_dict['create_time'] = 0
        
        # 确保enclosure不是None
        if row_dict.get('enclosure') is None:
            row_dict['enclosure'] = '默认enclosure'

        
        # 确保isrecommend不是None
        if row_dict.get('isrecommend') is None:
            row_dict['isrecommend'] = 0
        
        # 确保isheadline不是None
        if row_dict.get('isheadline') is None:
            row_dict['isheadline'] = 0
        
        # 确保update_user不是None
        if row_dict.get('update_user') is None:
            row_dict['update_user'] = 'admin'
        
        # 确保update_time不是None
        if row_dict.get('update_time') is None:
            row_dict['update_time'] = 0
        
        # 确保subscode不是None
        if row_dict.get('subscode') is None:
            row_dict['subscode'] = '默认subscode' 
        
        # 保存原始title
        original_title = row_dict['title']
        
        # 处理content字段中的图片链接
        if 'content' in row_dict and row_dict['content'] is not None:
            # 先记录图片链接
            new_content, image_mapping_list = record_image_links(row_dict['content'], row_dict['id'], new_image_url)
            # 再替换图片链接
            new_content = replace_image_links(new_content, image_mapping)
            row_dict['content'] = new_content
        
        # 准备插入数据，排除'id'字段
        insert_values = [row_dict[col] for col in common_columns]
        
        # 插入数据到目标数据库
        try:
            target_cursor.execute(f"INSERT INTO ay_content ({columns_str}) VALUES ({placeholders_str})", insert_values)
            # 获取插入记录的ID
            target_content_id = target_cursor.lastrowid
            # 更新图片映射列表，添加目标内容ID
            if 'content' in row_dict and row_dict['content'] is not None:
                updated_image_mapping_list = [(old_url, new_url, row_dict['id'], target_content_id) for old_url, new_url in image_mapping_list]
                all_image_mappings.extend(updated_image_mapping_list)
        except sqlite3.IntegrityError as e:
            print(f"插入数据时出错 (ID: {row_dict.get('id', 'unknown')}): {e}")
        
        

    
    # 提交事务
    target_conn.commit()
    
    # 保存图片链接到CSV文件
    # 去重
    unique_image_mappings = list(set(all_image_mappings))
    # 确保目录存在
    csv_dir = os.path.dirname(csv_file)
    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['原始链接', '新链接', '源内容ID', '目标内容ID'])  # 写入表头
        writer.writerows(unique_image_mappings)
    

    
    # 关闭数据库连接
    source_conn.close()
    target_conn.close()
    
    print(f'迁移完成，共处理 {len(rows)} 条数据')
    print(f'图片链接已保存到 {csv_file}')

# 程序入口

if __name__ == '__main__':
    migrate_content()