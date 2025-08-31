import sqlite3
import os
import re

def delete_result_db(result_db_path):
    """删除结果库文件"""
    if os.path.exists(result_db_path):
        os.remove(result_db_path)
        print(f"已删除结果库: {result_db_path}")
    else:
        print(f"结果库不存在: {result_db_path}")

def migrate_structures(source_db_path, target_db_path, result_db_path):
    """迁移表结构和索引"""
    # 连接数据库
    source_conn = sqlite3.connect(source_db_path)
    result_conn = sqlite3.connect(result_db_path)
    
    # 设置编码为UTF-8
    source_conn.execute('PRAGMA encoding = "UTF-8"')
    result_conn.execute('PRAGMA encoding = "UTF-8"')
    
    # 设置文本工厂以正确处理UTF-8编码
    source_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    result_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    
    source_cursor = source_conn.cursor()
    result_cursor = result_conn.cursor()
    
    # 获取所有表名
    source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = source_cursor.fetchall()
    print(f"源库中的所有表: {tables}")
    
    for table in tables:
        table_name = table[0]
        print(f"当前处理的表: {table_name}")
        
        # 跳过SQLite内部表
        if table_name.startswith('sqlite_'):
            continue
        print(f"未跳过的表: {table_name}")
        
        # 特殊处理ay_content_ext表，直接使用目标库的表结构
        print(f"检查表: {table_name}")
        if table_name == 'ay_content_ext':
            print(f"正在处理表: {table_name} (特殊处理)")
            # 连接目标库以获取表结构
            target_conn_for_structure = sqlite3.connect(target_db_path)
            target_cursor_for_structure = target_conn_for_structure.cursor()
            
            # 获取目标库表结构
            target_cursor_for_structure.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            target_create_table_sql = target_cursor_for_structure.fetchone()[0]
            print(f"目标库CREATE TABLE语句: {target_create_table_sql}")
            
            # 在结果库中创建表
            result_cursor.execute(target_create_table_sql)
            print(f"已创建表: {table_name}")
            
            # 验证结果库中的表结构
            result_cursor.execute(f"PRAGMA table_info({table_name});")
            result_columns = result_cursor.fetchall()
            print(f"结果库中{table_name}表的列信息: {result_columns}")
            
            # 迁移索引
            # 获取目标库的索引
            target_cursor_for_structure.execute(f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='{table_name}';")
            target_indexes = target_cursor_for_structure.fetchall()
            
            # 在结果库中创建索引
            for index_name, index_sql in target_indexes:
                if index_sql:  # 确保索引SQL不为空
                    try:
                        result_cursor.execute(index_sql)
                        print(f"已创建索引: {index_name}")
                    except Exception as e:
                        print(f"创建索引 {index_name} 失败: {e}")
            
            # 关闭目标库连接
            target_conn_for_structure.close()
        else:
            # 获取表结构
            source_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            create_table_sql = source_cursor.fetchone()[0]
            
            # 在结果库中创建表
            result_cursor.execute(create_table_sql)
            print(f"已创建表: {table_name}")
            
            # 获取并迁移索引
            source_cursor.execute(f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='{table_name}';")
            indexes = source_cursor.fetchall()
            
            for index_name, index_sql in indexes:
                if index_sql:  # 确保索引SQL不为空
                    try:
                        result_cursor.execute(index_sql)
                        print(f"已创建索引: {index_name}")
                    except Exception as e:
                        print(f"创建索引 {index_name} 失败: {e}")
    
    # 提交更改并关闭连接
    result_conn.commit()
    source_conn.close()
    result_conn.close()
    print("表结构和索引迁移完成")

def migrate_data(source_db_path, target_db_path, result_db_path):
    """迁移数据"""
    # 连接数据库
    source_conn = sqlite3.connect(source_db_path)
    target_conn = sqlite3.connect(target_db_path)
    result_conn = sqlite3.connect(result_db_path)
    
    # 设置编码为UTF-8
    source_conn.execute('PRAGMA encoding = "UTF-8"')
    target_conn.execute('PRAGMA encoding = "UTF-8"')
    result_conn.execute('PRAGMA encoding = "UTF-8"')
    
    # 设置文本工厂以正确处理UTF-8编码
    source_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    target_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    result_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    
    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()
    result_cursor = result_conn.cursor()
    
    # 获取所有表名
    target_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = target_cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        
        # 跳过SQLite内部表
        if table_name.startswith('sqlite_'):
            continue
        
        # 获取源库表结构
        source_cursor.execute(f"PRAGMA table_info({table_name});")
        source_columns = source_cursor.fetchall()
        source_column_names = [column[1] for column in source_columns]
        
        # 获取目标库表结构
        target_cursor.execute(f"PRAGMA table_info({table_name});")
        target_columns = target_cursor.fetchall()
        target_column_names = [column[1] for column in target_columns]
        
        # 检查结果库中是否存在该表
        result_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not result_cursor.fetchone():
            print(f"结果库中不存在表: {table_name}")
            continue
        
        # 获取结果库表结构
        result_cursor.execute(f"PRAGMA table_info({table_name});")
        result_columns = result_cursor.fetchall()
        result_column_names = [column[1] for column in result_columns]
        
        # 对于ay_content_ext，直接使用目标库的列
        if table_name in ['ay_content_ext']:
            # 直接使用目标库的所有列
            all_needed_columns = list(target_column_names)
            common_columns = all_needed_columns
            
            # 从目标库获取数据时，选择所有需要的列
            target_common_columns = all_needed_columns
            
            # 检查结果库中是否有所有需要的列，如果没有则添加
            print(f"处理表 {table_name} 的列:")
            print(f"  目标库列: {target_column_names}")
            print(f"  结果库中已有的列: {result_column_names}")
            
            # 获取目标库的所有列定义
            target_cursor.execute(f"PRAGMA table_info({table_name});")
            target_columns_info = target_cursor.fetchall()
            target_columns_dict = {col[1]: col for col in target_columns_info}
            
            for col in all_needed_columns:
                if col not in result_column_names:
                    print(f"  需要添加列: {col}")
                    # 获取列的定义
                    # 从目标库获取列定义
                    col_info = target_columns_dict.get(col)
                    
                    # 如果找到了列定义，则添加到结果库
                    if col_info is not None:
                        col_name = col_info[1]
                        col_type = col_info[2]
                        col_notnull = col_info[3]
                        col_default = col_info[4]
                        col_pk = col_info[5]
                        
                        print(f"    列定义: name={col_name}, type={col_type}, notnull={col_notnull}, default={col_default}, pk={col_pk}")
                        
                        # 构造ALTER TABLE语句
                        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                        if col_notnull == 1 and col_default is None and col_pk != 1:
                            # 对于NOT NULL且没有默认值的列，需要特殊处理
                            # 先添加列时不设置NOT NULL约束
                            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                            if col_default is not None:
                                alter_sql += f" DEFAULT {col_default}"
                            if col_pk == 1:
                                alter_sql += " PRIMARY KEY"
                            
                            try:
                                result_cursor.execute(alter_sql)
                                print(f"已添加列 {col_name} 到表 {table_name} (暂未设置NOT NULL约束)")
                                
                                # 然后更新现有行的该列值为默认值
                                update_sql = f"UPDATE {table_name} SET {col_name} = ?"
                                default_value = '' if 'TEXT' in col_type.upper() else 0
                                result_cursor.execute(update_sql, (default_value,))
                                
                                # 最后添加NOT NULL约束
                                # SQLite不支持直接修改列属性，需要重建表
                                print(f"注意：列 {col_name} 的NOT NULL约束需要手动处理")
                            except Exception as e:
                                print(f"添加列 {col_name} 到表 {table_name} 时出错: {e}")
                        else:
                            # 正常情况下的ALTER TABLE语句
                            if col_notnull == 1:
                                alter_sql += " NOT NULL"
                            if col_default is not None:
                                alter_sql += f" DEFAULT {col_default}"
                            if col_pk == 1:
                                alter_sql += " PRIMARY KEY"
                            
                            try:
                                result_cursor.execute(alter_sql)
                                print(f"已添加列 {col_name} 到表 {table_name}")
                            except Exception as e:
                                print(f"添加列 {col_name} 到表 {table_name} 时出错: {e}")
                    else:
                        print(f"无法找到列 {col} 的定义")
                else:
                    print(f"  列 {col} 已存在")
        else:
            # 对于其他表，找到源库、目标库和结果库的共同列
            common_columns = list(set(source_column_names) & set(target_column_names) & set(result_column_names))
            target_common_columns = common_columns
        
        if not common_columns:
            print(f"表 {table_name} 在源库、目标库和结果库中没有共同的列")
            continue
        
        # 从目标库获取数据
        # 对于ay_content_ext表，也直接从目标库获取数据
        target_cursor.execute(f"SELECT {', '.join(target_common_columns)} FROM {table_name};")
        rows = target_cursor.fetchall()
        
        # 插入数据到结果库
        if rows:
            # 获取结果库表信息以检查NOT NULL约束
            result_cursor.execute(f"PRAGMA table_info({table_name});")
            result_columns_info = result_cursor.fetchall()
            not_null_columns = {col[1] for col in result_columns_info if col[3] == 1}  # col[3]是notnull标志
            
            # 准备数据，确保满足NOT NULL约束
            processed_rows = []
            for row in rows:
                processed_row = []
                # 创建一个从列名到值的映射
                if table_name == 'ay_content_ext':
                    column_value_map = dict(zip(all_needed_columns, row))
                else:
                    column_value_map = dict(zip(target_common_columns, row))
                
                for column_name in common_columns:
                    # 获取列的值，如果列不存在则为None
                    value = column_value_map.get(column_name, None)
                    
                    # 如果列有NOT NULL约束且值为None，则设置默认值
                    if column_name in not_null_columns and value is None:
                        # 根据列类型设置默认值
                        # 找到该列在结果库中的信息
                        column_info = next((col for col in result_columns_info if col[1] == column_name), None)
                        if column_info:
                            column_type = column_info[2].upper()
                            if 'INT' in column_type:
                                processed_row.append(0)
                            elif 'TEXT' in column_type or 'CHAR' in column_type:
                                processed_row.append('')
                            elif 'REAL' in column_type or 'FLOA' in column_type or 'DOUB' in column_type:
                                processed_row.append(0.0)
                            else:
                                processed_row.append('')
                        else:
                            processed_row.append('')
                    else:
                        processed_row.append(value)
                processed_rows.append(tuple(processed_row))
            
            placeholders = ','.join(['?' for _ in common_columns])
            insert_sql = f"INSERT INTO {table_name} ({','.join(common_columns)}) VALUES ({placeholders})"
            result_cursor.executemany(insert_sql, processed_rows)
            print(f"已迁移 {len(rows)} 行数据到表: {table_name}")
    
    # 提交更改并关闭连接
    result_conn.commit()
    target_conn.close()
    result_conn.close()
    print("数据迁移完成")

def compare_tables(source_db_path, target_db_path, result_db_path):
    """比较表结构和记录数"""
    # 连接数据库
    source_conn = sqlite3.connect(source_db_path)
    target_conn = sqlite3.connect(target_db_path)
    result_conn = sqlite3.connect(result_db_path)
    
    # 设置编码为UTF-8
    source_conn.execute('PRAGMA encoding = "UTF-8"')
    target_conn.execute('PRAGMA encoding = "UTF-8"')
    result_conn.execute('PRAGMA encoding = "UTF-8"')
    
    # 设置文本工厂以正确处理UTF-8编码
    source_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    target_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    result_conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
    
    def get_tables(conn):
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [row[0] for row in cursor.fetchall()]
    
    def get_table_schema(conn, table_name):
        cursor = conn.cursor()
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        result = cursor.fetchone()
        if result:
            schema = result[0]
            # 保留完整的表结构定义，包括约束和默认值
            return schema
        return None
    
    def get_table_count(conn, table_name):
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT count(*) FROM {table_name};")
            return cursor.fetchone()[0]
        except:
            return "不存在"
    
    def get_table_indexes(conn, table_name):
        """获取表的索引信息"""
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{table_name}';")
        indexes = cursor.fetchall()
        return [index[0] for index in indexes]
    
    # 获取所有表名
    source_tables = get_tables(source_conn)
    target_tables = get_tables(target_conn)
    result_tables = get_tables(result_conn)
    
    print(f"源库表: {source_tables}")
    print(f"目标库表: {target_tables}")
    print(f"结果库表: {result_tables}")
    
    # 找出共同的表
    common_tables = list(set(source_tables) & set(target_tables) & set(result_tables))
    print(f"共同表: {common_tables}")
    
    # 对比每个表
    results = []
    for table in common_tables:
        source_schema = get_table_schema(source_conn, table)
        result_schema = get_table_schema(result_conn, table)
        
        source_indexes = get_table_indexes(source_conn, table)
        result_indexes = get_table_indexes(result_conn, table)
        
        target_count = get_table_count(target_conn, table)
        result_count = get_table_count(result_conn, table)
        
        schema_match = source_schema == result_schema
        indexes_match = set(source_indexes) == set(result_indexes)
        
        results.append({
            'table': table,
            'schema_match': schema_match,
            'indexes_match': indexes_match,
            'target_count': target_count,
            'result_count': result_count,
            'source_schema': source_schema,
            'result_schema': result_schema,
            'source_indexes': source_indexes,
            'result_indexes': result_indexes
        })
    
    # 打印结果
    print("| 表名 | 结构是否一致 | 索引是否一致 | 目标库记录数 | 结果库记录数 | 备注 |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- |")
    for result in results:
        note = ""
        if not result['schema_match']:
            note = "结构不一致"
        elif not result['indexes_match']:
            note = "索引不一致"
        print(f"| {result['table']} | {'是' if result['schema_match'] else '否'} | {'是' if result['indexes_match'] else '否'} | {result['target_count']} | {result['result_count']} | {note} |")
    
    # 不再导出表数据到文件，以减少生成的txt文件数量
    print("已跳过导出表数据到文件步骤")
    
    # 关闭数据库连接
    source_conn.close()
    target_conn.close()
    result_conn.close()

import sys

def main():
    # 数据库文件路径
    source_db_path = r"c:\\Users\t1835\Desktop\sql\pbootcms.db"  # 源库
    target_db_path = r"c:\\Users\t1835\Desktop\sql\pbootcms1.db"  # 目标库
    result_db_path = r"c:\\Users\t1835\Desktop\sql\target_backup.db"  # 结果库
    
    # 删除结果库
    delete_result_db(result_db_path)
    
    # 迁移结构
    migrate_structures(source_db_path, target_db_path, result_db_path)
    # 迁移数据
    migrate_data(source_db_path, target_db_path, result_db_path)
    # 比较表
    compare_tables(source_db_path, target_db_path, result_db_path)

if __name__ == "__main__":
    main()