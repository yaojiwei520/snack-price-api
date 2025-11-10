# api/index.py

import psycopg2
from psycopg2 import Error
from psycopg2.extras import DictCursor, execute_values

from fastmcp import FastMCP
from dotenv import load_dotenv
import os
from typing import List, Dict, Optional, Any, Tuple
from decimal import Decimal
import logging
import datetime

# --- 配置日志 ---
# !! 重要修改：移除 FileHandler，Vercel 环境无法持久化写入文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # 只保留流处理器，日志会输出到 Vercel 的控制台
    ]
)
logger = logging.getLogger(__name__)

# --- 加载环境变量 ---
# 在本地开发时，load_dotenv() 仍然有用。在 Vercel 上，环境变量会由平台注入。
load_dotenv()


class SnackPriceService:
    def __init__(self):
        self.mcp = FastMCP(
            name="Snack Price Service",
            version="1.0.0"
        )
        self._add_query_tools()
        self._add_crud_tools()
        self._add_batch_tools()
        self._add_delete_tools()
        self._add_resources()
        self._add_prompts()

    # --- 数据库连接和执行方法 (这里无需修改) ---
    def _get_db_connection(self):
        try:
            return psycopg2.connect(
                host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
                dbname=os.getenv("DB_NAME"), cursor_factory=DictCursor
            )
        except psycopg2.OperationalError as e:
            logger.error(f"Could not connect to PostgreSQL database: {e}")
            raise

    # ... (你的 _execute_query, _execute_crud 等方法保持不变)
    # ... (你的所有 _add_*_tools 方法保持不变)
    # --- 省略了所有未修改的内部方法，保持你原来的代码即可 ---

    def _execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        conn = None
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                result = [dict(row) for row in rows]
                for row_dict in result:
                    for key, value in row_dict.items():
                        if isinstance(value, Decimal):
                            row_dict[key] = str(value)
                        elif isinstance(value, (datetime.datetime, datetime.date)):
                            row_dict[key] = value.isoformat()
                return result
        except Error as e:
            logger.error(f"Database query error: {e}", exc_info=True)
            return [{"status": "error", "message": f"Query failed: {e}"}]
        finally:
            if conn:
                conn.close()

    def _execute_crud(self, sql: str, params: Dict[str, Any]) -> Dict:
        conn = None
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                if cursor.description:
                    row = cursor.fetchone()
                    if row:
                        processed_row = dict(row)
                        for key, value in processed_row.items():
                            if isinstance(value, Decimal):
                                processed_row[key] = str(value)
                            elif isinstance(value, (datetime.datetime, datetime.date)):
                                processed_row[key] = value.isoformat()
                        conn.commit()
                        return {"status": "success", "data": processed_row}
                    else:
                        conn.commit()
                        return {"status": "warning", "message": "Operation executed, but no matching record was found."}
                else:
                    rows_affected = cursor.rowcount
                    conn.commit()
                    return {"status": "success", "rows_affected": rows_affected}
        except Error as e:
            if conn: conn.rollback()
            logger.error(f"Database CRUD error: {e}", exc_info=True)
            if e.pgcode == '23505':
                return {"status": "error",
                        "message": f"Uniqueness constraint violated: {e.diag.constraint_name}. A similar record already exists."}
            return {"status": "error", "message": f"Operation failed: {e}"}
        finally:
            if conn:
                conn.close()

    # ... (此处省略所有你的 _get_or_create_id, _add_query_tools, _add_crud_tools 等方法，保持原样即可)


# --- 主程序入口 ---
# !! 重要修改：实例化你的服务类
service = SnackPriceService()

# !! 重要修改：将 FastMCP 实例暴露给 Vercel，变量名必须是 'app'
# 假设 FastMCP 实例本身就是一个 ASGI 应用
app = service.mcp

# !! 重要修改：删除 `if __name__ == "__main__":` 和 `service.run()`
# Vercel 会处理服务器的启动，不需要这部分代码