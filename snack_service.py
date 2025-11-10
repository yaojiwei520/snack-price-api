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

# --- 配置日志 (已为 Vercel 优化) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # 移除了 FileHandler，因为 Vercel 是只读文件系统
        # Vercel 会自动捕获 StreamHandler 的输出 (stdout/stderr)
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 加载环境变量 ---
# 这在本地开发时 .env 文件中加载
# 在 Vercel 上, 它会自动从 Dashboard 的环境变量中加载
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
        logger.info("SnackPriceService initialized.") # 确认 __init__ 运行

    # --- 数据库连接和执行方法 ---
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

    # ... (你所有的 _execute_query, _execute_crud, _get_or_create_id, _construct_daterange 方法 ... )
    # ... (这些都不需要修改，原样复制) ...

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

    # --- 辅助方法 ---
    def _get_or_create_id(self, cursor, table: str, name: str) -> int:
        cursor.execute(f"SELECT id FROM {table} WHERE name = %(name)s", {'name': name})
        result = cursor.fetchone()
        if result:
            return result['id']
        cursor.execute(f"INSERT INTO {table} (name) VALUES (%(name)s) RETURNING id", {'name': name})
        new_id = cursor.fetchone()['id']
        logger.info(f"Created new {table[:-1]}: '{name}' with ID: {new_id}")
        return new_id

    def _construct_daterange(self, start_date_str: Optional[str], end_date_str: Optional[str]) -> str:
        start_date = datetime.date.fromisoformat(start_date_str) if start_date_str else datetime.date.today()
        end_date = datetime.date.fromisoformat(end_date_str) if end_date_str else None
        return f"[{start_date},{end_date or ''}]"

    # --- 查询工具 ---
    def _add_query_tools(self):
        @self.mcp.tool()
        def query_snack_prices(
                shop_name: Optional[str] = None,
                shop_id: Optional[int] = None,
                snack_name: Optional[str] = None,
                min_price: Optional[Decimal] = None,
                max_price: Optional[Decimal] = None,
                category: Optional[str] = None,
                spec: Optional[str] = None,
                min_recorded_date: Optional[str] = None,
                max_recorded_date: Optional[str] = None,
                limit: int = 100,
                order_by: str = "updated_at",
                order_direction: str = "DESC"
        ) -> List[Dict]:
            """
            查询零食价格信息，支持按多种条件过滤。
            可以按店铺名称(shop_name)或店铺ID(shop_id)进行过滤。如果提供了shop_id，它将被优先使用。
            新增了排序功能，可以通过 order_by 和 order_direction 参数控制结果的排序。

            Args:
                order_by: 排序字段，可选值为 'price', 'updated_at', 'snack_name'。默认为 'updated_at'。
                order_direction: 排序方向，可选值为 'ASC' (升序), 'DESC' (降序)。默认为 'DESC'。
            """
            params = locals().copy()
            where_clauses = [
                "(%(snack_name)s IS NULL OR sn.name ILIKE %(snack_name_like)s)",
                "(%(min_price)s IS NULL OR p.price >= %(min_price)s)",
                "(%(max_price)s IS NULL OR p.price <= %(max_price)s)",
                "(%(category)s IS NULL OR c.name ILIKE %(category_like)s)",
                "(%(spec)s IS NULL OR sn.spec ILIKE %(spec_like)s)",
            ]

            if params.get('shop_id') is not None:
                where_clauses.append("s.id = %(shop_id)s")
            elif params.get('shop_name') is not None:
                where_clauses.append("s.name ILIKE %(shop_name_like)s")
                params['shop_name_like'] = f"%{params.get('shop_name', '')}%"

            params['snack_name_like'] = f"%{params.get('snack_name', '')}%"
            params['category_like'] = f"%{params.get('category', '')}%"
            params['spec_like'] = f"%{params.get('spec', '')}%"
            if params['min_recorded_date']: where_clauses.append("p.created_at >= %(min_recorded_date)s")
            if params['max_recorded_date']: where_clauses.append("p.created_at <= %(max_recorded_date)s")
            where_sql = " AND ".join(where_clauses)

            sort_column_map = {
                "price": "p.price",
                "updated_at": "p.updated_at",
                "snack_name": "sn.name"
            }
            sort_column = sort_column_map.get(order_by, "p.updated_at")
            sort_direction = "ASC" if order_direction.upper() == "ASC" else "DESC"

            sql = f"""
                SELECT s.name AS shop_name, s.address AS shop_address, sn.name AS snack_name, b.name AS brand,
                       c.name AS category, sn.spec, p.price, p.discount_price, lower(p.valid_period) AS start_date,
                       upper(p.valid_period) AS end_date, p.created_at, p.updated_at, p.id AS price_id
                FROM prices p
                JOIN shops s ON p.shop_id = s.id JOIN snacks sn ON p.snack_id = sn.id
                JOIN brands b ON sn.brand_id = b.id JOIN categories c ON sn.category_id = c.id
                WHERE {where_sql} 
                ORDER BY {sort_column} {sort_direction}
                LIMIT %(limit)s;
            """
            return self._execute_query(sql, params)

        @self.mcp.tool()
        def get_shop_list() -> List[Dict]:
            """获取所有店铺的列表。"""
            return self._execute_query("SELECT id, name, address, phone FROM shops ORDER BY name, address")

        @self.mcp.tool()
        def get_snack_list() -> List[Dict]:
            """获取所有零食的列表，包含品牌和分类信息。"""
            return self._execute_query("""
                SELECT sn.id, sn.name, b.name AS brand, c.name AS category,
                       sn.description, sn.spec, sn.barcode
                FROM snacks sn
                JOIN brands b ON sn.brand_id = b.id
                JOIN categories c ON sn.category_id = c.id
                ORDER BY sn.name;
            """)

        @self.mcp.tool()
        def get_snack_categories() -> List[Dict]:
            """获取所有零食分类及其数量。"""
            return self._execute_query("""
                SELECT c.name as category, COUNT(sn.id) as count
                FROM categories c
                LEFT JOIN snacks sn ON c.id = sn.category_id
                GROUP BY c.name
                ORDER BY c.name;
            """)

    # --- CRUD 工具 ---
    def _add_crud_tools(self):
        @self.mcp.tool()
        def add_shop(name: str, address: str, phone: Optional[str] = None) -> Dict:
            """添加新店铺。地址 (address) 必须是唯一的。"""
            return self._execute_crud(
                "INSERT INTO shops (name, address, phone) VALUES (%(name)s, %(address)s, %(phone)s) RETURNING *;",
                {'name': name, 'address': address, 'phone': phone})

        @self.mcp.tool()
        def add_snack(name: str, brand: str, category: str, description: Optional[str] = None,
                      spec: Optional[str] = None, barcode: Optional[str] = None) -> Dict:
            """添加新零食。如果品牌或分类不存在，将自动创建它们。"""
            logger.info(f"Adding snack: {name}, brand: {brand}, category: {category}")
            conn = None
            try:
                conn = self._get_db_connection()
                with conn.cursor() as cursor:
                    brand_id = self._get_or_create_id(cursor, 'brands', brand)
                    category_id = self._get_or_create_id(cursor, 'categories', category)
                    sql = """INSERT INTO snacks (name, brand_id, category_id, description, spec, barcode)
                             VALUES (%(name)s, %(brand_id)s, %(category_id)s, %(description)s, %(spec)s, 
                                     %(barcode)s) RETURNING *;"""
                    params = {'name': name, 'brand_id': brand_id, 'category_id': category_id,
                              'description': description, 'spec': spec, 'barcode': barcode}
                    cursor.execute(sql, params)
                    new_snack_row = cursor.fetchone()
                    if not new_snack_row:
                        conn.rollback()
                        logger.error("Failed to add snack: INSERT did not return the new row.")
                        return {"status": "error",
                                "message": "Internal server error: Failed to retrieve the newly created snack."}
                    new_snack = dict(new_snack_row)
                    conn.commit()
                    new_snack['brand'] = brand
                    new_snack['category'] = category
                    for key, value in new_snack.items():
                        if isinstance(value, (datetime.datetime, datetime.date)): new_snack[key] = value.isoformat()
                    return {"status": "success", "data": new_snack}
            except psycopg2.Error as db_error:
                if conn: conn.rollback()
                logger.error(f"Database error while adding snack '{name}': {db_error}", exc_info=True)
                if db_error.pgcode == '23505': return {"status": "error",
                                                      "message": "A snack with the same brand, name, and spec likely already exists."}
                return {"status": "error", "message": f"Database error: {db_error}"}
            except Exception as e:
                if conn: conn.rollback()
                logger.error(f"An unexpected error occurred while adding snack '{name}': {e}", exc_info=True)
                return {"status": "error", "message": f"An unexpected internal error occurred: {e}"}
            finally:
                if conn: conn.close()

        @self.mcp.tool()
        def add_price(shop_id: int, snack_id: int, price: Decimal, discount_price: Optional[Decimal] = None,
                      start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
            """添加零食价格信息，自动处理价格有效期。"""
            valid_period = self._construct_daterange(start_date, end_date)
            sql = """INSERT INTO prices (shop_id, snack_id, price, discount_price, valid_period)
                     VALUES (%(shop_id)s, %(snack_id)s, %(price)s, %(discount_price)s, 
                             %(valid_period)s) RETURNING id, shop_id, snack_id, price, discount_price, lower(valid_period) as start_date, upper(valid_period) as end_date;"""
            return self._execute_crud(sql, {'shop_id': shop_id, 'snack_id': snack_id, 'price': price,
                                            'discount_price': discount_price, 'valid_period': valid_period})

    # --- 批处理工具 ---
    def _add_batch_tools(self):
        @self.mcp.tool()
        def add_prices_batch(prices_data: List[Dict[str, Any]]) -> Dict:
            """批量添加零食价格信息，使用高效的 execute_values。"""
            if not prices_data: return {"status": "warning", "message": "No price data provided."}
            data_to_insert = [(item['shop_id'], item['snack_id'], item['price'], item.get('discount_price'),
                               self._construct_daterange(item.get('start_date'), item.get('end_date'))) for item in
                              prices_data]
            sql = "INSERT INTO prices (shop_id, snack_id, price, discount_price, valid_period) VALUES %s;"
            conn = None
            try:
                conn = self._get_db_connection()
                with conn.cursor() as cursor:
                    execute_values(cursor, sql, data_to_insert)
                    conn.commit()
                    return {"status": "success", "message": f"Successfully added {cursor.rowcount} price records."}
            except Error as e:
                if conn: conn.rollback()
                logger.error(f"Batch add price error: {e}", exc_info=True)
                return {"status": "error", "message": f"Batch add price failed: {e}"}
            finally:
                if conn: conn.close()

    # --- 删除工具 ---
    def _add_delete_tools(self):
        @self.mcp.tool()
        def delete_price(price_id: int) -> Dict:
            """删除指定ID的价格记录。"""
            return self._execute_crud("DELETE FROM prices WHERE id = %(price_id)s", {'price_id': price_id})

        @self.mcp.tool()
        def batch_delete_prices(price_ids: List[int]) -> Dict:
            """批量删除指定ID列表的价格记录。"""
            if not price_ids: return {"status": "warning", "message": "No price IDs provided."}
            return self._execute_crud("DELETE FROM prices WHERE id = ANY(%(price_ids)s);", {'price_ids': price_ids})

        @self.mcp.tool()
        def delete_snack(snack_id: int) -> Dict:
            """删除指定ID的零食记录。此操作会级联删除关联的价格。"""
            return self._execute_crud("DELETE FROM snacks WHERE id = %(snack_id)s", {'snack_id': snack_id})

        @self.mcp.tool()
        def delete_shop(shop_id: int) -> Dict:
            """删除指定ID的店铺记录。此操作会级联删除关联的价格。"""
            return self._execute_crud("DELETE FROM shops WHERE id = %(shop_id)s", {'shop_id': shop_id})

    # --- 资源和 Prompt ---
    def _add_resources(self):
        pass

    def _add_prompts(self):
        pass

    # --- 运行服务 (Vercel 不会调用) ---
    # Vercel 是一个 Serverless 环境, 它不会调用这个 .run() 方法.
    # Vercel 会通过 api/index.py 中的 ASGI/WSGI 实例 (app) 来启动服务.
    # def run(self, port: int = 5444, transport: str = 'sse', host: str = '127.0.0.1'):
    #     logger.info(f"Starting FastMCP Snack Price Service on {host}:{port} with transport {transport}...")
    #     self.mcp.run(port=port, transport=transport, host=host)


# --- 主程序入口 (Vercel 不会调用) ---
# Vercel 不会执行 if __name__ == "__main__": 模块.
# if __name__ == "__main__":
#     service = SnackPriceService()
#     service.run(port=5444, transport='sse', host='0.0.0.0')