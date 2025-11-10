# /api/index.py
import sys
import os
import logging

# Vercel 在 /var/task/api/index.py 运行此文件
# 你的 snack_service.py 在 /var/task/snack_service.py
# 需要将 /var/task 添加到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    # 导入服务类和在该文件中定义的 logger
    # logger 必须在这里导入，以便在冷启动时打印日志
    from snack_service import SnackPriceService, logger

except ImportError as e:
    # 严重错误：如果连服务都导入不了，Vercel 无法启动
    # 打印到 stderr，这会显示在 Vercel 的运行时日志中
    logging.critical(f"FATAL: Could not import SnackPriceService. Error: {e}")


    # 暴露一个简单的 ASGI 应用来显示错误
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send({
            'type': 'http.response.start',
            'status': 500,
            'headers': [[b'content-type', b'text/plain']],
        })
        await send({
            'type': 'http.response.body',
            'body': f"Server Import Error: {e}".encode('utf-8'),
        })

else:
    # --- Vercel 入口点 ---
    logger.info("Vercel cold start: Initializing SnackPriceService...")

    # 1. 在函数冷启动时实例化一次服务
    # 这会运行 __init__ 并注册所有的 @mcp.tool
    service_instance = SnackPriceService()

    # 2. 将 FastMCP 实例 (mcp) 暴露为 'app'
    # Vercel 会自动寻找名为 'app' 的 ASGI/WSGI 兼容实例
    app = service_instance.mcp

    logger.info("SnackPriceService initialized. 'app' (FastMCP) is ready.")