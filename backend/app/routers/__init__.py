"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import backfill, resource

# backfill 含 /api/backfill/* 固定路径，必须在 /api/{module} 动态路由之前注册，
# 否则补数专用接口会被通用资源路由抢先匹配（序号6主入口共享错误产物问题）。
ROUTERS = [backfill, resource]
