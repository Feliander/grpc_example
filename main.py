import asyncio
from contextlib import asynccontextmanager, suppress

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import order
from grpc_core.servers.manager import Server
from settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):  # pyright: ignore[reportUnusedParameter]
	server = Server()
	task = asyncio.create_task(server.run())
	try:
		yield
	finally:
		await server.stop()
		_ = task.cancel()
		with suppress(asyncio.CancelledError):
			await task


app = FastAPI(
	lifespan=lifespan,
	title="Example gRPC service on Python",
	description="This showing how to use gRPC on Python",
)


app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)


app.include_router(order.router)

if __name__ == "__main__":
	uvicorn.run("main:app", port=settings.SERVICE_PORT, host=settings.SERVICE_HOST_LOCAL, reload=True)
