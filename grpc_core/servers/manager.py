from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from grpc import aio
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from loguru import logger
from opentelemetry import trace
from opentelemetry.exporter.jaeger.proto.grpc import JaegerExporter
from opentelemetry.instrumentation.grpc import GrpcAioInstrumentorClient, GrpcAioInstrumentorServer
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from grpc_core.protos.check import check_pb2, check_pb2_grpc
from grpc_core.protos.echo import echo_pb2, echo_pb2_grpc
from grpc_core.protos.order import order_pb2, order_pb2_grpc
from grpc_core.servers.interceptors import AuthInterceptor
from grpc_core.servers.services.check import CheckStatusOrderService
from grpc_core.servers.services.echo import EchoService
from grpc_core.servers.services.health import HealthService
from grpc_core.servers.services.order import OrderService
from models.order import Order
from settings import settings


class Server:
	"""
	Singleton класс для настройки и запуска gRPC сервера.

	Класс обеспечивает создание единственного экземпляра сервера, который можно зарегистрировать и запустить.

	Атрибуты:
	---------
	_instance : Server
	    Приватный атрибут, содержащий единственный экземпляр класса Server.
	SERVER_ADDRESS : str
	    Адрес сервера в формате 'host:port'.
	server : grpc.aio.Server
	    Экземпляр асинхронного gRPC сервера.
	initialized : bool
	    Флаг, указывающий, была ли выполнена инициализация.

	Методы:
	-------
	__new__(cls, *args, **kwargs)
	    Создает и возвращает единственный экземпляр класса Server.
	__init__() -> None
	    Инициализирует сервер, если он еще не инициализирован.
	register() -> None
	    Регистрирует сервисы gRPC на сервере.
	async run() -> None
	    Запускает сервер и ожидает его завершения.
	async stop() -> None
	    Останавливает сервер.
	"""

	_instance: Server | None = None

	def __new__(cls) -> Server:
		"""
		Создает и возвращает единственный экземпляр класса Server.

		Если экземпляр уже существует, возвращает его. В противном случае создает новый экземпляр.
		"""
		if not cls._instance:
			cls._instance = super().__new__(cls)
		return cls._instance

	def __init__(self) -> None:
		"""
		Инициализирует сервер, если он еще не инициализирован.

		Устанавливает адрес сервера, создает сервер gRPC и добавляет незащищенный порт.
		"""
		if not hasattr(self, "initialized"):
			# Создаем экспортёр для отправки трассировочных данных в Jaeger.
			# Указываем адрес коллектора и разрешаем небезопасное соединение (без шифрования).
			jaeger_exporter = JaegerExporter(
				collector_endpoint=f"{settings.JAEGER_HOST}:{settings.JAEGER_PORT}", insecure=True
			)

			# Создаем процессор для пакетной обработки трассировочных данных (спанов).
			# Он будет собирать спаны и отправлять их в Jaeger с использованием созданного экспортёра.
			span_processor = BatchSpanProcessor(jaeger_exporter)

			# Устанавливаем глобальный провайдер трассировки.
			# Указываем, что ресурс трассировки будет иметь имя "Order".
			# Это имя будет использоваться для идентификации службы в системе трассировки.
			tracer_provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "Order"}))
			trace.set_tracer_provider(tracer_provider)

			# Добавляем созданный процессор спанов в провайдера трассировки.
			# Это необходимо для того, чтобы спаны обрабатывались и отправлялись в Jaeger.
			tracer_provider.add_span_processor(span_processor)

			# Создаем инструмент для автоматической трассировки gRPC сервера.
			grpc_server_instrumentor = GrpcAioInstrumentorServer()

			# Включаем автоматическую трассировку для gRPC сервера.
			# Это позволит автоматически отслеживать все вызовы gRPC на сервере.
			grpc_server_instrumentor.instrument()

			# Создаем инструмент для автоматической трассировки gRPC клиента.
			grpc_client_instrumentor = GrpcAioInstrumentorClient()

			# Включаем автоматическую трассировку для gRPC клиента.
			# Это позволит автоматически отслеживать все вызовы gRPC с клиента.
			grpc_client_instrumentor.instrument()

			self.SERVER_ADDRESS: str = f"{settings.GRPC_HOST_LOCAL}:{settings.GRPC_PORT}"
			self.server: aio.Server = aio.server(
				ThreadPoolExecutor(max_workers=10),
				interceptors=[
					AuthInterceptor(settings.SECRET_KEY),
				],
			)
			_ = self.server.add_insecure_port(self.SERVER_ADDRESS)

			# Определение кортежа SERVICE_NAMES, содержащего полные имена сервисов, зарегистрированных на сервере.
			SERVICE_NAMES = (  # pyright: ignore[reportUnknownVariableType]
				# Получение полных имени сервисов (OrderService, ...) из дескрипторов (order_pb2, ...).
				order_pb2.DESCRIPTOR.services_by_name["OrderService"].full_name,  # pyright: ignore[reportAny]
				echo_pb2.DESCRIPTOR.services_by_name["EchoService"].full_name,  # pyright: ignore[reportUnknownMemberType]
				health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,  # pyright: ignore[reportAny]
				check_pb2.DESCRIPTOR.services_by_name["CheckStatusOrderService"].full_name,  # pyright: ignore[reportUnknownMemberType]
				# Добавление стандартного имени сервиса reflection (reflection service).
				reflection.SERVICE_NAME,
			)
			# Включение отражения сервера для перечисленных в SERVICE_NAMES сервисов.
			reflection.enable_server_reflection(SERVICE_NAMES, self.server)  # pyright: ignore[reportUnknownArgumentType]

			self.initialized: bool = True

	def register(self) -> None:
		"""
		Регистрирует сервисы gRPC на сервере.

		Регистрирует сервис OrderService на gRPC сервере.
		"""
		order_pb2_grpc.add_OrderServiceServicer_to_server(  # pyright: ignore[reportUnknownMemberType]
			OrderService(), self.server
		)
		echo_pb2_grpc.add_EchoServiceServicer_to_server(  # pyright: ignore[reportUnknownMemberType]
			EchoService(), self.server
		)
		_ = (health_pb2_grpc.add_HealthServicer_to_server(HealthService(), self.server),)  # pyright: ignore[reportUnknownMemberType]
		check_pb2_grpc.add_CheckStatusOrderServiceServicer_to_server(  # pyright: ignore[reportUnknownMemberType]
			CheckStatusOrderService(), self.server
		)

	async def run(self) -> None:
		"""
		Запускает сервер и ожидает его завершения.

		Создает таблицу Order, если она еще не существует, регистрирует сервисы и запускает сервер.
		Логгирует информацию о запуске сервера.
		"""
		_ = await Order.create_table(if_not_exists=True)  # pyright: ignore[reportUnknownVariableType]
		self.register()
		await self.server.start()
		logger.info(f"*** Сервис gRPC запущен: {self.SERVER_ADDRESS} ***")
		_ = await self.server.wait_for_termination()

	async def stop(self) -> None:
		"""
		Останавливает сервер.

		Останавливает gRPC сервер без периода ожидания (grace period).
		Логгирует информацию об остановке сервера.
		"""
		logger.info("*** Сервис gRPC остановлен ***")
		await self.server.stop(grace=False)
