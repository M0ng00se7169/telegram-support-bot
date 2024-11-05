from faststream import FastStream
from faststream.kafka.broker import KafkaBroker
from settings import get_settings

from consumers.handlers import router


settings = get_settings()

broker = KafkaBroker(settings.KAFKA_BROKER_URL)
broker.include_router(router=router)

app = FastStream(broker=broker)
