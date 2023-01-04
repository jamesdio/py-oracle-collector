class Config(object):
    DEBUG = True
    DEVELOPMENT = True

    # Oracle DB 접속정보
    DB_USER = 'scott'
    DB_PASSWORD = 'tiger'
    SID = ['oracle']
    OUTPUT_FORMAT = 'kafka'

    # InfluxDB
    INFLUX_DB_SERVER = '127.0.0.1'
    INFLUX_DB_ID = 'admin'
    INFLUX_DB_PW = 'password'
    INFLUX_DB_PORT = '8086'
    INFLUX_DB_NAME = 'oracle'
    INFLUX_WRITE_BATCH_SIZE = 10000

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
    KAFKA_TOPIC = 'oracle-metrics'
    KAFKA_BATCH_SIZE = 1000
    KAFKA_LINGER_MS = 5000

    # OutPut Target
    OUTPUT_TARGET = 'kafka' # [kafka|influx]


class ProductionConfig(Config):
    DEVELOPMENT = False
    DEBUG = False

class DevelopmentConfig(Config):
    DEBUG = True

class TestingConfig(Config):
    DEBUG = True