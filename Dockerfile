# Используем официальный образ Rust в качестве базового
FROM rust:1.88.0-slim-bookworm as builder

# Устанавливаем необходимые зависимости для librdkafka
# (Debian/Ubuntu)

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    librdkafka-dev \
    cmake \
    build-essential \
    python3 \
    python-is-python3 \
    g++ \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*


# Создаем рабочую директорию
WORKDIR /app

# Копируем Cargo.toml и Cargo.lock для кэширования зависимостей
COPY Cargo.toml Cargo.lock ./

# Создаем фиктивный проект, чтобы Cargo закэшировал зависимости
#RUN mkdir src && echo "fn main() {println!(\"Hello\");}" > src/main.rs && \
#    cargo build --release && rm -rf src

# Просто выполняем cargo check для загрузки и кэширования зависимостей
# Это не создаст исполняемый файл, но заполнит кэш Cargo
RUN cargo check --release || true

# Копируем весь исходный код
COPY . .

# Собираем приложение
RUN cargo build --release

# --- Второй этап: Создание минимального образа ---
FROM debian:bookworm-slim

# Устанавливаем librdkafka (runtime dependency)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    netcat-traditional \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Копируем собранный исполняемый файл из первого этапа
COPY --from=builder /app/target/release/kafka-benchmark /usr/local/bin/

# Копируем папку config
COPY --from=builder /app/config /app/config

# Копируем скрипт entrypoint.sh и делаем его исполняемым
COPY entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

# Создаем пользователя, чтобы не запускать от root
RUN useradd -ms /bin/bash appuser

# Только теперь переключаемся на appuser
USER appuser

# Устанавливаем рабочую директорию для конфигурации
WORKDIR /app
    
# ENTRYPOINT
ENTRYPOINT ["/entrypoint.sh"]

# Определяем команду по умолчанию, которая будет запускать ваше приложение
#ENTRYPOINT ["/usr/local/bin/kafka-benchmark"]
# CMD аргументы будут добавлены после ENTRYPOINT
#CMD ["producer", "/app/config/base_producer.yaml", "10B_bursts"]
#CMD ["consumer", "/app/config/base_consumer.yaml", "10B_100MB"]

#docker build -t kafka-benchmark-app .
 #docker-compose down -v
 #docker compose up -d --build