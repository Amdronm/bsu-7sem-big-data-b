# Лабораторная работа №2: Обработка потоковых данных

Проект потоковой обработки данных на основе **Apache Kafka**, **Apache Spark Structured Streaming** и **Apache Flink DataStream API**.

---

## Структура проекта

```
lab2/
├── docker-compose.yml              # Локальный кластер Kafka + Zookeeper + Kafka UI
├── requirements.txt                # Зависимости Python
├── run_spark.sh                    # Быстрый запуск Spark Structured Streaming в Docker
├── run_producer.sh                 # Быстрый запуск продюсера событий (через .venv)
├── run_benchmarks.sh               # Быстрый запуск бенчмарков и построение графиков
├── producer/
│   └── producer.py                 # Симулятор потока пользовательских событий
├── spark_app/
│   └── spark_stream.py             # Стриминг-приложение на Spark Structured Streaming
├── flink_app/
│   └── flink_stream.py             # Стриминг-приложение на Flink DataStream API
├── benchmark/
│   └── run_live_benchmark.py       # Скрипт бенчмарков, замера задержек и генерации графиков
├── report/
│   ├── REPORT.md                   # Полный теоретический и практический отчёт
│   ├── latency_comparison.svg      # График сравнения задержек (Latency)
│   └── throughput_comparison.svg   # График сравнения пропускной способности (Throughput)
└── ЛАБОРАТОРНАЯ РАБОТА 2.pdf       # Исходное задание
```

---

## Быстрый старт

### 1. Запуск инфраструктуры Kafka
Перейдите в директорию `lab2` и запустите контейнеры:
```bash
cd lab2
docker compose up -d
```
После запуска будут доступны:
- **Kafka Broker:** `localhost:9092`
- **Zookeeper:** `localhost:2181`
- **Kafka UI:** [http://localhost:8080](http://localhost:8080) (веб-интерфейс для просмотра топиков и сообщений)

Проверить статус контейнеров:
```bash
docker compose ps
```

---

### 2. Запуск генератора событий (Producer)
Скрипт генерирует поток JSON-сообщений пользователей (`click`, `view`, `purchase`, `add_to_cart`) с метками event-time и возможностью симуляции запаздывания:

```bash
# Через готовый скрипт (использует ваш .venv):
./run_producer.sh --rate 20 --num-users 30

# Нагрузочный режим для бенчмарка (1000 сообщений в секунду):
./run_producer.sh --rate 1000 --num-users 100
```

---

### 3. Запуск Spark Structured Streaming
Приложение поддерживает режимы: `tumbling`, `sliding`, `session`, `stateful` или `all`. Запуск через Docker гарантирует отсутствие конфликтов с системным Java 25:

```bash
# Запуск со всеми оконными функциями и stateful-подсчетом:
./run_spark.sh all

# Запуск только сеансовых окон (Session Windows):
./run_spark.sh session

# Запуск только подсчёта по ключу за минуту:
./run_spark.sh stateful "2 seconds"
```

---

### 4. Запуск Flink DataStream API
Приложение обрабатывает поток событий с использованием водяных меток и `KeyedProcessFunction`:

```bash
# Запуск со всеми окнами и состоянием:
python3 flink_app/flink_stream.py --mode all

# Запуск отдельного режима:
python3 flink_app/flink_stream.py --mode stateful
```

---

### 5. Запуск замеров производительности (Benchmarks)
Скрипт проводит сравнительный анализ Latency и Throughput, выводит таблицы и сохраняет графики в `report/`:

```bash
# Полный цикл нагрузочного тестирования на живом кластере:
./run_benchmarks.sh

# Быстрая перегенерация графиков по уже сохранённым результатам:
./run_benchmarks.sh --plot-only
```

---

### 6. Остановка окружения
Чтобы остановить Kafka и удалить контейнеры:
```bash
docker compose down
```

