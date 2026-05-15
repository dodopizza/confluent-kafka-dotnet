# Copilot Instructions for confluent-kafka-dotnet

## Build & Test

```bash
# Build the entire solution
dotnet build Confluent.Kafka.sln

# Run all unit tests
dotnet test test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj
dotnet test test/Confluent.SchemaRegistry.UnitTests/Confluent.SchemaRegistry.UnitTests.csproj
dotnet test test/Confluent.SchemaRegistry.Serdes.UnitTests/Confluent.SchemaRegistry.Serdes.UnitTests.csproj

# Run a single test by fully-qualified name
dotnet test test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj --filter "FullyQualifiedName~Confluent.Kafka.UnitTests.ProducerTests.Constuctor"

# Integration tests require a running Kafka cluster (start from test/docker):
#   docker-compose up
# Then run e.g.:
#   dotnet test test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj
```

## Architecture

This is Confluent's .NET client for Apache Kafka — a managed wrapper around the native **librdkafka** C library.

### Layered structure

- **P/Invoke layer** (`src/Confluent.Kafka/Impl/`): `Librdkafka` static class declares native methods. `SafeKafkaHandle` wraps the native `rd_kafka_t*` handle using `SafeHandle` for correct lifecycle management. Platform-specific native method bindings live in `Impl/NativeMethods/`.
- **Core client layer** (`src/Confluent.Kafka/`): `Producer<TKey,TValue>`, `Consumer<TKey,TValue>`, and `AdminClient` are **internal** classes. They are exposed to consumers exclusively through public interfaces (`IProducer`, `IConsumer`, `IAdminClient`) and constructed via builders (`ProducerBuilder`, `ConsumerBuilder`, `AdminClientBuilder`).
- **Schema Registry layer** (`src/Confluent.SchemaRegistry/`): `ISchemaRegistryClient` / `CachedSchemaRegistryClient` communicate with Confluent Schema Registry over REST (`Rest/RestService.cs`). Serialization format packages (`Serdes.Avro`, `Serdes.Protobuf`, `Serdes.Json`) implement `IAsyncSerializer<T>` / `IAsyncDeserializer<T>`.
- **Encryption layer** (`src/Confluent.SchemaRegistry.Encryption*`): Client-side field-level encryption with KMS integrations (AWS, Azure, GCP, HcVault).

### Key design patterns

- **Builder pattern**: All Kafka clients are constructed via builders (e.g. `new ProducerBuilder<string, string>(config).Build()`). Configuration, handlers, and serializers are set on the builder before calling `.Build()`.
- **Config hierarchy**: `Config` → `ClientConfig` → `ProducerConfig` / `ConsumerConfig` / `AdminClientConfig`. Config classes implement `IEnumerable<KeyValuePair<string, string>>` mapping directly to librdkafka configuration properties.

## Key Conventions

- **`Config_gen.cs` is auto-generated** from librdkafka configuration metadata by `src/ConfigGen/`. Do not edit it manually.
- **License headers**: All source files must carry the Apache 2.0 license header (see existing files for format).
- **Strong-named assemblies**: Projects are signed with `.snk` key files. Keep `SignAssembly` and `AssemblyOriginatorKeyFile` in project files.
- **Target frameworks**: Source projects target `net6.0;net8.0` (core `Confluent.Kafka` also targets `netstandard2.0;net462`). Test projects target `net6.0;net8.0`. C# language version is 12 (set in root `Directory.Build.props`).
- **Test framework**: xunit with Moq for mocking.
- **`AllowUnsafeBlocks`** is enabled in `Confluent.Kafka` for P/Invoke interop — this is intentional, not a mistake.
