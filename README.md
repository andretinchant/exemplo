# API RESTful em F# com CQRS (Kafka + PostgreSQL + MongoDB)

Agora a API está em modelo **CQRS**:

- **Commands (`POST`, `PUT`, `DELETE`)**: entram em uma fila Kafka.
- **Write Model**: consumidor processa comandos e persiste no PostgreSQL.
- **Read Model (`GET`)**: leitura vem do MongoDB (projeção atualizada pelo consumidor).

## Arquitetura

1. Cliente chama `POST /equipamentos`, `PUT /equipamentos/{id}` ou `DELETE /equipamentos/{id}`.
2. API publica comando no tópico Kafka `equipamentos.commands`.
3. `KafkaCommandConsumer` consome comando:
   - grava no **PostgreSQL** (fonte de verdade de escrita),
   - atualiza projeção no **MongoDB**.
4. Cliente chama `GET /equipamentos` ou `GET /equipamentos/{id}` e recebe dados do MongoDB.

## Endpoints

- `GET /equipamentos` → lista via MongoDB
- `GET /equipamentos/{id}` → busca via MongoDB
- `POST /equipamentos` → enfileira comando de criação
- `PUT /equipamentos/{id}` → enfileira comando de atualização
- `DELETE /equipamentos/{id}` → enfileira comando de remoção

## Payload (POST/PUT)

```json
{
  "nome": "Osciloscópio"
}
```

## Subir infraestrutura

```bash
docker compose up -d
```

Serviços:
- Kafka: `localhost:9092`
- PostgreSQL: `localhost:5432`
- MongoDB: `localhost:27017`

## Executar API

```bash
dotnet run --project EquipamentosApi/EquipamentosApi.fsproj
```

## Configuração

As configurações estão em `EquipamentosApi/appsettings.json`:
- `Kafka:BootstrapServers`
- `Kafka:Topic`
- `Kafka:GroupId`
- `ConnectionStrings:Postgres`
- `ConnectionStrings:Mongo`
