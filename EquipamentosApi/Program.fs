open System
open System.Text.Json
open System.Threading.Tasks
open Confluent.Kafka
open Dapper
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open MongoDB.Bson
open MongoDB.Bson.Serialization.Attributes
open MongoDB.Driver
open Npgsql

type Equipamento = {
    Id: int
    Nome: string
}

type EquipamentoReadModel = {
    [<BsonId>]
    [<BsonRepresentation(BsonType.Int32)>]
    Id: int
    Nome: string
}

type CriarEquipamentoRequest = {
    Nome: string
}

type AtualizarEquipamentoRequest = {
    Nome: string
}

type TipoComando =
    | Criar
    | Atualizar
    | Remover

type EquipamentoCommand = {
    Tipo: TipoComando
    Id: int option
    Nome: string option
}

type KafkaSettings = {
    BootstrapServers: string
    Topic: string
    GroupId: string
}

type CommandProducer(settings: KafkaSettings) =
    let producerConfig = ProducerConfig(BootstrapServers = settings.BootstrapServers)

    member _.PublishAsync(command: EquipamentoCommand) =
        task {
            use producer = ProducerBuilder<Null, string>(producerConfig).Build()
            let payload = JsonSerializer.Serialize(command)
            let! _ = producer.ProduceAsync(settings.Topic, Message<Null, string>(Value = payload))
            return ()
        }

type ReadRepository(mongoConnection: string) =
    let client = MongoClient(mongoConnection)
    let database = client.GetDatabase("equipamentos_read")
    let collection = database.GetCollection<EquipamentoReadModel>("equipamentos")

    member _.ListAsync() =
        task {
            let! result = collection.Find(Builders<EquipamentoReadModel>.Filter.Empty).ToListAsync()
            return result |> Seq.sortBy (fun x -> x.Id)
        }

    member _.FindByIdAsync(id: int) =
        task {
            let filter = Builders<EquipamentoReadModel>.Filter.Eq((fun x -> x.Id), id)
            let! result = collection.Find(filter).FirstOrDefaultAsync()
            return result
        }

    member _.UpsertAsync(equipamento: Equipamento) =
        task {
            let filter = Builders<EquipamentoReadModel>.Filter.Eq((fun x -> x.Id), equipamento.Id)
            let model = {
                Id = equipamento.Id
                Nome = equipamento.Nome
            }
            let options = ReplaceOptions(IsUpsert = true)
            let! _ = collection.ReplaceOneAsync(filter, model, options)
            return ()
        }

    member _.DeleteAsync(id: int) =
        task {
            let filter = Builders<EquipamentoReadModel>.Filter.Eq((fun x -> x.Id), id)
            let! _ = collection.DeleteOneAsync(filter)
            return ()
        }

type WriteRepository(postgresConnection: string) =
    member _.EnsureSchemaAsync() =
        task {
            use conn = new NpgsqlConnection(postgresConnection)
            do! conn.OpenAsync()
            let sql = """
                CREATE TABLE IF NOT EXISTS equipamentos (
                    id SERIAL PRIMARY KEY,
                    nome TEXT NOT NULL
                );
            """
            let! _ = conn.ExecuteAsync(sql)
            return ()
        }

    member _.CreateAsync(nome: string) =
        task {
            use conn = new NpgsqlConnection(postgresConnection)
            do! conn.OpenAsync()
            let sql = "INSERT INTO equipamentos (nome) VALUES (@nome) RETURNING id;"
            let! id = conn.ExecuteScalarAsync<int>(sql, {| nome = nome |})
            return id
        }

    member _.UpdateAsync(id: int, nome: string) =
        task {
            use conn = new NpgsqlConnection(postgresConnection)
            do! conn.OpenAsync()
            let sql = "UPDATE equipamentos SET nome = @nome WHERE id = @id;"
            let! affected = conn.ExecuteAsync(sql, {| id = id; nome = nome |})
            return affected > 0
        }

    member _.DeleteAsync(id: int) =
        task {
            use conn = new NpgsqlConnection(postgresConnection)
            do! conn.OpenAsync()
            let sql = "DELETE FROM equipamentos WHERE id = @id;"
            let! affected = conn.ExecuteAsync(sql, {| id = id |})
            return affected > 0
        }

type KafkaCommandConsumer(settings: KafkaSettings, writeRepo: WriteRepository, readRepo: ReadRepository, logger: ILogger<KafkaCommandConsumer>) =
    inherit BackgroundService()

    override _.ExecuteAsync(stoppingToken) =
        task {
            let consumerConfig = ConsumerConfig(
                BootstrapServers = settings.BootstrapServers,
                GroupId = settings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            )

            use consumer = ConsumerBuilder<Ignore, string>(consumerConfig).Build()
            consumer.Subscribe(settings.Topic)

            while not stoppingToken.IsCancellationRequested do
                try
                    let consumeResult = consumer.Consume(stoppingToken)
                    if not (isNull consumeResult) then
                        let command = JsonSerializer.Deserialize<EquipamentoCommand>(consumeResult.Message.Value)

                        match command.Tipo with
                        | Criar ->
                            match command.Nome with
                            | Some nome ->
                                let! id = writeRepo.CreateAsync(nome)
                                do! readRepo.UpsertAsync({ Id = id; Nome = nome })
                            | None -> logger.LogWarning("Comando Criar inválido: Nome ausente")
                        | Atualizar ->
                            match command.Id, command.Nome with
                            | Some id, Some nome ->
                                let! updated = writeRepo.UpdateAsync(id, nome)
                                if updated then
                                    do! readRepo.UpsertAsync({ Id = id; Nome = nome })
                            | _ -> logger.LogWarning("Comando Atualizar inválido: Id ou Nome ausente")
                        | Remover ->
                            match command.Id with
                            | Some id ->
                                let! deleted = writeRepo.DeleteAsync(id)
                                if deleted then
                                    do! readRepo.DeleteAsync(id)
                            | None -> logger.LogWarning("Comando Remover inválido: Id ausente")

                        consumer.Commit(consumeResult) |> ignore
                with
                | :? OperationCanceledException -> ()
                | ex -> logger.LogError(ex, "Erro ao processar comando da fila Kafka")
        }

let builder = WebApplication.CreateBuilder(args)

let valueOrDefault defaultValue (value: string) =
    if isNull value then defaultValue else value

let kafkaSettings = {
    BootstrapServers = valueOrDefault "localhost:9092" builder.Configuration["Kafka:BootstrapServers"]
    Topic = valueOrDefault "equipamentos.commands" builder.Configuration["Kafka:Topic"]
    GroupId = valueOrDefault "equipamentos-worker" builder.Configuration["Kafka:GroupId"]
}

let postgresConnection = valueOrDefault "Host=localhost;Port=5432;Database=equipamentos;Username=postgres;Password=postgres" (builder.Configuration.GetConnectionString("Postgres"))
let mongoConnection = valueOrDefault "mongodb://localhost:27017" (builder.Configuration.GetConnectionString("Mongo"))

builder.Services.AddSingleton(kafkaSettings)
builder.Services.AddSingleton<CommandProducer>()
builder.Services.AddSingleton(fun _ -> WriteRepository(postgresConnection))
builder.Services.AddSingleton(fun _ -> ReadRepository(mongoConnection))
builder.Services.AddHostedService<KafkaCommandConsumer>()

let app = builder.Build()

let writeRepo = app.Services.GetRequiredService<WriteRepository>()
do writeRepo.EnsureSchemaAsync().GetAwaiter().GetResult()

app.MapGet("/equipamentos", Func<ReadRepository, Task<IResult>>(fun readRepo ->
    task {
        let! equipamentos = readRepo.ListAsync()
        return Results.Ok(equipamentos)
    }
))
|> ignore

app.MapGet("/equipamentos/{id:int}", Func<int, ReadRepository, Task<IResult>>(fun id readRepo ->
    task {
        let! equipamento = readRepo.FindByIdAsync(id)
        if isNull equipamento then
            return Results.NotFound()
        else
            return Results.Ok(equipamento)
    }
))
|> ignore

app.MapPost("/equipamentos", Func<CriarEquipamentoRequest, CommandProducer, Task<IResult>>(fun request producer ->
    task {
        if String.IsNullOrWhiteSpace(request.Nome) then
            return Results.BadRequest("O campo Nome é obrigatório.")
        else
            let command = {
                Tipo = Criar
                Id = None
                Nome = Some(request.Nome.Trim())
            }
            do! producer.PublishAsync(command)
            return Results.Accepted("/equipamentos", {| mensagem = "Comando de criação enfileirado." |})
    }
))
|> ignore

app.MapPut("/equipamentos/{id:int}", Func<int, AtualizarEquipamentoRequest, CommandProducer, Task<IResult>>(fun id request producer ->
    task {
        if String.IsNullOrWhiteSpace(request.Nome) then
            return Results.BadRequest("O campo Nome é obrigatório.")
        else
            let command = {
                Tipo = Atualizar
                Id = Some id
                Nome = Some(request.Nome.Trim())
            }
            do! producer.PublishAsync(command)
            return Results.Accepted($"/equipamentos/{id}", {| mensagem = "Comando de atualização enfileirado." |})
    }
))
|> ignore

app.MapDelete("/equipamentos/{id:int}", Func<int, CommandProducer, Task<IResult>>(fun id producer ->
    task {
        let command = {
            Tipo = Remover
            Id = Some id
            Nome = None
        }
        do! producer.PublishAsync(command)
        return Results.Accepted($"/equipamentos/{id}", {| mensagem = "Comando de remoção enfileirado." |})
    }
))
|> ignore

app.Run()
