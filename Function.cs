using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.Core;
using Amazon.Lambda.RuntimeSupport;
using Amazon.Lambda.Serialization.SystemTextJson;
using System.Text.Json.Serialization;

using WordList.Processing.UploadSourceChunks.Models;
using WordList.Common.Messaging;
using WordList.Common.Messaging.Messages;

namespace WordList.Processing.UploadSourceChunks;

public class Function
{

    public static async Task<string> FunctionHandler(SQSEvent input, ILambdaContext context)
    {
        var log = new LambdaContextLogger(context);

        log.Info("Entering UploadSourceChunks FunctionHandler");

        if (input.Records.Count > 1)
        {
            log.Info($"Attempting to process {input.Records.Count} messages - SQS batch size should be set to 1!");
        }

        var messages = MessageQueues.UploadSourceChunks.Receive(input, log);

        foreach (var message in messages)
        {
            if (message is null) continue;

            log.Info($"Starting source upload for {message.SourceId}");
            var chunkStatuses = await new SourceChunkUploader(message.SourceId, message.ReplaceExistingWords, log).UploadAsync();
            log.Info($"Retrieved statuses for {chunkStatuses.Length} chunk(s)");

            if (!chunkStatuses.All(s => s.IsUploaded))
            {
                log.Info($"At least one chunk failed to upload, aborting");
                await new SourceChunkDeleter(chunkStatuses, log).DeleteAllChunksAsync();
            }
            else
            {
                await MessageQueues.ProcessSourceChunk.SendBatchedMessagesAsync(
                    log,
                    chunkStatuses.Select(status => new ProcessSourceChunkMessage
                    {
                        SourceId = status.SourceId,
                        ChunkId = status.ChunkId,
                        Key = status.Key
                    })
                );
            }
        }

        log.Info("Exiting UploadSourceChunks FunctionHandler");

        return "ok";
    }

    public static async Task Main()
    {
        Func<SQSEvent, ILambdaContext, Task<string>> handler = FunctionHandler;
        await LambdaBootstrapBuilder.Create(handler, new SourceGeneratorLambdaJsonSerializer<LambdaFunctionJsonSerializerContext>())
            .Build()
            .RunAsync();
    }
}

[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(SQSEvent))]
[JsonSerializable(typeof(Source))]
public partial class LambdaFunctionJsonSerializerContext : JsonSerializerContext
{
}