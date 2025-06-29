using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.Core;
using Amazon.Lambda.RuntimeSupport;
using Amazon.Lambda.Serialization.SystemTextJson;
using System.Text.Json.Serialization;

using WordList.Processing.UploadSourceChunks.Models;
using WordList.Common.Messaging;
using WordList.Common.Messaging.Messages;
using WordList.Common.Status;
using WordList.Common.Status.Models;

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

        var messages = MessageQueues.UploadSourceChunks.Receive(input, log).GroupBy(m => m.CorrelationId);

        foreach (var group in messages)
        {
            foreach (var message in group)
            {
                if (message is null) continue;

                var status = new StatusClient(group.Key);

                log.Info($"Starting source upload for {message.SourceId}");
                var correlationId = await status.CreateStatusAsync(message.SourceId).ConfigureAwait(false);

                var chunkStatuses = await new SourceChunkUploader(message.SourceId, message.ReplaceExistingWords, status, log).UploadAsync().ConfigureAwait(false);
                log.Info($"Retrieved statuses for {chunkStatuses.Length} chunk(s)");

                await status.UpdateTotalChunksAsync(chunkStatuses.Length).ConfigureAwait(false);
                await status.UpdateStatusAsync(SourceStatus.CHUNKING).ConfigureAwait(false);

                if (!chunkStatuses.All(s => s.IsUploaded))
                {
                    log.Info($"At least one chunk failed to upload, aborting");
                    await new SourceChunkDeleter(chunkStatuses, log).DeleteAllChunksAsync().ConfigureAwait(false);
                    await status.UpdateStatusAsync(SourceStatus.FAILED).ConfigureAwait(false);
                }
                else
                {
                    await MessageQueues.ProcessSourceChunk.SendBatchedMessagesAsync(
                         log,
                         chunkStatuses.Select(status => new ProcessSourceChunkMessage
                         {
                             CorrelationId = message.CorrelationId,
                             SourceId = status.SourceId,
                             ChunkId = status.ChunkId,
                             Key = status.Key
                         })
                     ).ConfigureAwait(false);
                }
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