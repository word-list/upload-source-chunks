
using System.Runtime.Serialization;
using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;
using WordList.Processing.UploadSourceChunks.Models;

namespace WordList.Processing.UploadSourceChunks;

public class ProcessChunkMessageSender
{

    private static AmazonSQSClient s_sqs = new();

    private SemaphoreSlim _sendMessageLimiter = new(5);

    public SourceChunkUploader.ChunkStatus[] ChunkStatuses { get; init; }
    protected ILambdaLogger Logger { get; init; }

    public ProcessChunkMessageSender(SourceChunkUploader.ChunkStatus[] chunkStatuses, ILambdaLogger logger)
    {
        ChunkStatuses = chunkStatuses;
        Logger = logger;
    }

    public async Task SendMessageBatchAsync(SourceChunkUploader.ChunkStatus[] statusBatch)
    {
        await _sendMessageLimiter.WaitAsync();

        var entryMap = statusBatch
            .Select(status => new ProcessSourceChunkMessage
            {
                SourceId = status.SourceId,
                ChunkId = status.ChunkId,
                Key = status.Key
            })
            .Select(message =>
            {
                try
                {
                    return JsonHelpers.Serialize(message, LambdaFunctionJsonSerializerContext.Default.ProcessSourceChunkMessage);
                }
                catch (Exception ex)
                {
                    Logger.LogError($"[SourceId {message.SourceId}][ChunkId {message.ChunkId}] Could not serialise message:");
                    Logger.LogError($"[SourceId {message.SourceId}][ChunkId {message.ChunkId}] {ex.Message}");
                    return null;
                }
            })
            .Where(text => text is not null)
            .Select(text => new SendMessageBatchRequestEntry(Guid.NewGuid().ToString(), text))
            .ToDictionary(entry => entry.Id);

        for (var tryNumber = 1; tryNumber <= 3 && entryMap.Count != 0; tryNumber++)
        {
            var batchRequest = new SendMessageBatchRequest(Environment.GetEnvironmentVariable("PROCESS_SOURCE_CHUNK_QUEUE_URL"), entryMap.Values.ToList());

            Logger.LogInformation($"Try number {tryNumber}: Sending batch of {batchRequest.Entries.Count} message(s)");
            try
            {
                if (tryNumber > 1) await Task.Delay(250);

                var response = await s_sqs.SendMessageBatchAsync(batchRequest);
                foreach (var successful in response.Successful)
                {
                    entryMap.Remove(successful.Id);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError($"Failed to send message batch of {batchRequest.Entries.Count} message(s):");
                Logger.LogError(ex.Message);
            }
        }

        _sendMessageLimiter.Release();
    }

    public async Task SendAllMessagesAsync()
    {
        var tasks = ChunkStatuses.Chunk(10).Select(SendMessageBatchAsync);

        await Task.WhenAll(tasks);
    }
}