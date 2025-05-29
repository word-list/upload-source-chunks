using Amazon.Lambda.Core;
using Amazon.S3;

namespace WordList.Processing.UploadSourceChunks;

public class SourceChunkDeleter
{
    private static AmazonS3Client s_s3 = new();
    private SemaphoreSlim _deleteLimiter = new(5);

    public SourceChunkUploader.ChunkStatus[] ChunkStatuses { get; set; }

    protected ILambdaLogger Logger { get; init; }

    public SourceChunkDeleter(SourceChunkUploader.ChunkStatus[] chunkStatuses, ILambdaLogger logger)
    {
        ChunkStatuses = chunkStatuses;
        Logger = logger;
    }

    public async Task DeleteChunkAsync(SourceChunkUploader.ChunkStatus status)
    {
        var logPrefix = $"[SourceId {status.SourceId}][ChunkId {status.ChunkId}][Key {status.Key}]";

        Logger.LogInformation($"{logPrefix} Waiting to delete");

        await _deleteLimiter.WaitAsync();

        try
        {
            await s_s3.DeleteObjectAsync(Environment.GetEnvironmentVariable("SOURCE_CHUNKS_BUCKET_NAME"), status.Key);
        }
        catch (Exception ex)
        {
            Logger.LogInformation($"{logPrefix} Failed to delete!");
            Logger.LogInformation($"{logPrefix} {ex.Message}");
        }

        _deleteLimiter.Release();
    }

    public async Task DeleteAllChunksAsync()
    {
        var deleteTasks = ChunkStatuses.Select(s => DeleteChunkAsync(s));

        await Task.WhenAll(deleteTasks);
    }
}