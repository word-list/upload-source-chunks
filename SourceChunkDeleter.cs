using Amazon.Lambda.Core;
using Amazon.S3;
using WordList.Common.Logging;

namespace WordList.Processing.UploadSourceChunks;

public class SourceChunkDeleter
{
    private static AmazonS3Client s_s3 = new();
    private SemaphoreSlim _deleteLimiter = new(5);

    public SourceChunkUploader.ChunkStatus[] ChunkStatuses { get; set; }

    protected ILogger Log { get; init; }

    public SourceChunkDeleter(SourceChunkUploader.ChunkStatus[] chunkStatuses, ILogger logger)
    {
        ChunkStatuses = chunkStatuses;
        Log = logger;
    }

    public async Task DeleteChunkAsync(SourceChunkUploader.ChunkStatus status)
    {
        var log = Log.WithPrefix($"[SourceId {status.SourceId}][ChunkId {status.ChunkId}][Key {status.Key}]");

        log.Info($"Waiting to delete");

        await _deleteLimiter.WaitAsync().ConfigureAwait(false);

        try
        {
            await s_s3.DeleteObjectAsync(Environment.GetEnvironmentVariable("SOURCE_CHUNKS_BUCKET_NAME"), status.Key).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            log.Info($"Failed to delete: {ex.Message}");
        }

        _deleteLimiter.Release();
    }

    public async Task DeleteAllChunksAsync()
    {
        var deleteTasks = ChunkStatuses.Select(s => DeleteChunkAsync(s));

        await Task.WhenAll(deleteTasks).ConfigureAwait(false);
    }
}