using System.Text;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using WordList.Common.Logging;
using WordList.Processing.UploadSourceChunks.Models;

namespace WordList.Processing.UploadSourceChunks;

public class SourceChunkUploader
{
    private static readonly DynamoDBContext s_dynamoDb = new DynamoDBContextBuilder().Build();
    private static readonly HttpClient s_http = new();
    private static readonly AmazonS3Client s_s3 = new();

    public string SourceId { get; init; }
    public ILogger Log { get; init; }
    public int ChunkLineCount { get; set; }
    public bool ReplaceExistingWords { get; init; }

    public record struct ChunkStatus
    {
        public string SourceId { get; set; }
        public string ChunkId { get; set; }
        public string Key { get; set; }
        public bool IsUploaded { get; set; }
    }

    // Limit max number of uploads.
    private SemaphoreSlim _uploadConcurrencyLimiter = new(3);

    public SourceChunkUploader(string sourceId, bool replaceExistingWords, ILogger logger)
    {
        SourceId = sourceId;
        Log = logger;
        ChunkLineCount = 2_500;
        ReplaceExistingWords = replaceExistingWords;
    }

    protected static async Task<string?> GetUrlFromSourceIdAsync(string sourceId)
    {
        var tableName = Environment.GetEnvironmentVariable("SOURCES_TABLE_NAME");
        var result = await s_dynamoDb.LoadAsync<Source>(sourceId, new LoadConfig { OverrideTableName = tableName }).ConfigureAwait(false);

        return result?.Url;
    }

    private async Task<ChunkStatus> UploadChunkAsync(string content)
    {
        var chunkId = Guid.NewGuid().ToString();
        var key = $"{SourceId}/{chunkId}.chunk.txt";

        var log = Log.WithPrefix($"[SourceId {SourceId}] [ChunkId {chunkId}] ");

        log.Info($"Waiting to start chunk upload");

        await _uploadConcurrencyLimiter.WaitAsync().ConfigureAwait(false);

        log.Info($"Started chunk upload");

        var bucket = Environment.GetEnvironmentVariable("SOURCE_CHUNKS_BUCKET_NAME");

        var isUploaded = false;

        log.Info($"Building put request");

        var request = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            ContentBody = content,
            ContentType = "text/plain"
        };

        log.Info($"Sending put request");
        try
        {
            await s_s3.PutObjectAsync(request).ConfigureAwait(false);
            isUploaded = true;
            log.Info($"Uploaded");
        }
        catch (Exception ex)
        {
            log.Error($"Put request failed: {ex.Message}");
        }

        _uploadConcurrencyLimiter.Release();

        log.Info($"Finished chunk upload with isUploaded={isUploaded}");

        return new ChunkStatus
        {
            SourceId = SourceId,
            ChunkId = chunkId,
            Key = key,
            IsUploaded = isUploaded
        };
    }

    public async Task<ChunkStatus[]> UploadAsync()
    {
        Log.Info($"Started upload process for source with ID: {SourceId}");

        var url = await GetUrlFromSourceIdAsync(SourceId).ConfigureAwait(false);

        if (url is null)
        {
            Log.Error($"No URL for source with ID: {SourceId} - cannot continue");
            return [];
        }

        using var stream = await s_http.GetStreamAsync(url).ConfigureAwait(false);
        if (stream is null)
        {
            Log.Error($"No download stream for source with ID: {SourceId} - cannot continue");
            return [];
        }

        using var reader = new StreamReader(stream);

        int currentLineCount = 0;
        int outputLineCount = 0;
        var builder = new StringBuilder();
        var uploadTasks = new List<Task<ChunkStatus>>();

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync().ConfigureAwait(false);
            currentLineCount++;

            line = line?.Trim();

            if (string.IsNullOrEmpty(line))
                continue;

            if (!line.All(char.IsLetter))
                continue;

            builder.AppendLine($"{line},{ReplaceExistingWords}");
            if (++outputLineCount >= ChunkLineCount)
            {
                uploadTasks.Add(UploadChunkAsync(builder.ToString()));
                builder.Clear();
                outputLineCount = 0;
            }
        }

        if (builder.Length > 0)
            uploadTasks.Add(UploadChunkAsync(builder.ToString()));

        Log.Info($"Waiting for all chunks to finish uploading for source ID {SourceId}");
        var statuses = await Task.WhenAll(uploadTasks).ConfigureAwait(false);
        Log.Info($"All upload tasks for source ID {SourceId} completed");

        return statuses.ToArray();
    }

}