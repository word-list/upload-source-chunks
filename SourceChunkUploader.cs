using System.Text;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using WordList.Processing.UploadSourceChunks.Models;

namespace WordList.Processing.UploadSourceChunks;

public class SourceChunkUploader
{
    private static readonly DynamoDBContext s_dynamoDb = new DynamoDBContextBuilder().Build();
    private static readonly HttpClient s_http = new();
    private static readonly AmazonS3Client s_s3 = new();

    public string SourceId { get; init; }
    public ILambdaLogger Logger { get; init; }
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

    public SourceChunkUploader(string sourceId, bool replaceExistingWords, ILambdaLogger logger)
    {
        SourceId = sourceId;
        Logger = logger;
        ChunkLineCount = 2_500;
        ReplaceExistingWords = replaceExistingWords;
    }

    protected static async Task<string?> GetUrlFromSourceId(string sourceId)
    {
        var tableName = Environment.GetEnvironmentVariable("SOURCES_TABLE_NAME");
        var result = await s_dynamoDb.LoadAsync<Source>(sourceId, new LoadConfig { OverrideTableName = tableName });

        return result?.Url;
    }

    private async Task<ChunkStatus> UploadChunkAsync(string content)
    {
        var chunkId = Guid.NewGuid().ToString();
        var key = $"{SourceId}/{chunkId}.chunk.txt";

        var logPrefix = $"[SourceId {SourceId}] [ChunkId {chunkId}] ";

        Logger.LogInformation($"{logPrefix} Waiting to start chunk upload");

        await _uploadConcurrencyLimiter.WaitAsync();

        Logger.LogInformation($"{logPrefix} Started chunk upload");

        var bucket = Environment.GetEnvironmentVariable("SOURCE_CHUNKS_BUCKET_NAME");

        var isUploaded = false;

        Logger.LogInformation($"{logPrefix} Building put request");

        var request = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            ContentBody = content,
            ContentType = "text/plain"
        };

        Logger.LogInformation($"{logPrefix} Sending put request");
        try
        {
            await s_s3.PutObjectAsync(request);
            isUploaded = true;
            Logger.LogInformation($"{logPrefix}");
        }
        catch (Exception ex)
        {
            Logger.LogError($"{logPrefix} Put request failed!");
            Logger.LogError($"{logPrefix} {ex.Message}");
        }

        _uploadConcurrencyLimiter.Release();

        Logger.LogInformation($"{logPrefix} Finished chunk upload with isUploaded={isUploaded}");

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
        Logger.LogInformation($"Started upload process for source with ID: {SourceId}");

        var url = await GetUrlFromSourceId(SourceId);

        if (url is null)
        {
            Logger.LogError($"No URL for source with ID: {SourceId} - cannot continue");
            return [];
        }

        using var stream = await s_http.GetStreamAsync(url);
        if (stream is null)
        {
            Logger.LogError($"No download stream for source with ID: {SourceId} - cannot continue");
            return [];
        }

        using var reader = new StreamReader(stream);

        int currentLineCount = 0;
        int outputLineCount = 0;
        var builder = new StringBuilder();
        var uploadTasks = new List<Task<ChunkStatus>>();

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync();
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

        Logger.LogInformation($"Waiting for all chunks to finish uploading for source ID {SourceId}");
        var statuses = await Task.WhenAll(uploadTasks);
        Logger.LogInformation($"All upload tasks for source ID {SourceId} completed");

        return statuses.ToArray();
    }

}