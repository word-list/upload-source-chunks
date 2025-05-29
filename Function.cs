using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.Core;
using Amazon.Lambda.RuntimeSupport;
using Amazon.Lambda.Serialization.SystemTextJson;
using System.Text.Json.Serialization;

using WordList.Processing.UploadSourceChunks.Models;

namespace WordList.Processing.UploadSourceChunks;

public class Function
{

    public static async Task<string> FunctionHandler(SQSEvent input, ILambdaContext context)
    {
        context.Logger.LogInformation("Entering UploadSourceChunks FunctionHandler");

        if (input.Records.Count > 1)
        {
            context.Logger.LogWarning($"Attempting to process {input.Records.Count} messages - SQS batch size should be set to 1!");
        }

        var messages =
                input.Records.Select(record =>
                    {
                        try
                        {
                            return JsonHelpers.Deserialize(record.Body, LambdaFunctionJsonSerializerContext.Default.UploadSourceChunksMessage);
                        }
                        catch (Exception)
                        {
                            context.Logger.LogWarning($"Ignoring invalid message: {record.Body}");
                            return null;
                        }
                    });

        foreach (var message in messages)
        {
            if (message is null) continue;

            context.Logger.LogInformation($"Starting source upload for {message.SourceId}");
            var chunkStatuses = await new SourceChunkUploader(message.SourceId, message.ReplaceExistingWords, context.Logger).UploadAsync();
            context.Logger.LogInformation($"Retrieved statuses for {chunkStatuses.Length} chunk(s)");

            if (!chunkStatuses.All(s => s.IsUploaded))
            {
                context.Logger.LogInformation($"At least one chunk failed to upload, aborting");
                await new SourceChunkDeleter(chunkStatuses, context.Logger).DeleteAllChunksAsync();
            }
            else
            {
                await new ProcessChunkMessageSender(chunkStatuses, context.Logger).SendAllMessagesAsync();
            }
        }

        context.Logger.LogInformation("Exiting UploadSourceChunks FunctionHandler");

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
[JsonSerializable(typeof(UploadSourceChunksMessage))]
[JsonSerializable(typeof(Source))]
[JsonSerializable(typeof(ProcessSourceChunkMessage))]
public partial class LambdaFunctionJsonSerializerContext : JsonSerializerContext
{
}