using CSharpDB.Pipelines.Models;

namespace CSharpDB.Api.Dtos;

public sealed record ExecutePipelineRequest(PipelinePackageDefinition Package, string? Mode = null);
public sealed record SavePipelineRequest(PipelinePackageDefinition Package, string? Name = null);
