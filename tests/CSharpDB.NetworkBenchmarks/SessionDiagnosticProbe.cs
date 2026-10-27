using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CSharpDB.NetworkBenchmarks;

/// <summary>Benchmark-only C1 timing. Adds overhead and cannot qualify transport speed.</summary>
internal sealed class SessionDiagnosticProbe : IDisposable
{
    private readonly ConcurrentQueue<Dispatch> _dispatch = new();
    private readonly ManualResetEventSlim _stop = new();
    private readonly Thread _sampler;
    private Sample[] _samples = [];
    private Sample? _current;
    private string _api = "", _wire = "";
    private int _round;

    public SessionDiagnosticProbe()
    {
        DiagnosticEvents.Log.Clock(Stopwatch.GetTimestamp(), Stopwatch.Frequency);
        _sampler = new Thread(SampleDispatch) { IsBackground = true, Name = "Network diagnostic dispatch sampler" };
        _sampler.Start();
    }

    public void Configure(IServiceCollection services)
    {
        services.AddSingleton(new StageInterceptor(this));
        services.AddGrpc(options => options.Interceptors.Add<StageInterceptor>());
        var original = services.Last(x => x.ServiceType == typeof(ICSharpDbClient));
        services.Replace(ServiceDescriptor.Singleton<ICSharpDbClient>(provider =>
        {
            var proxy = DispatchProxy.Create<ICSharpDbClient, BackendProxy>();
            var state = (BackendProxy)(object)proxy;
            state.Inner = (ICSharpDbClient)original.ImplementationFactory!(provider);
            state.Probe = this;
            return proxy;
        }));
    }

    public void BeginBlock(string api, string wire, int round, int count)
    {
        _api = api; _wire = wire; _round = round;
        _samples = Enumerable.Range(0, count).Select(_ => new Sample()).ToArray();
        DiagnosticEvents.Log.Block(api, wire, round, 1, Stopwatch.GetTimestamp());
    }
    public void BeginSample(int index, long timestamp)
    {
        var sample = _samples[index];
        sample.ClientStart = timestamp;
        Volatile.Write(ref _current, sample);
    }
    public void EndSample(int index, long timestamp) => _samples[index].ClientEnd = timestamp;
    public void EndBlock()
    {
        Volatile.Write(ref _current, null);
        DiagnosticEvents.Log.Block(_api, _wire, _round, 0, Stopwatch.GetTimestamp());
    }
    public async Task SaveBlockAsync(string output, string stem, int completed, CancellationToken ct)
    {
        // A stream WriteAsync can finish just after the client sees the response.
        var timer = Stopwatch.StartNew();
        while (_wire == "Stream" && _samples.Take(completed).Any(x => Volatile.Read(ref x.ServerWriteEnd) == 0) && timer.Elapsed < TimeSpan.FromSeconds(5))
            await Task.Delay(1, ct);
        File.WriteAllLines(Path.Combine(output, stem + ".stages.csv"), new[]
        { "Sample,ClientStart,ServerReceived,EngineStart,EngineEnd,ServerResponseStart,ServerWriteEnd,ClientEnd" }
            .Concat(_samples.Take(completed).Select((x, i) => $"{i + 1},{x.ClientStart},{x.ServerReceived},{x.EngineStart},{x.EngineEnd},{x.ServerResponseStart},{x.ServerWriteEnd},{x.ClientEnd}")));
    }
    public void Save(string output)
    {
        _stop.Set();
        _sampler.Join(TimeSpan.FromSeconds(2));
        File.WriteAllLines(Path.Combine(output, "threadpool-dispatch.csv"), new[]
        { "QueuedQpc,RanQpc,PendingWork,ThreadCount,CompletedWork,Gen0,Gen1,Gen2" }
            .Concat(_dispatch.Select(x => $"{x.Queued},{Volatile.Read(ref x.Ran)},{x.Pending},{x.Threads},{x.Completed},{x.Gen0},{x.Gen1},{x.Gen2}")));
        ThreadPool.GetMinThreads(out int minimum, out int ioMinimum);
        File.WriteAllText(Path.Combine(output, "diagnostic-runtime.json"), JsonSerializer.Serialize(new
        { Stopwatch.Frequency, ThreadPoolMinimum = minimum, IoMinimum = ioMinimum, System.Runtime.GCSettings.IsServerGC, ThreadPool.ThreadCount,
            DispatchSamples = _dispatch.Count, DiagnosticOnly = true }));
    }
    private void SampleDispatch()
    {
        do
        {
            var sample = new Dispatch { Queued = Stopwatch.GetTimestamp(), Pending = ThreadPool.PendingWorkItemCount,
                Threads = ThreadPool.ThreadCount, Completed = ThreadPool.CompletedWorkItemCount,
                Gen0 = GC.CollectionCount(0), Gen1 = GC.CollectionCount(1), Gen2 = GC.CollectionCount(2) };
            _dispatch.Enqueue(sample);
            ThreadPool.UnsafeQueueUserWorkItem(static state => Volatile.Write(ref state.Ran, Stopwatch.GetTimestamp()), sample, preferLocal: false);
        } while (!_stop.Wait(100));
    }
    public void Dispose() { _stop.Set(); _sampler.Join(TimeSpan.FromSeconds(2)); _stop.Dispose(); }

    private sealed class Sample
    {
        public long ClientStart, ServerReceived, EngineStart, EngineEnd, ServerResponseStart, ServerWriteEnd, ClientEnd;
    }
    private sealed class Dispatch
    {
        public long Queued, Ran, Pending, Completed;
        public int Threads, Gen0, Gen1, Gen2;
    }

    public class BackendProxy : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        internal SessionDiagnosticProbe Probe = null!;
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) && Volatile.Read(ref Probe._current) is { } sample)
                return ObserveSqlAsync((string)args![0]!, (CancellationToken)args[1]!, sample);
            try { return targetMethod.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
        }
        private async Task<SqlExecutionResult> ObserveSqlAsync(string sql, CancellationToken ct, Sample sample)
        {
            sample.EngineStart = Stopwatch.GetTimestamp();
            try { return await Inner.ExecuteSqlAsync(sql, ct).ConfigureAwait(false); }
            finally { sample.EngineEnd = Stopwatch.GetTimestamp(); }
        }
    }
    public sealed class StageInterceptor(SessionDiagnosticProbe probe) : Interceptor
    {
        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            Sample? sample = request is SqlRequest ? Volatile.Read(ref probe._current) : null;
            if (sample is not null) sample.ServerReceived = Stopwatch.GetTimestamp();
            try { return await continuation(request, context).ConfigureAwait(false); }
            finally { if (sample is not null) sample.ServerResponseStart = Stopwatch.GetTimestamp(); }
        }
        public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> reader,
            IServerStreamWriter<TResponse> writer, ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
            => continuation(new Reader<TRequest>(reader, probe), new Writer<TResponse>(writer, probe), context);
        private sealed class Reader<T>(IAsyncStreamReader<T> inner, SessionDiagnosticProbe owner) : IAsyncStreamReader<T>
        {
            public T Current => inner.Current;
            public async Task<bool> MoveNext(CancellationToken ct)
            {
                if (!await inner.MoveNext(ct).ConfigureAwait(false)) return false;
                if (Current is CommandSessionRequest { ExecuteSql: not null } && Volatile.Read(ref owner._current) is { } sample)
                    sample.ServerReceived = Stopwatch.GetTimestamp();
                return true;
            }
        }
        private sealed class Writer<T>(IServerStreamWriter<T> inner, SessionDiagnosticProbe owner) : IServerStreamWriter<T>
        {
            public WriteOptions? WriteOptions { get => inner.WriteOptions; set => inner.WriteOptions = value; }
            public async Task WriteAsync(T message)
            {
                Sample? sample = message is CommandSessionResponse { Result: not null } ? Volatile.Read(ref owner._current) : null;
                if (sample is not null) sample.ServerResponseStart = Stopwatch.GetTimestamp();
                try { await inner.WriteAsync(message).ConfigureAwait(false); }
                finally { if (sample is not null) Volatile.Write(ref sample.ServerWriteEnd, Stopwatch.GetTimestamp()); }
            }
        }
    }
}

[EventSource(Name = "CSharpDB-Network-Diagnostic")]
internal sealed class DiagnosticEvents : EventSource
{
    public static readonly DiagnosticEvents Log = new();
    [Event(1, Level = EventLevel.Informational)]
    public void Clock(long qpc, long frequency) => WriteEvent(1, qpc, frequency);
    [Event(2, Level = EventLevel.Informational)]
    public void Block(string api, string wire, int round, int active, long qpc) => WriteEvent(2, api, wire, round, active, qpc);
}
