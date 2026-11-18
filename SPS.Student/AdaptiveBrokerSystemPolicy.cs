using System;
using System.Collections.Generic;
using System.Linq;
using SPS.Abstractions;

namespace SPS.Student;

public sealed class AdaptiveBrokerSystemPolicy : ISystemPolicy
{
    public IRequestPolicy RpcPolicy { get; } = new AdaptiveRequestPolicy();

    private readonly Dictionary<long, EndpointTracker> _endpoints = new();
    private readonly Dictionary<string, CacheKeyStats> _cacheStats = new(StringComparer.Ordinal);
    private readonly Dictionary<long, RequestMeta> _activeRequests = new();

    private SystemSettings? _settings;
    private TimeSpan _lastApiScaleUp = TimeSpan.Zero;
    private TimeSpan _lastApiScaleDown = TimeSpan.Zero;
    private TimeSpan _lastWorkerScaleUp = TimeSpan.Zero;
    private TimeSpan _lastWorkerScaleDown = TimeSpan.Zero;

    private double _emaPendingCalls;
    private double _emaQueuedJobs;

    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        _activeRequests[request.Id] = new RequestMeta(request.Kind, request.ResourceKey, request.MaxDataAge, request.CreatedAt);

        if (_cacheStats.TryGetValue(request.ResourceKey, out var stats))
        {
            if (request.MaxDataAge > stats.MaxObservedAge)
            {
                stats.MaxObservedAge = request.MaxDataAge;
            }
        }
        else
        {
            _cacheStats[request.ResourceKey] = new CacheKeyStats(request.MaxDataAge);
        }

        return request.Kind switch
        {
            RequestKind.Metadata => TimeSpan.FromMilliseconds(1600),
            RequestKind.Preview => TimeSpan.FromMilliseconds(3200),
            RequestKind.Generate => TimeSpan.FromSeconds(62),
            _ => null
        };
    }

    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var key = context.Request.ResourceKey;
        if (!_cacheStats.TryGetValue(key, out var stats))
        {
            stats = new CacheKeyStats(context.Request.MaxDataAge);
            _cacheStats[key] = stats;
        }
        else if (context.Request.MaxDataAge > stats.MaxObservedAge)
        {
            stats.MaxObservedAge = context.Request.MaxDataAge;
        }

        stats.Requests++;
        stats.LastAccess = context.Now;

        if (context.Entry != null)
        {
            var deliveryMargin = TimeSpan.FromMilliseconds(5);
            if (context.Now - context.Entry.SourceObservedAt + deliveryMargin <= context.Request.MaxDataAge)
            {
                stats.Hits++;
                return CacheReadDecision.ServeCached();
            }
        }

        FetchInfo? bestFetch = null;
        foreach (var fetch in context.PendingFetches)
        {
            var fetchAge = context.Now - fetch.StartedAt;
            if (fetchAge <= TimeSpan.FromMilliseconds(800) && fetchAge < context.Request.MaxDataAge)
            {
                if (bestFetch == null || fetch.StartedAt > bestFetch.StartedAt)
                {
                    bestFetch = fetch;
                }
            }
        }

        if (bestFetch != null)
        {
            return CacheReadDecision.JoinFetch(bestFetch.Id);
        }

        return CacheReadDecision.StartFetch();
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        if (context.Value.SizeBytes > context.CapacityBytes)
        {
            return CacheWriteDecision.Skip();
        }

        long otherBytes = 0;
        foreach (var entry in context.Entries)
        {
            if (entry.Key != context.Value.Key)
            {
                otherBytes += entry.SizeBytes;
            }
        }

        long needToFree = otherBytes + context.Value.SizeBytes - context.CapacityBytes;
        var evictions = new List<string>();

        if (needToFree > 0)
        {
            var candidates = context.Entries
                .Where(e => e.Key != context.Value.Key)
                .Select(e =>
                {
                    bool expired = e.ExpiresAt <= context.Now;
                    var st = _cacheStats.GetValueOrDefault(e.Key);
                    double hits = st != null ? st.Requests : 1.0;
                    double idleSec = st != null ? Math.Max(0.1, (context.Now - st.LastAccess).TotalSeconds) : 10.0;
                    double workWeight = e.Key.StartsWith("Preview:", StringComparison.Ordinal) ? 4.0 : 1.0;
                    double score = expired ? -1.0 : (hits * workWeight) / (Math.Max(1.0, e.SizeBytes / 1024.0) * Math.Sqrt(idleSec));
                    return (Entry: e, Score: score);
                })
                .OrderBy(x => x.Score)
                .ToList();

            long freed = 0;
            foreach (var item in candidates)
            {
                evictions.Add(item.Entry.Key);
                freed += item.Entry.SizeBytes;
                if (freed >= needToFree) break;
            }

            if (freed < needToFree)
            {
                return CacheWriteDecision.Skip();
            }
        }

        TimeSpan ttl = TimeSpan.FromSeconds(10);
        if (_cacheStats.TryGetValue(context.Value.Key, out var keyStats))
        {
            double maxAgeSec = Math.Clamp(keyStats.MaxObservedAge.TotalSeconds, 2.0, 30.0);
            ttl = TimeSpan.FromSeconds(maxAgeSec);
        }

        return CacheWriteDecision.Retain(ttl, evictions.ToArray());
    }

    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0)
        {
            return null;
        }

        EndpointInfo? best = null;
        double bestScore = double.MaxValue;
        int queueLimit = _settings?.Api.QueueLimit ?? 32;
        int effectiveQueueCap = queueLimit == 0 ? 0 : Math.Max(1, queueLimit - 2);

        foreach (var ep in context.Endpoints)
        {
            if (ep.OutstandingCalls >= ep.Slots + effectiveQueueCap)
            {
                continue;
            }

            var tracker = GetOrCreateTracker(ep.Id, ep.Role, context.Now);
            double effectiveSpeed = Math.Max(0.1, ep.Speed);
            double queueLoad = (ep.OutstandingCalls + 1.0) / (ep.Slots * effectiveSpeed);
            double latencyPenalty = tracker.EmaLatencyMs / 1000.0;
            double errorPenalty = tracker.ConsecutiveFailures * 0.5;

            double score = queueLoad + latencyPenalty * 0.25 + errorPenalty;
            if (score < bestScore)
            {
                bestScore = score;
                best = ep;
            }
        }

        if (best == null)
        {
            return null;
        }

        return EndpointSelectionDecision.Assign(best.Id);
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var ep = context.Endpoint;
        var tracker = GetOrCreateTracker(ep.Id, ep.Role, context.Now);

        if (ep.State != InstanceState.Ready)
        {
            return EndpointDecision.Allow();
        }

        if (ep.Role == InstanceRole.Api)
        {
            return ReviewApiEndpoint(context.Now, ep, context.RecentResults, tracker);
        }

        return ReviewWorkerEndpoint(context.Now, ep, context.RecentResults, tracker);
    }

    private EndpointDecision ReviewApiEndpoint(TimeSpan now, EndpointInfo ep, IReadOnlyList<CallResultInfo> recent, EndpointTracker tracker)
    {
        if (ep.Mode == EndpointMode.Probe)
        {
            var probeResult = FindLatestProbe(recent, tracker.LastProbeStartedAt);
            if (probeResult != null)
            {
                if (probeResult.Outcome == RequestOutcome.Success)
                {
                    tracker.ConsecutiveFailures = 0;
                    tracker.IsBlocked = false;
                    tracker.LastUnblockedAt = now;
                    tracker.Cooldown = TimeSpan.FromSeconds(1);
                    return EndpointDecision.Allow();
                }

                tracker.IsBlocked = true;
                tracker.Cooldown = TimeSpan.FromSeconds(Math.Min(8.0, tracker.Cooldown.TotalSeconds * 2.0));
                tracker.BlockedUntil = now + tracker.Cooldown;
                return EndpointDecision.Block();
            }

            if (!ep.ProbeOutstanding && now - tracker.LastProbeStartedAt > TimeSpan.FromMilliseconds(300))
            {
                tracker.LastProbeStartedAt = now;
            }

            return EndpointDecision.Probe();
        }

        if (ep.Mode == EndpointMode.BlockTraffic)
        {
            tracker.IsBlocked = true;
            if (now >= tracker.BlockedUntil && !ep.ProbeOutstanding)
            {
                tracker.LastProbeStartedAt = now;
                return EndpointDecision.Probe();
            }

            return EndpointDecision.Block();
        }

        int consecutiveBad = 0;
        int recentBad = 0;
        int checkedCount = 0;
        for (int i = recent.Count - 1; i >= 0 && checkedCount < 6; i--)
        {
            var r = recent[i];
            if (r.Time <= tracker.LastUnblockedAt) break;
            checkedCount++;

            bool isHardFailure = r.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable;
            if (isHardFailure)
            {
                recentBad++;
                if (checkedCount == recentBad)
                {
                    consecutiveBad++;
                }
            }
        }

        if (consecutiveBad >= 3 || (checkedCount >= 5 && recentBad >= 4))
        {
            tracker.IsBlocked = true;
            tracker.BlockedUntil = now + tracker.Cooldown;
            return EndpointDecision.Block();
        }

        tracker.IsBlocked = false;
        return EndpointDecision.Allow();
    }

    private EndpointDecision ReviewWorkerEndpoint(TimeSpan now, EndpointInfo ep, IReadOnlyList<CallResultInfo> recent, EndpointTracker tracker)
    {
        if (ep.Mode == EndpointMode.Probe)
        {
            var probeResult = FindLatestProbe(recent, tracker.LastProbeStartedAt);
            if (probeResult != null)
            {
                if (probeResult.Outcome == RequestOutcome.Success)
                {
                    tracker.IsBlocked = false;
                    tracker.LastUnblockedAt = now;
                    tracker.Cooldown = TimeSpan.FromSeconds(1);
                    return EndpointDecision.Allow();
                }

                tracker.IsBlocked = true;
                tracker.Cooldown = TimeSpan.FromSeconds(Math.Min(8.0, tracker.Cooldown.TotalSeconds * 2.0));
                tracker.BlockedUntil = now + tracker.Cooldown;
                return EndpointDecision.Block();
            }

            return EndpointDecision.Probe();
        }

        if (ep.Mode == EndpointMode.BlockTraffic)
        {
            tracker.IsBlocked = true;
            if (now >= tracker.BlockedUntil && !ep.ProbeOutstanding)
            {
                tracker.LastProbeStartedAt = now;
                return EndpointDecision.Probe();
            }

            return EndpointDecision.Block();
        }

        var heartbeatInterval = _settings?.HeartbeatInterval ?? TimeSpan.FromMilliseconds(500);
        var lastHb = ep.LastHeartbeat ?? tracker.ReadyAt;
        if (now - lastHb > TimeSpan.FromTicks(heartbeatInterval.Ticks * 3) &&
            now - tracker.LastUnblockedAt > TimeSpan.FromTicks(heartbeatInterval.Ticks * 3) &&
            !ep.ProbeOutstanding)
        {
            tracker.LastProbeStartedAt = now;
            return EndpointDecision.Probe();
        }

        tracker.IsBlocked = false;
        return EndpointDecision.Allow();
    }

    private static CallResultInfo? FindLatestProbe(IReadOnlyList<CallResultInfo> recent, TimeSpan since)
    {
        for (int i = recent.Count - 1; i >= 0; i--)
        {
            var r = recent[i];
            if (r.IsProbe && r.Time >= since)
            {
                return r;
            }
        }
        return null;
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0)
        {
            return null;
        }

        JobInfo? bestJob = null;
        double bestJobScore = double.MinValue;

        foreach (var job in context.QueuedJobs)
        {
            var age = context.Now - job.Request.CreatedAt;
            if (age > TimeSpan.FromSeconds(62))
            {
                return JobAssignmentDecision.Reject(job.Id);
            }

            double waitSec = Math.Max(0.0, (context.Now - job.QueuedAt).TotalSeconds);
            double workEst = Math.Max(0.25, job.Request.EstimatedWorkSeconds);
            double valueDensity = (double)job.Request.Revenue / workEst;
            double agingFactor = 1.0 + 0.08 * waitSec;
            double score = valueDensity * agingFactor;

            if (score > bestJobScore)
            {
                bestJobScore = score;
                bestJob = job;
            }
        }

        if (bestJob == null)
        {
            return null;
        }

        EndpointInfo bestWorker = context.AvailableWorkers[0];
        foreach (var worker in context.AvailableWorkers)
        {
            if (worker.Speed > bestWorker.Speed ||
                (Math.Abs(worker.Speed - bestWorker.Speed) < 1e-6 &&
                 (worker.LastHeartbeat ?? TimeSpan.Zero) > (bestWorker.LastHeartbeat ?? TimeSpan.Zero)))
            {
                bestWorker = worker;
            }
        }

        return JobAssignmentDecision.Assign(bestJob.Id, bestWorker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;
        if (job.WorkerId == null || job.AssignedAt == null)
        {
            return JobRecoveryDecision.KeepWaiting();
        }

        var sinceAssigned = context.Now - job.AssignedAt.Value;
        var lastHb = job.LastHeartbeat ?? job.AssignedAt.Value;
        var sinceHb = context.Now - lastHb;

        if (_endpoints.TryGetValue(job.WorkerId.Value, out var tracker) && tracker.IsBlocked)
        {
            if (sinceAssigned > TimeSpan.FromMilliseconds(750))
            {
                return JobRecoveryDecision.RequeueAfter(TimeSpan.FromMilliseconds(50));
            }
        }

        var hbThreshold = _settings != null
            ? TimeSpan.FromTicks(_settings.HeartbeatInterval.Ticks * 5)
            : TimeSpan.FromMilliseconds(2500);

        if (sinceHb > hbThreshold && job.Attempt < 3)
        {
            return JobRecoveryDecision.RequeueAfter(TimeSpan.FromMilliseconds(100));
        }

        double maxExpectedSec = Math.Max(15.0, job.Request.EstimatedWorkSeconds * 4.5 + 5.0);
        if (sinceAssigned.TotalSeconds > maxExpectedSec && job.Attempt < 2)
        {
            return JobRecoveryDecision.RequeueAfter(TimeSpan.FromMilliseconds(150));
        }

        return JobRecoveryDecision.KeepWaiting();
    }

    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var settings = context.Settings;
        _settings = settings;

        _emaPendingCalls = 0.7 * _emaPendingCalls + 0.3 * context.PendingCalls;
        _emaQueuedJobs = 0.7 * _emaQueuedJobs + 0.3 * context.QueuedJobs.Count;

        var apis = context.Instances.Where(i => i.Role == InstanceRole.Api).ToList();
        var workers = context.Instances.Where(i => i.Role == InstanceRole.Worker).ToList();

        int activeApis = apis.Count(i => i.State is InstanceState.Ready or InstanceState.Starting);
        int activeWorkers = workers.Count(i => i.State is InstanceState.Ready or InstanceState.Starting);
        int healthyReadyApis = apis.Count(i => i.State == InstanceState.Ready && i.Mode == EndpointMode.AllowTraffic);
        int healthyReadyWorkers = workers.Count(i => i.State == InstanceState.Ready && i.Mode == EndpointMode.AllowTraffic);

        int apiSlotsTotal = Math.Max(1, healthyReadyApis * settings.Api.Slots);
        double apiLoad = Math.Max(context.PendingCalls, _emaPendingCalls);
        double apiUtilization = apiLoad / apiSlotsTotal;

        int desiredApis = activeApis;
        if (apiUtilization > 0.75 || (healthyReadyApis == 0 && apiLoad > 0))
        {
            int needed = (int)Math.Ceiling(apiLoad / (settings.Api.Slots * 0.7));
            desiredApis = Math.Max(activeApis, Math.Min(settings.MaximumApis, Math.Max(activeApis + 1, needed)));
            _lastApiScaleUp = context.Now;
        }
        else if (apiUtilization < 0.25 && activeApis > 1 && context.Now - _lastApiScaleUp > TimeSpan.FromSeconds(6) &&
                 context.Now - _lastApiScaleDown > TimeSpan.FromSeconds(5))
        {
            desiredApis = Math.Max(1, activeApis - 1);
            _lastApiScaleDown = context.Now;
        }

        double queuedWorkSec = 0;
        foreach (var j in context.QueuedJobs)
        {
            queuedWorkSec += Math.Max(0.5, j.Request.EstimatedWorkSeconds);
        }
        int busyWorkers = workers.Count(i => i.AssignedJobs > 0);

        int desiredWorkers = activeWorkers;
        if ((context.QueuedJobs.Count >= 2 && queuedWorkSec > Math.Max(2.0, healthyReadyWorkers * 2.5)) ||
            (healthyReadyWorkers == 0 && context.QueuedJobs.Count > 0))
        {
            int extra = (int)Math.Ceiling(queuedWorkSec / 4.0);
            desiredWorkers = Math.Max(activeWorkers, Math.Min(settings.MaximumWorkers, Math.Max(activeWorkers + 1, busyWorkers + extra)));
            _lastWorkerScaleUp = context.Now;
        }
        else if (context.QueuedJobs.Count == 0 && _emaQueuedJobs < 0.3 && busyWorkers < activeWorkers &&
                 activeWorkers > 1 && context.Now - _lastWorkerScaleUp > TimeSpan.FromSeconds(8) &&
                 context.Now - _lastWorkerScaleDown > TimeSpan.FromSeconds(6))
        {
            desiredWorkers = Math.Max(1, Math.Max(busyWorkers, activeWorkers - 1));
            _lastWorkerScaleDown = context.Now;
        }

        desiredApis = Math.Clamp(desiredApis, 1, settings.MaximumApis);
        desiredWorkers = Math.Clamp(desiredWorkers, 1, settings.MaximumWorkers);

        while (desiredApis * settings.Api.ComputeUnits + desiredWorkers * settings.Worker.ComputeUnits > settings.ComputeUnitLimit)
        {
            if (desiredWorkers > 1 && (desiredWorkers > busyWorkers || desiredApis <= 1))
            {
                desiredWorkers--;
            }
            else if (desiredApis > 1)
            {
                desiredApis--;
            }
            else
            {
                break;
            }
        }

        var retireIds = new List<long>();
        if (desiredApis < activeApis)
        {
            int toRetire = activeApis - desiredApis;
            var candidates = apis
                .Where(i => i.State is InstanceState.Ready or InstanceState.Starting)
                .OrderBy(i => i.Mode == EndpointMode.AllowTraffic ? 1 : 0)
                .ThenBy(i => i.OutstandingCalls)
                .ThenBy(i => i.Speed)
                .Take(toRetire);
            foreach (var c in candidates) retireIds.Add(c.Id);
        }

        if (desiredWorkers < activeWorkers)
        {
            int toRetire = activeWorkers - desiredWorkers;
            var candidates = workers
                .Where(i => i.State is InstanceState.Ready or InstanceState.Starting)
                .OrderBy(i => i.Mode == EndpointMode.AllowTraffic ? 1 : 0)
                .ThenBy(i => i.AssignedJobs)
                .ThenBy(i => i.Speed)
                .Take(toRetire);
            foreach (var c in candidates) retireIds.Add(c.Id);
        }

        return new CapacityPlan(desiredApis, desiredWorkers, retireIds.Count > 0 ? retireIds : null);
    }

    public void Observe(SystemObservation observation)
    {
        switch (observation.Kind)
        {
            case ObservationKind.SimulationStarted:
                _settings = observation.Settings;
                break;

            case ObservationKind.InstanceReady:
                if (observation.EndpointId is { } readyId)
                {
                    var tracker = GetOrCreateTracker(readyId, InstanceRole.Api, observation.Now);
                    tracker.ReadyAt = observation.Now;
                }
                break;

            case ObservationKind.InstanceStopped:
                if (observation.EndpointId is { } stoppedId)
                {
                    _endpoints.Remove(stoppedId);
                }
                break;

            case ObservationKind.RpcAttemptCompleted:
                if (observation.EndpointId is { } epId)
                {
                    var tracker = GetOrCreateTracker(epId, InstanceRole.Api, observation.Now);
                    if (observation.Outcome == RequestOutcome.Success)
                    {
                        tracker.ConsecutiveFailures = 0;
                        if (observation.Elapsed is { } el)
                        {
                            double ms = el.TotalMilliseconds;
                            tracker.EmaLatencyMs = tracker.EmaLatencyMs <= 0 ? ms : 0.8 * tracker.EmaLatencyMs + 0.2 * ms;
                        }
                    }
                    else if (observation.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable)
                    {
                        tracker.ConsecutiveFailures++;
                    }
                }
                break;

            case ObservationKind.RequestSucceeded:
            case ObservationKind.RequestFailed:
                if (observation.RequestId is { } reqId)
                {
                    _activeRequests.Remove(reqId);
                }
                break;
        }
    }

    private EndpointTracker GetOrCreateTracker(long id, InstanceRole role, TimeSpan now)
    {
        if (!_endpoints.TryGetValue(id, out var tracker))
        {
            tracker = new EndpointTracker(role, now);
            _endpoints[id] = tracker;
        }
        return tracker;
    }

    private sealed class EndpointTracker
    {
        public InstanceRole Role { get; }
        public TimeSpan ReadyAt { get; set; }
        public TimeSpan LastUnblockedAt { get; set; } = TimeSpan.Zero;
        public TimeSpan BlockedUntil { get; set; } = TimeSpan.Zero;
        public TimeSpan LastProbeStartedAt { get; set; } = TimeSpan.Zero;
        public TimeSpan Cooldown { get; set; } = TimeSpan.FromSeconds(1);
        public int ConsecutiveFailures { get; set; }
        public double EmaLatencyMs { get; set; }
        public bool IsBlocked { get; set; }

        public EndpointTracker(InstanceRole role, TimeSpan now)
        {
            Role = role;
            ReadyAt = now;
        }
    }

    private sealed class CacheKeyStats
    {
        public long Requests { get; set; }
        public long Hits { get; set; }
        public TimeSpan LastAccess { get; set; } = TimeSpan.Zero;
        public TimeSpan MaxObservedAge { get; set; }

        public CacheKeyStats(TimeSpan maxObservedAge)
        {
            MaxObservedAge = maxObservedAge;
        }
    }

    private sealed record RequestMeta(RequestKind Kind, string Key, TimeSpan MaxDataAge, TimeSpan CreatedAt);

    // =========================================================================
    // RPC-политика из Лабораторной работы №1 (адаптирована для SPS.Abstractions)
    // =========================================================================
    private sealed class AdaptiveRequestPolicy : IRequestPolicy
    {
        private static readonly TimeSpan InitialTimeout = TimeSpan.FromMilliseconds(1500);
        private static readonly TimeSpan MinTimeout = TimeSpan.FromMilliseconds(100);
        private static readonly TimeSpan MaxTimeout = TimeSpan.FromMilliseconds(20000);
        private static readonly TimeSpan MinRetryDelay = TimeSpan.FromMilliseconds(10);

        private const int MaxAttempts = 3;
        private const int HistoryCapacity = 100;
        private const double RetryBudgetRatio = 0.25;
        private const double MaxRetryTokens = 50.0;

        private readonly Queue<long> _latencyHistory = new(HistoryCapacity);
        private long _maxRecentTicks;
        private double _srttTicks;
        private double _rttvarTicks;
        private double _retryTokens = 20.0;

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            long timeoutTicks;

            if (_latencyHistory.Count == 0)
            {
                timeoutTicks = InitialTimeout.Ticks;
            }
            else
            {
                // RTO = SRTT + 4 * RTTVAR
                double rto = _srttTicks + 4.0 * _rttvarTicks;

                // Защита от тяжелых хвостов распределения
                double tailEstimate = _maxRecentTicks * 1.5;

                timeoutTicks = (long)Math.Max(rto, tailEstimate);
            }

            // Ограничение диапазона допустимых таймаутов
            timeoutTicks = Math.Max(MinTimeout.Ticks, Math.Min(MaxTimeout.Ticks, timeoutTicks));

            // Экспоненциальное масштабирование таймаута для повторных попыток
            if (context.Attempt > 1)
            {
                double attemptMultiplier = Math.Pow(2.0, context.Attempt - 1);
                timeoutTicks = (long)(timeoutTicks * attemptMultiplier);
                timeoutTicks = Math.Min(MaxTimeout.Ticks, timeoutTicks);
            }

            return TimeSpan.FromTicks(timeoutTicks);
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            long elapsedTicks = observation.Elapsed.Ticks;

            if (observation.Outcome == RequestOutcome.Success)
            {
                // Пополнение бюджета ретраев
                _retryTokens = Math.Min(MaxRetryTokens, _retryTokens + RetryBudgetRatio);

                // Обновление скользящего окна задержек
                if (_latencyHistory.Count >= HistoryCapacity)
                {
                    _latencyHistory.Dequeue();
                }
                _latencyHistory.Enqueue(elapsedTicks);

                // Пересчёт максимума в окне
                long max = 0;
                foreach (long ticks in _latencyHistory)
                {
                    if (ticks > max) max = ticks;
                }
                _maxRecentTicks = max;

                // Обновление сглаженных оценок SRTT и RTTVAR
                if (_srttTicks == 0.0)
                {
                    _srttTicks = elapsedTicks;
                    _rttvarTicks = elapsedTicks / 2.0;
                }
                else
                {
                    double diff = Math.Abs(elapsedTicks - _srttTicks);
                    _srttTicks = 0.875 * _srttTicks + 0.125 * elapsedTicks;
                    _rttvarTicks = 0.75 * _rttvarTicks + 0.25 * diff;
                }

                return RetryDecision.NoRetry();
            }

            if (observation.Outcome is RequestOutcome.Stale or RequestOutcome.PolicyRejected)
            {
                return RetryDecision.NoRetry();
            }

            if (observation.Outcome == RequestOutcome.Timeout)
            {
                _maxRecentTicks = Math.Max(_maxRecentTicks, (long)(elapsedTicks * 2.0));
            }

            // Проверка лимита числа попыток
            if (observation.Attempt >= MaxAttempts)
            {
                return RetryDecision.NoRetry();
            }

            // Проверка доступного бюджета ретраев для защиты от лавинной деградации
            if (_retryTokens < 1.0)
            {
                return RetryDecision.NoRetry();
            }

            _retryTokens -= 1.0;

            // Расчёт задержки перед повторной попыткой (Exponential Backoff + Random Jitter)
            double refTicks = _srttTicks > 0.0 ? _srttTicks : elapsedTicks;
            double attemptFactor = Math.Pow(1.5, observation.Attempt - 1);
            double jitter = 0.8 + 0.4 * random.NextDouble(); // Джиттер от 80% до 120%

            double baseScale = observation.Outcome is RequestOutcome.Rejected or RequestOutcome.Unavailable ? 0.10 : 0.25;
            long delayTicks = (long)(refTicks * baseScale * attemptFactor * jitter);
            delayTicks = Math.Max(MinRetryDelay.Ticks, delayTicks);

            return RetryDecision.RetryAfter(TimeSpan.FromTicks(delayTicks));
        }
    }
}
