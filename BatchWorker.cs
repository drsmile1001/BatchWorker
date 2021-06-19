using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace BatchWorker
{
    public class BatchWorker : BackgroundService
    {
        private readonly ILogger<BatchWorker> _logger;

        private long _jobSource = 0;

        private readonly ConcurrentQueue<long> _queueJobs = new();

        private TaskCompletionSource? _waitForNextJob;

        private readonly int _queueSize = 10000;

        private readonly int _batchSize = 2000;

        private readonly int _batchMaxWait = 500;

        private int _createJobDelay = 20;

        public BatchWorker(ILogger<BatchWorker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _ = Work(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                var jobIds = new List<long>(200);
                for (int i = 0; i < 200; i++)
                {
                    _jobSource += 1;
                    jobIds.Add(_jobSource);
                }

                var result = Enqueue(jobIds.ToArray());
                if (result && _createJobDelay > 10)
                    _createJobDelay -= 2;
                else if (!result)
                    _createJobDelay += 10;
                Console.WriteLine($"_createJobDelay:{_createJobDelay}");
                await Task.Delay(_createJobDelay, stoppingToken);
            }
            Console.WriteLine("stop");
        }

        private bool Enqueue(long[] jobIds)
        {
            var count = _queueJobs.Count;
            if ((count + jobIds.Length) > _queueSize)
                return false;

            foreach (var jobId in jobIds)
            {
                _queueJobs.Enqueue(jobId);
            }
            Console.WriteLine($"列隊長度:{_queueJobs.Count}");
            _waitForNextJob?.TrySetResult();
            return true;
        }

        private async Task Work(CancellationToken stoppingToken)
        {
            var batch = new List<long>(_batchSize);
            while (!stoppingToken.IsCancellationRequested || !_queueJobs.IsEmpty)
            {
                while (batch.Count < _batchSize)
                {
                    var hasNext = _queueJobs.TryDequeue(out var jobId);
                    if (hasNext)
                    {
                        batch.Add(jobId);
                        continue;
                    }
                    Console.WriteLine("列隊耗盡");
                    var waitForNextJob = new TaskCompletionSource();
                    _waitForNextJob = waitForNextJob;
                    var waitForWhile = Task.Delay(_batchMaxWait, stoppingToken);
                    await Task.WhenAny(waitForNextJob.Task, waitForWhile);
                    if (waitForNextJob.Task.IsCompleted) continue;
                    Console.WriteLine("達到最大等待時間，停止累計本批次");
                    _waitForNextJob = null;
                    waitForNextJob.TrySetResult();
                    break;
                }
                if (batch.Any())
                {
                    await ProcessBatch(batch.ToArray());
                    batch.Clear();
                }
            }
        }

        /// <summary>
        /// 代表處理batch的方法
        /// </summary>
        /// <param name="inputs"></param>
        /// <returns></returns>
        private async Task ProcessBatch(long[] inputs)
        {

            await Task.Delay(20 + inputs.Length / 10);
            Console.WriteLine($"處理了一批，大小{inputs.Length} 最大值{inputs.Max()}");
        }
    }
}
