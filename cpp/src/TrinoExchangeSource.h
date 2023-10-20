/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <folly/Uri.h>

#include "src/utils/HttpClient.h"
#include "src/utils/ThreadUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/Exchange.h"

namespace io::trino::bridge {
class TrinoExchangeSource : public facebook::velox::exec::ExchangeSource {
 public:
  class RetryState {
   public:
    RetryState(int64_t maxWaitMs = 1'000)
        : maxWaitMs_(maxWaitMs), startMs_(facebook::velox::getCurrentTimeMs()) {}

    // Returns the delay in millis to wait before next try. This is an
    // exponential backoff delay with jitter. The first call to this always
    // returns 0.
    int64_t nextDelayMs() {
      if (++numTries_ == 1) {
        return 0;
      }
      auto rng = folly::ThreadLocalPRNG();
      return folly::futures::detail::retryingJitteredExponentialBackoffDur(
                 numTries_ - 1, std::chrono::milliseconds(kMinBackoffMs),
                 std::chrono::milliseconds(kMaxBackoffMs), kJitterParam, rng)
          .count();
    }

    // Returns whether we have exhausted all retries. We only retry if we spent
    // less than maxWaitMs_ time after we first started.
    bool isExhausted() const { return velox::getCurrentTimeMs() - startMs_ > maxWaitMs_; }

   private:
    int64_t maxWaitMs_;
    int64_t startMs_;
    size_t numTries_{0};

    static constexpr int64_t kMinBackoffMs = 100;
    static constexpr int64_t kMaxBackoffMs = 10000;
    static constexpr double kJitterParam = 0.1;
  };

  TrinoExchangeSource(const folly::Uri& baseUri, int destination,
                      std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
                      facebook::velox::memory::MemoryPool* pool,
                      const std::string& clientCertAndKeyPath_ = "",
                      const std::string& ciphers_ = "");

  bool supportsFlowControlV2() const override { return true; }

  bool shouldRequestLocked() override;

  static std::unique_ptr<ExchangeSource> createExchangeSource(
      const std::string& url, int destination,
      std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
      facebook::velox::memory::MemoryPool* pool);

  void close() override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {{"TrinoExchangeSource.nonEmptyRequestMicroRTTs", nonEmptyRequestMicroRTTs_},
            {"TrinoExchangeSource.errorResponseTimes", errorResponseTimes_},
            {"TrinoExchangeSource.emptyResponseTimes", emptyResponseTimes_},
            {"TrinoExchangeSource.requestTimes", requestTimes_}};
  }

  int testingFailedAttempts() const { return failedAttempts_; }

  /// Requests the producer to generate up to 'maxBytes' more data and reply
  /// within 'maxWaitSeconds'. Returns a future that completes when producer
  /// responds either with 'data' or with a message indicating that all data has
  /// been already produced or data will take more time to produce.
  folly::SemiFuture<Response> request(uint32_t /*maxBytes*/, uint32_t /*maxWaitSeconds*/);

  /// Invoked to track the node-wise memory usage queued in
  /// TrinoExchangeSource. If 'updateBytes' > 0, then increment the usage,
  /// otherwise decrement the usage.
  static void updateMemoryUsage(int64_t updateBytes);

  /// Invoked to get the node-wise queued memory usage from
  /// TrinoExchangeSource.
  static void getMemoryUsage(int64_t& currentBytes, int64_t& peakBytes);

  /// Invoked to reset the node-wise peak memory usage back to the current
  /// memory usage in TrinoExchangeSource. Instead of getting all time peak,
  /// this can be useful when tracking the peak within some fixed time
  /// intervals.
  static void resetPeakMemoryUsage();

  /// Used by test to clear the node-wise memory usage tracking.
  static void testingClearMemoryUsage();

 private:
  void doRequest(int64_t delayMs, uint32_t maxBytes, uint32_t maxWaitSeconds);

  void processDataResponse(std::unique_ptr<http::HttpResponse> response,
                           uint64_t startMicroSeconds);

  // If 'retry' is true, then retry the http request failure until reaches the
  // retry limit, otherwise just set exchange source error without retry. As
  // for now, we don't retry on the request failure which is caused by the
  // memory allocation failure for the http response data.
  void processDataError(const std::string& path, const std::string& error,
                        bool retry = true);

  void acknowledgeResults(int64_t ackSequence);

  void abortResults();

  // Returns a shared ptr owning the current object.
  std::shared_ptr<TrinoExchangeSource> getSelfPtr();

  // Tracks the currently node-wide queued memory usage in bytes.
  static std::atomic<int64_t>& currQueuedMemoryBytes() {
    static std::atomic<int64_t> currQueuedMemoryBytes{0};
    return currQueuedMemoryBytes;
  }

  // Records the node-wide peak queued memory usage in bytes.
  // Tracks the currently node-wide queued memory usage in bytes.
  static std::atomic<int64_t>& peakQueuedMemoryBytes() {
    static std::atomic<int64_t> peakQueuedMemoryBytes{0};
    return peakQueuedMemoryBytes;
  }

  const std::string basePath_;
  const std::string host_;
  const uint16_t port_;
  const std::string clientCertAndKeyPath_;
  const std::string ciphers_;

  std::unique_ptr<http::HttpClient> httpClient_;
  RetryState dataRequestRetryState_;
  int failedAttempts_;
  uint64_t requestTimes_{0};
  uint64_t emptyResponseTimes_{0};
  uint64_t errorResponseTimes_{0};
  uint64_t nonEmptyRequestMicroRTTs_{0};
  std::atomic_bool closed_{false};
  // A boolean indicating whether abortResults() call was issued and was
  // successfully processed by the remote server.
  std::atomic_bool abortResultsSucceeded_{false};
  facebook::velox::VeloxPromise<Response> promise_{
      facebook::velox::VeloxPromise<Response>::makeEmpty()};

  folly::CPUThreadPoolExecutor* driverThreadPool_{nullptr};
};
}  // namespace io::trino::bridge
