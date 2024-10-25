#include "DADSRemoteBinaryLoaderEngine.hpp"
#include <DADS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <mutex>
#include <ostream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <cmath>
#include <future>
#include <thread>

#ifdef _WIN32
// curl includes windows headers without using NOMINMAX (causing issues with
// std::min/max)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif //_WIN32
#include <curl/curl.h>

// #define DEBUG

using std::string_literals::operator""s;
using dads::utilities::operator""_;
using dads::ComplexExpression;
using dads::Span;
using dads::Symbol;

using dads::Expression;

namespace dads::engines::RBL {
constexpr static int64_t const DEFAULT_PADDING = 0;
constexpr static int64_t const DEFAULT_ALIGNMENT = 1;
constexpr static int64_t const DEFAULT_MAX_RANGES = 1;
constexpr static int64_t const UNLIMITED_MAX_RANGES = -1;
constexpr static int64_t const DEFAULT_MAX_REQUESTS = 1;
constexpr static int64_t const UNLIMITED_MAX_REQUESTS = -1;
constexpr static int64_t const DEFAULT_THREADS = 8;
constexpr static bool const DEFAULT_TRACKING_CACHE = true;
static std::string const BOUNDARY_KEY = "boundary=";
  std::mutex printMutex;
using std::move;
using BufferManager = Engine::BufferManager;
using MultipartResponseHandler = Engine::MultipartResponseHandler;
  using URLExistenceInfo = Engine::URLExistenceInfo;
namespace utilities {} // namespace utilities

static size_t writeDataDefault(void *buffer, size_t size, size_t nmemb,
                               void *userp) {
  return size * nmemb;
};

static size_t writeRangeToBuffer(void *contents, size_t size, size_t nmemb,
                                 void *userp) {
  BufferManager *bufferMan = static_cast<BufferManager *>(userp);
  size_t size_b = size * nmemb;
  memcpy((bufferMan->buffer + bufferMan->currentOffset), contents, size_b);
  bufferMan->currentOffset += size_b;
  return size_b;
};

static size_t writeBoundaryString(void *contents, size_t size, size_t nmemb,
                                  void *userp) {
  char *data = static_cast<char *>(contents);
  size_t size_b = size * nmemb;
  std::string header(data, size_b);

  size_t start = header.find(BOUNDARY_KEY);
  if (start != std::string::npos) {
    start += BOUNDARY_KEY.length();
    std::string boundary = header.substr(start);
    boundary.erase(remove(boundary.begin(), boundary.end(), '\r'),
                   boundary.end());
    boundary.erase(remove(boundary.begin(), boundary.end(), '\n'),
                   boundary.end());
#ifdef DEBUG
    std::cout << "CURRENT BOUNDARY: " << boundary << std::endl;
#endif
    boundary = "--" + boundary;
    MultipartResponseHandler *responseHandler =
        static_cast<MultipartResponseHandler *>(userp);
    responseHandler->resetBoundary(std::move(boundary));
  }
  return size_b;
};

static size_t writeMultipartToBuffer(void *contents, size_t size, size_t nmemb,
                                     void *userp) {
  MultipartResponseHandler *responseHandler =
      static_cast<MultipartResponseHandler *>(userp);
  char *data = static_cast<char *>(contents);
  size_t size_b = size * nmemb;

  auto res = responseHandler->handleMultipart(data, size_b, 0);

  return res;
}

  URLExistenceInfo Engine::checkURLExists(CURL *curl, std::string const &url) {
    CURLcode res;
    int64_t responseCode = 0;
    int64_t requestSize = 0;
    int64_t headerSize = 0;
    int64_t pretransferTime = 0;
    int64_t totalTime = 0;
    
    if (curl) {
      curl_easy_reset(curl);
      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
      curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

      
#ifdef DEBUG
    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif
    
      res = curl_easy_perform(curl);

      if (res == CURLE_OK) {
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
        curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T, &pretransferTime);
        curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T,
                                &totalTime);
      }
    }

    int64_t downloadTime = totalTime - pretransferTime;

    return { responseCode == 200, requestSize, headerSize, pretransferTime, downloadTime };
  }

void Engine::allocateRangeFromURL(BufferManager &bufferMan, CURL *curl,
                                  std::string const &range,
                                  std::string const &url,
                                  size_t (*writeResponseFunc)(void *, size_t,
                                                              size_t, void *),
                                  bool isTrackingOverhead) {
  CURLcode res;

  if (curl) {
    curl_easy_reset(curl);

#ifdef DEBUG
    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &bufferMan);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    res = curl_easy_perform(curl);
    if (isTrackingOverhead) {
      if (res == CURLE_OK) {
        int64_t downloadSize = 0;
        int64_t requestSize = 0;
        int64_t headerSize = 0;
        int64_t pretransferTime = 0;
        int64_t totalTime = 0;

#ifdef DEBUG
        std::cout << "COLLECTING DATA" << std::endl;
#endif

        res = curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD, &downloadSize);
        res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
                                &pretransferTime);
        res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);

#ifdef DEBUG
        std::cout << "COLLECTED DATA" << std::endl;
#endif

        bufferMan.totalDownloaded += downloadSize;
        bufferMan.totalRequested += requestSize;
        bufferMan.totalHeadersDownloaded += headerSize;
        bufferMan.totalPretransferTime += pretransferTime;
        bufferMan.totalDownloadTime += totalTime - pretransferTime;

#ifdef DEBUG
        std::cout << "ADDED DATA TO BUFFER MANAGER" << std::endl;
#endif
      }
    }

#ifdef DEBUG
    if (res != CURLE_OK) {
      std::cout << "ERROR CODE:" << std::endl;
      std::cout << res << std::endl;
      std::cout << "ERROR BUFFER:" << std::endl;
      std::cout << errbuf << std::endl;
    }
#endif
  }
};

void Engine::allocateRangesFromURL(BufferManager &bufferMan, CURL *curl,
                                   std::string const &ranges,
                                   std::string const &url,
                                   size_t (*writeResponseFunc)(void *, size_t,
                                                               size_t, void *),
                                   bool isTrackingOverhead) {
  CURLcode res;
  MultipartResponseHandler responseHandler(&bufferMan);

  if (curl) {
    curl_easy_reset(curl);

#ifdef DEBUG
    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_RANGE, ranges.c_str());
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, &writeBoundaryString);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &responseHandler);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseHandler);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    res = curl_easy_perform(curl);

    if (isTrackingOverhead) {
      if (res == CURLE_OK) {
	int64_t downloadSize = 0;
        int64_t requestSize = 0;
        int64_t headerSize = 0;
        int64_t pretransferTime = 0;
        int64_t totalTime = 0;

#ifdef DEBUG
        std::cout << "COLLECTING DATA" << std::endl;
#endif

        res = curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD, &downloadSize);
        res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
                                &pretransferTime);
        res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);

#ifdef DEBUG
        std::cout << "COLLECTED DATA" << std::endl;
#endif

        bufferMan.totalDownloaded += downloadSize;
        bufferMan.totalRequested += requestSize;
        bufferMan.totalHeadersDownloaded += headerSize;
        bufferMan.totalPretransferTime += pretransferTime;
        bufferMan.totalDownloadTime += totalTime - pretransferTime;

#ifdef DEBUG
        std::cout << "ADDED DATA TO BUFFER MANAGER" << std::endl;
#endif
      }
    }

#ifdef DEBUG
    if (res != CURLE_OK) {
      std::cout << "ERROR CODE:" << std::endl;
      std::cout << res << std::endl;
      std::cout << "ERROR BUFFER:" << std::endl;
      std::cout << errbuf << std::endl;
    }
#endif
  }
};

  void Engine::allocateRangesWithEasy(BufferManager &bufferMan, CURL* curlHand,
				       std::vector<std::string> const &rangeSets, 
				       std::string const &url,
				       size_t (*writeResponseFunc)(void *, size_t,
								   size_t, void *),
				      bool isTrackingOverhead) {
    for (auto const &rangeSet : rangeSets) {
      allocateRangesFromURL(bufferMan, curlHand, rangeSet, url, writeResponseFunc, isTrackingOverhead);
    }
  };


  void Engine::trackEasyTransferData(BufferManager &bufferMan, CURL* curl) {
    if (curl) {
      CURLcode res;
      int64_t downloadSize = 0;
      int64_t requestSize = 0;
      int64_t headerSize = 0;
      int64_t pretransferTime = 0;
      int64_t totalTime = 0;

#ifdef DEBUG
      std::cout << "COLLECTING DATA" << std::endl;
#endif

      res = curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD, &downloadSize);
      res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
      res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
      res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
			      &pretransferTime);
      res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);

#ifdef DEBUG
      std::cout << "COLLECTED DATA" << std::endl;
#endif

      bufferMan.totalDownloaded += downloadSize;
      bufferMan.totalRequested += requestSize;
      bufferMan.totalHeadersDownloaded += headerSize;
      bufferMan.totalPretransferTime += pretransferTime;
      bufferMan.totalDownloadTime += totalTime - pretransferTime;

#ifdef DEBUG
      std::cout << "ADDED DATA TO BUFFER MANAGER" << std::endl;
#endif
    }
  }

  void Engine::setUpEasyForMulti(MultipartResponseHandler &responseHandler, CURLM *multiCurl, CURL *curl,
                                   std::string const &ranges,
                                   std::string const &url,
                                   size_t (*writeResponseFunc)(void *, size_t,
                                                               size_t, void *)) {
    if (curl) {
      curl_easy_reset(curl);

      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
      curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 600L);
      curl_easy_setopt(curl, CURLOPT_RANGE, ranges.c_str());
      curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, &writeBoundaryString);
      curl_easy_setopt(curl, CURLOPT_HEADERDATA, &responseHandler);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponseFunc);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseHandler);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

      curl_multi_add_handle(multiCurl, curl);
    }
  }

  void Engine::allocateRangesWithMulti(BufferManager &bufferMan, CurlManager &curlMan,
				       std::vector<std::string> &rangeSets, 
				       std::string const &url,
				       size_t (*writeResponseFunc)(void *, size_t,
								   size_t, void *),
				       bool isTrackingOverhead) {

    auto totalRequests = rangeSets.size();
    auto maxHandles = curlMan.maxHandles;
    // std::cout << "TOTAL REQUESTS: " << totalRequests << " MAX HANDLES: " << maxHandles << std::endl;
#ifdef DEBUG
    std::cout << "TOTAL REQUESTS: " << totalRequests << " MAX HANDLES: " << maxHandles << std::endl;
#endif
    CURLM* multiHandle = curlMan.getMultiHandle();

    for (auto j = 0; j < totalRequests; j += maxHandles) {
      auto numRequests = maxHandles < totalRequests - j ? maxHandles : totalRequests - j;
      std::vector<MultipartResponseHandler> responseHandlers;
      std::vector<CURL*> usedEasyHandles;
      
      for (size_t i = 0; i < numRequests; i++) {
	MultipartResponseHandler responseHandler(&bufferMan);
	responseHandlers.push_back(std::move(responseHandler));
      }
    
      for (size_t i = 0; i < numRequests; i++) {
	CURL* easyHandle = curlMan.getEasyHandle();
	if (easyHandle) {
#ifdef DEBUG
	  std::cout << "I: " << i << " J: " << j  << std::endl; //  " RANGE: " << rangeSets[j + i] <<
#endif
	  setUpEasyForMulti(responseHandlers[i], multiHandle, easyHandle, rangeSets[j + i], url, writeResponseFunc);
	  usedEasyHandles.push_back(easyHandle);
	}
      }

      int stillRunning = 0;
      curl_multi_perform(multiHandle, &stillRunning);
      while (stillRunning) {
	int numfds = 0;
	CURLMcode mc = curl_multi_poll(multiHandle, nullptr, 0, 500, &numfds);

	if (mc != CURLM_OK) {
	  std::cerr << "curl_multi_wait() failed: " << curl_multi_strerror(mc) << std::endl;
	  break;
	}

	curl_multi_perform(multiHandle, &stillRunning);

	// int msgsLeft = 0;
	// while (CURLMsg* msg = curl_multi_info_read(multiHandle, &msgsLeft)) {
	// 	if (msg->msg == CURLMSG_DONE) {
	// 	  CURL* currEasy = msg->easy_handle;
	// 	  if (currEasy) {
	// 	    if (isTrackingOverhead) {
	// 	      trackEasyTransferData(bufferMan, currEasy);
	// 	    }
	// 	    curl_multi_remove_handle(multiHandle, currEasy);
	// 	    curlMan.releaseEasyHandle(currEasy);
	// 	  }
	// 	}
	// }
      }

      for (size_t i = 0; i < numRequests; i++) {
	CURL* easyHandle = usedEasyHandles[i];
	if (easyHandle) {
	  if (isTrackingOverhead) {
	    trackEasyTransferData(bufferMan, easyHandle);
	  }
	  curl_multi_remove_handle(multiHandle, easyHandle);
	  curlMan.releaseEasyHandle(easyHandle);
	}
      }
    }
  }  

uint64_t Engine::getFileLength(std::string const &url, CURL *curl) {
  CURLcode res;
  double fileSize = 0.0;

  if (curl) {
    curl_easy_reset(curl);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeDataDefault);
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res)
                << std::endl;
    } else {
      res =
          curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &fileSize);
      if ((res != CURLE_OK) || (fileSize <= 0.0)) {
        std::cerr << "Error: Failed to retrieve file size or file size unknown."
                  << std::endl;
      }
    }
  }
  return static_cast<uint64_t>(fileSize);
};

std::vector<std::pair<int64_t, int64_t>>
mergeBounds(std::vector<std::pair<int64_t, int64_t>> &intervals,
            int64_t padding, int64_t alignment) {
  std::for_each(intervals.begin(), intervals.end(),
		[&padding](auto &a) {
		  a.second += padding;
		});
  std::for_each(intervals.begin(), intervals.end(),
                [&alignment](auto &a) {
		  a.first -= a.first % alignment;
		  a.second += alignment - (a.second % alignment);
		});
  std::sort(
      intervals.begin(), intervals.end(),
      [](const std::pair<int64_t, int64_t> &a,
         const std::pair<int64_t, int64_t> &b) { return a.first < b.first; });

  std::vector<std::pair<int64_t, int64_t>> merged;
  for (auto &[first, second] : intervals) {
    if (merged.empty() || merged.back().second < first) {
      merged.emplace_back(first, second);
      // merged.emplace_back(first,
      //                     (second + padding) +
      //                         (alignment - ((second + padding) % alignment)));
    } else {
      merged.back().second = std::max(merged.back().second, second);
    }
  }
  return merged;
}

  std::vector<std::pair<int64_t, int64_t>> alignBoundsToRanges(std::vector<std::pair<int64_t, int64_t>> &bounds, int64_t ranges, int64_t requests) {
    auto totalRanges = ranges * requests;
    if (totalRanges < 1 || ranges < 1 || totalRanges > bounds.size()) {
      return bounds;
    }
    auto boundsPerRange = std::ceil(bounds.size() / totalRanges);

    std::vector<std::pair<int64_t, int64_t>> rangeMerged;
    rangeMerged.reserve(totalRanges);

    for (auto currBoundI = 0; currBoundI < bounds.size(); currBoundI += boundsPerRange) {
      auto possibleNextI = currBoundI + boundsPerRange;
      auto lastI = possibleNextI < bounds.size() ? possibleNextI - 1 : bounds.size() - 1;
      auto const &[firstLB, firstUB] = bounds[currBoundI];
      auto const &[lastLB, lastUB] = bounds[lastI];
      rangeMerged.emplace_back(firstLB, lastUB);
    }
    
    return std::move(rangeMerged);
  }

std::vector<std::pair<int64_t, int64_t>> Engine::extractBoundPairs(
    std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
    BufferManager &buffMan, int64_t padding, int64_t alignment, int64_t ranges, int64_t requests) {

  
#ifdef DEBUG
  std::cout << "REQUESTED COUNT: " << requestedBounds.size() << std::endl;
  // for (auto &[lb, ub] : requestedBounds) {
  //   std::cout << "(" << lb << ", " << ub << ")" << std::endl;
  // }

#endif
  std::vector<std::pair<int64_t, int64_t>> mergedBounds =
    mergeBounds(requestedBounds, padding, alignment);

#ifdef DEBUG
  std::cout << "MERGED: " << mergedBounds.size() << std::endl;
  // for (auto &[lb, ub] : mergedBounds) {
  //   std::cout << "(" << lb << ", " << ub << ")" << std::endl;
  // }
#endif
  std::vector<std::pair<int64_t, int64_t>> rangeMergedBounds =
    alignBoundsToRanges(mergedBounds, ranges, requests);
#ifdef DEBUG
  std::cout << "RANGE MERGED: " << rangeMergedBounds.size() << std::endl;
  // for (auto &[lb, ub] : rangeMergedBounds) {
  //   std::cout << "(" << lb << ", " << ub << ")" << std::endl;
  // }
#endif
  std::vector<std::pair<int64_t, int64_t>> neededBounds;
  for (const auto &[lb, ub] : rangeMergedBounds) {
    auto validBounds = buffMan.requestBounds(lb, ub);
    // auto validBounds = buffMan.addRange(lb, ub);
    neededBounds.insert(neededBounds.end(),
                        std::make_move_iterator(validBounds.begin()),
                        std::make_move_iterator(validBounds.end()));
  }
#ifdef DEBUG
  std::cout << "NEEDED: " << neededBounds.size() << std::endl;
  for (auto &[lb, ub] : neededBounds) {
    std::cout << "    (" << lb << ", " << ub << ")" << std::endl;
  }
#endif
  return neededBounds;
}

std::vector<std::pair<int64_t, int64_t>> Engine::extractBoundPairs(
    const std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
    DummyBufferManager &buffMan) {

  std::vector<std::pair<int64_t, int64_t>> neededBounds;
  for (const auto &[lb, ub] : requestedBounds) {
    auto validBounds = buffMan.requestBounds(lb, ub);
    neededBounds.insert(neededBounds.end(),
                        std::make_move_iterator(validBounds.begin()),
                        std::make_move_iterator(validBounds.end()));
  }
#ifdef DEBUG
  std::cout << "NEEDED: " << neededBounds.size() << std::endl;
  // for (auto &[lb, ub] : neededBounds) {
  //   std::cout << "(" << lb << ", " << ub << ")" << std::endl;
  // }
#endif
  return neededBounds;
}

std::vector<std::pair<int64_t, int64_t>>
extractBoundPairsFromExpression(ComplexExpression &&e) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();
  if (head != "List"_) {
    throw std::runtime_error("Cannot extract bounds from non-List expression");
  }

  std::vector<std::pair<int64_t, int64_t>> requestedBounds;
  if (spans.size() != 0) {
    size_t numBounds = std::accumulate(
      spans.begin(), spans.end(), (size_t)0,
      [](auto runningSum, auto const &argument) {
        if (std::holds_alternative<dads::Span<int64_t>>(argument)) {
          return runningSum + get<dads::Span<int64_t>>(argument).size();
        }
        return runningSum;
      });
    if (numBounds % 2 == 0) {
      requestedBounds.reserve((numBounds / 2));
    }
    
    for (auto it = std::make_move_iterator(spans.begin());
         it != std::make_move_iterator(spans.end()); std::advance(it, 1)) {
      if (std::holds_alternative<dads::Span<int64_t>>(*it)) {
        auto typedSpan = get<dads::Span<int64_t>>(*it);

        if (typedSpan.size() % 2 != 0) {
          throw std::runtime_error("Bounds spans must contain an even number "
                                   "of elements to represent "
                                   "lower and upper bound pairs");
        }
        for (auto spanIt = std::make_move_iterator(typedSpan.begin());
             spanIt != std::make_move_iterator(typedSpan.end());
             std::advance(spanIt, 2)) {
          auto const &lb = *spanIt;
          auto const &ub = *(spanIt + 1);
          requestedBounds.emplace_back(lb, ub);
        }
      }
    }
  } else {
    if (dynamics.size() % 2 != 0) {
      throw std::runtime_error(
          "Bounds lists must contain an even number of elements to represent "
          "lower and upper bound pairs");
    }
    requestedBounds.reserve((dynamics.size() / 2));

    for (auto it = dynamics.begin(); it != dynamics.end();
         std::advance(it, 2)) {
      auto const &lb = get<int64_t>(*it);
      auto const &ub = get<int64_t>(*(it + 1));
      requestedBounds.emplace_back(lb, ub);
    }
  }

  return std::move(requestedBounds);
}

dads::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      dads::utilities::overload(
          [this](ComplexExpression &&expression) -> dads::Expression {
	    if (!curlGlobalSet) {
	      curl_global_init(CURL_GLOBAL_DEFAULT);
	      curlGlobalSet = true;
	    }
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "Fetch"_) {
              if (std::holds_alternative<ComplexExpression>(dynamics[0])) {
                auto const &url = get<std::string>(dynamics[1]);
                int64_t padding = DEFAULT_PADDING;
                if (dynamics.size() > 2) {
                  padding = get<int64_t>(dynamics[2]);
                  padding = padding < 0 ? DEFAULT_PADDING : padding;
                };
                int64_t alignment = DEFAULT_ALIGNMENT;
                if (dynamics.size() > 3) {
                  alignment = get<int64_t>(dynamics[3]);
                  alignment = alignment < 1 ? DEFAULT_ALIGNMENT : alignment;
                };
                int64_t maxRanges = DEFAULT_MAX_RANGES;
                bool enableMultipart = false;
                if (dynamics.size() > 4) {
                  maxRanges = get<int64_t>(dynamics[4]);
                  maxRanges = maxRanges < 1 ? UNLIMITED_MAX_RANGES : maxRanges;
                }
                int64_t maxRequests = DEFAULT_MAX_REQUESTS;
                if (dynamics.size() > 5) {
                  maxRequests = get<int64_t>(dynamics[5]);
		  maxRequests = maxRequests < 1 ? UNLIMITED_MAX_REQUESTS : maxRequests;
                }
		bool trackingCache = DEFAULT_TRACKING_CACHE;
		if (dynamics.size() > 6) {
		  maxRequests = UNLIMITED_MAX_REQUESTS;
		  trackingCache = get<bool>(dynamics[6]);
		}
		NUM_THREADS = DEFAULT_THREADS;
		if (dynamics.size() > 7) {
		  NUM_THREADS = get<int64_t>(dynamics[7]);
		}
		
		auto requestsManagersIt = requestsManagersMap.find(url);
		if (requestsManagersIt == requestsManagersMap.end()) {
		  RequestsManager requestsMan(NUM_THREADS, NUM_HANDLES, url, 2);
		  requestsManagersMap.emplace(url, std::move(requestsMan));
		}
		RequestsManager &requestsMan = requestsManagersMap.find(url)->second;

		auto &existsInfo = requestsMan.existsInfo;
		auto &curl = requestsMan.mainHandle;
		auto &curlMan = requestsMan.mainManager;
  
                auto bufferIt = bufferMap.find(url);
                if (bufferIt == bufferMap.end()) {
		  auto const totalSize = getFileLength(url, curl);
                  bufferMap.emplace(url, Engine::BufferManager(totalSize, existsInfo.exists));
                  if (isTrackingOverhead && isTrackingRequired) {
                    overheadBufferMap.emplace(
                        url, Engine::DummyBufferManager(totalSize));
                  }
                }

                auto requestedBounds = extractBoundPairsFromExpression(
                    std::move(get<ComplexExpression>(dynamics[0])));

                if (isTrackingOverhead && isTrackingRequired) {
                  auto &dummyBufferMan = overheadBufferMap[url];
                  auto dummyBounds =
                      extractBoundPairs(requestedBounds, dummyBufferMan);
                  dummyBufferMan.addLoaded(dummyBounds);
                }

                auto &bufferMan = bufferMap[url];
		
		bufferMan.totalRequested += existsInfo.requested;
		bufferMan.totalHeadersDownloaded += existsInfo.headers;
		bufferMan.totalPretransferTime += existsInfo.pretransferTime;
		bufferMan.totalDownloadTime += existsInfo.downloadTime;
		if (!bufferMan.exists) {
		  throw std::runtime_error("Remote resource does not exist.");
		}

                auto bounds = trackingCache ? extractBoundPairs(requestedBounds, bufferMan,
								padding, alignment, maxRanges, maxRequests) :
		  std::move(requestedBounds);
		
                if (isTrackingOverhead) {
                  bufferMan.addLoaded(bounds);
                }

		auto numRequests = 0;
                if (bounds.size() > 1 && maxRanges != DEFAULT_MAX_RANGES) {
		  std::stringstream ranges;
                  bool isFirst = true;
                  if (maxRanges == UNLIMITED_MAX_RANGES) {
                    for (const auto &[lowerBound, upperBound] : bounds) {
                      if (isFirst) {
                        ranges << lowerBound << "-" << (upperBound - 1);
                        isFirst = false;
                      } else {
                        ranges << ", " << lowerBound << "-" << (upperBound - 1);
                      }
                    }
                    std::string rangesStr = ranges.str();

                    allocateRangesFromURL(bufferMan, curl, rangesStr, url,
                                          &writeMultipartToBuffer,
                                          isTrackingOverhead);
		    numRequests++;
                  } else {
		    
		    if (trackingCache) {
		      for (size_t i = 0; i < bounds.size(); i += maxRanges) {
			auto remaining = maxRanges < (bounds.size() - i)
			  ? maxRanges
			  : (bounds.size() - i);
			for (size_t j = 0; j < remaining; j++) {
			  const auto &[lowerBound, upperBound] = bounds[j + i];
			  if (isFirst) {
			    ranges << lowerBound << "-" << (upperBound - 1);
			    isFirst = false;
			  } else {
			    ranges << ", " << lowerBound << "-"
				   << (upperBound - 1);
			  }
			}
			std::string rangesStr = ranges.str();
			int64_t boundsLeft = bounds.size() - (i + remaining);
#ifdef DEBUG
		        std::cout << "     REQUEST NO: " << numRequests << " CURR BOUND I: " << i << " BOUNDS LEFT: " << boundsLeft << std::endl;
#endif
			allocateRangesFromURL(bufferMan, curl, rangesStr, url,
						  &writeMultipartToBuffer,
						  isTrackingOverhead);
			numRequests++;
		        isFirst = true;
			ranges.str("");
		      }
		    } else {
		      bool multithreaded = requestsMan.multithreaded;
		      if (multithreaded) {
		        size_t numThreads = NUM_THREADS;
			size_t numRangeSets = std::ceil((double) bounds.size() / (double) maxRanges);
			size_t numRangeSetsPerThread = std::ceil((double) numRangeSets / (double) numThreads);

			auto currRangeSetI = 0;
			auto threadI = 0;
			
			std::vector<std::vector<std::string>> rangesToDo(numThreads);
		        rangesToDo.clear();
			rangesToDo.resize(numThreads);
			for (size_t i = 0, currRangeSetI = 0; i < bounds.size(); i += maxRanges, currRangeSetI++) {
			  if (currRangeSetI >= numRangeSetsPerThread) {
			    currRangeSetI = 0;
			    threadI++;
			  }
			  auto remaining = maxRanges < (bounds.size() - i)
			    ? maxRanges
			    : (bounds.size() - i);
			  for (size_t j = 0; j < remaining; j++) {
			    const auto &[lowerBound, upperBound] = bounds[j + i];
			    if (isFirst) {
			      ranges << lowerBound << "-" << (upperBound - 1);
			      isFirst = false;
			    } else {
			      ranges << ", " << lowerBound << "-"
				     << (upperBound - 1);
			    }
			  }
			  std::string rangesStr = ranges.str();
			  int64_t boundsLeft = bounds.size() - (i + remaining);
#ifdef DEBUG
			  std::cout << "     REQUEST NO: " << numRequests << " CURR BOUND I: " << i << " BOUNDS LEFT: " << boundsLeft << std::endl;
#endif
	        	  rangesToDo[threadI].push_back(std::move(rangesStr));
			  isFirst = true;
			  ranges.str("");
			  ranges.clear();
			}

			// std::cout << "TOTAL REQUESTS: " << numRangeSets << " MAX THREADS: " << numThreads << " RANGE SETS PER THREAD: " << numRangeSetsPerThread << std::endl;

			requestsMan.createMultithreadedRequests(bufferMan, std::move(rangesToDo), &writeMultipartToBuffer, true);
			
		      } else {
			std::vector<std::string> rangesToDo;
			for (size_t i = 0; i < bounds.size(); i += maxRanges) {
			  auto remaining = maxRanges < (bounds.size() - i)
			    ? maxRanges
			    : (bounds.size() - i);
			  for (size_t j = 0; j < remaining; j++) {
			    const auto &[lowerBound, upperBound] = bounds[j + i];
			    if (isFirst) {
			      ranges << lowerBound << "-" << (upperBound - 1);
			      isFirst = false;
			    } else {
			      ranges << ", " << lowerBound << "-"
				     << (upperBound - 1);
			    }
			  }
			  std::string rangesStr = ranges.str();
			  int64_t boundsLeft = bounds.size() - (i + remaining);
#ifdef DEBUG
			  std::cout << "     REQUEST NO: " << numRequests << " CURR BOUND I: " << i << " BOUNDS LEFT: " << boundsLeft << std::endl;
#endif
			  // std::cout << "     REQUEST NO: " << numRequests << " CURR BOUND I: " << i << " BOUNDS LEFT: " << boundsLeft << std::endl;
			  rangesToDo.emplace_back(std::move(rangesStr));
			  isFirst = true;
			  ranges.str("");
			}
			allocateRangesWithMulti(bufferMan, curlMan, rangesToDo, url, &writeMultipartToBuffer, isTrackingOverhead);
		      }
		    }
                  }
                } else {
		  for (const auto &[lowerBound, upperBound] : bounds) {
                    bufferMan.currentOffset = lowerBound;
                    allocateRangeFromURL(bufferMan, curl,
                                         std::to_string(lowerBound) + "-" +
                                             std::to_string(upperBound - 1),
                                         url, &writeRangeToBuffer,
                                         isTrackingOverhead);
		    numRequests++;
                  }
                }
                auto resSpan =
                    dads::Span<int8_t>(bufferMan.buffer, bufferMan.totalSize, nullptr);

                return "ByteSequence"_(std::move(resSpan));
              } else {
                dads::ExpressionArguments args;
                for (size_t i = 0; i < dynamics.size(); i++) {
                  auto const &url = get<std::string>(dynamics[i]);
		  URLExistenceInfo existsInfo;

                  auto curlHandlesIt = curlHandlesMap.find(url);
                  if (curlHandlesIt == curlHandlesMap.end()) {
                    CURL *curl;
                    curl = curl_easy_init();
                    if (curl) {
		      existsInfo = checkURLExists(curl, url);
                      curlHandlesMap.emplace(url, curl);
                    } else {
                      throw std::runtime_error(
                          "Could not allocate resources for curl handle");
                    }
                  }
                  CURL *curl = curlHandlesMap[url];


                  auto bufferIt = bufferMap.find(url);
                  if (bufferIt == bufferMap.end()) {
		    auto const totalSize = getFileLength(url, curl);
                    bufferMap.emplace(url, Engine::BufferManager(totalSize, existsInfo.exists));
                    if (isTrackingOverhead && isTrackingRequired) {
                      overheadBufferMap.emplace(
                          url, Engine::DummyBufferManager(totalSize));
                    }
                  }
		  
                  auto &bufferMan = bufferMap[url];

		  bufferMan.totalRequested += existsInfo.requested;
		  bufferMan.totalHeadersDownloaded += existsInfo.headers;
		  bufferMan.totalPretransferTime += existsInfo.pretransferTime;
		  bufferMan.totalDownloadTime += existsInfo.downloadTime;
		  if (!bufferMan.exists) {
		    throw std::runtime_error("Remote resource does not exist.");
		  }
		  
                  std::vector<std::pair<int64_t, int64_t>> wholeBounds = {
                      {0, bufferMan.totalSize}};

                  if (isTrackingOverhead && isTrackingRequired) {
                    auto &dummyBufferMan = overheadBufferMap[url];
                    auto dummyBounds =
                        extractBoundPairs(wholeBounds, dummyBufferMan);
                    dummyBufferMan.addLoaded(dummyBounds);
                  }

                  auto bounds =
                      extractBoundPairs(wholeBounds, bufferMan, DEFAULT_PADDING,
                                        DEFAULT_ALIGNMENT, DEFAULT_MAX_RANGES, DEFAULT_MAX_REQUESTS);

                  if (isTrackingOverhead) {
                    bufferMan.addLoaded(bounds);
                  }

                  for (const auto &[lowerBound, upperBound] : bounds) {
                    bufferMan.currentOffset = lowerBound;
                    allocateRangeFromURL(bufferMan, curl,
                                         std::to_string(lowerBound) + "-" +
                                             std::to_string(upperBound - 1),
                                         url, &writeRangeToBuffer,
                                         isTrackingOverhead);
                  }

                  auto resSpan =
                      dads::Span<int8_t>(bufferMan.buffer, bufferMan.totalSize, nullptr);
                  auto resExpr = "ByteSequence"_(std::move(resSpan));
                  args.push_back(std::move(resExpr));

		  
		  curl_easy_cleanup(curl);
		  curlHandlesMap.erase(url);
                }

                return dads::ComplexExpression("ByteSequences"_, {},
                                               std::move(args), {});
              }
            } else if (head == "StartTrackingOverhead"_) {
	      if (dynamics.size() > 0) {
		isTrackingRequired =
		  std::holds_alternative<dads::Symbol>(dynamics[0]) &&
		  get<dads::Symbol>(dynamics[0]) == "TrackRequired"_;
	      }
              isTrackingOverhead = true;
              return "TrackingOverhead"_;
            } else if (head == "StopTrackingOverhead"_) {
	      isTrackingRequired = false;
              isTrackingOverhead = false;
              return "NotTrackingOverhead"_;
            } else if (head == "GetOverhead"_) {
              auto const &url = get<std::string>(dynamics[0]);
	      
	      auto totalRequired = -1;
	      if (isTrackingRequired) {
		auto overheadBufferIt = overheadBufferMap.find(url);
		if (overheadBufferIt == overheadBufferMap.end()) {
		  return "InvalidOverheadRequest"_();
		}
		auto &dummyBufferMan = overheadBufferMap[url];
		totalRequired = dummyBufferMan.totalRequired;
	      }
	      
              auto bufferIt = bufferMap.find(url);
              if (bufferIt == bufferMap.end()) {
                return "InvalidOverheadRequest"_();
              }
              auto &bufferMan = bufferMap[url];
	      
              return "Overhead"_(
                  "Size"_("Loaded"_(bufferMan.totalLoaded),
                          "Downloaded"_(bufferMan.totalDownloaded),
                          "Required"_(totalRequired),
                          "Requested"_(bufferMan.totalRequested),
                          "Headers"_(bufferMan.totalHeadersDownloaded)),
                  "Time"_("Pretransfer"_(bufferMan.totalPretransferTime),
                          "Download"_(bufferMan.totalDownloadTime)));
            } else if (head == "GetTotalOverhead"_) {
              int64_t totalLoaded = 0;
              int64_t totalDownloaded = 0;
              int64_t totalRequired = isTrackingRequired ? 0 : -1;
              int64_t totalRequested = 0;
              int64_t totalHeaders = 0;
              int64_t totalPretransferTime = 0;
              int64_t totalDownloadTime = 0;

              for (auto const &[url, bufferMan] : bufferMap) {
		if (isTrackingRequired) {
		  auto &dummyBufferMan = overheadBufferMap[url];
		  totalRequired += dummyBufferMan.totalRequired;
		}
                totalLoaded += bufferMan.totalLoaded;
                totalDownloaded += bufferMan.totalDownloaded;
                totalRequested += bufferMan.totalRequested;
                totalHeaders += bufferMan.totalHeadersDownloaded;
                totalPretransferTime += bufferMan.totalPretransferTime;
                totalDownloadTime += bufferMan.totalDownloadTime;
              }

              return "Overhead"_("Size"_("Loaded"_(totalLoaded),
                                         "Downloaded"_(totalDownloaded),
                                         "Required"_(totalRequired),
                                         "Requested"_(totalRequested),
                                         "Headers"_(totalHeaders)),
                                 "Time"_("Pretransfer"_(totalPretransferTime),
                                         "Download"_(totalDownloadTime)));

            } else if (head == "ClearCaches"_) {
	      for (auto &[url, curlHandle] : curlHandlesMap) {
		curl_easy_cleanup(curlHandle);
	      }
	      requestsManagersMap.clear();
	      curlHandlesMap.clear();
              bufferMap.clear();
              overheadBufferMap.clear();
              return "CachesCleared"_;
            } else if (head == "GetFileLength"_) {
	      auto const &url = get<std::string>(dynamics[0]);
	      CURL *curl;
	      curl = curl_easy_init();
	      if (curl) {
		auto const totalSize = getFileLength(url, curl);
		return (int64_t) totalSize;
	      } else {
		throw std::runtime_error("Could not allocate resources for curl handle");
	      }
            } else if (head == "GetEngineCapabilities"_) {
              return "List"_("Fetch"_, "StartTrackingOverhead"_,
                             "StopTrackingOverhead"_, "GetOverhead"_,
                             "GetTotalOverhead"_, "ClearCaches"_, "GetFileLength"_);
            }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return dads::ComplexExpression(
                std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> dads::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> dads::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

} // namespace dads::engines::RBL

static auto &enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<dads::engines::RBL::Engine>();
  if (!engine && initialise) {
    engine.reset(new dads::engines::RBL::Engine());
  }
  return engine;
}

extern "C" DADSExpression *evaluate(DADSExpression *e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto *r = new DADSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
