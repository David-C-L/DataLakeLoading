#include "BOSSRemoteBinaryLoaderEngine.hpp"
#include <BOSS.hpp>
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
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::RBL {
constexpr static int64_t const DEFAULT_PADDING = 0;
constexpr static int64_t const DEFAULT_ALIGNMENT = 1;
constexpr static int64_t const DEFAULT_MAX_RANGES = 1;
constexpr static int64_t const UNLIMITED_MAX_RANGES = -1;
static std::string const BOUNDARY_KEY = "boundary=";
using std::move;
using BufferManager = Engine::BufferManager;
using MultipartResponseHandler = Engine::MultipartResponseHandler;
namespace utilities {} // namespace utilities

  struct URLExistenceInfo {
    bool exists;
    int64_t requested;
    int64_t headers;
    int64_t pretransferTime;
    int64_t downloadTime;
  };

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

  URLExistenceInfo checkURLExists(CURL *curl, std::string const &url) {
    CURLcode res;
    int64_t responseCode = 0;
    int64_t requestSize = 0;
    int64_t headerSize = 0;
    int64_t pretransferTime = 0;
    int64_t totalTime = 0;
    
    if (curl) {
      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
      curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

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

    res = curl_easy_perform(curl);
    if (isTrackingOverhead) {
      if (res == CURLE_OK) {
        int64_t requestSize = 0;
        int64_t headerSize = 0;
        int64_t pretransferTime = 0;
        int64_t totalTime = 0;

#ifdef DEBUG
        std::cout << "COLLECTING DATA" << std::endl;
#endif

        res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
                                &pretransferTime);
        res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);

#ifdef DEBUG
        std::cout << "COLLECTED DATA" << std::endl;
#endif

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

    res = curl_easy_perform(curl);

    if (isTrackingOverhead) {
      if (res == CURLE_OK) {
        int64_t requestSize = 0;
        int64_t headerSize = 0;
        int64_t pretransferTime = 0;
        int64_t totalTime = 0;

#ifdef DEBUG
        std::cout << "COLLECTING DATA" << std::endl;
#endif

        res = curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &requestSize);
        res = curl_easy_getinfo(curl, CURLINFO_HEADER_SIZE, &headerSize);
        res = curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T,
                                &pretransferTime);
        res = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME_T, &totalTime);

#ifdef DEBUG
        std::cout << "COLLECTED DATA" << std::endl;
#endif

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

uint64_t Engine::getFileLength(std::string const &url, CURL *curl) {
  CURLcode res;
  double fileSize = 0.0;

  if (curl) {
    curl_easy_reset(curl);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeDataDefault);
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);

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

std::vector<std::pair<int64_t, int64_t>> Engine::extractBoundPairs(
    std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
    BufferManager &buffMan, int64_t padding, int64_t alignment) {

#ifdef DEBUG
  std::cout << "REQUESTED COUNT: " << requestedBounds.size() << std::endl;
#endif
  std::vector<std::pair<int64_t, int64_t>> mergedBounds =
      mergeBounds(requestedBounds, padding, alignment);
#ifdef DEBUG
  std::cout << "MERGED: " << mergedBounds.size() << std::endl;
#endif
  std::vector<std::pair<int64_t, int64_t>> neededBounds;
  for (const auto &[lb, ub] : mergedBounds) {
    auto validBounds = buffMan.requestBounds(lb, ub);
    neededBounds.insert(neededBounds.end(),
                        std::make_move_iterator(validBounds.begin()),
                        std::make_move_iterator(validBounds.end()));
  }
#ifdef DEBUG
  std::cout << "NEEDED: " << neededBounds.size() << std::endl;
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
        if (std::holds_alternative<boss::Span<int64_t>>(argument)) {
          return runningSum + get<boss::Span<int64_t>>(argument).size();
        }
        return runningSum;
      });
    if (numBounds % 2 == 0) {
      requestedBounds.reserve((numBounds / 2));
    }
    
    for (auto it = std::make_move_iterator(spans.begin());
         it != std::make_move_iterator(spans.end()); std::advance(it, 1)) {
      if (std::holds_alternative<boss::Span<int64_t>>(*it)) {
        auto typedSpan = get<boss::Span<int64_t>>(*it);

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

boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
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
		  
                auto bounds = extractBoundPairs(requestedBounds, bufferMan,
                                                padding, alignment);
                if (isTrackingOverhead) {
                  bufferMan.addLoaded(bounds);
                }

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
                  } else {
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

                      allocateRangesFromURL(bufferMan, curl, rangesStr, url,
                                            &writeMultipartToBuffer,
                                            isTrackingOverhead);

                      isFirst = true;
                      ranges.str("");
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
                  }
                }

                auto resSpan =
                    boss::Span<int8_t>(bufferMan.buffer, bufferMan.totalSize, nullptr);

                return "ByteSequence"_(std::move(resSpan));
              } else {
                boss::ExpressionArguments args;
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
                                        DEFAULT_ALIGNMENT);

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
                      boss::Span<int8_t>(bufferMan.buffer, bufferMan.totalSize, nullptr);
                  auto resExpr = "ByteSequence"_(std::move(resSpan));
                  args.push_back(std::move(resExpr));

		  
		  curl_easy_cleanup(curl);
		  curlHandlesMap.erase(url);
                }

                return boss::ComplexExpression("ByteSequences"_, {},
                                               std::move(args), {});
              }
            } else if (head == "StartTrackingOverhead"_) {
	      if (dynamics.size() > 0) {
		isTrackingRequired =
		  std::holds_alternative<boss::Symbol>(dynamics[0]) &&
		  get<boss::Symbol>(dynamics[0]) == "TrackRequired"_;
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
                          "Required"_(totalRequired),
                          "Requested"_(bufferMan.totalRequested),
                          "Headers"_(bufferMan.totalHeadersDownloaded)),
                  "Time"_("Pretransfer"_(bufferMan.totalPretransferTime),
                          "Download"_(bufferMan.totalDownloadTime)));
            } else if (head == "GetTotalOverhead"_) {
              int64_t totalLoaded = 0;
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
                totalRequested += bufferMan.totalRequested;
                totalHeaders += bufferMan.totalHeadersDownloaded;
                totalPretransferTime += bufferMan.totalPretransferTime;
                totalDownloadTime += bufferMan.totalDownloadTime;
              }

              return "Overhead"_("Size"_("Loaded"_(totalLoaded),
                                         "Required"_(totalRequired),
                                         "Requested"_(totalRequested),
                                         "Headers"_(totalHeaders)),
                                 "Time"_("Pretransfer"_(totalPretransferTime),
                                         "Download"_(totalDownloadTime)));

            } else if (head == "ClearCaches"_) {
              for (auto &[url, curlHandle] : curlHandlesMap) {
                curl_easy_cleanup(curlHandle);
              }
              curlHandlesMap.clear();
              bufferMap.clear();
              overheadBufferMap.clear();
              return "CachesCleared"_;
            } else if (head == "GetEngineCapabilities"_) {
              return "List"_("Fetch"_, "StartTrackingOverhead"_,
                             "StopTrackingOverhead"_, "GetOverhead"_,
                             "GetTotalOverhead"_, "ClearCaches"_);
            }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return boss::ComplexExpression(
                std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

} // namespace boss::engines::RBL

static auto &enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<boss::engines::RBL::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::RBL::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
