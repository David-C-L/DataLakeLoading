#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <curl/curl.h>
#include <iostream>
#include <set>
#include <utility>

// #define DEBUG

namespace boss::engines::RBL {

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() {
    for (auto &[url, curlHandle] : curlHandlesMap) {
      curl_easy_cleanup(curlHandle);
    }
  };

  boss::Expression evaluate(boss::Expression &&e);

  struct BufferManager {

    int8_t *buffer;
    bool exists;
    size_t totalSize;
    size_t currentOffset;

    int64_t totalLoaded;
    int64_t totalRequested;
    int64_t totalHeadersDownloaded;
    int64_t totalPretransferTime;
    int64_t totalDownloadTime;

    std::map<int64_t, int64_t> freeBoundsMap;

    std::vector<std::pair<int64_t, int64_t>> requestBounds(int64_t lb,
                                                           int64_t ub) {
      if (ub > totalSize) {
	ub = totalSize;
      }
      if (lb >= ub) {
        throw std::runtime_error(
            "All pairs of bounds <lb, ub> must be such that lb < ub.");
      }
      std::vector<std::pair<int64_t, int64_t>> toLoad;
      std::vector<std::pair<int64_t, int64_t>> toAdd;
      for (auto it = freeBoundsMap.begin(); it != freeBoundsMap.end();) {
        auto const &freeLB = it->first;
        auto const &freeUB = it->second;
        if (freeLB <= lb && ub <= freeUB) {
          if (lb != ub) {
            toLoad.emplace_back(lb, ub);
          }
          if (ub < freeUB)
            freeBoundsMap[ub] = freeUB;
          if (freeLB < lb) {
            freeBoundsMap[freeLB] = lb;
          } else {
            it = freeBoundsMap.erase(it);
          }
          return toLoad;
        }
        if (freeLB <= lb && freeUB <= ub && lb < freeUB) {
          toLoad.emplace_back(lb, freeUB);
          toAdd.emplace_back(freeLB, lb);
          it = freeBoundsMap.erase(it);
        } else if (lb <= freeLB && ub <= freeUB && freeLB < ub) {
          toLoad.emplace_back(freeLB, ub);
          toAdd.emplace_back(ub, freeUB);
          it = freeBoundsMap.erase(it);
        } else if (lb <= freeLB && freeUB <= ub) {
          if (freeLB != freeUB) {
            toLoad.emplace_back(freeLB, freeUB);
          }
          it = freeBoundsMap.erase(it);
        } else {
          ++it;
        }
      }

      for (const auto &[newLB, newUB] : toAdd) {
        freeBoundsMap[newLB] = newUB;
      }

      return toLoad;
    }

    void addLoaded(std::vector<std::pair<int64_t, int64_t>> &bounds) {
      for (const auto &[lb, ub] : bounds) {
        totalLoaded += ub - lb;
      }
    }

    explicit BufferManager(size_t totalSize, bool exists)
      : exists(exists), totalSize(totalSize), currentOffset(0), totalLoaded(0),
          totalRequested(0), totalHeadersDownloaded(0), totalPretransferTime(0),
          totalDownloadTime(0), freeBoundsMap{{0, totalSize}} {
      if (exists) {
	buffer = (int8_t *)std::malloc(totalSize);
      } else {
	buffer = nullptr;
      }
    }

    BufferManager(const BufferManager &other)
      : exists(other.exists), totalSize(other.totalSize), currentOffset(other.currentOffset),
          totalLoaded(other.totalLoaded), totalRequested(other.totalRequested),
          totalHeadersDownloaded(other.totalHeadersDownloaded),
          totalPretransferTime(other.totalPretransferTime),
          totalDownloadTime(other.totalDownloadTime),
          freeBoundsMap(other.freeBoundsMap) {
      if (exists) {
	buffer = (int8_t *)std::malloc(totalSize);
	std::memcpy(buffer, other.buffer, totalSize);
      }
    };
    BufferManager &operator=(const BufferManager &other) {
      if (this != &other) {
        std::free(buffer);
	exists = other.exists;
        totalSize = other.totalSize;
        currentOffset = other.currentOffset;
        totalLoaded = other.totalLoaded;
        totalRequested = other.totalRequested;
        totalHeadersDownloaded = other.totalHeadersDownloaded;
        totalPretransferTime = other.totalPretransferTime;
        totalDownloadTime = other.totalDownloadTime;
	if (exists) {
	  buffer = (int8_t *)std::malloc(totalSize);
	  std::memcpy(buffer, other.buffer, totalSize);
	}
        freeBoundsMap = other.freeBoundsMap;
      }
      return *this;
    };
    BufferManager(BufferManager &&other) noexcept
      : buffer(other.buffer), exists(other.exists), totalSize(other.totalSize),
          currentOffset(other.currentOffset), totalLoaded(other.totalLoaded),
          totalRequested(other.totalRequested),
          totalHeadersDownloaded(other.totalHeadersDownloaded),
          totalPretransferTime(other.totalPretransferTime),
          totalDownloadTime(other.totalDownloadTime),
          freeBoundsMap(std::move(other.freeBoundsMap)) {
      other.buffer = nullptr;
      other.exists = false;
      other.totalSize = 0;
      other.currentOffset = 0;
      other.totalLoaded = 0;
    };
    BufferManager &operator=(BufferManager &&other) noexcept {
      if (this != &other) {
        std::free(buffer);
        buffer = other.buffer;
	exists = other.exists;
        totalSize = other.totalSize;
        currentOffset = other.currentOffset;
        totalLoaded = other.totalLoaded;
        totalRequested = other.totalRequested;
        totalHeadersDownloaded = other.totalHeadersDownloaded;
        totalPretransferTime = other.totalPretransferTime;
        totalDownloadTime = other.totalDownloadTime;
        freeBoundsMap = std::move(other.freeBoundsMap);
        other.buffer = nullptr;
	other.exists = false;
        other.totalSize = 0;
        other.currentOffset = 0;
        other.totalLoaded = 0;
        other.totalRequested = 0;
        other.totalHeadersDownloaded = 0;
        other.totalPretransferTime = 0;
      }
      return *this;
    };
    BufferManager() = default;
    ~BufferManager() {
      if (buffer != nullptr)
        std::free(buffer);
    };
  };

  struct DummyBufferManager : private BufferManager {

    int64_t totalRequired;

    std::vector<std::pair<int64_t, int64_t>> requestBounds(int64_t lb,
                                                           int64_t ub) {
      return BufferManager::requestBounds(lb, ub);
    }

    void addLoaded(std::vector<std::pair<int64_t, int64_t>> &bounds) {
      for (const auto &[lb, ub] : bounds) {
        totalRequired += ub - lb;
      }
    }

    explicit DummyBufferManager(size_t totalSize) : totalRequired(0) {
      totalSize = totalSize;
      freeBoundsMap = {{0, totalSize}};
      buffer = (int8_t *)std::malloc(1);
    }

    DummyBufferManager(const DummyBufferManager &other)
        : totalRequired(other.totalRequired) {
      totalSize = other.totalSize;
      buffer = (int8_t *)std::malloc(1);
      freeBoundsMap = other.freeBoundsMap;
    };

    DummyBufferManager &operator=(const DummyBufferManager &other) {
      if (this != &other) {
        totalSize = other.totalSize;
        totalRequired = other.totalRequired;
        freeBoundsMap = other.freeBoundsMap;
      }
      return *this;
    };

    DummyBufferManager(DummyBufferManager &&other) noexcept
        : totalRequired(other.totalRequired) {
      totalSize = other.totalSize;
      buffer = other.buffer;
      freeBoundsMap = std::move(other.freeBoundsMap);
      other.totalSize = 0;
      other.buffer = nullptr;
      other.totalRequired = 0;
    };
    DummyBufferManager &operator=(DummyBufferManager &&other) noexcept {
      if (this != &other) {
        totalSize = other.totalSize;
        totalRequired = other.totalRequired;
        freeBoundsMap = std::move(other.freeBoundsMap);
        other.totalSize = 0;
        other.totalRequired = 0;
      }
      return *this;
    };
    DummyBufferManager() = default;
    ~DummyBufferManager() = default;
  };

  struct MIMEBoundaryHandler {

    enum MIMEBoundaryStage : int32_t { BOUNDARY, BOUNDARY_HANDLED };

    MIMEBoundaryStage currStage = BOUNDARY;
    size_t currBoundaryOffset = 0;
    std::string currBoundary;

    size_t findBoundary(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BOUNDARY) {
        return -1;
      }
      size_t boundaryLength = currBoundary.length();
      size_t i = currOffset;

      while (currBoundaryOffset != boundaryLength && i < size_b) {
        if (data[i] == currBoundary.at(currBoundaryOffset)) {
          currBoundaryOffset++;
        } else {
          currBoundaryOffset = 0;
        }
        i++;
      }

      if (currBoundaryOffset == boundaryLength) {
        currStage = BOUNDARY_HANDLED;
      }

      return i;
    }

    size_t handleBoundary(char *data, size_t size_b, size_t currOffset) {
#ifdef DEBUG
      std::cout << "HANDLE BOUNDARY AT: " << currOffset << std::endl;
#endif
      size_t i = currOffset;
      while (currStage != BOUNDARY_HANDLED && i < size_b) {
        switch (currStage) {
        case BOUNDARY:
          i = findBoundary(data, size_b, i);
          break;
        case BOUNDARY_HANDLED:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == BOUNDARY_HANDLED; }

    void resetHandlerStage() {
      currBoundaryOffset = 0;
      currStage = BOUNDARY;
    };

    MIMEBoundaryHandler(const MIMEBoundaryHandler &) = default;
    MIMEBoundaryHandler &operator=(const MIMEBoundaryHandler &) = default;
    MIMEBoundaryHandler(MIMEBoundaryHandler &&) = default;
    MIMEBoundaryHandler &operator=(MIMEBoundaryHandler &&) = default;
    MIMEBoundaryHandler() = default;
    ~MIMEBoundaryHandler() = default;
  };

  struct MultipartHeaderHandler {

    enum MultipartHeaderStage : int32_t {
      BYTE_RANGE_PREAMBLE,
      BYTE_RANGE,
      BYTE_RANGE_POSTAMBLE,
      MULTIPART_HEADERS_HANDLED
    };

    static inline std::string const BYTE_RANGE_KEY = "Content-range: bytes ";
    static inline std::string const RANGE_SPLIT = "-";
    static inline char const BYTE_RANGE_END = '/';

    MultipartHeaderStage currStage = BYTE_RANGE_PREAMBLE;

    size_t currPreambleOffset = 0;
    size_t preambleLength = BYTE_RANGE_KEY.length();

    int64_t currLB = -1;
    int64_t currUB = -1;
    std::string currRange;

    std::string postamble;
    size_t currPostambleOffset = 0;
    size_t postambleLength;

    void setPostamble(std::string &&postambleValue) {
      postambleLength = postambleValue.length();
      postamble = std::move(postambleValue);
      currPostambleOffset = 0;

#ifdef DEBUG
      std::cout << "CURR POSTAMBLE: " << postamble << std::endl;
#endif
    }

    void setBoundsFromCurrentRange() {
#ifdef DEBUG
      std::cout << "SET BOUNDS FROM: " << currRange << std::endl;
#endif
      size_t splitPos = currRange.find(RANGE_SPLIT);
      if (splitPos != std::string::npos) {
        currLB = std::stol(currRange.substr(0, splitPos));
        currUB = std::stol(currRange.substr(splitPos + 1));

#ifdef DEBUG
        std::cout << "FOUND BYTE RANGE WITH (LB, UB): (" << currLB << ", "
                  << currUB << ")" << std::endl;
#endif
      }
    }

    size_t findByteRange(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BYTE_RANGE_PREAMBLE) {
        return -1;
      }
      size_t i = currOffset;

      while (currPreambleOffset != preambleLength && i < size_b) {
        if (data[i] == BYTE_RANGE_KEY.at(currPreambleOffset)) {
          currPreambleOffset++;
        } else {
          currPreambleOffset = 0;
        }
        i++;
      }

      if (currPreambleOffset == preambleLength) {
        currStage = BYTE_RANGE;
      }

      return i;
    }

    size_t readByteRange(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BYTE_RANGE) {
        return -1;
      }
      size_t i = currOffset;

      while (i < size_b && data[i] != BYTE_RANGE_END) {
        currRange.push_back(data[i]);
        i++;
      }

      if (i < size_b && data[i] == BYTE_RANGE_END) {
        setBoundsFromCurrentRange();
        currStage = BYTE_RANGE_POSTAMBLE;
      }

      return i;
    }

    size_t consumePostamble(char *data, size_t size_b, size_t currOffset) {
      if (currStage != BYTE_RANGE_POSTAMBLE) {
        return -1;
      }
      size_t i = currOffset;

      while (currPostambleOffset != postambleLength && i < size_b) {
        if (data[i] == postamble.at(currPostambleOffset)) {
          currPostambleOffset++;
        } else {
          currPostambleOffset = 0;
        }
        i++;
      }

      if (currPostambleOffset == postambleLength) {
        currStage = MULTIPART_HEADERS_HANDLED;
      }

      return i;
    }

    size_t handleMultipartHeader(char *data, size_t size_b, size_t currOffset) {
#ifdef DEBUG
      std::cout << "HANDLE HEADER AT: " << currOffset << std::endl;
#endif
      size_t i = currOffset;
      while (currStage != MULTIPART_HEADERS_HANDLED && i < size_b) {
        switch (currStage) {
        case BYTE_RANGE_PREAMBLE:
          i = findByteRange(data, size_b, i);
          break;
        case BYTE_RANGE:
          i = readByteRange(data, size_b, i);
          break;
        case BYTE_RANGE_POSTAMBLE:
          i = consumePostamble(data, size_b, i);
          break;
        case MULTIPART_HEADERS_HANDLED:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == MULTIPART_HEADERS_HANDLED; }

    void resetHandlerStage() {
      currPreambleOffset = 0;
      currPostambleOffset = 0;
      currLB = -1;
      currUB = -1;
      currRange.clear();
      currStage = BYTE_RANGE_PREAMBLE;
    }

    size_t getMultipartLength() { return (currUB - currLB) + 1; }

    MultipartHeaderHandler(const MultipartHeaderHandler &) = default;
    MultipartHeaderHandler &operator=(const MultipartHeaderHandler &) = default;
    MultipartHeaderHandler(MultipartHeaderHandler &&) = default;
    MultipartHeaderHandler &operator=(MultipartHeaderHandler &&) = default;
    MultipartHeaderHandler() = default;
    ~MultipartHeaderHandler() = default;
  };

  struct MultipartDataHandler {

    enum MultipartDataStage : int32_t {
      MULTIPART_DATA_PREAMBLE,
      MULTIPART_DATA,
      MULTIPART_DATA_POSTAMBLE,
      MULTIPART_DATA_HANDLED
    };

    static inline std::string const DATA_PREAMBLE_STR = "\r\n\r\n";
    static inline std::string const DATA_POSTAMBLE_STR = "\r\n";

    MultipartDataStage currStage = MULTIPART_DATA_PREAMBLE;

    size_t currPreambleOffset = 0;
    size_t preambleLength = DATA_PREAMBLE_STR.length();

    size_t currMultipartDataSize;
    size_t currMultipartDataOffset = 0;

    size_t currPostambleOffset = 0;
    size_t postambleLength = DATA_POSTAMBLE_STR.length();

    void setMultipartData(size_t size) { currMultipartDataSize = size; }

    size_t writeMultipartData(BufferManager *bufferMan, char *data,
                              size_t size_b, size_t currOffset) {
      if (currStage != MULTIPART_DATA) {
        return -1;
      }

      int64_t multipartUnread = currMultipartDataSize - currMultipartDataOffset;
      int64_t dataUnread = size_b - currOffset;
      int64_t amountToRead;
      if (multipartUnread > dataUnread) {
        amountToRead = dataUnread;
      } else {
        amountToRead = multipartUnread;
        currStage = MULTIPART_DATA_POSTAMBLE;
      }

      memcpy((bufferMan->buffer + bufferMan->currentOffset),
             (data + currOffset), amountToRead);
      bufferMan->currentOffset += amountToRead;
      currMultipartDataOffset += amountToRead;

      return currOffset + amountToRead;
    }

    size_t consumePreamble(char *data, size_t size_b, size_t currOffset) {
      if (currStage != MULTIPART_DATA_PREAMBLE) {
        return -1;
      }
      size_t i = currOffset;

      while (currPreambleOffset != preambleLength && i < size_b) {
        if (data[i] == DATA_PREAMBLE_STR.at(currPreambleOffset)) {
          currPreambleOffset++;
        } else {
          currPreambleOffset = 0;
        }
        i++;
      }

      if (currPreambleOffset == preambleLength) {
        currStage = MULTIPART_DATA;
      }

      return i;
    }

    size_t consumePostamble(char *data, size_t size_b, size_t currOffset) {
      if (currStage != MULTIPART_DATA_POSTAMBLE) {
        return -1;
      }
      size_t i = currOffset;

      while (currPostambleOffset != postambleLength && i < size_b) {
        if (data[i] == DATA_POSTAMBLE_STR.at(currPostambleOffset)) {
          currPostambleOffset++;
        } else {
          currPostambleOffset = 0;
        }
        i++;
      }

      if (currPostambleOffset == postambleLength) {
        currStage = MULTIPART_DATA_HANDLED;
      }

      return i;
    }

    size_t handleMultipartData(BufferManager *bufferMan, char *data,
                               size_t size_b, size_t currOffset) {
#ifdef DEBUG
      std::cout << "HANDLE DATA AT: " << currOffset << std::endl;
#endif
      size_t i = currOffset;
      while (currStage != MULTIPART_DATA_HANDLED && i < size_b) {
        switch (currStage) {
        case MULTIPART_DATA_PREAMBLE:
          i = consumePreamble(data, size_b, i);
          break;
        case MULTIPART_DATA:
          i = writeMultipartData(bufferMan, data, size_b, i);
          break;
        case MULTIPART_DATA_POSTAMBLE:
          i = consumePostamble(data, size_b, i);
          break;
        case MULTIPART_DATA_HANDLED:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == MULTIPART_DATA_HANDLED; }

    void resetHandlerStage() {
      currPreambleOffset = 0;
      currPostambleOffset = 0;
      currMultipartDataOffset = 0;
      currStage = MULTIPART_DATA_PREAMBLE;
    }

    MultipartDataHandler(const MultipartDataHandler &) = default;
    MultipartDataHandler &operator=(const MultipartDataHandler &) = default;
    MultipartDataHandler(MultipartDataHandler &&) = default;
    MultipartDataHandler &operator=(MultipartDataHandler &&) = default;
    MultipartDataHandler() = default;
    ~MultipartDataHandler() = default;
  };

  struct MultipartResponseHandler {

    enum MultipartResponseStage : int32_t {
      BOUNDARY,
      CHECK_END,
      HEADERS,
      DATA,
      RESET,
      DONE
    };

    static inline std::string const END_STR = "--";
    size_t currEndOffset = 0;
    size_t endLength = END_STR.length();
    bool done = false;

    MultipartResponseStage currStage = BOUNDARY;

    MIMEBoundaryHandler boundaryHandler;
    MultipartHeaderHandler headersHandler;
    MultipartDataHandler dataHandler;

    BufferManager *bufferMan;

    size_t isEnd(char *data, size_t size_b, size_t currOffset) {
      if (currStage != CHECK_END) {
        return -1;
      }
      size_t i = currOffset;

      while (currEndOffset != endLength && i < size_b) {
        done = data[i] == END_STR.at(currEndOffset);
        currEndOffset++;
        i++;
      }

      if (done) {
        currStage = DONE;
      } else {
        currStage = HEADERS;
      }

      return i;
    }

    void resetHandlerStage() { currStage = BOUNDARY; };

    void resetHandlers() {
#ifdef DEBUG
      std::cout << "HANDLE RESET" << std::endl;
#endif
      boundaryHandler.resetHandlerStage();
      headersHandler.resetHandlerStage();
      dataHandler.resetHandlerStage();
      resetHandlerStage();
    }

    void resetBoundary(std::string &&boundary) {
      boundaryHandler.currBoundary = std::move(boundary);
      boundaryHandler.currBoundaryOffset = 0;
    }

    size_t handleMultipart(char *data, size_t size_b, size_t currOffset) {
      size_t i = currOffset;
#ifdef DEBUG
      std::cout << "HANDLE MULTIPART WITH SIZE: " << size_b << std::endl;
#endif
      while (currStage != DONE && i < size_b) {
        switch (currStage) {
        case BOUNDARY:
          i = boundaryHandler.handleBoundary(data, size_b, i);
          if (boundaryHandler.isDone()) {
            currStage = CHECK_END;
          }
          break;
        case CHECK_END:
#ifdef DEBUG
          std::cout << "HANDLE IS_END AT: " << i << std::endl;
#endif
          i = isEnd(data, size_b, i);
          break;
        case HEADERS:
          i = headersHandler.handleMultipartHeader(data, size_b, i);
          if (headersHandler.isDone()) {
            currStage = DATA;
            bufferMan->currentOffset = headersHandler.currLB;
            dataHandler.setMultipartData(headersHandler.getMultipartLength());
          }
          break;
        case DATA:
          i = dataHandler.handleMultipartData(bufferMan, data, size_b, i);
          if (dataHandler.isDone()) {
            currStage = RESET;
          }
          break;
        case RESET:
          resetHandlers();
          break;
        case DONE:
          break;
        }
      }
      return i;
    }

    bool isDone() { return currStage == DONE; }

    MultipartResponseHandler(BufferManager *bufferMan) : bufferMan(bufferMan) {
      std::string size = std::to_string(bufferMan->totalSize);
      headersHandler.setPostamble(std::move(size));
    };

    MultipartResponseHandler(const MultipartResponseHandler &) = default;
    MultipartResponseHandler &
    operator=(const MultipartResponseHandler &) = default;
    MultipartResponseHandler(MultipartResponseHandler &&) = default;
    MultipartResponseHandler &operator=(MultipartResponseHandler &&) = default;
    MultipartResponseHandler() = default;
    ~MultipartResponseHandler() = default;
  };

private:
  bool isTrackingOverhead = false;
  bool isTrackingRequired = false;
  std::unordered_map<std::string, CURL *> curlHandlesMap;
  std::unordered_map<std::string, BufferManager> bufferMap;
  std::unordered_map<std::string, DummyBufferManager> overheadBufferMap;

  std::vector<std::pair<int64_t, int64_t>> extractBoundPairs(
      const std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
      DummyBufferManager &buffMan);
  std::vector<std::pair<int64_t, int64_t>>
  extractBoundPairs(std::vector<std::pair<int64_t, int64_t>> &requestedBounds,
                    BufferManager &buffMan, int64_t padding, int64_t alignment);

  static void allocateRangeFromURL(BufferManager &bufferMan, CURL *curl,
                                   std::string const &range,
                                   std::string const &url,
                                   size_t (*writeResponseFunc)(void *, size_t,
                                                               size_t, void *),
                                   bool isTrackingOverhead);
  static void allocateRangesFromURL(BufferManager &bufferMan, CURL *curl,
                                    std::string const &range,
                                    std::string const &url,
                                    size_t (*writeResponseFunc)(void *, size_t,
                                                                size_t, void *),
                                    bool isTrackingOverhead);

  static uint64_t getFileLength(std::string const &url, CURL *curl);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::RBL
