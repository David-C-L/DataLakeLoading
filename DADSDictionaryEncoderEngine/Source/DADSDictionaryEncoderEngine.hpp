#include <DADS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <set>
#include <unordered_map>
#include <utility>
#include <iostream>

namespace dads::engines::DictionaryEncoder {

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

  dads::Expression evaluate(dads::Expression &&e);
  dads::Expression encodeTable(ComplexExpression &&e);
  dads::Expression encodeColumn(Expression &&e);
  dads::Expression decodeTable(ComplexExpression &&e);
  dads::Expression decodeColumn(Expression &&e);

  
  struct Dictionary {
    int32_t nextId;
    std::unordered_map<std::string, int32_t> dictionary;
    std::vector<std::string> reverseDictionary;

    dads::Span<std::string const> decode(dads::Span<int32_t const> &&encoded) {
      std::vector<std::string> res;
      res.reserve(encoded.size());

      for (auto &id : encoded) {
        if (id >= 0 && id < reverseDictionary.size()) {
          res.push_back(reverseDictionary[id]);
        } else {
          res.push_back("INVALID_ENCODING");
        }
      }

      return dads::Span<std::string const>(std::move(res));
    }

    dads::Span<std::string const> decode(dads::Span<int32_t> &&encoded) {
      std::vector<std::string> res;
      res.reserve(encoded.size());

      for (auto &id : encoded) {
        if (id >= 0 && id < reverseDictionary.size()) {
          res.push_back(reverseDictionary[id]);
        } else {
          res.push_back("INVALID_ENCODING");
        }
      }

      return dads::Span<std::string const>(std::move(res));
    }

    dads::Span<int32_t> encode(dads::Span<std::string> &&input) {
      std::vector<int32_t> res;
      for (auto &str : input) {
        auto it = dictionary.find(str);
        if (it == dictionary.end()) {
          reverseDictionary.push_back(str);
          dictionary[str] = nextId;
          res.push_back(nextId);
          nextId++;
        } else {
          res.push_back(it->second);
        }
      }
      return dads::Span<int32_t>(std::move(res));
    }

    int32_t getEncoding(std::string key) {
      auto it = dictionary.find(key);
      if (it == dictionary.end()) {
	return -1;
      }
      return it->second;
    }

    Dictionary(const Dictionary &other)
        : nextId(other.nextId), dictionary(other.dictionary),
          reverseDictionary(other.reverseDictionary){};
    Dictionary &operator=(const Dictionary &other) {
      if (this != &other) {
        nextId = other.nextId;
        dictionary = other.dictionary;
        reverseDictionary = other.reverseDictionary;
      }
      return *this;
    };
    Dictionary(Dictionary &&other) noexcept
        : nextId(other.nextId), dictionary(std::move(other.dictionary)),
          reverseDictionary(std::move(other.reverseDictionary)) {
      other.nextId = 0;
    }
    Dictionary &operator=(Dictionary &&other) noexcept {
      if (this != &other) {
        nextId = other.nextId;
        dictionary = std::move(other.dictionary);
        reverseDictionary = std::move(other.reverseDictionary);
        other.nextId = 0;
      }
      return *this;
    }
    Dictionary() = default;
    ~Dictionary() = default;
  };
  
private:
  
  std::unordered_map<dads::Symbol, Dictionary> dictionaries;
  std::unordered_map<dads::Symbol, dads::Expression> tables;
};

extern "C" DADSExpression *evaluate(DADSExpression *e);
} // namespace dads::engines::DictionaryEncoder
