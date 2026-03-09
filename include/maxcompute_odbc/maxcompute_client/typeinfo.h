#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// enum class 定义保持不变
enum class OdpsType {
  TINYINT,
  BIGINT,
  INT,
  SMALLINT,
  FLOAT,
  DOUBLE,
  STRING,
  DATE,
  DATETIME,
  TIMESTAMP,
  TIMESTAMP_NTZ,
  BOOLEAN,
  BINARY,
  DECIMAL,
  VARCHAR,
  CHAR,
  ARRAY,
  MAP,
  STRUCT,
  JSON
};

// --- 基类 (已重构为 class) ---
class typeinfo {
 public:
  // 1. 对于多态基类，虚析构函数是必须的
  typeinfo() = default;
  virtual ~typeinfo() = default;

  // 2. Rule of Five: 明确定义拷贝和移动语义
  // 因为使用了 unique_ptr，这个体系是不可拷贝的
  typeinfo(const typeinfo &) = delete;
  typeinfo &operator=(const typeinfo &) = delete;
  // 明确声明默认的移动操作，这是解决 Cpp17MoveInsertable 错误的关键
  typeinfo(typeinfo &&) = default;
  typeinfo &operator=(typeinfo &&) = default;

  // 3. 公共接口 (纯虚函数)
  virtual OdpsType getTypeID() const = 0;  // 重命名以避免与派生类的成员混淆
  virtual std::string toString() const = 0;
  virtual std::unique_ptr<typeinfo> clone() const = 0;
};

// 1. 简单类型
class PrimitiveTypeInfo final : public typeinfo {
 public:
  explicit PrimitiveTypeInfo(OdpsType t) : type_(t) {}

  // 重写基类接口
  OdpsType getTypeID() const override { return type_; }
  std::string toString() const override;
  std::unique_ptr<typeinfo> clone() const override {
    return std::make_unique<PrimitiveTypeInfo>(type_);
  }

 private:
  OdpsType type_;
};

// 2. 带参数的类型
class VarcharTypeInfo final : public typeinfo {
 public:
  explicit VarcharTypeInfo(int len) : length_(len) {}

  OdpsType getTypeID() const override { return OdpsType::VARCHAR; }
  std::string toString() const override;
  std::unique_ptr<typeinfo> clone() const override {
    return std::make_unique<VarcharTypeInfo>(length_);
  }

  // Getter
  int getLength() const { return length_; }

 private:
  int length_;
};

class CharTypeInfo final : public typeinfo {
 public:
  explicit CharTypeInfo(int len) : length_(len) {}

  OdpsType getTypeID() const override { return OdpsType::CHAR; }
  std::string toString() const override;
  std::unique_ptr<typeinfo> clone() const override {
    return std::make_unique<CharTypeInfo>(length_);
  }

  // Getter
  int getLength() const { return length_; }

 private:
  int length_;
};

class DecimalTypeInfo final : public typeinfo {
 public:
  DecimalTypeInfo(int p, int s) : precision_(p), scale_(s) {}

  OdpsType getTypeID() const override { return OdpsType::DECIMAL; }
  std::string toString() const override;
  std::unique_ptr<typeinfo> clone() const override {
    return std::make_unique<DecimalTypeInfo>(precision_, scale_);
  }

  // Getters
  int getPrecision() const { return precision_; }
  int getScale() const { return scale_; }

 private:
  int precision_;
  int scale_;
};

// 3. 复杂（复合）类型
class ArrayTypeInfo final : public typeinfo {
 public:
  explicit ArrayTypeInfo(std::unique_ptr<typeinfo> elem)
      : element_type_(std::move(elem)) {
    if (!element_type_) {
      throw std::invalid_argument(
          "ArrayTypeInfo's element type cannot be null.");
    }
  }

  OdpsType getTypeID() const override { return OdpsType::ARRAY; }
  std::string toString() const override;
  std::unique_ptr<typeinfo> clone() const override {
    return std::make_unique<ArrayTypeInfo>(
        element_type_ ? element_type_->clone() : nullptr);
  }

  // Getter for the element type (returns a const reference to the owned object)
  const typeinfo &getElementType() const { return *element_type_; }

 private:
  std::unique_ptr<typeinfo> element_type_;
};

class MapTypeInfo final : public typeinfo {
 public:
  MapTypeInfo(std::unique_ptr<typeinfo> k, std::unique_ptr<typeinfo> v)
      : key_type_(std::move(k)), value_type_(std::move(v)) {}

  OdpsType getTypeID() const override { return OdpsType::MAP; }
  std::string toString() const override;
  std::unique_ptr<typeinfo> clone() const override {
    return std::make_unique<MapTypeInfo>(
        key_type_ ? key_type_->clone() : nullptr,
        value_type_ ? value_type_->clone() : nullptr);
  }

  // Getters
  const typeinfo &getKeyType() const { return *key_type_; }
  const typeinfo &getValueType() const { return *value_type_; }

 private:
  std::unique_ptr<typeinfo> key_type_;
  std::unique_ptr<typeinfo> value_type_;
};

class StructTypeInfo final : public typeinfo {
 public:
  StructTypeInfo(std::vector<std::string> names,
                 std::vector<std::unique_ptr<typeinfo>> types)
      : field_names_(std::move(names)), field_types_(std::move(types)) {
    if (field_names_.size() != field_types_.size()) {
      throw std::invalid_argument(
          "Struct field names and types must have the same size.");
    }
  }

  OdpsType getTypeID() const override { return OdpsType::STRUCT; }
  std::string toString() const override;
  std::unique_ptr<typeinfo> clone() const override {
    std::vector<std::unique_ptr<typeinfo>> cloned_types;
    cloned_types.reserve(field_types_.size());
    for (const auto &type : field_types_) {
      if (type) {
        cloned_types.push_back(type->clone());
      } else {
        cloned_types.push_back(nullptr);
      }
    }
    return std::make_unique<StructTypeInfo>(field_names_,
                                            std::move(cloned_types));
  }

  // Getters (return const references to avoid copies)
  const std::vector<std::string> &getFieldNames() const { return field_names_; }
  const std::vector<std::unique_ptr<typeinfo>> &getFieldTypes() const {
    return field_types_;
  }

 private:
  std::vector<std::string> field_names_;
  std::vector<std::unique_ptr<typeinfo>> field_types_;
};

class TypeInfoParser {
 public:
  // 公共静态入口，类似 Java 的 getTypeInfoFromTypeString
  // 使用 string_view 避免不必要的拷贝
  static std::unique_ptr<typeinfo> parse(std::string_view typeString);

 private:
  // 构造函数设为私有，强制通过静态方法使用
  explicit TypeInfoParser(std::string_view typeString);

  // 解析主逻辑
  std::unique_ptr<typeinfo> doParse();

  // --- 词法分析 (Tokenizer) ---
  void tokenize();
  static bool isTypeInfoChar(char c);

  // --- 语法分析 (Parser) ---
  std::unique_ptr<typeinfo> parseTypeInfoInternal();
  std::unique_ptr<typeinfo> parseArrayTypeInfo();
  std::unique_ptr<typeinfo> parseMapTypeInfo();
  std::unique_ptr<typeinfo> parseStructTypeInfo();
  std::unique_ptr<typeinfo> parseVarcharTypeInfo();
  std::unique_ptr<typeinfo> parseCharTypeInfo();
  std::unique_ptr<typeinfo> parseComplexDecimalTypeInfo();

  // --- 辅助方法 ---
  const std::string &consume();     // 获取当前 token 并前进
  const std::string &peek() const;  // 查看当前 token 但不前进
  void expect(const std::string &expected);
  int getInteger();
  OdpsType stringToOdpsType(const std::string &s) const;

  // --- 成员变量 ---
  const std::string originalString;  // 持有字符串的副本
  std::vector<std::string> tokens;
  size_t currentIndex = 0;
};
