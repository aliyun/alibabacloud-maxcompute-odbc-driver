#include "maxcompute_odbc/maxcompute_client/typeinfo.h"
#include <algorithm>
#include <map>
#include <sstream>

// --- PrimitiveTypeInfo ---
std::string PrimitiveTypeInfo::toString() const {
  // 访问私有成员 type_
  switch (type_) {
    case OdpsType::TINYINT:
      return "TINYINT";
    case OdpsType::BIGINT:
      return "BIGINT";
    case OdpsType::INT:
      return "INT";
    case OdpsType::SMALLINT:
      return "SMALLINT";
    case OdpsType::FLOAT:
      return "FLOAT";
    case OdpsType::DOUBLE:
      return "DOUBLE";
    case OdpsType::STRING:
      return "STRING";
    case OdpsType::DATE:
      return "DATE";
    case OdpsType::DATETIME:
      return "DATETIME";
    case OdpsType::TIMESTAMP:
      return "TIMESTAMP";
    case OdpsType::TIMESTAMP_NTZ:
      return "TIMESTAMP_NTZ";
    case OdpsType::BOOLEAN:
      return "BOOLEAN";
    case OdpsType::BINARY:
      return "BINARY";
    case OdpsType::DECIMAL:
      return "DECIMAL";
    case OdpsType::VARCHAR:
      return "VARCHAR";
    case OdpsType::CHAR:
      return "CHAR";
    case OdpsType::ARRAY:
      return "ARRAY";
    case OdpsType::MAP:
      return "MAP";
    case OdpsType::STRUCT:
      return "STRUCT";
    case OdpsType::JSON:
      return "JSON";
  }
  return "UNKNOWN";
}

// --- VarcharTypeInfo ---
std::string VarcharTypeInfo::toString() const {
  // 访问私有成员 length_
  return "VARCHAR(" + std::to_string(length_) + ")";
}

// --- CharTypeInfo ---
std::string CharTypeInfo::toString() const {
  // 访问私有成员 length_
  return "CHAR(" + std::to_string(length_) + ")";
}

// --- DecimalTypeInfo ---
std::string DecimalTypeInfo::toString() const {
  // 访问私有成员 precision_ 和 scale_
  return "DECIMAL(" + std::to_string(precision_) + "," +
         std::to_string(scale_) + ")";
}

// --- ArrayTypeInfo ---
std::string ArrayTypeInfo::toString() const {
  // 访问私有成员 element_type_
  return "ARRAY<" + element_type_->toString() + ">";
}

// --- MapTypeInfo ---
std::string MapTypeInfo::toString() const {
  // 访问私有成员 key_type_ 和 value_type_
  return "MAP<" + key_type_->toString() + "," + value_type_->toString() + ">";
}

// --- StructTypeInfo ---
std::string StructTypeInfo::toString() const {
  std::ostringstream oss;
  oss << "STRUCT<";
  // 访问私有成员 field_names_ 和 field_types_
  for (size_t i = 0; i < field_names_.size(); ++i) {
    if (i > 0) {
      oss << ",";
    }
    oss << field_names_[i] << ":" << field_types_[i]->toString();
  }
  oss << ">";
  return oss.str();
}

// --- Helper map for string to enum conversion ---
static const std::map<std::string, OdpsType, std::less<>> typeMap = {
    {"TINYINT", OdpsType::TINYINT},
    {"SMALLINT", OdpsType::SMALLINT},
    {"INT", OdpsType::INT},
    {"BIGINT", OdpsType::BIGINT},
    {"FLOAT", OdpsType::FLOAT},
    {"DOUBLE", OdpsType::DOUBLE},
    {"STRING", OdpsType::STRING},
    {"DATE", OdpsType::DATE},
    {"DATETIME", OdpsType::DATETIME},
    {"TIMESTAMP", OdpsType::TIMESTAMP},
    {"TIMESTAMP_NTZ", OdpsType::TIMESTAMP_NTZ},
    {"BOOLEAN", OdpsType::BOOLEAN},
    {"BINARY", OdpsType::BINARY},
    {"DECIMAL", OdpsType::DECIMAL},
    {"VARCHAR", OdpsType::VARCHAR},
    {"CHAR", OdpsType::CHAR},
    {"ARRAY", OdpsType::ARRAY},
    {"MAP", OdpsType::MAP},
    {"STRUCT", OdpsType::STRUCT}};

// --- 公共入口 ---
std::unique_ptr<typeinfo> TypeInfoParser::parse(std::string_view typeString) {
  if (typeString.empty()) {
    throw std::invalid_argument("Type string cannot be empty.");
  }
  return TypeInfoParser(typeString).doParse();
}

// --- 构造函数 ---
TypeInfoParser::TypeInfoParser(std::string_view typeString)
    : originalString(typeString) {
  tokenize();
}

std::unique_ptr<typeinfo> TypeInfoParser::doParse() {
  auto typeInfo = parseTypeInfoInternal();
  if (currentIndex != tokens.size()) {
    throw std::runtime_error(
        "Failed to parse type string: unexpected characters at the end of '" +
        std::string(originalString) + "'");
  }
  return typeInfo;
}

// --- 词法分析器 ---
bool TypeInfoParser::isTypeInfoChar(char c) {
  return std::isalnum(c) || c == '_' || c == '.' || c == '`';
}

void TypeInfoParser::tokenize() {
  // Store the uppercase string as a member to ensure string_views remain valid
  std::string uppercaseString = originalString;
  std::transform(uppercaseString.begin(), uppercaseString.end(),
                 uppercaseString.begin(), ::toupper);

  size_t pos = 0;
  while (pos < uppercaseString.length()) {
    if (std::isspace(uppercaseString[pos])) {
      pos++;
      continue;
    }

    if (isTypeInfoChar(uppercaseString[pos])) {
      size_t start = pos;
      pos++;
      while (pos < uppercaseString.length() &&
             isTypeInfoChar(uppercaseString[pos])) {
        pos++;
      }
      // Store the token string to ensure it remains valid
      std::string tokenStr = uppercaseString.substr(start, pos - start);
      tokens.emplace_back(tokenStr);
    } else {
      // 单字符 token, e.g., '<', '>', ',', '(', ')'
      std::string tokenStr = uppercaseString.substr(pos, 1);
      tokens.emplace_back(tokenStr);
      pos++;
    }
  }
}

// --- 解析器辅助方法 ---
const std::string &TypeInfoParser::peek() const {
  if (currentIndex >= tokens.size()) {
    throw std::runtime_error("Unexpected end of type string: '" +
                             std::string(originalString) + "'");
  }
  return tokens[currentIndex];
}

const std::string &TypeInfoParser::consume() {
  if (currentIndex >= tokens.size()) {
    throw std::runtime_error("Unexpected end of type string: '" +
                             std::string(originalString) + "'");
  }
  return tokens[currentIndex++];
}

void TypeInfoParser::expect(const std::string &expected) {
  auto token = consume();
  if (token != expected) {
    throw std::runtime_error("Parse error in '" + std::string(originalString) +
                             "': expected '" + expected + "' but found '" +
                             token + "'");
  }
}

int TypeInfoParser::getInteger() {
  auto token = consume();
  try {
    return std::stoi(token);
  } catch (const std::exception &e) {
    throw std::runtime_error("Parse error in '" + std::string(originalString) +
                             "': expected an integer but found '" + token +
                             "'");
  }
}

OdpsType TypeInfoParser::stringToOdpsType(const std::string &s) const {
  auto it = typeMap.find(s);
  if (it == typeMap.end()) {
    throw std::runtime_error("Unknown type: '" + s + "' in '" +
                             std::string(originalString) + "'");
  }
  return it->second;
}

// --- 各类型解析逻辑 ---
std::unique_ptr<typeinfo> TypeInfoParser::parseTypeInfoInternal() {
  OdpsType typeCategory = stringToOdpsType(peek());

  // consume the type name token
  consume();

  switch (typeCategory) {
    case OdpsType::ARRAY:
      return parseArrayTypeInfo();
    case OdpsType::MAP:
      return parseMapTypeInfo();
    case OdpsType::STRUCT:
      return parseStructTypeInfo();
    case OdpsType::CHAR:
      return parseCharTypeInfo();
    case OdpsType::VARCHAR:
      return parseVarcharTypeInfo();
    case OdpsType::DECIMAL:
      if (currentIndex < tokens.size() && peek() == "(") {
        return parseComplexDecimalTypeInfo();
      } else {
        // 否则，这是一个无参数的 DECIMAL，使用默认值
        return std::make_unique<DecimalTypeInfo>(38, 18);
      }
    default:
      // Primitive types
      return std::make_unique<PrimitiveTypeInfo>(typeCategory);
  }
}

std::unique_ptr<typeinfo> TypeInfoParser::parseArrayTypeInfo() {
  expect("<");
  auto elementType = parseTypeInfoInternal();
  expect(">");
  return std::make_unique<ArrayTypeInfo>(std::move(elementType));
}

std::unique_ptr<typeinfo> TypeInfoParser::parseMapTypeInfo() {
  expect("<");
  auto keyType = parseTypeInfoInternal();
  expect(",");
  auto valueType = parseTypeInfoInternal();
  expect(">");
  return std::make_unique<MapTypeInfo>(std::move(keyType),
                                       std::move(valueType));
}

std::unique_ptr<typeinfo> TypeInfoParser::parseStructTypeInfo() {
  expect("<");
  std::vector<std::string> fieldNames;
  std::vector<std::unique_ptr<typeinfo>> fieldTypes;

  do {
    // Parse field name
    auto fieldNameToken = consume();
    fieldNames.emplace_back(fieldNameToken);

    expect(":");

    // Parse field type
    auto fieldType = parseTypeInfoInternal();
    fieldTypes.push_back(std::move(fieldType));

    // Check if there are more fields
    if (peek() == ",") {
      expect(",");
    } else {
      break;
    }
  } while (currentIndex < tokens.size() && peek() != ">");

  expect(">");
  return std::make_unique<StructTypeInfo>(std::move(fieldNames),
                                          std::move(fieldTypes));
}

std::unique_ptr<typeinfo> TypeInfoParser::parseCharTypeInfo() {
  expect("(");
  int length = getInteger();
  expect(")");
  return std::make_unique<CharTypeInfo>(length);
}

std::unique_ptr<typeinfo> TypeInfoParser::parseVarcharTypeInfo() {
  expect("(");
  int length = getInteger();
  expect(")");
  return std::make_unique<VarcharTypeInfo>(length);
}

std::unique_ptr<typeinfo> TypeInfoParser::parseComplexDecimalTypeInfo() {
  expect("(");
  int precision = getInteger();
  expect(",");
  int scale = getInteger();
  expect(")");
  return std::make_unique<DecimalTypeInfo>(precision, scale);
}
