#pragma once
#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#define NOMINMAX
#endif

#include "maxcompute_odbc/odbc_api/handles.h"  // 包含 OdbcHandle 基类
#include <memory>
#include <mutex>
#include <sql.h>
#include <unordered_map>

namespace maxcompute_odbc {

/**
 * @brief 句柄注册表（Handle Registry）
 *
 * 该类用于管理 ODBC 驱动内部所有句柄（EnvHandle, ConnHandle,
 * StmtHandle）的生命周期。
 *
 * ### 为什么需要这个类？
 *
 * 在早期实现中，我们直接将 C++ 对象指针（如 new ConnHandle()）作为 SQLHANDLE
 * 返回给 ODBC 应用程序。 这种做法在以下场景会失败：
 *
 * 1. **ODBC Driver Manager（DM）介入时**：
 *    - 应用程序调用的是 DM 的 API（如 unixODBC、Windows ODBC）。
 *    - DM 会创建自己的包装句柄（如 DMHDBC），并把你的驱动返回的真实句柄保存为
 * driver_dbc。
 *    - 当后续调用（如 SQLAllocHandle(STMT)）发生时，DM 传给你的是 driver_dbc，
 *      而你在测试代码中打印的 hDbc 是 DM 的句柄，两者地址不同 →
 * 导致你以为“指针被篡改”。
 *    - 更严重的是：如果 DM 或应用程序误操作了裸指针（如重复
 * free），会导致崩溃。
 *
 * 2. **安全性与封装性差**：
 *    - 直接暴露 C++ 对象地址等于把内存布局暴露给外部，违反封装原则。
 *    - 无法防止非法句柄（如野指针、已释放指针）被传入。
 *
 * 3. **生命周期管理困难**：
 *    - StmtHandle 的销毁逻辑复杂（需通知 ConnHandle 移除自己）。
 *    - 如果依赖裸指针 delete，容易漏掉或 double-free。
 *
 * ### 本方案如何解决问题？
 *
 * - **句柄不再是指针，而是一个无意义的 token（如 1, 2, 3...）**。
 *   - 外部看到的 SQLHANDLE = reinterpret_cast<void*>(token)
 *   - 内部通过 map[token] 查找真实对象
 * - **完全兼容 ODBC Driver Manager**：
 *   - DM 传入的任何 SQLHANDLE 都会被安全地映射到内部对象
 *   - 地址不一致不再是问题，因为根本不依赖地址
 * - **自动内存管理**：
 *   - 所有对象由 std::unique_ptr 管理
 *   - free() 时自动析构，无需手动 delete
 * - **安全检查**：
 *   - get() 会验证句柄是否有效，无效则返回 nullptr，避免崩溃
 *
 * ### 使用方式示例：
 *
 * // 分配环境句柄
 * auto env = std::make_unique<EnvHandle>();
 * SQLHANDLE hEnv = HandleRegistry::instance().allocate(std::move(env));
 *
 * // 获取环境句柄
 * EnvHandle* pEnv = HandleRegistry::instance().get<EnvHandle>(hEnv);
 *
 * // 释放句柄
 * HandleRegistry::instance().free(hEnv);
 */
class HandleRegistry {
 public:
  /**
   * @brief 获取全局单例实例
   * @return HandleRegistry 的唯一实例
   */
  static HandleRegistry &instance() {
    static HandleRegistry inst;
    return inst;
  }

  /**
   * @brief 分配一个新的句柄 token，并注册一个对象
   * @tparam T 句柄类型（必须继承自 OdbcHandle）
   * @param obj 要注册的对象（unique_ptr，转移所有权）
   * @return SQLHANDLE（实际是 token 的 void* 形式）
   */
  template <typename T>
  SQLHANDLE allocate(std::unique_ptr<T> obj) {
    static_assert(std::is_base_of_v<OdbcHandle, T>,
                  "T must inherit from OdbcHandle");
    std::lock_guard<std::mutex> lock(mutex_);
    HandleToken token = next_token_++;
    // 将派生类 unique_ptr 转为基类 unique_ptr
    objects_[token] = std::move(obj);
    return reinterpret_cast<SQLHANDLE>(token);
  }

  /**
   * @brief 根据 SQLHANDLE 获取对应的 C++ 对象指针（不转移所有权）
   * @tparam T 期望的句柄类型
   * @param h ODBC 句柄
   * @return 成功则返回 T*，失败（包括 h == NULL）则返回 nullptr
   */
  template <typename T>
  T *get(SQLHANDLE h) {
    static_assert(std::is_base_of_v<OdbcHandle, T>,
                  "T must inherit from OdbcHandle");
    if (h == SQL_NULL_HANDLE) {
      return nullptr;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    HandleToken token = reinterpret_cast<HandleToken>(h);
    auto it = objects_.find(token);
    if (it == objects_.end()) {
      // Log for debugging
      maxcompute_odbc::Logger::getInstance().error(
          "HandleRegistry::get: token {} not found, objects_ size={}", token,
          objects_.size());
      return nullptr;  // 无效句柄
    }
    // 安全向下转型
    return dynamic_cast<T *>(it->second.get());
  }

  /**
   * @brief 释放句柄，自动析构内部对象
   * @param h 要释放的 ODBC 句柄
   */
  void free(SQLHANDLE h) {
    if (h == SQL_NULL_HANDLE) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    HandleToken token = reinterpret_cast<HandleToken>(h);
    objects_.erase(token);
    // unique_ptr 自动析构对象
  }

 private:
  // 禁止外部构造和复制
  HandleRegistry() = default;
  ~HandleRegistry() = default;
  HandleRegistry(const HandleRegistry &) = delete;
  HandleRegistry &operator=(const HandleRegistry &) = delete;

  // 使用 uintptr_t 作为 token，足够大且可安全转为 void*
  using HandleToken = std::uintptr_t;

  mutable std::mutex mutex_;    ///< 线程安全锁
  HandleToken next_token_ = 1;  ///< 下一个可用 token（0 保留为 NULL）
  std::unordered_map<HandleToken, std::unique_ptr<OdbcHandle>>
      objects_;  ///< token -> 对象映射
};

}  // namespace maxcompute_odbc
