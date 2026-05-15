add_library(common INTERFACE)
target_include_directories(common INTERFACE
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(common INTERFACE tl::expected Crc32c::crc32c)


add_library(config STATIC config/config.cpp)
target_link_libraries(config PUBLIC common)


add_library(maxcompute_client STATIC
        maxcompute_client/client.cpp
        maxcompute_client/models.cpp
        maxcompute_client/request_signer.cpp
        maxcompute_client/api_client.cpp
        maxcompute_client/setting_parser.cpp
        maxcompute_client/proto_deserializer.cpp
        maxcompute_client/tunnel.cpp
        maxcompute_client/typeinfo.cpp
        # cpp-base64 don't know how to use vcpkg
        ${CPP_BASE64_INCLUDE_DIRS}/cpp-base64/base64.cpp
)

target_link_libraries(maxcompute_client
        PUBLIC
        common
        config
        httplib::httplib
        protobuf::libprotobuf
        nlohmann_json::nlohmann_json
        pugixml::pugixml
        OpenSSL::SSL
        OpenSSL::Crypto
        ZLIB::ZLIB
        Crc32c::crc32c
        tl::expected
        date::date-tz
)
if (WIN32)
    target_link_libraries(maxcompute_client
        PUBLIC
        winhttp
    )
endif()

add_library(odbc_api SHARED
        odbc_api/handles.cpp
        odbc_api/entry_points.cpp
        odbc_api/conversions.cpp
        odbc_api/encoding.cpp
        odbc_api/odbc_exports.def
        ${CMAKE_CURRENT_SOURCE_DIR}/version.rc
)

set_target_properties(odbc_api PROPERTIES OUTPUT_NAME "maxcompute_odbc")

target_link_libraries(odbc_api
        PRIVATE
        maxcompute_client
)
if(NOT WIN32)
    target_link_libraries(odbc_api PRIVATE Iconv::Iconv)
endif()
if (WIN32)
    # On Windows, link required ODBC system libraries
    # Include legacy libraries to resolve string handling functions
    target_link_libraries(odbc_api
        PRIVATE
        odbc32
        winhttp  # Required for WinHTTP functions
        wininet  # Required for proxy-related functions
    )
else()
    target_link_libraries(odbc_api PRIVATE ODBC::ODBC)
    if (TARGET PkgConfig::LIBLTDL)
        target_link_libraries(odbc_api PRIVATE PkgConfig::LIBLTDL)
    else ()
        # Try to find libltdl through other means
        find_library(LIBLTDL_LIBRARY NAMES ltdl libltdl)
        if (LIBLTDL_LIBRARY)
            target_link_libraries(odbc_api PRIVATE ${LIBLTDL_LIBRARY})
        endif ()
    endif ()
endif ()

if (APPLE)
    target_link_libraries(odbc_api PRIVATE
            "-framework CoreFoundation"
            "-framework CFNetwork"
            "-framework Security"
            "-framework SystemConfiguration"
    )
endif ()