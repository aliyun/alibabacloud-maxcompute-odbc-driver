#pragma once

#if defined(_WIN32) || defined(_WIN64)
// Reduce extraneous macros and exclude rarely-used APIs before including
// windows headers
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
