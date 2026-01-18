#pragma once

#include <stdexcept>
#include <string>

#define THROW_NOT_IMPLEMENTED                                                         \
    throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                             ": not implemented")

#define THROW_RUNTIME_ERROR(msg) \
    throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " + (msg))

#define ASSERT(cond)                                                                          \
    do {                                                                                      \
        if (!(cond)) {                                                                        \
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                                     ": condition '" + #cond + "' is not satistfied");        \
        }                                                                                     \
    } while (false)