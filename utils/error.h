#ifndef ERROR_H
#define ERROR_H

#include <iostream>
#include <boost/system/error_code.hpp>

inline void fail(boost::beast::error_code ec, char const* what) {
    (void)ec;
    (void)what;
    //std::cerr << what << ": " << ec.message() << "\n";
}

#endif