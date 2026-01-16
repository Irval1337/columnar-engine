#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include <iostream>

#include "columnar/add.h"

ABSL_FLAG(int, x, 1, "x");
ABSL_FLAG(int, y, 2, "y");

int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);
    int x = absl::GetFlag(FLAGS_x);
    int y = absl::GetFlag(FLAGS_y);
    std::cout << "Add(" << x << ", " << y << ") = " << columnar::Add(x, y) << "\n";
    return 0;
}
