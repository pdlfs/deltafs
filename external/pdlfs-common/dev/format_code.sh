#!/bin/sh -x

#
# designed to run in the top src directory:
#   $ ./dev/format_code.sh
#
# Aug-05-2016 zhengq@cs.cmu.edu
#

fmt=
for fmt in clang-format clang-format-3.9 \
           clang-format-3.8 \
           clang-format-3.7 \
           clang-format-3.6 \
           clang-format-3.5 \
           clang-format-3.4 \
           clang-format-3.3
do
  full_fmt=`which $fmt`
  if test -n "$full_fmt"; then break; fi
done

if test -z "$full_fmt"; then
  echo "No clang-format found!"
  exit 1
fi

STYLE_CONF="{BasedOnStyle: Google, DerivePointerAlignment: false}"

# Reformat all headers
find | grep '\.h$' | xargs $full_fmt -style="$STYLE_CONF" -i
# Reformat all CXX sources
find | grep '\.cc$' | xargs $full_fmt -style="$STYLE_CONF" -i

exit 0
