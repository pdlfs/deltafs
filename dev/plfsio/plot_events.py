#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Copyright (c) 2015-2017, Carnegie Mellon University
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the university nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import collections
import fileinput
import getopt
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def parse_file(files):
    data = collections.defaultdict(list)
    errs = collections.defaultdict(list)
    nums = collections.defaultdict(lambda: 0)
    for line in fileinput.input(files):
        line = line.strip()
        if len(line) != 0:
            inp = line.split(',')
            if len(inp) == 3:
                if inp[1] != 'io':
                    series = 1 + int(inp[1])
                else:
                    series = 0
                if (inp[2].lower() != 'start'):
                    errs[series].append(float(inp[0]) - data[series][-1])
                else:
                    data[series].append(float(inp[0]))
                    nums[series] += 1
    return max(nums.values()), data, errs


def plot(f, data, errs):
    with plt.style.context('seaborn-notebook'):
        fig, ax = plt.subplots()
        fig.set_dpi(150)
        fig.set_size_inches((50, 10), forward=True)
        fig.set_tight_layout(True)
        max_x = 0
        max_y = len(data) - 1
        for series in data.keys():
            zeros = np.zeros(len(errs[series]))
            xerr = np.array(errs[series])
            x = np.array(data[series])
            y = np.ones(len(data[series])) * series
            max_x = max(max_x, (int(max(x)) + 1) / 2 * 2)
            ax.errorbar(x, y, xerr=[zeros, xerr], capsize=8, capthick=2, fmt='.')
        ax.set_xlim(0, max_x + 2)
        ax.set_xticks(np.arange(0, max_x + 4, 2))
        ax.set_ylim(-0.5, max_y + 0.5)
        ax.set_yticks(np.arange(0, max_y + 1, 1))
        plt.savefig(f)


def print_console(n, data, errs):
    for i in xrange(n):
        for series in data.keys():
            if i < len(data[series]):
                sys.stdout.write("%.3f,%.3f,%s," % (data[series][i], errs[series][i], series))
            else:
                sys.stdout.write(",,,")
        sys.stdout.write("\n")


def usage():
    print '== Usage: %s --output=[file, -]' % sys.argv[0]


def main():
    f = 'a.pdf'
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'ho:', ['help', 'output='])
    except getopt.GetoptError as err:
        print err
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif opt in ("-o", "--output"):
            f = arg
        else:
            pass

    n, data, errs = parse_file(args)
    if f in ('-', 'console', 'terminal', 'text'):
        print_console(n, data, errs)
    else:
        plot(f, data, errs)

    return


if __name__ == '__main__':
    main()
