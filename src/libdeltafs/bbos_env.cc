/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
*
* All rights reserved.
*
* Use of this source code is governed by a BSD-style license that can be
* found in the LICENSE file. See the AUTHORS file for names of contributors.
*/

#include <stdio.h>

#include <bbos/bbos_api.h>

/* XXXCDC: PUT SOMETHING HERE ! */
void *just_testing_delete_me() {
  void *ret;
  ret = (void *)bbos_init;   /* force lib to load */
  return(ret);
}
/* XXXCDC: PUT SOMETHING HERE ! */
