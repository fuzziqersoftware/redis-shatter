#include <stdio.h>

#include "debug.h"

void print_data(const void* ds, unsigned long long data_size) {

  if (data_size == 0)
    return;

  unsigned char* data_source = (unsigned char*)ds;
  unsigned long long x, y, off = 0;
  char buffer[17] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  printf("0000000000000000 | ");
  for (x = 0; x < data_size; x++) {

    if (off == 16) {
      printf("| ");
      for (y = 0; y < 16; y++) {
        if (buffer[y] < 0x20 || buffer[y] == 0x7F)
          putc(' ', stdout);
        else
          putc(buffer[y], stdout);
      }
      printf("\n%016llX | ", x);
      off = 0;
    }

    buffer[off] = data_source[x];
    printf("%02X ", data_source[x]);

    off++;
  }
  buffer[off] = 0;
  for (y = 0; y < 16 - off; y++)
    printf("   ");
  printf("| ");
  for (y = 0; y < off; y++) {
    if (buffer[y] < 0x20 || buffer[y] == 0x7F)
      putc(' ', stdout);
    else
      putc(buffer[y], stdout);
  }
  printf("\n");
}

void print_indent(int level) {
  for (; level > 0; level--)
    printf("  ");
}