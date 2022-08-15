//
// Created by sj on 22-7-23.
//
// #include <libpmem.h>
#include <thread>
static char __ch[1024];

int abs_diff(int a, int b) {
    int c = a - b;
    if (c < 0) {
        return -c;
    }
    return c;
}

int crc32(const int *buf, int len) {
    int res = 0;
    len = len >> 2;
    for (int i = 1; i < len; i += 2) { //需要从1开始
        res += buf[i];
    }
    return res;
}

short crc16(const short *buf, int len) {
    int* int_buf = (int*)buf;
    int res = 0;
    len = len >> 2;
    for (int i = 1; i < len; i += 2) { //需要从1开始
        res += *(int_buf + i);
    }
    return (short)res;
}

char crc8(const char *buf, int len) {
    char res = 0;
    for (int i = 1; i < len; i += 4) { //需要从1开始
        res += buf[i];
    }
    return res;
}
