/* 
 * File:   main.c
 * Author: Ye Jin
 *
 * Created on November 14, 2011, 10:51 AM
 */

#include <stdio.h>
#include <stdlib.h>
//#include <omp.h>
#include <assert.h>
#include <sys/stat.h>
#include <pthread.h>
#include <mpi.h>
#include "external_sort.h"

#define GLOBAL_ELEMENT_COUNT    1073741824
/*
 * 
 */

void print_usage_error_exit(const char *_err_msg)
{
    printf("ERROR: %s", _err_msg);
    printf("\n");
    exit(-1);
}

void parse_commandline_options(const int _argc, const char** _argv,
                               struct program_information *_prog_info)
{
    if (_argc < 2)
        print_usage_error_exit("missing input file name!\n");

    if (_argc < 3)
        print_usage_error_exit("missing number of read and write thread!\n");
    
    if (_argc < 4)
        print_usage_error_exit("missing number of distribute thread!\n");
    
    if (_argc < 5)
        print_usage_error_exit("missing number of send and/or receive thread!\n");
    
    if (_argc < 6)
        print_usage_error_exit("missing megabyte size of reading and writing buffer pool!\n");
    
    if (_argc < 7)
        print_usage_error_exit("missing megabyte size of distributing buffer pool!\n");
    
    if (_argc < 8)
        print_usage_error_exit("missing megabyte size of sending and receiving buffer pool!\n");
    
    // process input file information
    struct stat stat_info;
    
    _prog_info->input_file_name = _argv[1];
    
    if (stat(_prog_info->input_file_name, &stat_info))
        print_usage_error_exit("Unable to stat input file!\n");
    
    _prog_info->input_file_size = (size_t) stat_info.st_size;
    
    _prog_info->element_byte_size = 100;
    _prog_info->element_key_size = 10;

    _prog_info->element_count = (long) _prog_info->input_file_size / _prog_info->element_byte_size;
    
    printf("Input File Name : %s\n", _prog_info->input_file_name);
    printf("Element Count   : %ld\n", _prog_info->element_count);
    
    
    if (sscanf(_argv[2], "%d", &_prog_info->fileIO_threads) != 1)
        print_usage_error_exit("invalid input for threads!\n");
    
    if (sscanf(_argv[3], "%d", &_prog_info->distribute_threads) != 1)
        print_usage_error_exit("invalid input for threads!\n");
    
    if (sscanf(_argv[4], "%d", &_prog_info->communicate_threads) != 1)
        print_usage_error_exit("invalid input for threads!\n");
    
    if (sscanf(_argv[5], "%d", &_prog_info->fileIO_buffer_byte_size) != 1)
        print_usage_error_exit("invalid megabyte size of reading and writing buffer pool!\n");
    
    _prog_info->fileIO_buffer_byte_size *= 1024;
    
    if (sscanf(_argv[6], "%d", &_prog_info->ditribute_buffer_byte_size) != 1)
        print_usage_error_exit("invalid megabyte size of distributing buffer pool!\n");
    
    _prog_info->ditribute_buffer_byte_size *= 1024;
    
    if (sscanf(_argv[7], "%d", &_prog_info->communicate_buffer_byte_size) != 1)
        print_usage_error_exit("invalid megabyte size of sending and receiving buffer pool!\n");
    
    _prog_info->communicate_buffer_byte_size *= 1024;
    
    _prog_info.running_Pthreads = _prog_info->fileIO_threads * 2
                                + _prog_info->communicate_threads * 2
                                + _prog_info->distribute_threads;

    /*
#pragma omp parallel // necessary for getting number of threads (if statement only)
    if(omp_get_thread_num() == 0)
    {
        _prog_info->running_Pthreads = omp_get_num_threads();
        printf("Number of Threads   : %d\n", _prog_info->running_threads);
    }
*/
    assert(_prog_info->running_threads != 0);

//    omp_set_num_threads(_prog_info->running_threads);

    printf("\n");
}

void distribute(int _num_nodes, int *_target_node, struct element _current_element)
{
    char key_first_byte;
    key_first_byte = &_current_element.e[0];
    
    *_target_node = (int)((int)key_first_byte / _num_nodes);   
}

int main(int argc, char** argv) {
   
    // MPI Initialization
    system("/Users/hawkwoodye/NetBeansProjects/gensort-1.4/gensort");
    // Gensort input
    
    // Start 
    struct program_information prog_info;
    
    parse_commandline_options(argc, argv, &prog_info);

    // 
    
    
    // MPI Finalization
    
    
    return (EXIT_SUCCESS);
}

