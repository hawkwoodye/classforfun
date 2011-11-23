/* 
 * File:   external_sort.h
 * Author: hawkwoodye
 *
 * Created on November 14, 2011, 10:52 AM
 */

#include <mpi.h>

#ifndef EXTERNAL_SORT_H
#define	EXTERNAL_SORT_H

#ifdef	__cplusplus
extern "C" {
#endif


#ifdef	__cplusplus
}
#endif

struct program_information
{
    char        input_file_name[50];      // input file name
    long        input_file_size;           // input file size in bytes
    int         number_of_process;
    
    int         element_byte_size;      // 100 bytes
    long        element_count;
    long        element_key_size;       // 10 bytes
    
    long        fileIO_threads;           // number of read or write nodes
    long        distribute_threads;       // number of distribute nodes
    long        communicate_threads;      // number of send or receive nodes
    
    long        fileIO_buffer_byte_size;        // byte size of buffer pool for file IO
    long        ditribute_buffer_byte_size;     // byte size of buffer pool for distribute read buffer
    long        communicate_buffer_byte_size;   // byte size of buffer pool for send or receive distributed buffer 
 
    int         running_Pthreads;

};

struct element
{
    char e[100];
};

struct read_buffer
{
    long current_size;
    struct element *e;
};

struct ditribute_bucket
{
    long bucket_size;
    struct element *e;
};

struct sort_buffer
{
    int sorted_flag;
    long current_size;
    struct element *e;
};

struct parm_recv
{
    int _my_process;
    MPI_Status _status;
    struct element * _recv_buffer;
    struct program_information _prog_info;
    long * _final_index;
};

#endif	/* EXTERNAL_SORT_H */

