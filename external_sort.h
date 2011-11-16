/* 
 * File:   external_sort.h
 * Author: hawkwoodye
 *
 * Created on November 14, 2011, 10:52 AM
 */

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
    const char* input_file_name;      // input file name
    long        input_file_size;           // input file size in bytes
    
    int         element_byte_size;      // 100 bytes
    long        element_count;
    long        element_key_size;       // 10 bytes
    
    int         fileIO_threads;           // number of read or write nodes
    int         distribute_threads;       // number of distribute nodes
    int         communicate_threads;      // number of send or receive nodes
    
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
    int sorted_flag = 0;
    long current_size;
    struct element *e;
};

struct read_pthread_args
{
    
};

struct write_pthread_args
{
    
};

struct distribute_pthread_args
{

};

struct send_pthread_args
{
};

struct receive_pthread_args
{

};

#endif	/* EXTERNAL_SORT_H */

