/* 
 * File:   main.c
 * Author: Ye Jin
 *
 * Created on November 14, 2011, 10:51 AM
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/stat.h>
#include <pthread.h>
#include <mpi.h>
#include <string.h>
#include "external_sort.h"

//#define GLOBAL_ELEMENT_COUNT    1073741824
/*
 * 
 */

void print_usage_error_exit(const char* _err_msg)
{
    printf("ERROR: %s", _err_msg);
    printf("\n");
    exit(-1);
}

void parse_commandline_options( int _argc,  char** _argv,
                               struct program_information *_prog_info)
{
    //if (_argc < 2)
        //print_usage_error_exit("missing input file name!\n");

    if (_argc < 2)
        print_usage_error_exit("missing number of read and write thread!\n");
    
    if (_argc < 3)
        print_usage_error_exit("missing number of distribute thread!\n");
    
    if (_argc < 4)
        print_usage_error_exit("missing number of send and/or receive thread!\n");
    
    if (_argc < 5)
        print_usage_error_exit("missing megabyte size of reading and writing buffer pool!\n");
    
    if (_argc < 6)
        print_usage_error_exit("missing megabyte size of distributing buffer pool!\n");
    
    if (_argc < 7)
        print_usage_error_exit("missing megabyte size of sending and receiving buffer pool!\n");
    
    
    if (sscanf(_argv[1], "%ld", &_prog_info->fileIO_threads) != 1)
        print_usage_error_exit("invalid input for threads!\n");
    
    if (sscanf(_argv[2], "%ld", &_prog_info->distribute_threads) != 1)
        print_usage_error_exit("invalid input for threads!\n");
    
    if (sscanf(_argv[3], "%ld", &_prog_info->communicate_threads) != 1)
        print_usage_error_exit("invalid input for threads!\n");
    
    if (sscanf(_argv[4], "%ld", &_prog_info->fileIO_buffer_byte_size) != 1)
        print_usage_error_exit("invalid megabyte size of reading and writing buffer pool!\n");
    
    _prog_info->fileIO_buffer_byte_size *= 1024;
    
    if (sscanf(_argv[5], "%ld", &_prog_info->ditribute_buffer_byte_size) != 1)
        print_usage_error_exit("invalid megabyte size of distributing buffer pool!\n");
    
    _prog_info->ditribute_buffer_byte_size *= 1024;
    
    if (sscanf(_argv[6], "%ld", &_prog_info->communicate_buffer_byte_size) != 1)
        print_usage_error_exit("invalid megabyte size of sending and receiving buffer pool!\n");
    
    _prog_info->communicate_buffer_byte_size *= 1024;
    
    _prog_info->running_Pthreads = _prog_info->fileIO_threads * 2
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
    assert(_prog_info->running_Pthreads != 0);

//    omp_set_num_threads(_prog_info->running_threads);

    printf("\n");
}

void distribute(int _num_nodes, int *_target_node, struct element _current_element)
{
    char key_first_byte;
    key_first_byte = _current_element.e[0];
    
    *_target_node = (int)((int)key_first_byte / _num_nodes);   
}

////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) {
   
    int my_rank; /* rank of process */
    int num_process;       /* number of processes */
    int i, j, k; //for loop
    
    // MPI Initialization
    MPI_Status status ;   /* return status for receive */
    MPI_Status probe_status;    /* return status for probe */
   
    /* start up MPI */
    MPI_Init(&argc, &argv);
	
    /* find out process rank */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); 
	
    /* find out number of processes */
    MPI_Comm_size(MPI_COMM_WORLD, &num_process); 
   
    // Gensort generate input files named partition.ranknumber
    char gensort_fname[50];
    char arg_bN[10];
    char arg_NUMRECS[10];
    char system_call[100];
    
    sprintf(gensort_fname, "partition.%d", my_rank);
    sprintf(arg_bN, " -b%d", (my_rank*1000));
    sprintf(arg_NUMRECS, " %d ", (my_rank*1000 + 1000));

    strcpy(system_call, "/Users/hawkwoodye/NetBeansProjects/gensort-1.4/gensort ");
    strcat(system_call, arg_bN);
    strcat(system_call, arg_NUMRECS);
    strcat(system_call, gensort_fname);
    
    system(system_call);
    
    // Parameters Set Up
    struct program_information prog_info;
    parse_commandline_options(argc, argv, &prog_info);

    // Process Input File Information
    struct stat stat_info;
    
    char file_path[52] = "./";
    strcat(file_path, gensort_fname);
    strcpy(prog_info.input_file_name, file_path);
    
    if (stat(prog_info.input_file_name, &stat_info))
    {   
        printf("Rank %d %s ", my_rank, prog_info.input_file_name);
        print_usage_error_exit("Unable to stat input file!\n");
    }
    prog_info.input_file_size = (size_t) stat_info.st_size;
    
    prog_info.element_byte_size = 100;
    prog_info.element_key_size = 10;
    
    prog_info.element_count = (long) prog_info.input_file_size / prog_info.element_byte_size;
    
    printf("Input File Name : %s\n", prog_info.input_file_name);
    printf("Element Count   : %ld\n", prog_info.element_count);
    
    ////////////////////////////////////////////
    //                                        //
    //            Our Sort Start              //
    //                                        //
    ////////////////////////////////////////////

    // !!! Pipeline First
    // !!! Pipeline First
    // !!! Pipeline First
    
    ////////////////////////////////////////////
    //                                        //
    //    STEP 1: Reading Disk -> Memory      //
    //                                        //
    ////////////////////////////////////////////
     
    FILE * file_ptr;
    struct element * records_per_buffer;
    long readbuffer_size;                   // in bytes
    
    // read buffer malloc
    records_per_buffer = (struct element *)malloc(readbuffer_size);
    
    // temporary set to the whole file will be changed to buffer size...
    // ...
    readbuffer_size = prog_info.input_file_size;
    
    file_ptr = fopen(prog_info.input_file_name, "rb");
    
    // will add for loop for read one buffer each time
    // ...
    
    if(file_ptr == NULL)
        print_usage_error_exit("File open error!\n");

    if(fseek(file_ptr, 0L, SEEK_SET) != 0)
        print_usage_error_exit("Unable to seek input file!");
    
    if(fread(records_per_buffer, 1, readbuffer_size, file_ptr) != readbuffer_size)
    {
        print_usage_error_exit("File reading error!\n");
    }
    
    fclose(file_ptr);
     
    ////////////////////////////////////////////
    //                                        //
    //    STEP 2: Distribute (Communication)  //
    //                                        //
    ////////////////////////////////////////////
    
    // Each node count the first byte of each element and
    //      decide which target node this element should be sent to
    
    // 
    // Double Buffering will be added in
    // ...
    //
    
    // buffers for sending
    struct element *    sending_buckets[num_process];
    long                buckets_index[num_process];
    // buffer for receiving
    struct element *    final_distributed_records;
    long                final_index;
    
    // bucket size in count of element, will be changed later, right now it is the total element count, which will not be reached
    long                bucket_element_count;
    
    bucket_element_count = prog_info.element_count ;
    
    for(i = 0; i < num_process; i++)
    {
        sending_buckets[i] = (struct element *)malloc(bucket_element_count * prog_info.element_byte_size );
        buckets_index[i]   = 0;
    }
    
    long count_of_dist_records = prog_info.element_count;
    
    struct element  temp_record;
    char            temp_key;
    long            temp_bucket_index;
    int             target_node;
    
    for(i = 0; i < prog_info.element_count; i++)
    {
        temp_record = records_per_buffer[i];
        temp_key = temp_record.e[0];
        target_node = (int)temp_key;
        target_node = target_node / num_process;
        temp_bucket_index = buckets_index[target_node];
        
        sending_buckets[target_node][temp_bucket_index] = temp_record;
        buckets_index[target_node]++;
        
        // when one bucket is full send it and memset to zero
        if(buckets_index[target_node] == bucket_element_count)
        {
            // MPI SEND
            MPI_Send(sending_buckets[target_node], (bucket_element_count * 100), MPI_BYTE, target_node, my_rank, MPI_COMM_WORLD); 
            buckets_index[target_node] = 0;
        }
        // MPI RECV
        for (j = 0; j < num_process; j++) {
            if(MPI_Iprobe(j, j, MPI_COMM_WORLD, &probe_status))
            {
                MPI_Recv(&final_distributed_records[final_index], (bucket_element_count * 100), MPI_BYTE, j, j, MPI_COMM_WORLD, &status);
                final_index = final_index + bucket_element_count;
            }
        }
    }
    
    // last time send those unfilled buckects
    for(i = 0; i < num_process; i++)
    {
        if(buckets_index[i]!=0)
        {
            //MPI SEND !!CAUTION!! only send (buckets_index[i] + 1) elements
            MPI_Send(sending_buckets[target_node], (buckets_index[target_node] + 1) * 100, MPI_BYTE, target_node, my_rank, MPI_COMM_WORLD); 
        }
        //MPI RECV
        for (j = 0; j < num_process; j++)
        {
            int count = 0;
            
            if(MPI_Iprobe(j, j, MPI_COMM_WORLD, &probe_status))
            {
                MPI_Get_count( &probe_status, MPI_BYTE, &count);
                MPI_Recv(&final_distributed_records[final_index], count, MPI_BYTE, j, j, MPI_COMM_WORLD, &status);
            }
        }
    }
    
    ////////////////////////////////////////////
    //                                        //
    //    STEP 3: Sort and Write to file      //
    //                                        //
    ////////////////////////////////////////////

    // sort final_distributed_records
    
    // write to disk
    
    /* shut down MPI */
    MPI_Finalize(); 
    return (EXIT_SUCCESS);
}

























