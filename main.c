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

/*
void distribute(int _num_nodes, int *_target_node, struct element _current_element)
{
    char key_first_byte;
    key_first_byte = _current_element.e[0];
    
    *_target_node = (int)((int)key_first_byte / _num_nodes);   
}
*/

int key_compare(struct element a, struct element b)
{
    int i, ia, ib;
    for(i = 0; i < 10; i++)
    {
        ia = (int)a.e[i];
        ib = (int)b.e[i];
        if (ia < ib) {
            return(1);
        }
        if (ia > ib) {
            return(0);
        }
    }
    return(2);
}

void quickSort(struct element data[], long left, long right)
{
    struct element temp = data[left];
    long ptr = left;
    long i = left + 1, j = right;
    
    if(key_compare(data[i], temp))
    {
        data[ptr] = data[i];
        ptr = i;
    }
    while(i!=j)
    {
        if(!key_compare(data[i], temp))
        {
            j--;
        }
        else
        {
            data[ptr] = data[j];
            ptr = j;
            while(key_compare(data[i], temp) && i != j)
            {
                i++;
            }
            data[ptr] = data[i];
            ptr = i;
        }
    }
    data[ptr] = temp;
    if(left < ptr - 1)
        quickSort(data, left, ptr - 1);
    if(ptr + 1 < right)
        quickSort(data, ptr + 1, right);
}

int tag_checking(int * tags, int np)
{
<<<<<<< HEAD
    int i, r;
    r = 1;
    for( i = 0; i< np; i++)
    {
        if(tags[i]==255)
        {
            continue;
        }
        else
        {
            r = 0;
            break;
        }
    }
    return(r);
}

void * background_probe_recv(void * parm)
{    
=======
    printf("thread runnning!\n");
>>>>>>> parent of 4a45ff3... 
    
    struct parm_recv * input_parms = (struct parm_recv *)parm;
 
    long * temp_final_index;

    //MPI_Request temp_request;
    MPI_Status temp_status = input_parms->_status;
    
    struct element * temp_recv_buffer;
    struct program_information temp_prog_info = input_parms->_prog_info;

    temp_recv_buffer = input_parms->_recv_buffer;
    temp_final_index = input_parms->_final_index;

    int flag = 0;
<<<<<<< HEAD
    int temp_count = 0;
    int _my_process = input_parms->_my_process;
    int tags[temp_prog_info.number_of_process];
=======
    
>>>>>>> parent of 4a45ff3... 
    
    // MPI RECV
    while(!flag)
    {
<<<<<<< HEAD
	printf("Rank %d, entering while loop...\n", my_process);
=======
        /*
        MPI_Iprobe(MPI_ANY_SOURCE, my_process, MPI_COMM_WORLD, &flag, &temp_status);
>>>>>>> parent of 4a45ff3... 
        if(flag)
        {
            if(temp_status.MPI_TAG == 255)
            {
                tags[temp_status.MPI_SOURCE] = 255;
                if(tag_checking(tags, temp_prog_info.number_of_process))
                {
                    MPI_Recv(tags, 0, MPI_BYTE, temp_status.MPI_SOURCE, temp_status.MPI_TAG, MPI_COMM_WORLD, &temp_status);
                    break;
                }else{
                    MPI_Recv(tags, 0, MPI_BYTE, temp_status.MPI_SOURCE, temp_status.MPI_TAG, MPI_COMM_WORLD, &temp_status);
                    printf("%d get tag  255 from %d\n", _my_process, temp_status.MPI_SOURCE);
                    flag=0;
                    continue;
                }
            }
            //printf("probed tag=%d!!!\n",temp_status.MPI_TAG);
            MPI_Get_count(&temp_status, MPI_BYTE, &temp_count);
<<<<<<< HEAD
            //printf("Process %d Receving %ld! temp count is %d\n", _my_process, *temp_final_index, temp_count);
            *temp_final_index = *temp_final_index + temp_count/100;
            MPI_Recv(&temp_recv_buffer[*temp_final_index], temp_count, MPI_BYTE, temp_status.MPI_SOURCE, temp_status.MPI_TAG, MPI_COMM_WORLD, &temp_status);
            flag=0;
            
        }
    }
    printf("%d receive done!\n", _my_process);
=======
            printf("Receving from %ld! temp count is %d\n", *temp_final_index, temp_count);
            *temp_final_index = *temp_final_index + temp_count;
            MPI_Recv(&temp_recv_buffer[*temp_final_index], temp_count, MPI_BYTE, temp_status.MPI_SOURCE, my_process, MPI_COMM_WORLD, &temp_status);
        }
         */

        MPI_Recv(&temp_recv_buffer[*temp_final_index], temp_prog_info.input_file_size , MPI_BYTE, MPI_ANY_SOURCE, my_process, MPI_COMM_WORLD, &temp_status);
        printf("Received!\n");
        MPI_Get_count( &temp_status,  MPI_BYTE, &temp_count );
        *temp_final_index = *temp_final_index + temp_count;
        printf("Received %d records!\n", temp_count);
        if(*temp_final_index == temp_prog_info.element_count)
        {
            flag = 1;
        }else{flag = 0;}
        
         
    }
>>>>>>> parent of 4a45ff3... 
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) {
   
    int my_rank; /* rank of process */
    int num_process;       /* number of processes */
    int i, j, k; //for loop
    
    // MPI Initialization
    MPI_Status status ;   /* return status for receive */
    MPI_Status probe_status;    /* return status for probe */
    MPI_Request request;
   
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
    sprintf(arg_NUMRECS, " %d ", (1000));

    
    // CAUTION!!!
    // CAUTION!!!
    // CAUTION!!!

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
    prog_info.number_of_process = num_process;
    
    prog_info.element_byte_size = 100;
    prog_info.element_key_size = 10;
    
    prog_info.element_count = (long) prog_info.input_file_size / prog_info.element_byte_size;
    
    printf("Rank %d Input File Name : %s\n", my_rank, prog_info.input_file_name);
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
    
    // temporary set to the whole file will be changed to buffer size...
    // ...
    readbuffer_size = prog_info.input_file_size;
    
    // read buffer malloc
    records_per_buffer = (struct element *)malloc(readbuffer_size);
    
    file_ptr = fopen(prog_info.input_file_name, "rb");
    
    // will add for loop for read one buffer each time
    // ...
    
    if(file_ptr == NULL)
        print_usage_error_exit("File open error!\n");

    if(fseek(file_ptr, 0, SEEK_SET) != 0)
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
<<<<<<< HEAD

    final_distributed_records = (struct element *)malloc(2 * prog_info.input_file_size);
=======
    final_distributed_records = (struct element *)malloc(prog_info.input_file_size);
>>>>>>> parent of 4a45ff3... 
    
    // bucket size in count of element, will be changed later, right now it is the total element count, which will not be reached
    long                bucket_element_count;
    
    // Start Receiving Thread keep running until receive all~
    long * final_index;
    final_index = (long*)malloc(sizeof(long));
    *final_index = 0;
    struct parm_recv thread_recv_parm;
    
    thread_recv_parm._status = probe_status;
    thread_recv_parm._recv_buffer = final_distributed_records;
    thread_recv_parm._prog_info = prog_info;
    thread_recv_parm._final_index = final_index;
    thread_recv_parm._my_process = my_rank;
    
    pthread_t   recv_thread;
    pthread_create(&recv_thread, NULL, background_probe_recv, (void*)&thread_recv_parm);
    
    bucket_element_count = prog_info.element_count ;
      
    for(i = 0; i < num_process; i++)
    {
        sending_buckets[i] = (struct element *)malloc(bucket_element_count * prog_info.element_byte_size );
        buckets_index[i]   = 0;
    }
    
    long count_of_dist_records = prog_info.element_count;
    
    struct element  temp_record;
    char            temp_key;
    int             target_node;
    int             probe_flag = 0;
  
    for(i = 0; i < prog_info.element_count; i++)
    {
        temp_record = records_per_buffer[i];
        temp_key = temp_record.e[0];
        target_node = (int)temp_key + 128;
        target_node = target_node / (256 / num_process);
        /*
        printf("Rank %d: key is %c or %d\n", my_rank, temp_key, target_node);
        if(target_node == num_process)
        {
            printf("!ERROR !\n");
            break;
        }
        */
                
        sending_buckets[target_node][buckets_index[target_node]] = temp_record;
        buckets_index[target_node]++;
        
        // when one bucket is full send it and memset to zero
        if(buckets_index[target_node] == bucket_element_count)
        {
            printf("Init start sending: %ld -> %d!!\n", buckets_index[target_node]*100, target_node);
            // MPI SEND
            MPI_Send(sending_buckets[target_node], (buckets_index[target_node] * 100), MPI_BYTE, target_node, target_node, MPI_COMM_WORLD); 
            buckets_index[target_node] = 0;
        }
    }

    
    // last time send those unfilled buckects
    for(i = 0; i < num_process; i++)
    {
        if(buckets_index[i] != 0)
        {
<<<<<<< HEAD
=======
            printf("Start sending!!\n");
>>>>>>> parent of 4a45ff3... 
            //MPI SEND !!CAUTION!! only send (buckets_index[i] + 1) elements
            MPI_Send(sending_buckets[i], (buckets_index[i]) * 100, MPI_BYTE, i, i, MPI_COMM_WORLD); 
        }
    }
    // call all process end
    for(i = 0; i < num_process; i++)
    {
        printf("%d send tag to %d!\n", my_rank, i);
        MPI_Send(&i, 0, MPI_BYTE, i, 255, MPI_COMM_WORLD); 
    }
    
    ////////////////////////////////////////////
    //                                        //
    //    STEP 3: Sort and Write to file      //
    //                                        //
    ////////////////////////////////////////////

    //Barrier()
<<<<<<< HEAD

    MPI_Barrier(MPI_COMM_WORLD);
    pthread_join(recv_thread, NULL);

=======
    MPI_Barrier(MPI_COMM_WORLD);
    
>>>>>>> parent of 4a45ff3... 
    // sort final_distributed_records
    quickSort(final_distributed_records, 0, (prog_info.element_count - 1) );
    
    // write to disk
    FILE *outFile;
    
    char output_file_name[57]="./sorted_";
    strcat(output_file_name, gensort_fname);
    
    // open the file we are writing to 
    if(!(outFile = fopen(output_file_name, "wb")))
    {
        print_usage_error_exit("File writing error!\n");
    }
    
    // use fwrite to write binary data to the file 
    fwrite(final_distributed_records, prog_info.input_file_size , 1, outFile);
    
    fclose(outFile);
<<<<<<< HEAD
    
    ////////////////////////////////////////////
    //                                        //
    //    STEP 4: Garbage Collection          //
    //                                        //
    ////////////////////////////////////////////
    
    free(final_distributed_records);
    free(final_index);
    for(i = 0; i < num_process; i++)
    {
        free(sending_buckets[i]);
    }
    free(records_per_buffer);
    
=======

    pthread_join(recv_thread, NULL);
>>>>>>> parent of 4a45ff3... 
    // shut down MPI 
    MPI_Finalize(); 
    return (EXIT_SUCCESS);
}

























