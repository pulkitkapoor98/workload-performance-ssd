#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <time.h>
#include <pthread.h>

#define EXIT_FAILURE 1
#define g 1024*1024*1024
#define m 1024*1024
#define k 1024

typedef struct filearg {
	char path[20];
	int iosize;
	int randFlag;
	int fileSize;
	int fsyncFlag;
	int startFile;
	int filePerThread;
	int fsyncReq;
} filearg_t;

int convertToBytes(char *size){
	char num[10];
	strncpy(num,size,strlen(size)-1);
	num[strlen(size)-1]= '\0';
	if(size[strlen(size)-1] == 'g')
		return atoi(num)*g;
	else if(size[strlen(size)-1] == 'm')
		return atoi(num)*m;
	else if(size[strlen(size)-1] == 'k')
		return atoi(num)*k;
	else
		return atoi(num);
}

char * allocateIOBuf(int size){
	char *buf = (char *)malloc(size); 
	if(buf == NULL){
		fprintf(stderr,"Malloc Error.\n");
		exit(EXIT_FAILURE);
	}
	return buf;
}

void freeIOBuf(char *buf){
	free((void *)buf);
}

void writeWholeFile(int fd, int size, int iosize){
	 int ret,i;
	 char *buf;
	 buf = allocateIOBuf(iosize);
	 int numIO = size/iosize;
	 for(i=0;i<numIO;i++){
//	 	printf("Writing the %dth I/O to the file.\n",i+1);
	 	if(write(fd,buf,iosize) == -1){
	 		fprintf(stderr,"Write Error.\n");
			pthread_exit(NULL);
	 	}
	 }
	 freeIOBuf(buf);
}

//Function used to create #numFiles each of #size using incremental I/O's of #iosize
void * createFile(void *arg){
	filearg_t *targ = (filearg_t *)arg;
	int fd,i;
	for(i=targ->startFile;i<(targ->startFile+targ->filePerThread);i++){
		char fName[10];
		char path[30];
		sprintf(fName,"%d.txt",i);
		strcpy(path,targ->path);
		strcat(path,fName);
		printf("Opening the file %s\n",path);
		fd = open(path,O_CREAT|O_WRONLY|O_TRUNC,0666);
		if(fd == -1){
			fprintf(stderr,"Error in opeining the following file : %s\n",path);
			pthread_exit(NULL);
		}
		writeWholeFile(fd,targ->fileSize,targ->iosize);
		close(fd);
	}	
}

void createFiles(char *p, int iosize, int numFiles, int numThreads, int fileSize){
	int i;
	pthread_t *tids = (pthread_t *)malloc(sizeof(pthread_t) * numThreads);
	int filePerThread = numFiles/numThreads;
	filearg_t targ[numThreads];
	for(i=0;i<numThreads;i++){
		strcpy(targ[i].path,p);
		targ[i].iosize = iosize;
		targ[i].fileSize = fileSize;
		targ[i].startFile = i * filePerThread;
		targ[i].filePerThread = filePerThread;
		pthread_create(&tids[i], NULL, createFile, &targ[i]);
		printf("Thread created to create the following file %s with thread id - %ld and starting file number %d\n",targ[i].path,tids[i],targ[i].startFile);
	}
	for(i=0;i<numThreads;i++){
		pthread_join(tids[i], NULL);
	}
	free(tids);
}

//Appending to end of the file
void appendFile(int fd,int size){
	int ret;
	char *buf;
	 if(lseek(fd, 0, SEEK_END) == -1){
	 	fprintf(stderr,"Error in performing lseek.\n");
		exit(EXIT_FAILURE);
	 }
	 buf = allocateIOBuf(size);
	 if(write(fd,buf,size) == -1){
	 	fprintf(stderr,"Write Error.\n");
		exit(EXIT_FAILURE);
	 }
	freeIOBuf(buf);
}

//Reading a file(Randomly or sequentially)
void * readFile(void *arg){
	filearg_t *targ = (filearg_t *)arg;
	//Opening the file
	int fd,i;
	for(i=targ->startFile;i<(targ->startFile+targ->filePerThread);i++){
		char fName[10];
		char path[30];
		sprintf(fName,"%d.txt",i);
		strcpy(path,targ->path);
		strcat(path,fName);
		printf("Opening the file %s\n",path);
		fd = open(path,O_RDONLY);
		if(fd == -1){
			fprintf(stderr,"Error in opeining the following file : %s\n",path);
			pthread_exit(NULL);
		}
		int numIO = (targ->fileSize)/(targ->iosize),i;
		//printf("the num of I/O are : %d",numIO);
		char *buf;
		buf = allocateIOBuf(targ->iosize);
		if(targ->randFlag == 1){
			srandom(time(NULL));
			long randOffset;
			for(i=0;i<numIO;i++){
				randOffset = random()%((targ->fileSize)-(targ->iosize));
				if(pread(fd,buf,targ->iosize,randOffset) == -1){
					fprintf(stderr,"Read Error.\n");
					pthread_exit(NULL);
				}
			}
		}
		else{
			for(i=0;i<numIO;i++){
				if(read(fd,buf,targ->iosize) == -1){
					fprintf(stderr,"Read Error.\n");
					pthread_exit(NULL);
				}
				//printf("Read the %dth I/O in the %ld thread\n",i,thId);
			}
		}
	}
}

void readFiles(char *p, int iosize, int numFiles, int numThreads, int randFlag, int fileSize){
	int i;
	pthread_t *tids = (pthread_t *)malloc(sizeof(pthread_t) * numThreads);
	int filePerThread = numFiles/numThreads;
	filearg_t targ[numThreads];
	for(i=0;i<numThreads;i++){
		strcpy(targ[i].path,p);
		targ[i].iosize = iosize;
		targ[i].randFlag = randFlag;
		targ[i].fileSize = fileSize;
		targ[i].startFile = i * filePerThread;
		targ[i].filePerThread = filePerThread;
		pthread_create(&tids[i], NULL, readFile, &targ[i]);
		printf("Thread created to read the following file %s with thread id - %ld\n",targ[i].path,tids[i]);
	}
	for(i=0;i<numThreads;i++){
		pthread_join(tids[i], NULL);
	}
	free(tids);
}

//Writing a file (Randomly or sequentially)
void  * writeFile(void *arg){
	filearg_t *targ = (filearg_t *)arg;
	int fd,i;
	for(i=targ->startFile;i<(targ->startFile+targ->filePerThread);i++){
		char fName[10];
		char path[30];
		sprintf(fName,"%d.txt",i);
		strcpy(path,targ->path);
		strcat(path,fName);
		printf("Opening the file %s\n",path);
		fd = open(path,O_WRONLY);
		if(fd == -1){
			fprintf(stderr,"Error in opeining the following file : %s\n",path);
			pthread_exit(NULL);
		}
		int numIO = (targ->fileSize)/(targ->iosize),i;
		char *buf;
		int fsyncReq = targ->fsyncReq; 
		buf = allocateIOBuf(targ->iosize);
		if(targ->randFlag == 1){
			srandom(time(NULL));
			long randOffset;
			for(i=0;i<numIO;i++){
				fsyncReq--;
				randOffset = random()%((targ->fileSize)-(targ->iosize));
				if(pwrite(fd,buf,targ->iosize,randOffset) == -1){
					fprintf(stderr,"Write Error.\n");
					pthread_exit(NULL);
				}
				if(fsyncReq == 0){
					if(targ->fsyncFlag == 1){
						fsync(fd);
					}
					fsyncReq = targ->fsyncReq;
				}
			}
		}
		else{
			for(i=0;i<numIO;i++){
				fsyncReq--;
				if(write(fd,buf,targ->iosize) == -1){
					fprintf(stderr,"Write Error.\n");
					pthread_exit(NULL);
				}
				if(fsyncReq == 0){
					if(targ->fsyncFlag == 1){
						fsync(fd);
					}
					fsyncReq = targ->fsyncReq;
				}
			}
		}
	}
}

void writeFiles(char *p, int iosize, int numFiles, int numThreads, int randFlag, int fileSize, int fsyncFlag, int fsyncReq){
	int i;
	pthread_t *tids = (pthread_t *)malloc(sizeof(pthread_t) * numThreads);
	int filePerThread = numFiles/numThreads;
	filearg_t targ[numThreads];
	for(i=0;i<numThreads;i++){
		strcpy(targ[i].path,p);
		targ[i].iosize = iosize;
		targ[i].randFlag = randFlag;
		targ[i].fileSize = fileSize;
		targ[i].startFile = i * filePerThread;
		targ[i].filePerThread = filePerThread;
		targ[i].fsyncFlag = fsyncFlag;
		targ[i].fsyncReq = fsyncReq;
		pthread_create(&tids[i], NULL, writeFile, &targ[i]);
		printf("Thread created to write the following file %s with thread id - %ld\n",targ[i].path,tids[i]);
	}
	for(i=0;i<numThreads;i++){
		pthread_join(tids[i], NULL);
	}
	free(tids);
}

void deleteFiles(char *p, int numFiles, int numDelFiles){
	int i=0;
	srand(time(NULL));
	while(i < numDelFiles){
		int file = rand()%numFiles;
		char fName[10],path[30];
		sprintf(fName,"%d.txt",file);
		strcpy(path,p);
		strcat(path,fName);
		printf("Deleting the following file %s\n",path);
		if(unlink(path) == -1){
			printf("The file deletion error.");
		}
		else
			i++;
	}
}

int main(int argc, char **argv){
	int opt;
	opterr = 0;
	char *operation,*numFiles,*fileSize,*ioSize,*path,*numThreads;
	/* Flags available - 
		o - operation
		n - number of files
		s - file size
		i - I/O Size
		p - path where the files have to be created
		t - number of threads/number of files to be deleted in case of delete op
	*/
	while ((opt = getopt(argc, argv, "o:n:s:i:p:t:")) != -1) {
        switch (opt) {
        	case 'o':
            	operation = optarg;
            	break;
            case 'n':
            	numFiles = optarg;
            	break;
            case 's':
            	fileSize = optarg;
            	break;
            case 'i':
            	ioSize = optarg;
            	break;
            case 'p':
            	path = optarg;
            	break;
            case 't':
            	numThreads = optarg;
            	break;
        	default: 
            	fprintf(stderr, "Usage: generateWorkload -o operation -n numFiles -s fileSize -i I/OSize -p path/filename -t numThreads\n");
            	exit(EXIT_FAILURE);
        }
    }
    int fSize = convertToBytes(fileSize);
    int ioSiz = convertToBytes(ioSize);
    printf("%d %d\n",fSize,ioSiz);
    if(strcmp(operation,"create") == 0){
    	createFiles(path, ioSiz, atoi(numFiles), atoi(numThreads), fSize);
    }
    else if(strcmp(operation,"sread") == 0){
    	readFiles(path, ioSiz, atoi(numFiles), atoi(numThreads), 0, fSize);
    }
    else if(strcmp(operation,"rread") == 0){
    	readFiles(path, ioSiz, atoi(numFiles), atoi(numThreads), 1, fSize);
    }
    else if(strcmp(operation,"swrite") == 0){
    	writeFiles(path, ioSiz, atoi(numFiles), atoi(numThreads), 0, fSize, 0, 10);
    }
    else if(strcmp(operation,"rwrite") == 0){
    	writeFiles(path, ioSiz, atoi(numFiles), atoi(numThreads), 1, fSize, 0, 10);
    }
    else if(strcmp(operation,"swriteFsync") == 0){
    	writeFiles(path, ioSiz, atoi(numFiles), atoi(numThreads), 0, fSize, 1, 10);
    }
    else if(strcmp(operation,"rwriteFsync") == 0){
    	writeFiles(path, ioSiz, atoi(numFiles), atoi(numThreads), 1, fSize, 1, 10);
    }
    else if(strcmp(operation,"delete") == 0){
    	deleteFiles(path, atoi(numFiles), atoi(numThreads));
    }
	exit(0);
}
