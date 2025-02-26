#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <bits/stdc++.h>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 33
#define MAX_CHUNKS 100
#define MAX_REQUEST 25

// tags for different types of messages
#define PEER_FILE_INFO 1
#define PEER_CONFIRMATION 2
#define MESSAGE_TYPE 3
#define REQUEST_FILE 4
#define GET_PEER_UPLOADS 5
#define GET_SEGMENT 6
#define UPDATE_SWARM 7
#define MESSAGE_TYPE_PEER 8
#define GET_SEGMENT_FILE 9
#define GET_SEGMENT_HASH 11
#define DOWNLOAD_FILE_FINISHED 12

using namespace std;

/* struct used by clients to keep
   information about requested files */
struct swarm_and_hashes {
    int nr_segments;
    vector<int> swarm;
    vector<string> hashes; 
};

/* struct used by tracker to keep
   information about files */
struct tracker_file_info {
    int nr_segments;
    vector<pair<int, string>> peers_seeds_swarm;
    vector<string> hashes;
};

// mutex used in download and upload threads
pthread_mutex_t peer_map_mutex;
// map to keep information about files that are partially or fully owned by a client
unordered_map<string, vector<string>> peer_files_map;
// map to keep information about files requested by the client
unordered_map<string, swarm_and_hashes> peer_requested_files_map;
// vector that contains the name of the files wanted by the client
vector<string> wanted_files;

/* comparator for sorting peers/seeds from
   a swarm by total number of uploads */
bool comp(const pair<int, int>& pair1, const pair<int, int>& pair2) 
{
    return (pair1.second < pair2.second);
}    

// function for sorting peers/seeds in a swarm by total number of uploads
vector<pair<int, int>> get_peer_uploads(string file, int rank)
{
    // vector that contains pairs of peers/seeds and their respective number of uploads
    vector<pair<int, int>> peer_uploads;

    for (long unsigned int i = 0; i < peer_requested_files_map[file].swarm.size(); i++) {
        if (peer_requested_files_map[file].swarm[i] != rank) {
            char msg[MAX_REQUEST] = "GET_NR_UPLOADS";
            int nr_uploads;
            // request the number of uploads from each peer/seed from the swarm
            MPI_Send(msg, MAX_REQUEST, MPI_CHAR, peer_requested_files_map[file].swarm[i], MESSAGE_TYPE_PEER, MPI_COMM_WORLD);
            MPI_Recv(&nr_uploads, 1, MPI_INT, peer_requested_files_map[file].swarm[i], GET_PEER_UPLOADS, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            peer_uploads.push_back({peer_requested_files_map[file].swarm[i], nr_uploads});    
        }
    }

    sort(peer_uploads.begin(), peer_uploads.end(), comp);

    return peer_uploads;
}

// function for updating the swarm of a file
vector<pair<int, int>> update_swarm(string file, int rank)
{
    vector<pair<int, int>> peer_uploads;

    char msg_type[MAX_REQUEST] = "SWARM_UPDATE";
    MPI_Send(msg_type, MAX_REQUEST, MPI_CHAR, TRACKER_RANK, MESSAGE_TYPE, MPI_COMM_WORLD);
    MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, UPDATE_SWARM, MPI_COMM_WORLD);

    // get the swarm size and swarm from the tracker
    int swarm_size;
    MPI_Recv(&swarm_size, 1, MPI_INT, TRACKER_RANK, UPDATE_SWARM, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    vector<int> swarm(swarm_size);
    MPI_Recv(swarm.data(), swarm_size, MPI_INT, TRACKER_RANK, UPDATE_SWARM, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    peer_requested_files_map[file].swarm.assign(swarm.begin(), swarm.end());
    // get the number of uploads for each peer/seed from the swarm
    peer_uploads = get_peer_uploads(file, rank);

    return peer_uploads;
}

void *download_thread_func(void *arg)
{
    int rank = *(int*) arg;
    int nr_downloaded_segments = 0;
    vector<pair<int, int>> peer_uploads;

    // get information for all wanted files
    for (string file : wanted_files) {
        char msg_type[MAX_REQUEST] = "FILE_REQUEST";
        MPI_Send(msg_type, MAX_REQUEST, MPI_CHAR, TRACKER_RANK, MESSAGE_TYPE, MPI_COMM_WORLD);
        MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, REQUEST_FILE, MPI_COMM_WORLD);

        int swarm_size;
        MPI_Recv(&swarm_size, 1, MPI_INT, TRACKER_RANK, REQUEST_FILE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        vector<int> swarm(swarm_size);
        MPI_Recv(swarm.data(), swarm_size, MPI_INT, TRACKER_RANK, REQUEST_FILE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        peer_requested_files_map[file].swarm.assign(swarm.begin(), swarm.end());

        int nr_segments;
        MPI_Recv(&nr_segments, 1, MPI_INT, TRACKER_RANK, REQUEST_FILE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        peer_requested_files_map[file].nr_segments = nr_segments;

        for (int i = 0; i < nr_segments; i++) {
            char hash[HASH_SIZE];
            MPI_Recv(hash, HASH_SIZE, MPI_CHAR, TRACKER_RANK, REQUEST_FILE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            peer_requested_files_map[file].hashes.push_back(string(hash));
        }

        for (int i = 0; i < swarm_size; i++) {
            // initially all peers/seeds from a swarm will have 0 uploads
            if (peer_requested_files_map[file].swarm[i] != rank) {
                peer_uploads.push_back({peer_requested_files_map[file].swarm[i], 0});
            }
        }
    }

    for (string file : wanted_files) {
        /* for every file we start downloading except the first one (since
           we already have the current swarm for it) we need the updated swarm */
        if (nr_downloaded_segments > 0) {
            peer_uploads = update_swarm(file, rank);
        }

        for (int i = 0; i < peer_requested_files_map[file].nr_segments; i++) {
            /* we traverse the sorted vector of peers/seeds from the swarm
               to always download segments from those with fewer uploads */
            for (const auto& peer : peer_uploads) {
                char msg[MAX_REQUEST] = "GET_SEGMENT";
                // request the segment from a peer/seed
                MPI_Send(msg, MAX_REQUEST, MPI_CHAR, peer.first, MESSAGE_TYPE_PEER, MPI_COMM_WORLD);
                MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, peer.first, GET_SEGMENT_FILE, MPI_COMM_WORLD);
                MPI_Send(peer_requested_files_map[file].hashes[i].c_str(), HASH_SIZE, MPI_CHAR, peer.first, GET_SEGMENT_HASH, MPI_COMM_WORLD);

                char resp[5];
                // get peer/seed response
                MPI_Recv(resp, 5, MPI_CHAR, peer.first, GET_SEGMENT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // check if the peer/seed has the segment (ACK response)
                if (strcmp(resp, "ACK") == 0) {
                    pthread_mutex_lock(&peer_map_mutex);
                    peer_files_map[file].push_back(peer_requested_files_map[file].hashes[i]);
                    pthread_mutex_unlock(&peer_map_mutex);
                    nr_downloaded_segments++;
                    break;
                }
            }

            // after every 10 segments downloaded the client requests an update for the swarm
            if (nr_downloaded_segments % 10 == 0) {
                peer_uploads = update_swarm(file, rank);
            }
        }

        /* after completing the download of a file, the client
           announces the tracker and writes the hashes to a file */
        char msg_type[MAX_REQUEST] = "DOWNLOAD_FINISHED";
        MPI_Send(msg_type, MAX_REQUEST, MPI_CHAR, TRACKER_RANK, MESSAGE_TYPE, MPI_COMM_WORLD);
        MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, DOWNLOAD_FILE_FINISHED, MPI_COMM_WORLD);
        string out_filename = "client" + to_string(rank) + "_" + file;
        ofstream fout(out_filename);

        for (int i = 0; i < peer_requested_files_map[file].nr_segments - 1; i++) {
            fout << peer_requested_files_map[file].hashes[i] << "\n";
        }

        fout << peer_requested_files_map[file].hashes[peer_requested_files_map[file].nr_segments - 1];
        fout.close();
    }

    /* after finishing the download of all wanted files the client announces
       the tracker and closes the download thread */
    char msg_type[MAX_REQUEST] = "ALL_FILES_FINISHED";
    MPI_Send(msg_type, MAX_REQUEST, MPI_CHAR, TRACKER_RANK, MESSAGE_TYPE, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    int rank = *(int*) arg;
    int nr_uploaded_segments = 0;

    while (1) {
        char msg_type[MAX_REQUEST];
        MPI_Status status;
        // receive a message from another client
        MPI_Recv(msg_type, MAX_REQUEST, MPI_CHAR, MPI_ANY_SOURCE, MESSAGE_TYPE_PEER, MPI_COMM_WORLD, &status);

        // get client's total number of uploads
        if (strcmp(msg_type, "GET_NR_UPLOADS") == 0) {
            MPI_Send(&nr_uploaded_segments, 1, MPI_INT, status.MPI_SOURCE, GET_PEER_UPLOADS, MPI_COMM_WORLD);
        // download a segment from the client
        } else if (strcmp(msg_type, "GET_SEGMENT") == 0) {
            char file[MAX_FILENAME];
            char hash[HASH_SIZE];
            MPI_Recv(file, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, GET_SEGMENT_FILE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string file_str(file);
            MPI_Recv(hash, HASH_SIZE, MPI_CHAR, status.MPI_SOURCE, GET_SEGMENT_HASH, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            pthread_mutex_lock(&peer_map_mutex);
            /* if the file exists in the map, it means that
               the client has started downloading segments for that file */
            if (peer_files_map.find(file_str) != peer_files_map.end()) {
                // search for the wanted segment
                auto elem = find(peer_files_map[file_str].begin(), peer_files_map[file_str].end(), string(hash));

                // if the client has the segment it sends "ACK"; otherwise, it sends "NACK"
                if (elem != peer_files_map[file_str].end()) {
                    char resp[5] = "ACK";
                    MPI_Send(resp, 5, MPI_CHAR, status.MPI_SOURCE, GET_SEGMENT, MPI_COMM_WORLD);
                    nr_uploaded_segments++;
                } else {
                    char resp[5] = "NACK";
                    MPI_Send(resp, 5, MPI_CHAR, status.MPI_SOURCE, GET_SEGMENT, MPI_COMM_WORLD);
                }
            } else {
                char resp[5] = "NACK";
                MPI_Send(resp, 5, MPI_CHAR, status.MPI_SOURCE, GET_SEGMENT, MPI_COMM_WORLD);
            }
            pthread_mutex_unlock(&peer_map_mutex);
        // if all other clients have finished downloading, the client closes the upload thread
        } else if (strcmp(msg_type, "ALL_CLIENTS_FINISHED") == 0) {
            break;
        }
    }

    return NULL;
}

void tracker(int numtasks, int rank) {
    int nr_finished_clients = 0;
    // map to keep information about files
    unordered_map<string, tracker_file_info> tracker_map;

    for (int i = 1; i < numtasks; i++) {
        int files_nr;
        MPI_Recv(&files_nr, 1, MPI_INT, i, PEER_FILE_INFO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int j = 0; j < files_nr; j++) {
            int nr_segments;
            char file[MAX_FILENAME];
            MPI_Recv(file, MAX_FILENAME, MPI_CHAR, i, PEER_FILE_INFO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string file_str(file);
            MPI_Recv(&nr_segments, 1, MPI_INT, i, PEER_FILE_INFO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // check if the file entry has already been added in the map
            bool file_exists_in_map = (tracker_map.find(file_str) == tracker_map.end());

            if (file_exists_in_map) {
                tracker_map[file_str].nr_segments = nr_segments;
            }

            for (int k = 0; k < nr_segments; k++) {
                char hash[HASH_SIZE];
                MPI_Recv(hash, HASH_SIZE, MPI_CHAR, i, PEER_FILE_INFO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                if (file_exists_in_map) {
                    tracker_map[file_str].hashes.push_back(string(hash));
                }
            }

            // every client that owns the file is added to the file's swarm as "seed"
            tracker_map[file_str].peers_seeds_swarm.push_back({i, "seed"});
        }
    }

    /* once the tracker has received files from all clients,
       it sends "ACK" to signal them to start inter-client communication */
    for (int i = 1; i < numtasks; i++) {
        char resp[5] = "ACK";
        MPI_Send(resp, 5, MPI_CHAR, i, PEER_CONFIRMATION, MPI_COMM_WORLD);
    }

    while (1) {
        char msg_type[MAX_REQUEST];
        MPI_Status status;
        MPI_Recv(msg_type, MAX_REQUEST, MPI_CHAR, MPI_ANY_SOURCE, MESSAGE_TYPE, MPI_COMM_WORLD, &status);

        // request for file swarm and hashes from a client
        if (strcmp(msg_type, "FILE_REQUEST") == 0) {
            char file[MAX_FILENAME];
            MPI_Recv(file, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, REQUEST_FILE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string file_str(file);
            int swarm_size = tracker_map[file_str].peers_seeds_swarm.size();
            vector<int> file_swarm(swarm_size);

            for (int i = 0; i < swarm_size; i++) {
                file_swarm[i] = tracker_map[file_str].peers_seeds_swarm[i].first;
            }

            // send the swarm size and the list of all peers/seeds from the file swarm
            MPI_Send(&swarm_size, 1, MPI_INT, status.MPI_SOURCE, REQUEST_FILE, MPI_COMM_WORLD);
            MPI_Send(file_swarm.data(), swarm_size, MPI_INT, status.MPI_SOURCE, REQUEST_FILE, MPI_COMM_WORLD);
            MPI_Send(&tracker_map[file_str].nr_segments, 1, MPI_INT, status.MPI_SOURCE, REQUEST_FILE, MPI_COMM_WORLD);

            for (int i = 0; i < tracker_map[file_str].nr_segments; i++) {
                MPI_Send(tracker_map[file_str].hashes[i].c_str(), HASH_SIZE, MPI_CHAR, status.MPI_SOURCE, REQUEST_FILE, MPI_COMM_WORLD);
            }

            // add the client in the file swarm as "peer"
            tracker_map[file_str].peers_seeds_swarm.push_back({status.MPI_SOURCE, "peer"});
        // request for swarm update from a client
        } else if (strcmp(msg_type, "SWARM_UPDATE") == 0) {
            char file[MAX_FILENAME];
            MPI_Recv(file, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, UPDATE_SWARM, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string file_str(file);
            int swarm_size = tracker_map[file_str].peers_seeds_swarm.size();
            vector<int> file_swarm(swarm_size);

            for (int i = 0; i < swarm_size; i++) {
                file_swarm[i] = tracker_map[file_str].peers_seeds_swarm[i].first;
            }

            // send the swarm size and the list of all peers/seeds from the file swarm
            MPI_Send(&swarm_size, 1, MPI_INT, status.MPI_SOURCE, UPDATE_SWARM, MPI_COMM_WORLD);
            MPI_Send(file_swarm.data(), swarm_size, MPI_INT, status.MPI_SOURCE, UPDATE_SWARM, MPI_COMM_WORLD);
        // a client announces the tracker that it has finished downloading the file
        } else if (strcmp(msg_type, "DOWNLOAD_FINISHED") == 0) {
            char file[MAX_FILENAME];
            MPI_Recv(file, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, DOWNLOAD_FILE_FINISHED, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string file_str(file);
            int swarm_size = tracker_map[file_str].peers_seeds_swarm.size();

            for (int i = 0; i < swarm_size; i++) {
                // the client becomes seed for the file
                if (tracker_map[file_str].peers_seeds_swarm[i].first == status.MPI_SOURCE) {
                    tracker_map[file_str].peers_seeds_swarm[i].second = "seed";
                    break;
                }
            }
        /* the tracker keeps track of the number of clients that have
           finished downloading all the files */
        } else if (strcmp(msg_type, "ALL_FILES_FINISHED") == 0) {
            nr_finished_clients++;
        }

        /* if all clients have finished downloading, the tracker sends
           a message to each client to stop, and then it also stops */
        if (nr_finished_clients == numtasks - 1) {
            for (int i = 1; i < numtasks; i++) {
                char msg_type[MAX_REQUEST] = "ALL_CLIENTS_FINISHED";
                MPI_Send(msg_type, MAX_REQUEST, MPI_CHAR, i, MESSAGE_TYPE_PEER, MPI_COMM_WORLD);
            }

            break;
        }
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;
    string filename = "in" + to_string(rank) + ".txt";
    int own_files_nr;
    ifstream fin(filename);

    if (!fin.is_open()) {
        printf("Eroare la deschiderea fisierului %s.\n", filename.c_str());
        exit(-1);
    }

    pthread_mutex_init(&peer_map_mutex, NULL);

    // send files number to the tracker
    fin >> own_files_nr;
    MPI_Send(&own_files_nr, 1, MPI_INT, TRACKER_RANK, PEER_FILE_INFO, MPI_COMM_WORLD);

    // for each file send the filename and the hashes to the tracker
    for (int i = 0; i < own_files_nr; i++) {
        string file;
        int nr_segments;
        fin >> file >> nr_segments;
        MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, PEER_FILE_INFO, MPI_COMM_WORLD);
        MPI_Send(&nr_segments, 1, MPI_INT, TRACKER_RANK, PEER_FILE_INFO, MPI_COMM_WORLD);

        for (int j = 0; j < nr_segments; j++) {
            string hash;
            fin >> hash;
            peer_files_map[file].push_back(hash);
            MPI_Send(hash.c_str(), HASH_SIZE, MPI_CHAR, TRACKER_RANK, PEER_FILE_INFO, MPI_COMM_WORLD);
        }
    }

    int wanted_files_nr;
    fin >> wanted_files_nr;

    for (int i = 0; i < wanted_files_nr; i++) {
        string wanted_file;
        fin >> wanted_file;
        wanted_files.push_back(wanted_file);
    }

    fin.close();

    char resp[5];
    // wait for tracker's confirmation message before starting the download and upload threads
    MPI_Recv(resp, 5, MPI_CHAR, TRACKER_RANK, PEER_CONFIRMATION, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }

    pthread_mutex_destroy(&peer_map_mutex);
}

int main (int argc, char *argv[]) {
    int numtasks, rank;

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
