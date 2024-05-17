#ifndef HELPER_HEADER
#define HELPER_HEADER

#include <stdio.h>
#include <bits/stdc++.h> 
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <malloc.h>
#include <cstdlib>
#include <pthread.h>
#include <utility>
#include <sys/stat.h>
#include <chrono>
#include <random>
#include <mutex>
#include <condition_variable>


using namespace std;

// Define message types that can be sent between servers
enum MessageType {
    HEARTBEAT=1,
    VOTE_REQUEST=2,
    VOTE_RESPONSE=3,
    LOG_REQUEST=4,
    LOG_RESPONSE=5,
    FORWARD_MESSAGE=6
};


struct LogEntry {
    string message;
    int term;
    int index;

    // Assuming LogEntry needs these functions:
    string serialize() const {
        ostringstream oss;
        oss << term << " " << index << " " << message.size() << " " << message;
        return oss.str();
    }

    static LogEntry deserialize(istringstream& iss) {
        int term, index, size;
        string message;
        iss >> term >> index >> size;
        message.resize(size);
        iss.read(&message[0], size); // directly read into the string's buffer
        return LogEntry{message, term, index};
    }

    LogEntry(string m, int t, int idx) : message(move(m)), term(t), index(idx) {}
};


struct VoteRequest {
    int candidate_id; // The candidate port number
    int current_term;
    int log_len;
    int last_term;

    // Serialize the VoteRequest into a string for sending over the network
    string serialize() const {
        ostringstream oss;
        oss << candidate_id << " " << current_term << " " << log_len << " " << last_term;
        return oss.str();
    }

    // Deserialize a VoteRequest from a string received over the network
    static VoteRequest deserialize(const string& data) {
        istringstream iss(data);
        VoteRequest vr;
        iss >> vr.candidate_id >> vr.current_term >> vr.log_len >> vr.last_term;
        return vr;
    }
};


struct VoteResponse {
    int voter_id; // The voter port number
    int current_term;
    bool is_vote_granted; // Represented as bool for clarity in code

    // Serialize the VoteResponse into a string for sending over the network
    string serialize() const {
        ostringstream oss;
        oss << voter_id << " " << current_term << " " << static_cast<int>(is_vote_granted);
        return oss.str();
    }

    // Deserialize a VoteResponse from a string received over the network
    static VoteResponse deserialize(const string& data) {
        istringstream iss(data);
        VoteResponse vr;
        int granted;
        iss >> vr.voter_id >> vr.current_term >> granted;
        vr.is_vote_granted = static_cast<bool>(granted);
        return vr;
    }
};


struct LogRequest {
    int leader_id;
    int current_term;
    string new_log;

    string serialize() const {
        ostringstream oss;
        oss << leader_id << " " << current_term << " " << new_log;
        return oss.str();
    }

    static LogRequest deserialize(const string& data) {
        istringstream iss(data);
        LogRequest lr;
        iss >> lr.leader_id >> lr.current_term;
        iss.ignore();
        getline(iss, lr.new_log);
        return lr;
    }
};


struct LogResponse {
    int follower_id;  // The follower port number
    int current_term;
    int ack;
    bool is_good;

    // Serialize the LogResponse into a string for sending over the network
    string serialize() const {
        ostringstream oss;
        oss << follower_id << " " << current_term << " " << ack << " " << static_cast<int>(is_good);
        return oss.str();
    }

    // Deserialize a LogResponse from a string received over the network
    static LogResponse deserialize(const string& data) {
        istringstream iss(data);
        LogResponse lr;
        int goodAsInt;
        iss >> lr.follower_id >> lr.current_term >> lr.ack >> goodAsInt;
        lr.is_good = static_cast<bool>(goodAsInt);
        return lr;
    }
};


struct AcceptedSocket {
    int client_fd;
    struct sockaddr_in addr;
    bool successful;
    bool is_client_socket;
};


struct PeerInfo {
    int peer_port;
    string message;
};


struct AcceptedSocket* accept_incoming_connection(int fd) {
    struct sockaddr_in client_address;
    socklen_t client_address_size = (socklen_t) sizeof(struct sockaddr_in);
    int client_socket_fd = accept(fd, (struct sockaddr *) &client_address, &client_address_size);

    struct AcceptedSocket* client_socket = (struct AcceptedSocket*) malloc(sizeof(struct AcceptedSocket));
    client_socket->client_fd = client_socket_fd;
    client_socket->addr = client_address;
    client_socket->successful = client_socket_fd > 0;    
    return client_socket;
}


// Helper function to extract the command
inline string extract_command(string mes) {
    size_t pos = mes.find(" ");
    if (pos != std::string::npos)
        return mes.substr(0, pos);
    else
        return mes;
}


// Helper function to extract the message id 
inline string extract_message_id(string mes) {
    size_t first_space = mes.find(' ');
    size_t second_space = mes.find(' ', first_space + 1);
    return mes.substr(first_space + 1, second_space - first_space - 1);    
}


// Helper function to extract the message
inline string extract_message(string mes) {
    size_t first_space = mes.find(' ');
    if (first_space != std::string::npos) {
        size_t second_space = mes.find(' ', first_space + 1);
        if (second_space != std::string::npos)
            return mes.substr(second_space + 1);
    }
    return "";
}

#endif