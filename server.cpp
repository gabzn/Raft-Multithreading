#include "helper.hpp"

using namespace std;

int current_term = 0;
int voted_for = -1;
int commit_len = 0;
vector<LogEntry> chat_log;
pthread_mutex_t chat_log_lock = PTHREAD_MUTEX_INITIALIZER;


pthread_mutex_t role_lock = PTHREAD_MUTEX_INITIALIZER;
string current_role = "follower";
int current_leader = -1;
unordered_set<int> votes_received;
unordered_map<int, int> sent_len;
unordered_map<int, int> acked_len;

// Socket for clients
int server_fd;
int port;
struct sockaddr_in address;
bool crash_needed = false;
unordered_map<int, AcceptedSocket*> client_connections;

// Socket for peers
int peer_fd;
int peer_port;
struct sockaddr_in peer_address;
unordered_map<int, AcceptedSocket*> peer_connections;

int total_number_of_servers;
int current_server;

mutex mtx;
condition_variable cv;
bool is_leader_alive = false;
chrono::milliseconds election_timeout;


struct ThreadData {
    string serialized_str;
    int index;
};


void reset_timeout() {
    static random_device rd;
    static mt19937 gen(rd());
    uniform_int_distribution<> dist(1500, 5000);
    election_timeout = chrono::milliseconds(dist(gen));
}


// Helper function to send the chat log back to the client
void send_chat_log(int client_fd) {
    string chat_log_response = "chatLog ";
    
    pthread_mutex_lock(&chat_log_lock);
    for(const LogEntry& l: chat_log) {
        chat_log_response += l.message;
        chat_log_response += ",";
    }
    pthread_mutex_unlock(&chat_log_lock);    

    // Pop the ending comma
    chat_log_response.pop_back();
    chat_log_response += "\n\0";

    cout << "Server is going to send this to the client: " << chat_log_response;

    ssize_t bytes_sent = send(client_fd, chat_log_response.data(), chat_log_response.size(), 0);
    if (bytes_sent == -1) 
        cout << "Failed to send log to client\n"; 
    else
        cout << "Server has sent chat log to client\n";
}


// Helper function to shut down a socket connection with fd 
void close_connection_with_fd(int fd, bool is_client) {
    cout << "Closing down socket with fd: " << fd << "\n";
    
    if (is_client) {
        auto it = client_connections.find(fd);
        if (it != client_connections.end()) {
            delete it->second;
            client_connections.erase(it);
        }
    } else {
        auto it = peer_connections.find(fd);
        if (it != peer_connections.end()) {
            delete it->second;
            peer_connections.erase(it);
        }
    }

    close(fd);    
}


// Helper function to shut down all client connections
void close_all_connections() {
    for(auto& p: client_connections) {
        delete p.second;
        cout << "Closing down client with fd: " << p.first << "\n";
        close(p.first);
    }

    client_connections.clear();
    close(server_fd);
    exit(0);
}


// Thread function to handle client connections
void* handle_client_connections(void* arg) {
    int client_fd = *((int*)arg);
    char buffer[1024];

    while(true) {
        // Clear the buffer before receiving next message
        memset(buffer, 0, sizeof(buffer));

        ssize_t message_size = recv(client_fd, buffer, 1023, 0);
        if (message_size <= 0)
            break;

        buffer[message_size] = '\0';
        string message;
        message.assign(buffer);
        if (message.back() == '\n')
            message.pop_back();

        string command = extract_command(message);
        if (command == "crash") {
            cout << "Client sent in crash command\n";
            crash_needed = true;
            break;
        }

        if (command == "get") {
            cout << "Client with fd "<< client_fd << " requested the chat log\n";
            send_chat_log(client_fd);
            continue;
        }

        string message_id = extract_message_id(message);
        message = extract_message(message);
        if (command == "msg") {
            cout << "Client sent in a message: " << message << "\n";

            if (current_role == "leader") {
                pthread_mutex_lock(&chat_log_lock);
                chat_log.emplace_back(message, current_term, chat_log.size());
                pthread_mutex_unlock(&chat_log_lock);                    

                string ack_response = "ack " + message_id + " " + to_string(chat_log.back().index) + "\n\0";
                ssize_t bytes_sent = send(client_fd, ack_response.data(), ack_response.size(), 0);
                if(bytes_sent == -1)
                    cout << "Failed to send ack to client\n";
                else
                    cout << "Server has sent ack to client\n";
            } else {
                int leader_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
                while (current_leader == -1)
                    sleep(1); 
                int port_to_send = 20000 + current_leader;
                struct sockaddr_in addr;
                addr.sin_family = AF_INET;
                addr.sin_addr.s_addr = inet_addr("127.0.0.1");
                addr.sin_port = htons(port_to_send);

                int connection_result = connect(leader_socket_fd, (struct sockaddr *)&addr, sizeof(addr));
                if (connection_result < 0) {
                    cout << "Cannot connect to server with port " + to_string(port_to_send) + "\n";
                    return nullptr;
                }    

                // Send the message_type first
                int message_type = FORWARD_MESSAGE;
                message_type = htonl(message_type); 
                send(leader_socket_fd, &message_type, sizeof(message_type), 0);

                // Send the actual message
                send(leader_socket_fd, message.c_str(), message.length(), 0);

                // Wait for the leader to commit the message to the log and get the confirmation
                int confirmation;
                recv(leader_socket_fd, &confirmation, sizeof(confirmation), 0);
                close(leader_socket_fd);

                // Send back ack to client
                string ack_response = "ack " + message_id + " " + to_string(confirmation) + "\n\0";
                send(client_fd, ack_response.data(), ack_response.size(), 0);           
            }
            continue;
        }

        cout << "Wrong message type! Message discarded...\n";
    }

    if (crash_needed)
        close_all_connections();
    else
        close_connection_with_fd(client_fd, true);

    return nullptr;
}


// Thread function to handle message for each connection
void* handle_server_connections(void* arg) {
    int socket_fd = *((int*)arg);

    int message_type;
    recv(socket_fd, &message_type, sizeof(message_type), 0);
    message_type = ntohl(message_type);
    
    // Handle requests based on the type
    if (message_type == 1) {
        char buffer[1024];
        memset(buffer, 0, sizeof(buffer));
        ssize_t message_size = recv(socket_fd, buffer, 1023, 0); 
        buffer[message_size] = '\0';

        string message;
        message.assign(buffer);
        if (message.back() == '\n')
            message.pop_back();   
        
        LogRequest log_request = LogRequest::deserialize(message);
        cout << "Received heartbeat from leader " << log_request.leader_id << "\n";
        current_leader = log_request.leader_id;
        current_term = log_request.current_term;
        string new_log = log_request.new_log;
        if (new_log != "None") {
            cout << "Received new log: " << new_log << "\n";
            pthread_mutex_lock(&chat_log_lock);
            chat_log.clear();

            string l;
            istringstream iss(new_log);
            while (getline(iss, l, ',')) 
                chat_log.emplace_back(l, current_term, chat_log.size());
            pthread_mutex_unlock(&chat_log_lock);           
        }
    }
    else if (message_type == 2) {
        if (voted_for != -1) {
            cout << "This server has already voted for someone else\n";

            VoteResponse vote_response;
            vote_response.current_term = current_term;
            vote_response.voter_id = current_server;
            vote_response.is_vote_granted = false;
            string serialized_vote_response = vote_response.serialize();
            send(socket_fd, serialized_vote_response.c_str(), serialized_vote_response.length(), 0);
        } else {
            cout << "Received a vote request \n";
            pthread_mutex_lock(&role_lock);
            current_role = "follower";
            pthread_mutex_unlock(&role_lock);

            char buffer[1024];
            memset(buffer, 0, sizeof(buffer));
            ssize_t message_size = recv(socket_fd, buffer, 1023, 0); 
            buffer[message_size] = '\0';
            
            string message;
            message.assign(buffer);
            if (message.back() == '\n')
                message.pop_back();

            VoteRequest vote_request = VoteRequest::deserialize(message);
            voted_for = vote_request.candidate_id;
            cout << "Voted for server " << voted_for << "\n";

            VoteResponse vote_response;
            vote_response.current_term = current_term;
            vote_response.voter_id = current_server;
            vote_response.is_vote_granted = true;
            string serialized_vote_response = vote_response.serialize();
            send(socket_fd, serialized_vote_response.c_str(), serialized_vote_response.length(), 0);
        }
    }
    else if (message_type == 6) {
        cout << "Received a forwarded message\n";
        char buffer[1024];
        memset(buffer, 0, sizeof(buffer));
        ssize_t message_size = recv(socket_fd, buffer, 1023, 0); 
        buffer[message_size] = '\0';
        
        string message;
        message.assign(buffer);
        if (message.back() == '\n')
            message.pop_back();   

        chat_log.emplace_back(message, current_term, chat_log.size());
        int con = chat_log.back().index;
        send(socket_fd, &con, sizeof(con), 0);
    }

    close_connection_with_fd(socket_fd, false);
    return nullptr;
}


// Helper function to handle each connection on a separate thread
void process_connection_in_different_thread(struct AcceptedSocket* socket) {
    pthread_t thread_id;

    if (socket->is_client_socket)
        pthread_create(&thread_id, NULL, handle_client_connections, &(socket->client_fd));
    else
        pthread_create(&thread_id, NULL, handle_server_connections, &(socket->client_fd));
}


// Thread function to create a separate socket to accept peers' connections
void* accept_peer_connections(void* arg) {
    // Creating socket file descriptor
    peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0) {
        perror("Socket creation failed\n");
        return nullptr;
    }

    // Bind the socket to the address and port number
    peer_address.sin_family = AF_INET;
    peer_address.sin_addr.s_addr = INADDR_ANY;
    peer_address.sin_port = htons(peer_port);
    if (bind(peer_fd, (struct sockaddr *) &peer_address, sizeof(peer_address)) < 0) {
        perror("Binding failed\n");
        return nullptr;
    }
    
    // Listen for incoming peer's connection
    if (listen(peer_fd, 10) < 0) {
        perror("Listen failed\n");
        return nullptr;
    }
    cout << "Server starts listening on port: " << peer_port << " for peers' connections\n";

    // Whenever there's a new connection from other servers, open up a new thread to handle that connection
    while(true) {
        struct AcceptedSocket* peer_socket = accept_incoming_connection(peer_fd);
        if (!peer_socket->successful) {
            cout << "Error accepting peer's connection\n";
            close(peer_socket->client_fd);
        }

        peer_socket->is_client_socket = false;
        peer_connections[peer_socket->client_fd] = peer_socket;
        cout << "New peer's connection with fd: " << peer_socket->client_fd << " accepted\n";
        process_connection_in_different_thread(peer_socket);
    }
    
    cout << "Server closing socket for peers\n";
    close(peer_fd);
    return nullptr;
}


// Create a socket on a separate thread to accept peer connections 
void start_accepting_new_peer_connections() {
    pthread_t thread_id;
    pthread_create(&thread_id, NULL, accept_peer_connections, nullptr);
}


// Helper function to listen to client's connection
void start_accepting_new_client_connections(int server_fd) {
    while(true) {
        struct AcceptedSocket* client_socket = accept_incoming_connection(server_fd);
        if (!client_socket->successful) {
            cout << "Error accepting connection\n";
            close(client_socket->client_fd);
        }

        client_socket->is_client_socket = true;
        client_connections[client_socket->client_fd] = client_socket;
        cout << "New connection with fd: " << client_socket->client_fd << " accepted\n";
        process_connection_in_different_thread(client_socket);
    }
    
    cout << "Server closing\n";
    close(server_fd);
}


void* send_vote_request(void* arg) {
    ThreadData* data = static_cast<ThreadData*>(arg);
    string serialized_vr = data->serialized_str;
    int i = data->index;
    int port_to_send = 20000 + i;
    int peer_socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port_to_send);

    int connection_result = connect(peer_socket_fd, (struct sockaddr *)&addr, sizeof(addr));
    if (connection_result < 0) {
        cout << "Cannot connect to server with port " + to_string(port_to_send) + "\n";
        return nullptr;
    }    

    // Send the message_type first
    int message_type = VOTE_REQUEST;
    message_type = htonl(message_type); 
    send(peer_socket_fd, &message_type, sizeof(message_type), 0);

    // Send the vote request body
    send(peer_socket_fd, serialized_vr.c_str(), serialized_vr.length(), 0);

    // Wait for the vote response
    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    ssize_t message_size = recv(peer_socket_fd, buffer, 1023, 0); 
    buffer[message_size] = '\0';
    
    string message;
    message.assign(buffer);
    if (message.back() == '\n')
        message.pop_back();

    // Look at the field to see if received any vote
    VoteResponse vote_response = VoteResponse::deserialize(message);
    if (vote_response.is_vote_granted) {
        votes_received.insert(vote_response.voter_id);
        if (votes_received.size() > total_number_of_servers / 2) {
            cout << "This server has received enough votes to become a leader now\n";
            pthread_mutex_lock(&role_lock);
            current_role = "leader";
            pthread_mutex_unlock(&role_lock);
        }
    }
    return nullptr;
}


void start_sending_vote_request(int current_server, int current_term, int s, int last_term, int i) {
    // Send vote request to others
    VoteRequest vr;
    vr.candidate_id = current_server;
    vr.current_term = current_term;
    vr.log_len = s;
    vr.last_term = last_term;

    ThreadData* data = new ThreadData();
    data->serialized_str = vr.serialize();
    data->index = i;
    
    pthread_t thread_id;
    pthread_create(&thread_id, NULL, send_vote_request, data);    
}


void initialize_election() {
    if (voted_for != -1) 
        return;
    
    pthread_mutex_lock(&role_lock);
    current_role = "candidate";
    pthread_mutex_unlock(&role_lock);
    current_term++;
    voted_for = current_server;
    votes_received.insert(current_server);

    int last_term = 0;
    if (chat_log.size() > 0)
        last_term = chat_log.back().term;
    
    for(int i = 0; i < total_number_of_servers; i++) {
        if (i == current_server) 
            continue;
        
        // Open new threads and send the vote request
        start_sending_vote_request(current_server, current_term, chat_log.size(), last_term, i);
    }
}


void* wait_for_heartbeats_from_leader_with_timeout(void* arg) {
    unique_lock<mutex> lk(mtx);
    reset_timeout();

    while(true) {
        if (current_role == "leader" || current_role == "candidate" || current_leader != -1) {
            break;
        }
        else if (!cv.wait_for(lk, election_timeout, [] { return is_leader_alive; })) {
            if (voted_for != -1)
                break;

            cout << "Did not receive heartbeat from leader. Initializing leader election now\n";
            initialize_election();
        }
        is_leader_alive = false;
        reset_timeout(); 
    }

    return nullptr;
}


void start_checking_for_heartbeats() {
    pthread_t thread_id;
    pthread_create(&thread_id, NULL, wait_for_heartbeats_from_leader_with_timeout, nullptr);    
}


void* send_heartbeat(void* arg) {
    int i = *((int*)arg);
    int port_to_send = 20000 + i;
    int peer_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_socket_fd < 0) {
        perror("Socket creation failed\n");
        return nullptr;
    }    

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port_to_send);
    // sleep(1);

    int connection_result = connect(peer_socket_fd, (struct sockaddr *)&addr, sizeof(addr));
    if (connection_result < 0) {
        cout << "Cannot connect to server with port " + to_string(port_to_send) + "\n";
        return nullptr;
    }    

    // Send the message_type first
    int message_type = HEARTBEAT;
    message_type = htonl(message_type); 
    send(peer_socket_fd, &message_type, sizeof(message_type), 0);
    
    // Send the log to update their logs
    string new_log = "";
    for(auto& l: chat_log) {
        new_log += l.message;
        new_log += ",";
    }
    // Pop the ending comma
    if (new_log.length() > 0) {
        new_log.pop_back();
        new_log += "\n\0";
    } 

    LogRequest log_request;
    log_request.leader_id = current_server;
    log_request.current_term = current_term;
    if (new_log.length() > 0) 
        log_request.new_log = new_log;
    else
        log_request.new_log = "None";

    // cout << log_request.new_log << "\n";
    string message = log_request.serialize();
    send(peer_socket_fd, message.c_str(), message.length(), 0);
    close(peer_socket_fd);
    return nullptr;
}


void* send_heartbeats_periodically(void* arg) {
    while(true) {
        if (current_leader != -1 && current_leader != current_server)
            break;
        
        pthread_mutex_lock(&role_lock);
        if (current_role == "leader") {    
            for(int i = 0; i < total_number_of_servers; i++) {
                if (i == current_server) 
                    continue;

                // cout << "Sending hearbeats to server on port " << to_string(20000 + i) << "\n";
                int peer_id = i;

                // Open new threads and send the vote request
                pthread_t thread_id;
                pthread_create(&thread_id, NULL, send_heartbeat, &peer_id); 
                sleep(1);
            }            
        }
        pthread_mutex_unlock(&role_lock);
        sleep(1);
    }

    return nullptr;
}


void start_sending_heartbeats_if_become_leader() {
    pthread_t thread_id;
    pthread_create(&thread_id, NULL, send_heartbeats_periodically, nullptr);
}


int main(int argc, char const *argv[]) {
    if (argc != 5) {
        cerr << "Usage: " << argv[0] << " <PORT CLIENTS CONNECT TO> <PORT PEERS CONNECT TO> <TOTAL NUMBER OF SERVERS> <CURRENT SERVER>\n";
        return 1;
    }

    port = atoi(argv[1]);
    peer_port = atoi(argv[2]);
    total_number_of_servers = atoi(argv[3]);
    current_server = atoi(argv[4]);

    // Creating socket file descriptor
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed\n");
        return 1;
    }

    // Bind the socket to the address and port number
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    if (bind(server_fd, (struct sockaddr *) &address, sizeof(address)) < 0) {
        perror("Binding failed\n");
        return 1;
    }
    
    // Listen for incoming connections
    if (listen(server_fd, 10) < 0) {
        perror("Listen failed\n");
        return 1;
    }
    cout << "Server starts listening on port: " << port << " for clients' connections\n";  

    start_accepting_new_peer_connections();
    start_sending_heartbeats_if_become_leader();
    start_checking_for_heartbeats();
    start_accepting_new_client_connections(server_fd);
    return 0;
}