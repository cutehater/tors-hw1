#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <stdbool.h>

#define MASTER_PORT 6000
#define BROADCAST_MIN_PORT 6001
#define BUFFER_SIZE 1024
#define WORKERS_COUNT 3
#define MAX_RETRIES 3

typedef struct {
    struct sockaddr_in addr;
    int socket;
    bool active;
} Server;

typedef struct {
    int assignee;
    bool is_finished;
    int index;
    double start, end, result;
} Task;

void send_broadcast(int udp_sock, struct sockaddr_in *broadcast_addr, int port) {
    broadcast_addr->sin_port = htons(port);
    const char *message = "DISCOVER";
    sendto(udp_sock, message, strlen(message), 0,
           (struct sockaddr *)broadcast_addr, sizeof(*broadcast_addr));
}

bool assign_task(Server *server, Task *task) {

    ssize_t bytes_sent = write(server->socket, &task->index, sizeof(task->assignee));
    if (bytes_sent < 0) {
        server->active = false;
        return false;
    }
    bytes_sent = write(server->socket, &task->start, sizeof(double));
    if (bytes_sent < 0) {
        server->active = false;
        return false;
    }
    bytes_sent = write(server->socket, &task->end, sizeof(double));
    if (bytes_sent < 0) {
        server->active = false;
        return false;
    }
    printf("Task %d assigned to server %d\n", task->index, ntohs(server->addr.sin_port));
    return true;
}

int main() {
    struct sockaddr_in broadcast_addr, master_addr;
    Server servers[WORKERS_COUNT] = {0};

    fd_set read_fds;

    int udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    int broadcast_enable = 1;
    setsockopt(udp_sock, SOL_SOCKET, SO_BROADCAST, &broadcast_enable, sizeof(broadcast_enable));
    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);

    int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(MASTER_PORT);
    master_addr.sin_addr.s_addr = INADDR_ANY;
    bind(tcp_sock, (struct sockaddr *)&master_addr, sizeof(master_addr));
    listen(tcp_sock, WORKERS_COUNT);

    for (int i = 0; i < WORKERS_COUNT; i++) {
        send_broadcast(udp_sock, &broadcast_addr, BROADCAST_MIN_PORT + i);
        printf("Sent broadcast to discover servers on port %d...\n", BROADCAST_MIN_PORT + i);
    }

    struct sockaddr_in server_addr;
    socklen_t addr_len = sizeof(server_addr);
    char buffer[BUFFER_SIZE];
    struct timeval timeout = { 1, 0};

    FD_ZERO(&read_fds);
    FD_SET(udp_sock, &read_fds);
    int max_fd = udp_sock;
    int server_count = 0;
    int master_retries = 0;

    while (server_count < WORKERS_COUNT) {
        fd_set temp_fds = read_fds;
        int activity = select(max_fd + 1, &temp_fds, NULL, NULL, &timeout);

        if (activity > 0) {
            if (FD_ISSET(udp_sock, &temp_fds)) {
                recvfrom(udp_sock, buffer, sizeof(buffer), 0, (struct sockaddr *)&server_addr, &addr_len);

                bool already_connected = false;
                for (int i = 0; i < WORKERS_COUNT; i++) {
                    if (servers[i].active == 1 && servers[i].addr.sin_addr.s_addr == server_addr.sin_addr.s_addr
                        && servers[i].addr.sin_port == server_addr.sin_port) {
                        printf("Worker already connected: %s:%d\n", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));
                        already_connected = true;
                    }
                }
                if (already_connected) {
                    continue;
                }

                printf("Found worker: %s:%d\n", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));

                int server_socket = socket(AF_INET, SOCK_STREAM, 0);
                int retries = 0;
                while (connect(server_socket, (struct sockaddr *)&server_addr, addr_len) < 0 && retries < MAX_RETRIES) {
                    perror("Connection failed, retrying");
                    retries++;
                }
                if (retries == MAX_RETRIES) {
                    close(server_socket);
                    continue;
                }

                servers[server_count].addr = server_addr;
                servers[server_count].socket = server_socket;
                servers[server_count].active = true;
                server_count++;
            }
        } else {
            printf("Select failed or timeout expired with no worker connected\n");
            if (master_retries < MAX_RETRIES) {
                master_retries++;
                for (int i = 0; i < WORKERS_COUNT; i++) {
                    send_broadcast(udp_sock, &broadcast_addr, BROADCAST_MIN_PORT + i);
                    printf("Resent broadcast to discover servers on port %d...\n", BROADCAST_MIN_PORT + i);
                }
            } else {
                break;
            }
        }
    }

    int task_count = server_count;
    double range_start = 0, range_end = 10, step = (range_end - range_start) / task_count;
    Task tasks[task_count];
    for (int i = 0; i < task_count; ++i) {
        tasks[i] = (Task){i, false, i, range_start + i * step, range_start + (i + 1) * step, 0.0};
        assign_task(&servers[i], &tasks[i]);
    }

    FD_ZERO(&read_fds);
    for (int i = 0; i < server_count; ++i) {
        FD_SET(servers[i].socket, &read_fds);
        if (servers[i].socket > max_fd) max_fd = servers[i].socket;
    }

    bool is_finished = false;
    while (!is_finished) {
        is_finished = true;

        FD_ZERO(&read_fds);
        max_fd = 0;
        for (int i = 0; i < task_count; ++i) {
            if (!tasks[i].is_finished) {
                FD_SET(servers[tasks[i].assignee].socket, &read_fds);
                if (servers[tasks[i].assignee].socket > max_fd) max_fd = servers[tasks[i].assignee].socket;
            }
        }
        select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

        bool activity = false;
        for (int i = 0; i < task_count; ++i) {
            if (tasks[i].is_finished) {
                continue;
            }

            int s = tasks[i].assignee;
            if (FD_ISSET(servers[s].socket, &read_fds)) {
                servers[s].active = true;
                int task_index;
                ssize_t bytes_read = read(servers[s].socket, &task_index, sizeof(task_index));
                if (bytes_read <= 0) {
                    servers[s].active = false;
                    is_finished = false;
                    continue;
                }
                bytes_read = read(servers[s].socket, &tasks[task_index].result, sizeof(double));
                if (bytes_read <= 0) {
                    servers[s].active = false;
                    is_finished = false;
                    continue;
                }
                tasks[task_index].is_finished = true;
                activity = true;
            } else {
                is_finished = false;
            }
        }

        if (activity) {
            continue;
        }

        sleep(10);
        for (int i = 0; i < WORKERS_COUNT; i++) {
            send_broadcast(udp_sock, &broadcast_addr, BROADCAST_MIN_PORT + i);
            printf("Sent broadcast to discover servers on port %d...\n", BROADCAST_MIN_PORT + i);
        }

        fd_set udp_fds;
        FD_ZERO(&udp_fds);
        FD_SET(udp_sock, &udp_fds);
        struct timeval connect_timeout = { 0, 10000};
        select(max_fd + 1, &udp_fds, NULL, NULL, &connect_timeout);

        if (FD_ISSET(udp_sock, &udp_fds)) {
            recvfrom(udp_sock, buffer, sizeof(buffer), 0, (struct sockaddr *)&server_addr, &addr_len);
            for (int i = 0; i < server_count; ++i) {
                if (servers[i].addr.sin_addr.s_addr == server_addr.sin_addr.s_addr && servers[i].addr.sin_port == server_addr.sin_port) {
                    if (i < server_count && servers[i].active) {
                        break;
                    }

                    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
                    int retries = 0;
                    printf("Connecting to server on port %d...\n", ntohs(server_addr.sin_port));
                    while (connect(server_socket, (struct sockaddr *)&server_addr, addr_len) < 0 && retries < MAX_RETRIES) {
                        perror("Connection failed, retrying");
                        retries++;
                    }
                    if (retries == MAX_RETRIES) {
                        close(server_socket);
                    } else {
                        servers[i].socket = server_socket;
                        servers[i].active = true;
                    }
                }
            }
        }

        for (int i = 0; i < task_count; ++i) {
            if (!tasks[i].is_finished) {
                int j = i;
                do {
                    if (servers[j].active && assign_task(&servers[j], &tasks[i])) {
                        tasks[i].assignee = j;
                        break;
                    }
                    j = (j + 1) % server_count;
                } while (j != i);
            }
        }
    }

    double total_result = 0;
    for (int i = 0; i < task_count; ++i) {
        total_result += tasks[i].result;
    }
    printf("Integral result: %.4lf\n", total_result);

    for (int i = 0; i < server_count; ++i) {
        close(servers[i].socket);
    }
    close(udp_sock);
    close(tcp_sock);
    return 0;
}
