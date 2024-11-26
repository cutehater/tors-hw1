#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <math.h>

#define BUFFER_SIZE 1024
#define MAX_RETRIES 3

double compute_integral(double a, double b) {
    // x^2 integral
    int n = 1000;
    double h = (b - a) / n;
    double result = 0.0;
    for (int i = 0; i < n; i++) {
        double x = a + i * h + h / 2;
        result += x * x * h;
    }
    return result;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <PORT>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    struct sockaddr_in udp_addr, tcp_addr;

    int udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock < 0) {
        perror("UDP socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset(&udp_addr, 0, sizeof(udp_addr));
    udp_addr.sin_family = AF_INET;
    udp_addr.sin_addr.s_addr = INADDR_ANY;
    udp_addr.sin_port = htons(port);

    if (bind(udp_sock, (struct sockaddr *)&udp_addr, sizeof(udp_addr)) < 0) {
        perror("UDP bind failed");
        close(udp_sock);
        exit(EXIT_FAILURE);
    }

    printf("Worker node listening for UDP broadcasts on port %d\n", port);

    char buffer[BUFFER_SIZE];
    struct sockaddr_in master_addr;
    socklen_t master_len = sizeof(master_addr);
    recvfrom(udp_sock, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&master_addr, &master_len);

    printf("Received broadcast from master. Preparing TCP listener...\n");
    char response[] = "I am here";

    sendto(udp_sock, response, sizeof(response), 0, (struct sockaddr *)&master_addr, master_len);

    int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_sock < 0) {
        perror("TCP socket creation failed");
        close(udp_sock);
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(tcp_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        close(udp_sock);
        close(tcp_sock);
        exit(EXIT_FAILURE);
    }

    memset(&tcp_addr, 0, sizeof(tcp_addr));
    tcp_addr.sin_family = AF_INET;
    tcp_addr.sin_addr.s_addr = INADDR_ANY;
    tcp_addr.sin_port = htons(port);

    if (bind(tcp_sock, (struct sockaddr *)&tcp_addr, sizeof(tcp_addr)) < 0) {
        perror("TCP bind failed");
        close(udp_sock);
        close(tcp_sock);
        exit(EXIT_FAILURE);
    }

    listen(tcp_sock, 5);
    int client_sock = accept(tcp_sock, NULL, NULL);
    if (client_sock < 0) {
        perror("Accept failed");
        return 0;
    }
    printf("Waiting for tasks...\n");

    while (1) {
        int index;
        double range[2];
        read(client_sock, &index, sizeof(index));
        read(client_sock, &range[0], sizeof(double));
        ssize_t bytes_read = read(client_sock, &range[1], sizeof(double));
        if (bytes_read == 0) {
            continue;
        }

        printf("Received task: compute integral on range [%f, %f]\n", range[0], range[1]);
        double result = compute_integral(range[0], range[1]);
        write(client_sock, &index, sizeof(index));
        write(client_sock, &result, sizeof(result));
        printf("Task completed. Result: %f\n", result);
        printf("Waiting for tasks...\n");
    }
}