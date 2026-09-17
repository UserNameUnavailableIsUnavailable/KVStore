// client.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <server_ip> <num1> <num2>\n", argv[0]);
        return 1;
    }
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_event *event = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_qp *qp = NULL;
    struct ibv_mr *send_mr = NULL, *recv_mr = NULL;
    uint64_t send_buf[2], recv_buf[1];
    struct ibv_wc wc;

    send_buf[0] = strtoull(argv[2], NULL, 10);
    send_buf[1] = strtoull(argv[3], NULL, 10);

    // 1. Setup connection management
    ec = rdma_create_event_channel();
    rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(7471);
    inet_pton(AF_INET, argv[1], &addr.sin_addr);

    // 2. Resolve address and route
    rdma_resolve_addr(conn, NULL, (struct sockaddr *)&addr, 2000);
    rdma_get_cm_event(ec, &event); // ADDR_RESOLVED
    rdma_ack_cm_event(event);

    rdma_resolve_route(conn, 2000);
    rdma_get_cm_event(ec, &event); // ROUTE_RESOLVED
    rdma_ack_cm_event(event);

    // 3. Create Verbs resources
    pd = ibv_alloc_pd(conn->verbs);
    cq = ibv_create_cq(conn->verbs, 10, NULL, NULL, 0);
    struct ibv_qp_init_attr qp_attr = {
        .send_cq = cq, .recv_cq = cq,
        .cap = { .max_send_wr = 1, .max_recv_wr = 1, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC
    };
    rdma_create_qp(conn, pd, &qp_attr);
    qp = conn->qp;

    // 4. Register memory
    send_mr = ibv_reg_mr(pd, send_buf, sizeof(send_buf), 0);
    recv_mr = ibv_reg_mr(pd, recv_buf, sizeof(recv_buf), IBV_ACCESS_LOCAL_WRITE);

    // 5. Post a receive buffer for the reply
    struct ibv_recv_wr recv_wr = {0}, *bad_recv_wr;
    struct ibv_sge recv_sge = { .addr = (uintptr_t)recv_buf, .length = sizeof(recv_buf), .lkey = recv_mr->lkey };
    recv_wr.wr_id = 0; recv_wr.sg_list = &recv_sge; recv_wr.num_sge = 1;
    ibv_post_recv(qp, &recv_wr, &bad_recv_wr);

    // 6. Connect
    rdma_connect(conn, NULL);
    rdma_get_cm_event(ec, &event); // ESTABLISHED
    rdma_ack_cm_event(event);
    printf("Connected to server.\n");

    // 7. Send the two numbers
    struct ibv_send_wr send_wr = {0}, *bad_send_wr;
    struct ibv_sge send_sge = { .addr = (uintptr_t)send_buf, .length = sizeof(send_buf), .lkey = send_mr->lkey };
    send_wr.wr_id = 1; send_wr.sg_list = &send_sge; send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND; send_wr.send_flags = IBV_SEND_SIGNALED;
    ibv_post_send(qp, &send_wr, &bad_send_wr);

    // 8. Wait for send completion and then for the reply
    while (ibv_poll_cq(cq, 1, &wc) == 0);
    printf("Sent: %lu and %lu\n", send_buf[0], send_buf[1]);

    while (ibv_poll_cq(cq, 1, &wc) == 0);
    printf("Received result: %lu\n", recv_buf[0]);

    // 9. Cleanup
    ibv_dereg_mr(send_mr); ibv_dereg_mr(recv_mr);
    rdma_destroy_qp(conn); ibv_destroy_cq(cq); ibv_dealloc_pd(pd);
    rdma_destroy_id(conn); rdma_destroy_event_channel(ec);
    return 0;
}
